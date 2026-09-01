import contextlib
import importlib.util
import re
import sys
from datetime import date
from pathlib import Path

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")
DDL_PATH = ENTITY_DIR / "migrations/create_table.sql"

COLUMN_DEFINITION = re.compile(
    r"^\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+[A-Za-z]", re.MULTILINE
)


@contextlib.contextmanager
def _isolated_job_package():
    """Имя пакета `job` занято десятком энтити репозитория, и соседний тест
    (ci_test/test_query_id_features.py) кэширует в sys.modules своё
    `job.entities` без уборки. Снимаем чужой кэш на время загрузки и
    возвращаем его на место, чтобы не сломать ни себя, ни соседей."""
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "job" or name.startswith("job.")
    }
    for name in saved:
        del sys.modules[name]
    saved_path = list(sys.path)
    sys.path.insert(0, str(ENTITY_DIR))
    try:
        yield
    finally:
        sys.path[:] = saved_path
        for name in [n for n in sys.modules if n == "job" or n.startswith("job.")]:
            del sys.modules[name]
        sys.modules.update(saved)


def load_module(name):
    with _isolated_job_package():
        spec = importlib.util.spec_from_file_location(
            f"ranking_logs_{name}", ENTITY_DIR / "job" / f"{name}.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


def load_settings():
    return load_module("settings").load_dataset_settings(ENTITY_DIR / "config.yaml")


def build_query():
    return load_module("query").build_dataset_query(
        collection_date=date(2026, 9, 13),
        event_date_start=date(2026, 9, 6),
        event_date_end=date(2026, 9, 13),
        settings=load_settings(),
    )


def ddl_columns():
    body = DDL_PATH.read_text(encoding="utf-8")
    body = body[body.index("(") + 1 : body.index("\n)\nUSING iceberg")]
    return COLUMN_DEFINITION.findall(body)


def final_select_aliases(query: str):
    """Алиасы верхнеуровневого SELECT: последний SELECT ... FROM в запросе."""
    tail = query[query.rindex("SELECT") :]
    projection = tail[: tail.index("\nFROM ")]
    aliases = []
    for line in projection.splitlines()[1:]:
        item = line.strip().rstrip(",")
        if not item or item.startswith("--"):
            continue
        aliases.append(re.split(r"\s+AS\s+", item)[-1].strip("`"))
    return aliases


def test_query_projects_ddl_columns_in_order():
    assert final_select_aliases(build_query()) == ddl_columns()


def test_query_filters_the_configured_model_only():
    assert "e.model_name = 'search_unified_model_v9_cold_start'" in build_query()


def test_query_samples_requests_deterministically():
    query = build_query()
    # sample_percent = 1 -> порог 100 из 10000. pmod, а не abs: xxhash64 может
    # вернуть Long.MIN_VALUE, у которого abs отрицателен и условие отсечёт всё.
    assert "pmod(xxhash64(e.request_id), 10000) < 100" in query
    assert "abs(xxhash64" not in query


def test_query_uses_the_seven_day_window():
    query = build_query()
    assert "DATE '2026-09-13' AS collection_date" in query
    assert "DATE '2026-09-06' AS event_date_start" in query
    assert "DATE '2026-09-13' AS event_date_end" in query


def test_query_never_reads_the_145_feature_vector():
    query = build_query()
    assert "model_input['input']" not in query
    assert "'$.input'" not in query


def test_query_explodes_all_aligned_arrays_together():
    query = build_query()
    # Один общий posexplode(arrays_zip(...)) — единственный способ гарантировать,
    # что кандидат и все его скоры взяты по одному индексу.
    assert query.count("posexplode") == 1
    for array_name in (
        "ranking_candidates",
        "final_scores",
        "model_output",
        "cm2_features",
        "dssm_scores",
        "linear_scores",
        "normalized_linear_scores",
        "cpo_adv_percents",
        "bid_amounts",
    ):
        assert array_name in query


def test_query_defaults_unknown_queries_to_low_frequency():
    assert "COALESCE(fq.frequency_group, 'LF')" in build_query()


def test_query_left_joins_every_enrichment():
    query = build_query()
    # Обогащения не должны терять строки лога: только LEFT JOIN.
    assert query.count("LEFT JOIN") == 3


def final_select_projection(query: str) -> str:
    """Текст верхнеуровневого SELECT-списка (без 'FROM ...' и ниже). Та же
    логика среза, что final_select_aliases: последний SELECT ... FROM."""
    tail = query[query.rindex("SELECT") :]
    return tail[: tail.index("\nFROM ")]


def test_query_casts_index_reads_with_their_exact_alias():
    """Пин на маппинг выражение -> алиас для всех тринадцати индексных чтений
    (5 model_output + 8 cm2_features), а не только на порядок алиасов: swap
    cm2_features[0]/cm2_features[1] или порча алиаса проходит мимо
    test_query_projects_ddl_columns_in_order, но не мимо этого теста."""
    query = build_query()
    exact_projections = (
        "CAST(c.candidate.model_output[1] AS DOUBLE) AS model_probability",
        "CAST(c.candidate.model_output[2] AS DOUBLE) AS alpha_component",
        "CAST(c.candidate.model_output[3] AS DOUBLE) AS beta_component",
        "CAST(c.candidate.model_output[4] AS DOUBLE) AS gamma_component",
        "CAST(c.candidate.model_output[5] AS DOUBLE) AS delta_component",
        "CAST(c.candidate.cm2_features[0] AS DOUBLE) AS commission_percent",
        "CAST(c.candidate.cm2_features[1] AS DOUBLE) AS seller_price",
        "CAST(c.candidate.cm2_features[2] AS DOUBLE) AS logistics_fee",
        "CAST(c.candidate.cm2_features[3] AS DOUBLE) AS cpi_cost",
        "CAST(c.candidate.cm2_features[4] AS DOUBLE) AS cpm_bid",
        "CAST(c.candidate.cm2_features[5] AS DOUBLE) AS cpo_percent",
        "CAST(c.candidate.cm2_features[6] AS DOUBLE) AS vat_rate",
        "CAST(c.candidate.cm2_features[7] AS DOUBLE) AS items_quantity",
    )
    for projection in exact_projections:
        assert projection in query, projection


def test_query_dereferences_every_candidate_field_by_its_real_name():
    """Каждое из девяти полей exploded-структуры candidate (по одному на
    массив, зазипованный в arrays_zip) должно быть прочитано под своим
    настоящим именем внутри финального SELECT — не просто где-то в тексте
    запроса, иначе опечатка вроде dssm_scores -> dssm_score (зелёный тест,
    падение на анализе плана в Spark) прошла бы незамеченной. Срез именно
    финального SELECT (та же логика, что у final_select_aliases) гарантирует,
    что проверка не удовлетворяется списком аргументов arrays_zip: там колонки
    названы s.<field>, а не candidate.<field>."""
    query = build_query()
    projection = final_select_projection(query)
    fields_read_in_final_select = (
        "final_scores",
        "model_output",
        "cm2_features",
        "dssm_scores",
        "linear_scores",
        "normalized_linear_scores",
        "cpo_adv_percents",
        "bid_amounts",
    )
    for field in fields_read_in_final_select:
        assert f"c.candidate.{field}" in projection, field
    # ranking_candidates — единственное из девяти полей, чей CAST переехал в
    # CTE candidates (в sku_group_id, чинка #3): финальный SELECT берёт уже
    # готовое c.sku_group_id, а не c.candidate.ranking_candidates напрямую.
    # Подстрока "candidate.ranking_candidates" всё равно однозначно ловит
    # опечатку в имени поля независимо от того, к какой версии запроса
    # применяется тест (до или после чинки #3), и не может совпасть со
    # списком arrays_zip: там колонка называется s.ranking_candidates.
    assert "candidate.ranking_candidates" in query


def test_sample_threshold_scales_with_percent():
    query_module = load_module("query")
    assert query_module.sample_threshold(1) == 100
    assert query_module.sample_threshold(7) == 700
    assert query_module.sample_threshold(100) == 10000
