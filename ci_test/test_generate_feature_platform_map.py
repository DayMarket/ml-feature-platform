import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_generator_module():
    module_path = ROOT / "scripts" / "generate_feature_platform_map.py"
    spec = importlib.util.spec_from_file_location("generate_feature_platform_map", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_generated_feature_platform_map_is_current():
    generator = load_generator_module()
    expected = generator.render_repository_map(ROOT)
    actual = (ROOT / "docs" / "feature_platform_map.md").read_text(encoding="utf-8")
    assert actual == expected
    assert "## 1. Production-critical DAGs — Search" in actual
    assert "## 2. Production-critical DAGs — Logistics" in actual
    assert "## 3. Offline, training and backfill DAGs" in actual
    assert "title Production-critical DAG starts (UTC)" in actual
    assert "title Logistics-critical DAG starts (UTC)" in actual
    assert "title Offline, training and backfill DAG starts (UTC)" in actual
    assert "section Production-critical" in actual
    assert "section Logistics" in actual
    assert "section Training" in actual
    assert "section Backfill" in actual
    assert "section Other" in actual
    assert "UTC 03:00" in actual
    assert 'd0["`search_query_atc_features\nUTC 03:00 · P3 · medium`"]' in actual
    assert "<br" not in actual


def test_map_contains_cross_dag_dependencies_and_schedules():
    generator = load_generator_module()
    records = {record.dag_id: record for record in generator.discover_dags(ROOT)}

    qid_id = (
        "feature-platform.layers.gold.query_sku_group_id."
        "sku_group_query_atc_order_features_qid"
    )
    qid = records[qid_id]
    assert qid.schedule == "0 6 * * *"
    assert generator.Dependency(
        "feature-platform.layers.gold.query_text_version.search_query_id",
        "sensor",
        60,
    ) in qid.dependencies

    # search_query_id читает внешнюю "dwh-iceberg".silver.search_logs, у которой нет
    # DAG'а-владельца в репозитории, поэтому сенсоров у него нет вообще. Downstream
    # (qid выше) по-прежнему ждёт сам DAG, а не его источник.
    query_id = records[
        "feature-platform.layers.gold.query_text_version.search_query_id"
    ]
    assert query_id.dependencies == ()

    es_writer_id = (
        "feature-platform.layers.silver.query_sku_group_id."
        "search_query_sku_group_es_features"
    )
    es_collect_id = f"{es_writer_id}.elasticsearch_collect"
    assert generator.Dependency(es_collect_id, "sensor", 0) in records[es_writer_id].dependencies

    ranking_upload = records["feature_platform_ranking_features_upload_dag"]
    assert generator.Dependency(
        "feature-platform.layers.gold.query_sku_group_id."
        "sku_group_query_atc_order_features.v2",
        "upload-sensor",
        60,
    ) in ranking_upload.dependencies

    dataset = records["feature-platform.datasets.search.search_ranking.v1"]
    assert dataset.severity == "P4"
    assert dataset.workload == "training"
    assert dataset.expected_severity == "P4"

    logistics_records = [
        record
        for record in records.values()
        if record.group_tag in generator._critical_group_tags(ROOT, "Logistics")
        or record.dag_id in generator._critical_dag_ids(ROOT, "Logistics")
    ]
    assert len(logistics_records) == 9
    assert {record.area for record in logistics_records} == {"silver", "gold"}
    assert (
        "feature-platform.layers.silver.category_level_category_id."
        "order_completion_category_features"
    ) in {record.dag_id for record in logistics_records}

    assert generator.severity_policy_violations(list(records.values())) == []

    production_records = [
        record for record in records.values() if record.workload == "production"
    ]
    offline_records = [
        record for record in records.values() if record.workload != "production"
    ]
    assert ranking_upload in production_records
    assert dataset in offline_records


def test_module_level_constants_resolve_sensor_dependencies():
    """DAG'и выносят upstream id и delta в константы модуля; парсер обязан их читать."""
    generator = load_generator_module()
    records = {record.dag_id: record for record in generator.discover_dags(ROOT)}

    history = records[
        "feature-platform.layers.gold.account_id.buyout_account_history_features"
    ]
    assert generator.Dependency(
        "dbt.source.trino.ml_feature_platform_silver."
        "feature_platform_account_lifetime_facts.dq",
        "legacy-dq",
        1560,
    ) in history.dependencies


def test_fstring_dag_id_resolves():
    """dag_id=f"{CONFIG['dag']['id']}.backfill" — рабочая идиома backfill-DAG'ов."""
    generator = load_generator_module()
    records = {record.dag_id: record for record in generator.discover_dags(ROOT)}
    assert (
        "feature-platform.layers.silver.city_id_dimensional_group."
        "delivery_cpi_city_features.backfill"
    ) in records


def test_unparsed_dag_is_a_note_not_an_exception():
    """Незнакомая идиома не валит генератор: она попадает в notes и в карту."""
    import ast

    generator = load_generator_module()
    notes = []
    source = (
        "from airflow.sdk import dag\n"
        "@dag(dag_id=UNRESOLVABLE, schedule='0 1 * * *')\n"
        "def entity():\n"
        "    pass\n"
    )
    tree = ast.parse(source)
    records = generator._records_from_tree(
        tree, ROOT, ROOT / "layers" / "silver" / "x" / "y" / "v1" / "dag.py", notes
    )
    assert records == []
    assert len(notes) == 1
    assert "dag_id" in notes[0].message


def test_severity_policy_allows_p1_and_p2():
    """P2 — согласованная с владельцем severity; гейт не имеет права её запрещать."""
    generator = load_generator_module()
    records = generator.discover_dags(ROOT)
    assert generator.severity_policy_violations(records) == []
    assert any(record.severity == "P2" for record in records)


def test_dq_and_feature_stats_are_tracked_per_dag():
    """dq и feature_stats — штатные шаги; карта показывает, у кого их нет."""
    generator = load_generator_module()
    records = {record.dag_id: record for record in generator.discover_dags(ROOT)}

    price_features = records[
        "feature-platform.layers.gold.sku_group_id.sku_group_price_features"
    ]
    assert price_features.has_dq
    assert price_features.has_feature_stats
    assert generator.Dependency(
        "feature-platform.layers.silver.sku_group_id.sku_group_id_prices",
        "dq",
        60,
    ) in price_features.dependencies

    rendered = generator.render_repository_map(ROOT)
    assert "## 4. DQ, feature_stats и статус миграции сенсоров" in rendered
    assert "iceberg.silver.feature_platform_dq_results" in rendered
    assert "iceberg.silver.feature_platform_feature_stats" in rendered


def test_stale_exclusion_is_a_note_not_an_exception():
    """Удалённый DAG в списке пауз не должен ронять сборку карты."""
    generator = load_generator_module()
    generator._map_exclusions = lambda repo_root: {
        "feature-platform.layers.silver.gone.deleted_entity": "Paused; confirmed on 2026-01-01"
    }
    notes = []
    records = generator.discover_dags(ROOT, notes)

    assert records
    assert any("deleted_entity" in note.message for note in notes)


def test_paused_dags_are_excluded_from_map():
    generator = load_generator_module()
    records = {record.dag_id: record for record in generator.discover_dags(ROOT)}
    excluded_dags = generator._map_exclusions(ROOT)
    rendered = generator.render_repository_map(ROOT)

    assert excluded_dags
    for paused_dag_id in excluded_dags:
        assert paused_dag_id not in records
        assert f"`{paused_dag_id}`" not in rendered


if __name__ == "__main__":
    test_generated_feature_platform_map_is_current()
    test_map_contains_cross_dag_dependencies_and_schedules()
    test_module_level_constants_resolve_sensor_dependencies()
    test_fstring_dag_id_resolves()
    test_unparsed_dag_is_a_note_not_an_exception()
    test_severity_policy_allows_p1_and_p2()
    test_dq_and_feature_stats_are_tracked_per_dag()
    test_paused_dags_are_excluded_from_map()
    test_stale_exclusion_is_a_note_not_an_exception()
    print("Feature Platform map tests completed successfully")
