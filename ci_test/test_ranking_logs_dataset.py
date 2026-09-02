import re
import sys
from datetime import datetime
from pathlib import Path

import yaml

sys.path.insert(0, ".")
from dq.config import load_dq_settings

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")
CONFIG_PATH = ENTITY_DIR / "config.yaml"
DDL_PATH = ENTITY_DIR / "migrations/create_table.sql"

# Порядок колонок — контракт между DDL и SELECT'ом джоба: DataFrameWriterV2.
# overwritePartitions() строит OverwritePartitionsDynamic.byName, то есть
# резолвит колонки по имени, а не позиционно. Переименованный или пропавший
# алиас в SELECT'е из-за этого падает на записи, а не молча съезжает по
# позиции. Порядок здесь пиннится не ради самого резолва, а чтобы схема DDL и
# список колонок запроса читались бок о бок и не расходились по составу.
EXPECTED_COLUMNS = [
    "collection_date",
    "event_date",
    "fired_at",
    "model_name",
    "request_id",
    "install_id",
    "search_query",
    "category_id",
    "promo_id",
    "position",
    "sku_group_id",
    "final_score",
    "model_probability",
    "alpha_component",
    "beta_component",
    "gamma_component",
    "delta_component",
    "dssm_score",
    "linear_score",
    "normalized_linear_score",
    "commission_percent",
    "seller_price",
    "logistics_fee",
    "cpi_cost",
    "cpm_bid",
    "cpo_percent",
    "vat_rate",
    "items_quantity",
    "alpha",
    "beta",
    "gamma",
    "delta",
    "sku_group_age_days",
    "product_rating",
    "total_reviews_count",
    "frequency_group",
    "users_total",
    "query_rank",
]

COLUMN_DEFINITION = re.compile(
    r"^\s*[`\"]?([A-Za-z_][A-Za-z0-9_]*)[`\"]?\s+[A-Za-z]", re.MULTILINE
)


def load_config():
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


def ddl_columns():
    body = DDL_PATH.read_text(encoding="utf-8")
    body = body[body.index("(") + 1 : body.index("\n)\nUSING iceberg")]
    return COLUMN_DEFINITION.findall(body)


def test_table_contract():
    table = load_config()["table"]
    assert table["catalog"] == "iceberg"
    assert table["schema"] == "silver"
    assert table["name"] == "feature_platform_ranking_logs_dataset_v1"
    assert table["primary_key"] == "collection_date,event_date,request_id,sku_group_id"
    assert table["meta"]["team"] == "team:search"


def test_dag_and_alert_contract():
    config = load_config()
    assert config["dag"]["schedule"] == "0 12 * * 0"
    assert config["dag"]["team"] == "search"
    assert config["alerts"]["team"] == "search"
    assert config["alerts"]["severity"] == "P4"
    assert config["alerts"]["oncall_webhook_conn_id"] == "oncall_webhook_search"


def test_dq_and_feature_stats_look_at_the_same_partition():
    config = load_config()
    dq = config["dq"]
    stats = config["feature_stats"]
    assert dq["partition_column"] == "collection_date"
    assert stats["partition_column"] == dq["partition_column"]
    assert stats["partition_date_template"] == dq["partition_date_template"]
    assert "data_interval_end" in dq["partition_date_template"]


def test_resolved_dq_settings_are_all_warn_and_growth_is_disabled():
    """Проверяет разрешённые (resolved) параметры DQ, не только YAML.

    dq/config.py инъецирует базовые тесты (base tests) в разрешённые параметры:
    любой базовый тест, отсутствующий из явного списка, получает severity: error
    по умолчанию. Это тесто проверяет, что row_count_growth явно отключен
    (enabled: false), а все остальные тесты имеют severity: warn.
    """
    config = load_config()
    settings = load_dq_settings(config)

    specs = settings.tests
    names = [spec.name for spec in specs]

    # row_count_growth должен быть отключён (enabled: false)
    assert "row_count_growth" not in names, (
        "row_count_growth should be disabled via enabled: false, "
        "but found in resolved specs. This means the framework's BASE_TESTS "
        "auto-injected it at severity error."
    )

    # Все остальные тесты должны иметь severity warn
    for spec in specs:
        assert spec.severity == "warn", f"Test {spec.name} has severity {spec.severity}, expected warn"


def test_dataset_parameters_are_declared_in_config():
    dataset = load_config()["dataset"]
    assert dataset["model_name"] == "search_unified_model_v9_cold_start"
    assert dataset["sample_percent"] == 1


def test_ddl_columns_match_the_agreed_order():
    assert ddl_columns() == EXPECTED_COLUMNS


def test_ddl_is_partitioned_by_collection_date():
    body = DDL_PATH.read_text(encoding="utf-8")
    assert "PARTITIONED BY (collection_date)" in body
    assert "'engine.hive.lock-enabled' = 'false'" in body


def test_readme_states_the_contract():
    readme = (ENTITY_DIR / "README.md").read_text(encoding="utf-8")
    assert "iceberg.silver.feature_platform_ranking_logs_dataset_v1" in readme
    assert "feature-platform.datasets.search.ranking_logs.v1" in readme
    assert "datasets/search/ranking_logs/v1" in readme


def test_ddl_sets_distribution_mode_none_for_the_single_partition_write():
    body = DDL_PATH.read_text(encoding="utf-8")
    assert "'write.distribution-mode' = 'none'" in body


def test_start_date_weekday_matches_the_cron_day_of_week():
    # start_date и schedule обязаны согласовываться по дню недели: расхождение
    # сдвигает первый data_interval (и первый фактический ран) на срок до
    # недели от задуманного. Оба значения читаются из config.yaml, чтобы тест
    # не мог рассинхронизироваться сам с собой при следующей правке конфига.
    config = load_config()
    dag = config["dag"]

    schedule_fields = dag["schedule"].split()
    assert len(schedule_fields) == 5, f"unexpected cron format: {dag['schedule']!r}"
    cron_dow = int(schedule_fields[4])
    cron_dow = 0 if cron_dow == 7 else cron_dow  # cron допускает 0 и 7 для воскресенья

    start_date = dag["start_date"]
    normalized = start_date[:-1] + "+00:00" if start_date.endswith("Z") else start_date
    parsed_date = datetime.fromisoformat(normalized).date()
    # date.weekday(): Monday=0..Sunday=6; cron dow: Sunday=0..Saturday=6.
    start_date_cron_dow = (parsed_date.weekday() + 1) % 7

    assert start_date_cron_dow == cron_dow, (
        f"dag.start_date {start_date!r} falls on cron dow {start_date_cron_dow}, "
        f"but dag.schedule {dag['schedule']!r} expects dow {cron_dow}"
    )
