import re
from pathlib import Path

import yaml

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")
CONFIG_PATH = ENTITY_DIR / "config.yaml"
DDL_PATH = ENTITY_DIR / "migrations/create_table.sql"

# Порядок колонок — контракт между DDL и SELECT'ом джоба: writeTo() сопоставляет
# их позиционно, поэтому расхождение молча перепутает значения местами.
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
    "cpo_adv_percent",
    "bid_amount",
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


def test_all_dq_tests_are_warn_and_growth_is_absent():
    tests = load_config()["dq"]["tests"]
    names = [test["name"] for test in tests]
    # row_count_growth берёт baseline как partition_date - 1 day (dq/tests.py),
    # у недельной партиции предыдущего дня не существует — тест бесполезен.
    assert "row_count_growth" not in names
    assert {"primary_key_not_null", "primary_key_unique", "freshness", "row_count_min"} <= set(names)
    for test in tests:
        assert test["severity"] == "warn", test["name"]


def test_dataset_parameters_are_declared_in_config():
    dataset = load_config()["dataset"]
    assert dataset["model_name"] == "search_unified_model_v9_cold_start"
    assert dataset["sample_percent"] == 7


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
