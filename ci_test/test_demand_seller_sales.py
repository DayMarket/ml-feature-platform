"""Точные seller/SKU distinct, source SQL и согласованные whole-run бюджеты."""

from datetime import date
from importlib import import_module
from pathlib import Path

import pyarrow as pa
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SELLER = "layers/silver/sku_id_seller_key/demand_seller_sales_observed_daily/v1"
query = import_module(SELLER.replace("/", ".") + ".job.query")
rollup = import_module("layers.silver.sku_id.demand_sales_daily.v1.job.seller_rollup")


def config():
    return yaml.safe_load((ROOT / SELLER / "config.yaml").read_text())


def sample():
    return pa.table({
        "date": pa.array([date(2022, 9, 1)] * 3, type=pa.date32()),
        "sku_id": [1, 1, 1], "seller_key": ["seller:42", "seller:43", "unknown"],
        "sales_orders": [1, 1, 1], "sales_order_items": [1, 1, 1],
        "sku_sales_orders": [2, 2, 2], "sku_sales_order_items": [3, 3, 3],
    })


def change(table, name, values):
    return table.set_column(table.schema.get_field_index(name), name, pa.array(values, type=table[name].type))


def test_shared_order_not_double_counted():
    result = rollup.exact_sku_counts(sample()).to_pylist()
    assert result == [{"date": date(2022, 9, 1), "sku_id": 1, "sales_orders": 2, "sales_order_items": 3}]


def test_days_do_not_mix():
    second = change(sample(), "date", [date(2022, 9, 2)] * 3)
    assert len(rollup.exact_sku_counts(pa.concat_tables([sample(), second]))) == 2


def test_skus_do_not_mix():
    second = change(sample(), "sku_id", [2] * 3)
    assert len(rollup.exact_sku_counts(pa.concat_tables([sample(), second]))) == 2


@pytest.mark.parametrize("name,values", [
    ("sku_sales_orders", [1, 2, 2]),
    ("sku_sales_orders", [4, 4, 4]),
    ("sku_sales_order_items", [4, 4, 4]),
    ("sales_orders", [0, 1, 1]),
    ("sales_order_items", [None, 1, 1]),
    ("sku_id", [-1, 1, 1]),
    ("seller_key", ["seller:42", "seller:42", "unknown"]),
])
def test_invalid_counts_or_keys_block_rollup(name, values):
    with pytest.raises(ValueError):
        rollup.exact_sku_counts(change(sample(), name, values))


def test_union_total_cannot_be_less_than_one_seller():
    table = change(sample(), "sales_orders", [3, 1, 1])
    table = change(table, "sales_order_items", [3, 1, 1])
    with pytest.raises(ValueError, match="границ"):
        rollup.exact_sku_counts(table)


def test_empty_table_preserves_schema():
    result = rollup.exact_sku_counts(sample().slice(0, 0))
    assert result.num_rows == 0
    assert result.column_names == ["date", "sku_id", "sales_orders", "sales_order_items"]


def test_missing_controls_are_not_recomputed_as_sum():
    with pytest.raises(ValueError):
        rollup.exact_sku_counts(sample().drop(["sku_sales_orders"]))


def test_large_string_and_non_overflowing_union_bound():
    table = sample().slice(0, 2)
    for name in (*rollup.COUNTS, *(f"sku_{name}" for name in rollup.COUNTS)):
        table = change(table, name, [2**63 - 1] * 2)
    table = table.set_column(2, "seller_key", pa.array(["seller:42", "seller:43"], type=pa.large_string()))
    assert rollup.exact_sku_counts(table)["sales_orders"].to_pylist() == [2**63 - 1]


@pytest.mark.parametrize("key", ["", "seller:0", "seller:-1", "seller:01", "seller:foo"])
def test_bad_seller_keys_block(key):
    with pytest.raises(ValueError, match="seller_key"):
        rollup.exact_sku_counts(change(sample(), "seller_key", [key, "seller:43", "unknown"]))


def test_one_ch_capture_with_grouping_before_filter():
    sql = query.source_query(config(), date(2022, 9, 1), fx_available=True)
    assert sql.count("FROM `marts`.`order_items` FINAL") == 1
    assert "GROUP BY GROUPING SETS ((sku_id, seller_key), (sku_id))" in sql
    assert "GROUPING(seller_key) AS is_sku_total" in sql
    assert "maxIf(sales_orders, is_sku_total = 1) OVER (PARTITION BY sku_id)" in sql
    assert "WHERE is_sku_total = 0" in sql
    assert "ORDER BY sku_id, seller_key" in sql
    assert "JOIN" not in sql
    assert "order_item_status NOT IN ('CREATED', 'NOT_CREATED')" in sql
    assert "2022-08-31 19:00:00" in sql


def test_money_and_channels_preserved():
    sql = query.source_query(config(), date(2022, 9, 1), fx_available=True)
    assert sql.count("daily_uzs_to_usd(date, ") == 10
    assert "sum(toDecimal128(ke_promo_value, 0)) AS sales_marketplace_promo_value" in sql
    for channel in query.CHANNELS:
        assert f"AS sales_units_{channel}" in sql
        assert f"AS sales_gmv_{channel}" in sql
    assert "seller_id IS NULL OR seller_id = 0" in sql
    assert "countIf(seller_id < 0) AS invalid_sellers" in query.coverage_query(config(), date(2022, 9, 1))


def test_no_fx_is_unknown_not_zero():
    sql = query.source_query(config(), date(2022, 9, 1), fx_available=False)
    assert "daily_uzs_to_usd(" not in sql
    assert sql.count("CAST(NULL AS Nullable(Float64))") == 10


def test_schema_ownership_and_dq_match_new_grain():
    cfg = config()
    ddl = (ROOT / SELLER / "migrations/create_table.sql").read_text()
    assert cfg["table"]["primary_key"] == "date,sku_id,seller_key"
    assert cfg["table"]["name"] == "feature_platform_demand_seller_sales_observed_daily"
    assert cfg["table"]["meta"]["team"] == "team:operations"
    assert cfg["dq"]["warmup_days"] == 0
    assert "max_absolute_change" not in cfg["dq"]["tests"][0]
    for name in ("sku_sales_orders", "sku_sales_order_items"):
        assert f"{name} BIGINT NOT NULL" in ddl
        assert name in cfg["feature_stats"]["exclude_columns"]
    assert "TBLPROPERTIES ('engine.hive.lock-enabled' = 'false')" in ddl


@pytest.mark.parametrize("path", [
    SELLER, "layers/silver/sku_id/demand_sales_daily/v1",
    "layers/silver/sku_id/demand_stock_daily/v1",
    "layers/silver/sku_id_seller_key/demand_finance_daily/v1",
    "layers/gold/sku_id/demand_observed_daily/v1",
])
def test_approved_whole_run_budgets(path):
    cfg = yaml.safe_load((ROOT / path / "config.yaml").read_text())
    assert cfg["runtime"]["run_timeout_seconds"] == {"regular": 21600, "manual": 604800}


def test_restored_uses_one_manual_budget():
    path = ROOT / "layers/silver/sku_id_estimate_kind/demand_restored_daily/v1/config.yaml"
    cfg = yaml.safe_load(path.read_text())
    assert cfg["runtime"]["run_timeout_seconds"] == 604800
