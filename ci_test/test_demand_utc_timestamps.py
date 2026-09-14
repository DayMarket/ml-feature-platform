"""UTC timestamp Iceberg совместим с валидаторами и Trino readers demand-цепочки."""

from importlib import import_module

import pyarrow as pa
import pytest


SAME_TYPE_MODULES = [
    "layers.silver.seller_id.demand_catalog_seller.v1.job.preparation",
    "layers.silver.seller_id.demand_catalog_seller.v1.job.orchestration",
    "layers.silver.sku_id.demand_catalog_sku.v1.job.preparation",
    "layers.silver.level_node_id.demand_catalog_tree.v1.job.preparation",
    "layers.silver.sku_id_seller_key.demand_finance_daily.v1.job.preparation",
    "layers.silver.sku_id_seller_key.demand_finance_daily.v1.job.service_schema",
    "layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily.v1.job.preparation",
    "layers.silver.sku_id_seller_key.demand_seller_sales_observed_daily.v1.job.orchestration",
    "layers.silver.sku_id.demand_stock_daily.v1.job.preparation",
    "layers.silver.sku_id.demand_stock_daily.v1.job.service_schema",
    "layers.silver.sku_id.demand_sales_daily.v1.job.preparation",
    "layers.silver.sku_id_estimate_kind.demand_restored_daily.v1.job.preparation",
    "layers.silver.sku_id_estimate_kind.demand_restored_daily.v1.job.orchestration",
    "layers.gold.sku_id.demand_observed_daily.v1.job.preparation",
]


@pytest.mark.parametrize("module_name", SAME_TYPE_MODULES)
def test_schema_compatibility_accepts_only_naive_or_utc_microseconds(module_name):
    same_type = import_module(module_name).same_type
    expected = pa.timestamp("us")
    assert same_type(pa.timestamp("us"), expected)
    assert same_type(pa.timestamp("us", "UTC"), expected)
    assert not same_type(pa.timestamp("ms", "UTC"), expected)
    assert not same_type(pa.timestamp("us", "Asia/Tashkent"), expected)


@pytest.mark.parametrize("module_name", [
    "layers.silver.sku_id.demand_sales_daily.v1.job.seller_reader",
    "layers.gold.sku_id.demand_observed_daily.v1.job.reader",
])
def test_range_reader_requires_trino_timestamp_with_time_zone(module_name):
    reader = import_module(module_name)
    assert reader.trino_type_matches(pa.timestamp("us", "UTC"), "timestamp(6) with time zone")
    assert not reader.trino_type_matches(pa.timestamp("us", "UTC"), "timestamp(6)")


@pytest.mark.parametrize("module_name", [
    "layers.silver.sku_id.demand_catalog_sku.v1.job.seller_reader",
    "layers.silver.level_node_id.demand_catalog_tree.v1.job.reader",
])
def test_catalog_reader_accepts_trino_utc_timestamp_metadata(module_name):
    reader = import_module(module_name)
    schema = pa.schema([pa.field("captured", pa.timestamp("us", "UTC"), nullable=False)])
    description = [("captured", "timestamp(6) with time zone", None, None, None, None, None)]
    reader.validate_description(description, schema)
