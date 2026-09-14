"""UTC timestamp Iceberg совместим с валидаторами и Trino readers demand-цепочки."""

from datetime import date, datetime, timezone
from importlib import import_module
from pathlib import Path

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


def test_sku_sales_reader_matches_utc_receipt_timestamp(monkeypatch):
    reader = import_module("layers.silver.sku_id.demand_sales_daily.v1.job.seller_reader")
    day = date(2022, 9, 1)
    captured = datetime(2026, 9, 14, 18, 14, 47, 580801, tzinfo=timezone.utc)
    schema = pa.schema([
        pa.field("date", pa.date32(), nullable=False),
        pa.field("sku_id", pa.int64(), nullable=False),
        pa.field("seller_key", pa.string(), nullable=False),
        pa.field("source_manifest_id", pa.string(), nullable=False),
        pa.field("source_contract_version", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", "UTC"), nullable=False),
    ])
    receipt = {
        "date": day.isoformat(),
        "rows_written": 1,
        "table_uuid": "source-uuid",
        "source_manifest_id": "source-manifest",
        "source_contract_version": "source-v1",
        "ingested_at": captured.isoformat(),
    }
    version = {"snapshot_id": 17, "table_uuid": "source-uuid", "day_receipt": receipt}
    description = [
        ("date", "date", None, None, None, None, None),
        ("sku_id", "bigint", None, None, None, None, None),
        ("seller_key", "varchar", None, None, None, None, None),
        ("source_manifest_id", "varchar", None, None, None, None, None),
        ("source_contract_version", "varchar", None, None, None, None, None),
        ("ingested_at", "timestamp(6) with time zone", None, None, None, None, None),
    ]

    class Cursor:
        def __init__(self):
            self.description = description
            self.rows = [[day, 1, "seller:1", "source-manifest", "source-v1", captured]]
            self.closed = False

        def execute(self, _sql):
            return None

        def fetchmany(self, size):
            result, self.rows = self.rows[:size], self.rows[size:]
            return result

        def close(self):
            self.closed = True

    cursor = Cursor()

    class Connection:
        def cursor(self):
            return cursor

    source = {"table": {"catalog": "iceberg", "schema": "silver", "name": "source_table"}}
    monkeypatch.setattr(reader, "trino_catalog_alias", lambda *_: "dwh-iceberg")

    batches = list(reader.read_batches(
        Connection(), source, Path("."), version, schema, day=day,
        max_batch_rows=10, max_batch_bytes=100000,
    ))

    assert batches[0]["ingested_at"][0].as_py() == captured
    assert cursor.closed
