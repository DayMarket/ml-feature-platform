"""Gold observed: bounded sparse join, physical schema, NULL и Decimal parity."""

from datetime import date, datetime, timezone
from decimal import Decimal
from importlib import import_module
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/sku_id/demand_observed_daily/v1"
PREP = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.preparation")
DAY = date(2026, 9, 1)
NOW = datetime(2026, 9, 9, 4, tzinfo=timezone.utc)
VERSIONS = {"sales": {"snapshot_id": 101, "table_uuid": "11111111-1111-4111-8111-111111111111"},
            "stock": {"snapshot_id": 102, "table_uuid": "22222222-2222-4222-8222-222222222222"}}


def schema(path):
    kinds = {"DATE": pa.date32(), "BIGINT": pa.int64(), "STRING": pa.string(),
             "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us"),
             "DOUBLE": pa.float64(), "DECIMAL(38,0)": pa.decimal128(38, 0)}
    ddl = (path / "migrations/create_table.sql").read_text()
    fields = re.findall(r"^    (\w+) ([A-Z]+(?:\(\d+,\d+\))?)( NOT NULL)? COMMENT ", ddl, re.M)
    return pa.schema([pa.field(name, kinds[kind], nullable=not bool(required))
                      for name, kind, required in fields])


def source(kind, ids, **changes):
    path = ROOT / "layers/silver/sku_id" / f"demand_{kind}_daily/v1"
    source_schema = schema(path)
    rows = [{"date": DAY, "sku_id": sku, "source_updated_at": NOW.replace(tzinfo=None),
             "fx_rate_source": "unavailable", "fx_captured_at": NOW.replace(tzinfo=None),
             "source_manifest_id": kind, "source_contract_version": "v1",
             "ingested_at": NOW.replace(tzinfo=None), **changes} for sku in ids]
    return pa.Table.from_pylist(rows, schema=source_schema)


def join(sales, stock, **options):
    return PREP.join_batches(sales, stock, schema(ENTITY), day=DAY, inputs=VERSIONS,
                            manifest="gold-join", version="observed_daily_join_v1", ingested_at=NOW,
                            max_batch_rows=options.get("max_batch_rows", 2))


def test_schema_contains_all_upstream_fields_without_collisions():
    output = schema(ENTITY)
    PREP.validate_schema(output)
    assert len(output) == 47
    cfg = yaml.safe_load((ENTITY / "config.yaml").read_text())
    for kind in ("sales", "stock"):
        upstream = schema((ROOT / cfg["inputs"][f"{kind}_config"]).parent)
        assert upstream.names == PREP.SOURCE_FIELDS[kind]
        if kind == "sales":
            for field in upstream:
                target = output.field(PREP.output_name(field.name))
                assert target.type == field.type
                if field.name not in PREP.KEYS:
                    assert target.nullable


def test_sparse_union_and_null_preservation_across_batch_boundaries(tmp_path):
    amount = Decimal("12345678901234567890123456789012345678")
    sales = [source("sales", [1], sales_units=0, sales_gmv=amount),
             source("sales", [3], sales_units=7, sales_gmv=Decimal("15"))]
    stock = [source("stock", [2, 3]), source("stock", [4])]
    batches = list(join(sales, stock))
    assert [b.num_rows for b in batches] == [2, 2]
    result = pa.concat_tables(batches)
    rows = result.to_pylist()
    assert [r["sku_id"] for r in rows] == [1, 2, 3, 4]
    assert rows[0]["sales_gmv"] == amount and rows[0]["is_in_stock_eod"] is False
    assert rows[0]["sales_units"] == 0 and rows[1]["sales_units"] is None
    assert rows[1]["is_in_stock_eod"] is True and rows[3]["is_in_stock_eod"] is True
    assert [(r["sales_component_present"], r["is_in_stock_eod"]) for r in rows] == [
        (True, False), (False, True), (True, True), (False, True)]
    assert rows[1]["sales_source_manifest_id"] is None
    assert all(r["sales_snapshot_id"] == 101 and r["stock_snapshot_id"] == 102 for r in rows)
    path = tmp_path / "observed.parquet"
    pq.write_table(result, path)
    assert pq.ParquetFile(path).read().equals(result, check_metadata=False)


def test_empty_component_not_zero_and_both_empty_not_synthetic_rows():
    rows = pa.concat_tables(list(join([], [source("stock", [1])]))).to_pylist()
    assert rows[0]["sales_component_present"] is False and rows[0]["sales_gmv"] is None
    assert rows[0]["is_in_stock_eod"] is True
    assert list(join([], [])) == []


@pytest.mark.parametrize("kind", ["sales", "stock"])
@pytest.mark.parametrize("ids", [[2, 1], [1, 1], [0], [-1], [None]])
def test_invalid_keys_rejected(kind, ids):
    batches = {"sales": [], "stock": []}
    batches[kind] = [source(kind, ids)]
    with pytest.raises(ValueError, match="SKU"):
        list(join(**batches))


@pytest.mark.parametrize("kind", ["sales", "stock"])
def test_duplicate_across_chunks_rejected(kind):
    batches = {"sales": [], "stock": []}
    batches[kind] = [source(kind, [1]), source(kind, [1])]
    with pytest.raises(ValueError, match="SKU"):
        list(join(**batches))


@pytest.mark.parametrize("change", ["date", "float_sku", "missing_column", "null_lineage", "future_capture"])
def test_bad_source_schema_or_day_cannot_be_silently_cast(change):
    sales = source("sales", [1])
    if change == "date":
        sales = source("sales", [1], date=date(2026, 8, 31))
    elif change == "float_sku":
        sales = sales.set_column(1, "sku_id", pa.array([1.0]))
    elif change == "missing_column":
        sales = sales.drop(["sales_gmv"])
    elif change == "null_lineage":
        sales = source("sales", [1], source_manifest_id=None)
    else:
        sales = source("sales", [1], ingested_at=datetime(2026, 9, 10))
    with pytest.raises(ValueError):
        list(join([sales], []))


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_batch_limit_rejected(limit):
    with pytest.raises(ValueError):
        list(join([], [], max_batch_rows=limit))


def test_closes_started_source_generators_on_early_consumer_close():
    closed = []
    def batches(kind, ids):
        try:
            for sku in ids:
                yield source(kind, [sku])
        finally:
            closed.append(kind)
    iterator = join(batches("sales", [1, 3, 5]), batches("stock", [2, 4, 6]), max_batch_rows=1)
    assert next(iterator).num_rows == 1
    iterator.close()
    assert sorted(closed) == ["sales", "stock"]


def test_rejects_ambiguous_output_schema_and_missing_versions():
    output = schema(ENTITY)
    with pytest.raises(ValueError):
        PREP.validate_schema(output.remove(3))
    with pytest.raises(ValueError):
        PREP.metadata({"sales": VERSIONS["sales"]}, "x", "v1", NOW)
    with pytest.raises(ValueError):
        PREP.metadata({"sales": {"snapshot_id": True, "table_uuid": "x"}, "stock": VERSIONS["stock"]}, "x", "v1", NOW)
