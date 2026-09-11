"""Полный seller-каталог: схема, NULL, master-коллизии и точный Parquet round-trip."""

from datetime import date, datetime, timezone
from importlib import import_module
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/silver/seller_id/demand_catalog_seller/v1"
PREP = import_module("layers.silver.seller_id.demand_catalog_seller.v1.job.preparation")
NOW = datetime(2026, 9, 8, 21, tzinfo=timezone.utc)


def source(ids=(3, 1, 2), masters=(None, " master-a ", "")):
    return pa.table({"seller_id": pa.array(ids, type=pa.uint64()),
                     "source_master_seller_id": pa.array(masters, type=pa.string()),
                     "is_1p": pa.array([None, True, False], type=pa.bool_()),
                     "seller_registered_at": pa.array([None, NOW, NOW], type=pa.timestamp("us", "UTC"))})


def prepare(raw=None, **options):
    defaults = dict(expected_source_rows=3, catalog_version="catalog-1", source_manifest_id="source-1",
                    source_contract_version="current_seller_catalog_v1", ingested_at=NOW)
    return PREP.prepare_catalog(source() if raw is None else raw, PREP.expected_schema(), **(defaults | options))


def test_null_master_is_not_unmatched_and_source_text_is_preserved():
    rows = prepare().to_pylist()
    assert [row["seller_id"] for row in rows] == [1, 2, 3]
    assert [row["master_seller_id"] for row in rows] == ["master-a", "2", None]
    assert [row["seller_mapping_status"] for row in rows] == ["matched", "unmatched", "unavailable"]
    assert [row["has_master"] for row in rows] == [True, False, None]
    assert rows[0]["source_master_seller_id"] == " master-a "
    assert all(row["date"] == date(2026, 9, 9) for row in rows)
    assert rows[0]["seller_registered_at"] == NOW.replace(tzinfo=None)
    assert rows[-1]["seller_registered_at"] is None and rows[-1]["is_1p"] is None


def test_shared_master_is_valid_but_fallback_collision_is_not():
    assert prepare(source(masters=("master-a", "master-a", ""))).num_rows == 3
    with pytest.raises(ValueError, match="конфликтует"):
        prepare(source(masters=("2", "master-a", "")))


@pytest.mark.parametrize("ids", [(1, 1, 2), (0, 1, 2), (None, 1, 2), (2**63, 1, 2)])
def test_invalid_ids_rejected_without_filtering(ids):
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        prepare(source(ids=ids))


@pytest.mark.parametrize("expected", [None, True, 0, 2, 4])
def test_independent_count_is_required(expected):
    with pytest.raises(ValueError, match="полный захват"):
        prepare(expected_source_rows=expected)


@pytest.mark.parametrize("name,kind,values", [
    ("seller_id", pa.float64(), [1.0, 2.0, 3.0]),
    ("source_master_seller_id", pa.int64(), [1, 2, 3]),
    ("is_1p", pa.int64(), [1, 0, None]),
    ("seller_registered_at", pa.timestamp("us"), [NOW.replace(tzinfo=None)] * 3),
])
def test_no_implicit_source_conversions(name, kind, values):
    raw = source()
    raw = raw.set_column(raw.schema.get_field_index(name), name, pa.array(values, type=kind))
    with pytest.raises(ValueError):
        prepare(raw)


@pytest.mark.parametrize("changes", [{"catalog_version": ""}, {"source_manifest_id": None},
    {"source_contract_version": " "}, {"ingested_at": NOW.replace(tzinfo=None)}])
def test_provenance_required(changes):
    with pytest.raises(ValueError):
        prepare(**changes)


def test_parquet_and_migration_schema_match(tmp_path):
    ddl = (ENTITY / "migrations/create_table.sql").read_text()
    fields = re.findall(r"^    (\w+) (DATE|BIGINT|STRING|BOOLEAN|TIMESTAMP)( NOT NULL)? COMMENT", ddl, re.M)
    types = {"DATE": pa.date32(), "BIGINT": pa.int64(), "STRING": pa.string(), "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us")}
    schema = pa.schema([pa.field(n, types[t], nullable=not required) for n, t, required in fields])
    assert len(schema) == 12 and schema == PREP.expected_schema()
    result = prepare()
    path = tmp_path / "catalog.parquet"
    pq.write_table(result, path)
    assert pq.read_table(path).equals(result)
    assert "protected" not in ddl and "IF NOT EXISTS" in ddl and "engine.hive.lock-enabled" in ddl


def test_config_uses_capture_partition_dq_for_unknown_mappings():
    cfg = yaml.safe_load((ENTITY / "config.yaml").read_text())
    assert PREP.target_ref(cfg, "iceberg") == ("silver", "feature_platform_demand_catalog_seller")
    assert cfg["dq"]["scope"] == "partition"
    assert cfg["dq"]["partition_granularity"] == "timestamp"
    assert cfg["feature_stats"]["enabled"] is True
    assert cfg["dq"]["warmup_days"] == 0
    assert cfg["dq"]["partition_date_template"] == cfg["feature_stats"]["partition_date_template"]
    forbidden = next(t for t in cfg["dq"]["tests"] if t["name"] == "not_accepted_values")
    assert forbidden["values"] == ["unavailable", "conflict"]
    sql = PREP.source_sql(cfg)
    assert "FROM `marts`.`sellers_info` ORDER BY seller_id" in sql
    assert " WHERE " not in sql and " JOIN " not in sql and "SELECT *" not in sql
    assert "toTimeZone(registration_date, 'UTC')" in sql
