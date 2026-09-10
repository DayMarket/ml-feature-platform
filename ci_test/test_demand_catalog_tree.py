"""Проверить дерево нормализованных путей и перенос каталожных схем 4.1."""

from datetime import date, datetime, timezone
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from layers.silver.level_node_id.demand_catalog_tree.v1.job import preparation as prep

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/silver/level_node_id/demand_catalog_tree/v1"
SKU = ROOT / "layers/silver/sku_id/demand_catalog_sku/v1"
DAY = date(2026, 9, 9)
NOW = datetime(2026, 9, 9, 5, tzinfo=timezone.utc)


def source(ids=(1, 2), paths=None, statuses=None):
    if paths is None:
        paths = [("market", "l1:1", "l2:1", "l3:1", "l4:1", "l5:1", "leaf:10")] * len(ids)
    fields = [pa.field("date", pa.date32()), pa.field("sku_id", pa.int64()),
              pa.field("catalog_version", pa.string()), pa.field("category_path_status", pa.string())]
    fields += [pa.field(level, pa.string()) for level in prep.LEVELS]
    statuses = statuses or ["valid"] * len(ids)
    rows = [dict(date=DAY, sku_id=sku, catalog_version="v1", category_path_status=status,
                 **dict(zip(prep.LEVELS, path, strict=True))) for sku, path, status in zip(ids, paths, statuses, strict=True)]
    return pa.Table.from_pylist(rows, schema=pa.schema(fields))


def run(batches, **options):
    arguments = dict(capture_date=DAY, catalog_version="v1", source_snapshot_id=123, expected_source_rows=2,
                     source_manifest_id="source", source_contract_version="current_catalog_tree_v1", ingested_at=NOW)
    return prep.prepare_tree(batches, prep.expected_schema(), **(arguments | options))


def test_duplicate_paths_collapsed_after_validating_parents(tmp_path):
    batch = source()
    output, audit = run([batch.slice(0, 1), batch.slice(1)])
    assert output.num_rows == 7 and audit["source_sku_rows"] == 2
    assert output["is_passthrough"].to_pylist() == [False, False, True, True, True, True, False]
    assert output["parent_id"][0].as_py() is None
    assert output["catalog_sku_snapshot_id"].to_pylist() == [123] * 7
    assert run([batch])[1] == audit
    path = tmp_path / "tree.parquet"
    pq.write_table(output, path)
    assert pq.read_table(path).equals(output)


def test_missing_category_keeps_source_count_without_unknown_node():
    paths = [source().to_pylist()[0], dict.fromkeys(prep.LEVELS)]
    batch = source(paths=[tuple(row[level] for level in prep.LEVELS) for row in paths], statuses=["valid", "missing"])
    output, audit = run([batch])
    assert audit["missing_category_sku"] == 1 and audit["valid_category_sku"] == 1
    assert output.num_rows == 7


def test_conflicting_parent_is_not_resolved_by_dedup():
    one = ("market", "l1:1", "l2:1", "l3:1", "l4:1", "l5:1", "leaf:10")
    two = ("market", "l1:2", "l2:2", "l3:2", "l4:2", "l5:2", "leaf:10")
    with pytest.raises(ValueError, match="родителей"):
        run([source(paths=[one, two])])


@pytest.mark.parametrize("value", [None, "l1:01", "l2:1", "l1:0", "l1:-1", f"l1:{2**63}"])
def test_invalid_node_is_rejected(value):
    path = ["market", value, "l2:1", "l3:1", "l4:1", "l5:1", "leaf:10"]
    with pytest.raises(ValueError):
        run([source(paths=[path, path])])


@pytest.mark.parametrize("ids", [(1, 1), (2, 1), (0, 1), (None, 2)])
def test_source_sku_keys_checked_across_batches(ids):
    batch = source(ids=ids)
    with pytest.raises(ValueError, match="SKU"):
        run([batch.slice(0, 1), batch.slice(1)])


@pytest.mark.parametrize("options", [{"expected_source_rows": 1}, {"expected_source_rows": 3},
    {"expected_source_rows": True}, {"source_snapshot_id": 0}, {"catalog_version": "other"},
    {"capture_date": date(2026, 9, 8)}, {"source_manifest_id": ""}, {"ingested_at": NOW.replace(tzinfo=None)}])
def test_bad_metadata_does_not_produce_tree(options):
    with pytest.raises(ValueError):
        run([source()], **options)


def test_failed_source_stream_is_closed():
    closed = []
    def stream():
        try:
            yield source(statuses=["conflict", "valid"])
        finally:
            closed.append(True)
    with pytest.raises(ValueError, match="Конфликт"):
        run(stream())
    assert closed == [True]


def test_all_missing_does_not_publish_empty_tree():
    with pytest.raises(ValueError, match="Нет валидных"):
        run([source(paths=[(None,) * 7] * 2, statuses=["missing", "missing"])])


def test_migration_schema_and_capture_configs():
    kinds = {"DATE": pa.date32(), "STRING": pa.string(), "INT": pa.int32(), "BIGINT": pa.int64(),
             "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us")}
    for entity, count in [(SKU, 38), (ENTITY, 11)]:
        ddl = (entity / "migrations/create_table.sql").read_text()
        fields = re.findall(r"^    (\w+) ([A-Z]+)( NOT NULL)? COMMENT ", ddl, re.M)
        assert len(fields) == count
        cfg = yaml.safe_load((entity / "config.yaml").read_text())
        assert cfg["dq"]["scope"] == "partition"
        assert cfg["dq"]["partition_granularity"] == "timestamp"
        assert cfg["feature_stats"]["enabled"] is True
        assert cfg["dq"]["partition_date_template"] == cfg["feature_stats"]["partition_date_template"]
        assert cfg["dq"]["warmup_days"] == 0 and cfg["dag"]["schedule"] == "0 4 * * *"
        assert cfg["dag"]["start_date"] == "2026-09-08T00:00:00Z" and cfg["dag"]["catchup"] is False
        assert "IF NOT EXISTS" in ddl and "engine.hive.lock-enabled" in ddl
        assert "protected" not in ddl
        if entity == ENTITY:
            schema = pa.schema([pa.field(name, kinds[kind], nullable=not required) for name, kind, required in fields])
            assert schema == prep.expected_schema()
