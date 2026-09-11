"""Проверить exact SKU binding и весь tree runtime без production/сети."""

from copy import deepcopy
from datetime import timedelta
import json
import re
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml

pytest.importorskip("pyiceberg")
pytest.importorskip("sqlalchemy")

from ci_test.test_demand_catalog_tree import DAY, NOW, ROOT, source
from ci_test.test_demand_catalog_tree_writer import env as tree_env, target  # noqa: F401
from layers.silver.level_node_id.demand_catalog_tree.v1.job import inputs, reader, runtime
from layers.silver.level_node_id.demand_catalog_tree.v1.job.preparation import target_ref


def description(schema):
    types = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint", pa.bool_(): "boolean",
             pa.timestamp("us"): "timestamp(6)", pa.float64(): "double"}
    return [(field.name, "varchar" if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
             else types[field.type], None, None, None, None, None) for field in schema]


@pytest.fixture
def full(tree_env):  # noqa: F811
    env = tree_env
    cfg, catalog, _ = env
    src, schema = inputs.source_config(cfg, ROOT)
    table = catalog.create_table(target_ref(src, catalog.name), schema=schema)
    rows = []
    for base in source().to_pylist():
        row = dict.fromkeys(schema.names)
        row.update(base)
        row.update(golden_mapping_status="unmatched", seller_mapping_status="unmatched",
                   catalog_seller_snapshot_id=100, source_contract_version=src["source"]["contract_version"],
                   source_manifest_id="sku-capture", ingested_at=NOW.replace(tzinfo=None))
        rows.append(row)
    table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))
    receipt = dict(status="written", snapshot_id=table.current_snapshot().snapshot_id,
                   table_uuid=str(table.metadata.table_uuid), date_min=DAY.isoformat(), date_max=DAY.isoformat(),
                   rows_written=2, catalog_version="v1", source_manifest_id="sku-capture",
                   source_contract_version=src["source"]["contract_version"], ingested_at=NOW.isoformat())
    ref = {"dag_id": src["dag"]["id"], "run_id": "sku-run"}
    checked = dict(dq_status="passed", **ref, receipt=receipt)
    for path in (ROOT / "dq/results", ROOT / "feature_stats/results"):
        service = yaml.safe_load((path / "config.yaml").read_text())
        catalog.create_table(target_ref(service, catalog.name), schema=inputs.migration_schema(path))
    return env, src, table, ref, checked


class Connection:
    """DB-API double выполняет только локальные Iceberg чтения указанных версий."""
    def __init__(self, catalog):
        self.catalog = catalog
        self.queries, self.cursors = [], []
        self.transform = lambda records: records
        self.mutate_description = lambda desc: desc

    def cursor(self):
        owner = self
        class Cursor:
            closed = False
            def execute(self, sql):
                owner.queries.append(sql)
                match = re.search(r'FROM "[^"]+"\."([^"]+)"\."([^"]+)"', sql)
                assert match, sql
                table = owner.catalog.load_table((match[1], match[2]))
                snapshot = re.search(r"FOR VERSION AS OF ([0-9]+)", sql)
                snapshot_id = int(snapshot[1]) if snapshot else None
                schema = table.schemas()[table.snapshot_by_id(snapshot_id).schema_id].as_arrow() if snapshot else table.schema().as_arrow()
                if "LIMIT 0" in sql:
                    self.description = description(schema)
                    self.records = []
                else:
                    assert sql.endswith('ORDER BY "sku_id"') and snapshot
                    batch = table.scan(snapshot_id=snapshot_id).to_arrow().select(inputs.READ_COLUMNS).sort_by([("sku_id", "ascending")])
                    self.description = owner.mutate_description(description(batch.schema))
                    self.records = owner.transform([list(row.values()) for row in batch.to_pylist()])
            def fetchmany(self, count):
                records, self.records = self.records[:count], self.records[count:]
                return records
            def close(self):
                self.closed = True
        cursor = Cursor()
        self.cursors.append(cursor)
        return cursor


def execute(full, **options):
    env, _, _, ref, checked = full
    cfg = deepcopy(env[0])
    cfg["runtime"]["max_batch_rows"] = 1
    connection = options.pop("connection", Connection(env[1]))
    result = runtime.execute_load(cfg, ROOT, catalog=env[1], connection=connection, reference=ref,
                                  get_checked=options.pop("get_checked", lambda _: checked), source_manifest_id="tree-run",
                                  ingested_at=NOW + timedelta(minutes=1), **options)
    return result, connection


def bound(full, checked=None):
    _, src, _, ref, initial = full
    return inputs.bind_source(src, ref, initial if checked is None else checked, captured_at=NOW + timedelta(minutes=1))


def test_runtime_reads_exact_snapshot_and_returns_complete_audit(full):
    result, conn = execute(full)
    assert result["status"] == "written" and result["rows_written"] == 7
    audit = result["source_audit"]
    assert audit["source_sku_rows"] == 2 and audit["missing_category_sku"] == 0
    assert audit["reference"] == full[3] and audit["receipt"] == full[4]["receipt"]
    assert audit["schema_id"] == full[2].current_snapshot().schema_id
    assert json.loads(json.dumps(result)) == result
    assert len(conn.queries) == 5 and all(cursor.closed for cursor in conn.cursors)
    assert all("FOR VERSION AS OF" in query for query in conn.queries if "demand_catalog_sku" in query)
    assert "WHERE" not in conn.queries[-1] and "LIMIT" not in conn.queries[-1]


def test_old_checked_snapshot_is_valid_after_new_head(full):
    from pyiceberg.types import StringType
    table = full[2]
    old = table.scan().to_arrow()
    # Новая текущая схема не должна применяться к старому точному snapshot.
    with table.update_schema() as update:
        update.add_column("new_optional", StringType())
    table.append(old)
    assert table.current_snapshot().snapshot_id != full[4]["receipt"]["snapshot_id"]
    result, _ = execute(full)
    assert result["source_audit"]["source_sku_rows"] == 2
    assert result["catalog_sku_snapshot_id"] == full[4]["receipt"]["snapshot_id"]


@pytest.mark.parametrize("key,value", [("snapshot_id", True), ("snapshot_id", 0), ("rows_written", None),
    ("rows_written", 0), ("table_uuid", "wrong"), ("catalog_version", ""), ("source_manifest_id", " "),
    ("source_contract_version", "old"), ("ingested_at", NOW.replace(tzinfo=None).isoformat()),
    ("ingested_at", (NOW + timedelta(days=1)).isoformat()), ("date_min", "2026-09-08"), ("date_max", "2026-09-10")])
def test_invalid_receipt_rejected(full, key, value):
    checked = deepcopy(full[4])
    checked["receipt"][key] = value
    with pytest.raises(ValueError):
        bound(full, checked)


@pytest.mark.parametrize("mutation", ["failed", "wrong_run", "missing_receipt", "wrong_dag"])
def test_requires_exact_successful_dq(full, mutation):
    checked = deepcopy(full[4])
    if mutation == "failed":
        checked["dq_status"] = "failed"
    elif mutation == "wrong_run":
        checked["run_id"] = "other"
    elif mutation == "wrong_dag":
        checked["dag_id"] = "other"
    else:
        checked.pop("receipt")
    conn = Connection(full[0][1])
    with pytest.raises(ValueError):
        execute(full, get_checked=lambda _: checked, connection=conn)
    assert not conn.queries and target(full[0]).current_snapshot() is None


def test_missing_exact_snapshot_and_wrong_uuid(full):
    for key, value in [("snapshot_id", 1), ("table_uuid", "00000000-0000-0000-0000-000000000000")]:
        checked = deepcopy(full[4])
        checked["receipt"][key] = value
        with pytest.raises(ValueError, match="latest запрещён"):
            execute(full, get_checked=lambda _: checked)
    assert target(full[0]).current_snapshot() is None


def test_changed_dq_before_commit_preserves_previous_tree(full):
    execute(full)
    before = target(full[0]).current_snapshot().snapshot_id
    changed = deepcopy(full[4])
    changed["receipt"]["catalog_version"] = "v2"
    getter = Mock(side_effect=[full[4], changed])
    with pytest.raises(ValueError, match="DQ payload"):
        execute(full, get_checked=getter)
    assert getter.call_count == 2
    assert target(full[0]).current_snapshot().snapshot_id == before


@pytest.mark.parametrize("kind", ["truncated", "extra", "duplicate", "wrong_manifest", "wrong_day", "bool_sku", "string_date"])
def test_bad_stream_closes_cursor_without_writing(full, kind):
    conn = Connection(full[0][1])
    def transform(rows):
        if kind == "truncated":
            return rows[:1]
        if kind == "extra":
            rows.append(list(rows[-1]))
            rows[-1][inputs.READ_COLUMNS.index("sku_id")] = 3
        elif kind == "duplicate":
            rows[-1][inputs.READ_COLUMNS.index("sku_id")] = 1
        else:
            name, value = {"wrong_manifest": ("source_manifest_id", "other"), "wrong_day": ("date", DAY - timedelta(days=1)),
                           "bool_sku": ("sku_id", True), "string_date": ("date", DAY.isoformat())}[kind]
            rows[-1][inputs.READ_COLUMNS.index(name)] = value
        return rows
    conn.transform = transform
    with pytest.raises(ValueError):
        execute(full, connection=conn)
    assert all(cursor.closed for cursor in conn.cursors)
    assert target(full[0]).current_snapshot() is None


def test_stream_byte_limit_early_close_and_bad_metadata(full):
    env, src, table, _, _ = full
    version = bound(full)
    conn = Connection(env[1])
    with pytest.raises(ValueError, match="размер входной"):
        list(reader.read_batches(conn, src, ROOT, version, table.schema().as_arrow(), max_batch_rows=1, max_batch_bytes=1))
    assert conn.cursors[-1].closed
    stream = reader.read_batches(conn, src, ROOT, version, table.schema().as_arrow(), max_batch_rows=1, max_batch_bytes=100000)
    next(stream)
    stream.close()
    assert conn.cursors[-1].closed
    conn.mutate_description = lambda rows: [("bad", *rows[0][1:]), *rows[1:]]
    with pytest.raises(ValueError, match="metadata"):
        execute(full, connection=conn)
    assert all(cursor.closed for cursor in conn.cursors)


def test_missing_service_table_fails_before_any_trino_read(full):
    catalog = full[0][1]
    cfg = yaml.safe_load((ROOT / "feature_stats/results/config.yaml").read_text())
    catalog.drop_table(target_ref(cfg, catalog.name))
    conn = Connection(catalog)
    with pytest.raises(ValueError, match="служебной таблицы"):
        execute(full, connection=conn)
    assert not conn.queries and target(full[0]).current_snapshot() is None


def test_source_contract_and_schema_preflight(full):
    _, src, table, _, _ = full
    _, expected = inputs.source_config(full[0][0], ROOT)
    assert len(expected) == 38
    inputs.validate_schema(table.schema().as_arrow(), expected)
    with pytest.raises(ValueError, match="схема|Схема"):
        inputs.preflight_source(src, full[0][1], bound(full), expected.remove(2))
    cfg = deepcopy(full[0][0])
    cfg["inputs"]["sku_config"] = "../config.yaml"
    with pytest.raises(ValueError, match="внутри FP"):
        inputs.source_config(cfg, ROOT)
