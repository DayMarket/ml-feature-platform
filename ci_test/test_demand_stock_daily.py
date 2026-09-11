"""Контракт разреженного EOD-наличия и атомарной записи дня."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import re

import pyarrow as pa
import pytest
import yaml

from layers.silver.sku_id.demand_stock_daily.v1.job import checkpoint, preparation as prep
from layers.silver.sku_id.demand_stock_daily.v1.job import query, runtime, writer

ENTITY = Path("layers/silver/sku_id/demand_stock_daily/v1")
DAY = date(2026, 9, 1)
CAPTURE = datetime(2026, 9, 2, tzinfo=timezone.utc)


def schema():
    types = {"DATE": pa.date32(), "BIGINT": pa.int64(), "STRING": pa.string(),
             "TIMESTAMP": pa.timestamp("us")}
    fields = re.findall(
        r"^    (\w+) (DATE|BIGINT|STRING|TIMESTAMP)( NOT NULL)? COMMENT",
        (ENTITY / "migrations/create_table.sql").read_text(),
        re.M,
    )
    assert len(fields) == 5
    return pa.schema([pa.field(name, types[kind], nullable=not bool(required))
                      for name, kind, required in fields])


def config():
    result = yaml.safe_load((ENTITY / "config.yaml").read_text())
    result["runtime"]["max_batch_rows"] = 2
    return result


def raw(*, day=DAY, ids=(1, 2)):
    return pa.table({
        "date": pa.array([day] * len(ids), type=pa.date32()),
        "sku_id": pa.array(ids, type=pa.int64()),
    })


def batch(target_schema, *, day=DAY, ids=(1, 2), manifest="run"):
    return prep.prepare_batch(
        raw(day=day, ids=ids),
        target_schema,
        day=day,
        manifest=manifest,
        version=config()["source"]["contract_version"],
        ingested_at=CAPTURE,
    )


class SourceClient:
    def __init__(self, day=DAY):
        self.day = day
        self.ids = (1, 2)
        self.calls = []
        self.audit_reads = 0
        self.changed = False

    def execute(self, sql):
        self.calls.append(sql)
        self.audit_reads += 1
        ids = self.ids
        key_hash = 101 if not self.changed or self.audit_reads == 1 else 102
        return [(len(ids), len(set(ids)), 0, 0, key_hash, datetime(2026, 9, 1, 23, 0))]

    def execute_iter(self, sql, **kwargs):
        self.calls.append(sql)
        size = kwargs["chunk_size"]
        assert size == kwargs["settings"]["max_block_size"] == 2
        source = raw(day=self.day, ids=self.ids)
        rows = list(zip(source["date"].to_pylist(), source["sku_id"].to_pylist()))
        payload = [[("date", "Date"), ("sku_id", "Int64")], *rows]
        for start in range(0, len(payload), size):
            yield payload[start:start + size]


@pytest.fixture
def env(tmp_path):
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.transforms import IdentityTransform

    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("silver")
    cfg = config()
    table = catalog.create_table(prep.target_ref(cfg, catalog.name), schema=schema())
    with table.update_spec() as update:
        update.add_field("date", IdentityTransform(), "date")
    yield cfg, catalog, table
    catalog.engine.dispose()


def write(env, *, day=DAY, ids=(1, 2), manifest="run"):
    cfg, catalog, table = env
    return writer.write_day(
        cfg,
        catalog,
        [batch(table.schema().as_arrow(), day=day, ids=ids, manifest=manifest)],
        day=day,
        expected_rows=len(ids),
        manifest=manifest,
        version=cfg["source"]["contract_version"],
        ingested_at=CAPTURE,
        verify_source=lambda: True,
    )


def test_migration_and_config_define_sparse_contract():
    ddl = (ENTITY / "migrations/create_table.sql").read_text()
    assert schema().names == ["date", "sku_id", "source_manifest_id",
                              "source_contract_version", "ingested_at"]
    assert "quantity_active_eod > 0" in ddl and "quantity_fbs_eod > 0" in ddl
    cfg = config()
    assert cfg["source"]["contract_version"] == "source_eod_positive_availability_v1"
    assert "currency" not in cfg["source"]


def test_query_filters_zero_rows_in_clickhouse():
    sql = query.source_query(config(), DAY)
    assert "FROM `marts`.`daily_sku_quantity_eod` FINAL" in sql
    assert "quantity_active_eod` > 0 OR `quantity_fbs_eod` > 0" in sql
    assert "ORDER BY sku_id" in sql and "max_execution_time=1200" in sql
    audit = query.count_query(config(), DAY)
    assert "key_hash" in audit and "max(updated_at)" in audit


def test_preparation_rejects_bad_keys_and_schema():
    target = schema()
    ready = batch(target)
    assert ready.schema.equals(target) and ready.num_rows == 2
    with pytest.raises(ValueError, match="порядок"):
        prep.prepare_batch(raw(ids=(2, 1)), target, day=DAY, manifest="m", version="v",
                           ingested_at=CAPTURE)
    with pytest.raises(ValueError, match="sku_id"):
        prep.prepare_batch(raw(ids=(0,)), target, day=DAY, manifest="m", version="v",
                           ingested_at=CAPTURE)


def test_runtime_uses_driver_chunks_and_writes_sparse_day(env):
    cfg, catalog, table = env
    receipt = runtime.load_day(cfg, catalog, SourceClient(), day=DAY, manifest="capture")
    assert receipt["rows_written"] == 2 and receipt["status"] == "written"
    table.refresh()
    result = table.scan().to_arrow().sort_by([("sku_id", "ascending")])
    assert result["date"].to_pylist() == raw()["date"].to_pylist()
    assert result["sku_id"].to_pylist() == raw()["sku_id"].to_pylist()


def test_source_signature_change_blocks_commit(env):
    cfg, catalog, table = env
    client = SourceClient()
    client.changed = True
    with pytest.raises(ValueError, match="Source изменился"):
        runtime.load_day(cfg, catalog, client, day=DAY, manifest="capture")
    table.refresh()
    assert table.current_snapshot() is None


def test_atomic_replace_resume_and_neighbor(env):
    first = write(env)
    second = write(env, day=DAY + timedelta(days=1))
    resumed = checkpoint.resume_day(
        env[0], env[1], day=DAY, manifest="run",
        version=env[0]["source"]["contract_version"],
    )
    assert resumed["resumed"] is True and resumed["snapshot_id"] == second["snapshot_id"]
    assert first["snapshot_id"] != second["snapshot_id"]
    env[2].refresh()
    assert env[2].scan().count() == 4


@pytest.mark.parametrize("value", [0, -1, True, None])
def test_runtime_rejects_invalid_batch_limit_before_source(env, value):
    cfg, catalog, _ = env
    cfg["runtime"]["max_batch_rows"] = value
    client = SourceClient()
    with pytest.raises(ValueError, match="лимиты"):
        runtime.load_day(cfg, catalog, client, day=DAY, manifest="capture")
    assert client.calls == []
