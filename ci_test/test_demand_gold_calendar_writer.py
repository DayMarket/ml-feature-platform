"""Проверить full refresh gold на локальном Iceberg/SQLite без внешних сервисов."""

import pytest
import pyarrow as pa

pytest.importorskip("pyiceberg")
pytest.importorskip("sqlalchemy")
from pyiceberg.catalog.sql import SqlCatalog
from test_demand_gold_calendar import batch, config, schema_from_ddl, writer
from test_demand_event_writer import env as _silver_env, query_result

silver_env = _silver_env


@pytest.fixture
def env(tmp_path):
    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("gold")
    cfg = config()
    identifier = cfg["table"]["schema"], cfg["table"]["name"]
    table = catalog.create_table(identifier, schema=schema_from_ddl())
    yield cfg, catalog, table
    catalog.engine.dispose()


def test_retry_and_removed_dates(env):
    cfg, catalog, table = env
    data = batch(table.schema().as_arrow())
    for _ in range(2):
        receipt = writer.write_prepared(cfg, catalog, data)
        assert receipt["rows_written"] == 3
    replacement = data.slice(0, 1)
    receipt = writer.write_prepared(cfg, catalog, replacement)
    table.refresh()
    assert table.scan().to_arrow().equals(replacement, check_metadata=False)
    assert set(table.refs()) == {"main"}
    assert table.current_snapshot().snapshot_id == receipt["snapshot_id"]


@pytest.mark.parametrize("kind", ["empty", "duplicate", "count", "flags", "lineage"])
def test_invalid_batch_keeps_previous_snapshot(env, kind):
    cfg, catalog, table = env
    data = batch(table.schema().as_arrow())
    receipt = writer.write_prepared(cfg, catalog, data)
    if kind == "empty":
        broken = data.slice(0, 0)
    elif kind == "duplicate":
        broken = pa.concat_tables([data, data])
    else:
        field, values = {"count": ("big_sale_event_count", [-1, 0, 0]),
                         "flags": ("big_sale_created", [None, None, None]),
                         "lineage": ("calendar_snapshot_id", [1, 2, 3])}[kind]
        broken = data.set_column(data.schema.get_field_index(field), data.schema.field(field),
                                 pa.array(values, type=data.schema.field(field).type))
    with pytest.raises(ValueError):
        writer.write_prepared(cfg, catalog, broken)
    table.refresh()
    assert table.current_snapshot().snapshot_id == receipt["snapshot_id"]


def test_full_runtime_reads_exact_silver_snapshots(silver_env):
    from datetime import datetime, timezone
    from pathlib import Path
    import yaml
    from test_demand_gold_calendar import ROOT, runtime
    from test_demand_event_writer import writer as event_writer

    event_cfg, catalog, calendar_receipt = silver_env
    events_receipt = event_writer.load_events(
        event_cfg, catalog, ROOT, calendar_receipt=calendar_receipt,
        source_manifest_id="events-run", ingested_at=datetime(2026, 9, 8, 3, 10, tzinfo=timezone.utc),
        query_records=lambda sql: query_result())
    cfg = config()
    catalog.create_namespace("gold")
    catalog.create_table((cfg["table"]["schema"], cfg["table"]["name"]), schema=schema_from_ddl())
    # Служебные таблицы здесь нужны только preflight, DQ SQL тестируется отдельно.
    for relative in ("dq/results/config.yaml", "feature_stats/results/config.yaml"):
        service = yaml.safe_load((Path(ROOT) / relative).read_text())["table"]
        catalog.create_table((service["schema"], service["name"]), schema=pa.schema([pa.field("date", pa.date32())]))
    sources = runtime.source_configs(cfg, ROOT)
    receipts = {"calendar": calendar_receipt, "events": events_receipt}
    refs = {name: {"dag_id": sources[name]["dag"]["id"], "run_id": receipt["source_manifest_id"],
                   "logical_date": "2026-09-08T03:00:00+00:00"} for name, receipt in receipts.items()}
    checked = {name: {"dq_status": "passed", "dag_id": refs[name]["dag_id"],
                      "run_id": refs[name]["run_id"], "receipt": receipt}
               for name, receipt in receipts.items()}
    result = runtime.execute_load(cfg, ROOT, "gold-run", "manual", refs, checked,
                                  catalog=catalog, now=datetime(2026, 9, 8, 4, tzinfo=timezone.utc))
    assert result["rows_written"] == calendar_receipt["rows_written"]
    table = catalog.load_table((cfg["table"]["schema"], cfg["table"]["name"]))
    assert table.scan().to_arrow()["big_sale_canceled"].to_pylist() == [None, True, True]
    checked["calendar"]["receipt"] = {**calendar_receipt, "snapshot_id": 1}
    with pytest.raises(ValueError, match="другой версии"):
        runtime.execute_load(cfg, ROOT, "wrong", "manual", refs, checked, catalog=catalog,
                             now=datetime(2026, 9, 8, 4, tzinfo=timezone.utc))
    table.refresh()
    assert table.current_snapshot().snapshot_id == result["snapshot_id"]
