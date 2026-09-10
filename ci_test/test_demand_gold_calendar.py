"""Дневной gold: ключи, NULL, статусы, версии источников и Parquet."""

from datetime import date, datetime, timezone
from importlib import import_module
from pathlib import Path
import re
import sys
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
PACKAGE = "layers.gold.date.demand_calendar_daily.v1.job"
preparation = import_module(PACKAGE + ".preparation")
runtime = import_module(PACKAGE + ".runtime")
writer = import_module(PACKAGE + ".writer")
ENTITY = ROOT / "layers/gold/date/demand_calendar_daily/v1"
NOW = datetime(2026, 9, 8, 4, tzinfo=timezone.utc)


def schema_from_ddl(entity=ENTITY):
    kinds = {"DATE": pa.date32(), "INT": pa.int32(), "BIGINT": pa.int64(),
             "STRING": pa.large_string(), "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us")}
    fields = re.findall(r"^    (\w+) (\w+)( NOT NULL)? COMMENT ",
                        (entity / "migrations/create_table.sql").read_text(), re.M)
    return pa.schema([pa.field(name, kinds[kind], nullable=not bool(required))
                      for name, kind, required in fields])


def config():
    return yaml.safe_load((ENTITY / "config.yaml").read_text())


def inputs():
    schema = schema_from_ddl()
    calendar_schema = pa.schema([schema.field(name) for name in preparation.CALENDAR_FIELDS])
    calendar = pa.Table.from_pylist([
        {"date": date(2026, 9, day), "calendar_id": "uz_official", "month": 9,
         "is_public_holiday": None if day == 1 else False, "holiday_name": ""}
        for day in (1, 2, 4)], schema=calendar_schema)
    events = pa.Table.from_pylist([
        {"date": date(2026, 9, 1), "event_code": str(i), "source_kind": "marketing_sale",
         "source_type": kind, "source_status": status}
        for i, (kind, status) in enumerate([
            ("BIG_SALE", "CREATED"), ("BIG_SALE", "CANCELED"),
            ("BIG_SALE", None), ("TODAY_DEALS", "CREATED")])])
    return calendar, events


def batch(schema=None):
    calendar, events = inputs()
    return preparation.prepare(calendar, events, schema or schema_from_ddl(),
                               calendar_receipt={"snapshot_id": 1, "source_manifest_id": "cal"},
                               events_receipt={"snapshot_id": 2, "source_manifest_id": "events"},
                               run_id="gold", ingested_at=NOW)


def test_calendar_fields_and_nulls_preserved_without_fanout():
    result = batch()
    assert result.num_rows == 3 and len(result.column_names) == 28
    assert result["date"].to_pylist() == [date(2026, 9, day) for day in (1, 2, 4)]
    first, second, _ = result.to_pylist()
    assert first["is_public_holiday"] is None and first["holiday_name"] == ""
    assert first["big_sale_event_count"] == 3
    assert all(first[name] is True for name in preparation.FLAGS)
    assert second["big_sale_event_count"] == 0
    assert all(second[name] is None for name in preparation.FLAGS)
    assert second["promotion_coverage_status"] == "no_registry_rows"
    assert "big_sale_confirmed" not in result.column_names
    writer.validate_batch(result, schema_from_ddl())


def test_parquet_matches_physical_schema_and_values(tmp_path):
    result = batch()
    path = tmp_path / "calendar.parquet"
    pq.write_table(result, path)
    restored = pq.read_table(path)
    assert restored.equals(result, check_metadata=False)
    writer.validate_batch(restored, schema_from_ddl())


@pytest.mark.parametrize("kind", ["duplicate_calendar", "duplicate_event", "outside", "wrong_calendar"])
def test_invalid_input_is_rejected(kind):
    calendar, events = inputs()
    if kind == "duplicate_calendar":
        calendar = pa.concat_tables([calendar, calendar.slice(0, 1)])
    elif kind == "duplicate_event":
        events = pa.concat_tables([events, events.slice(0, 1)])
    elif kind == "outside":
        events = events.set_column(0, "date", pa.array([date(2026, 9, 3)] * 4))
    else:
        calendar = calendar.set_column(1, "calendar_id", pa.array(["other"] * 3))
    with pytest.raises(ValueError):
        preparation.prepare(calendar, events, schema_from_ddl(), calendar_receipt={},
                            events_receipt={}, run_id="gold", ingested_at=NOW)


@pytest.mark.parametrize("value", ["2026-09-08T04:00:00", "2026-09-08T04:00:00Z",
                                  "2026-09-08T09:00:00+05:00", "2026-09-08 04:00:00+00:00",
                                  "2026-09-08 04:00:00"])
def test_timestamp_and_schedule(value):
    assert runtime.utc_timestamp(value) == NOW
    cfg = config()
    refs = runtime.scheduled_references(cfg, runtime.source_configs(cfg, ROOT), value, "2026-09-09T04:00:00Z")
    assert refs["calendar"]["run_id"] == "scheduled__2026-09-09T03:00:00+00:00"
    assert refs["events"]["logical_date"] == "2026-09-08T03:10:00+00:00"


@pytest.mark.parametrize("kind", ["failed", "wrong_run", "written_only", "missing_snapshot"])
def test_unchecked_sources_block_before_catalog_io(kind):
    cfg = config()
    sources = runtime.source_configs(cfg, ROOT)
    refs = runtime.scheduled_references(cfg, sources, NOW, "2026-09-09T04:00:00Z")
    checked = {name: {"dq_status": "passed", "dag_id": ref["dag_id"], "run_id": ref["run_id"],
                      "receipt": {"status": "written", "source_manifest_id": ref["run_id"],
                                  "ingested_at": NOW.isoformat()}}
               for name, ref in refs.items()}
    if kind == "failed":
        checked["calendar"]["dq_status"] = "failed"
    elif kind == "wrong_run":
        checked["events"]["run_id"] = "other"
    elif kind == "written_only":
        checked["calendar"] = checked["calendar"]["receipt"]
    catalog = Mock()
    with pytest.raises(ValueError):
        runtime.execute_load(cfg, ROOT, "gold", "regular", refs, checked, catalog=catalog, now=NOW)
    assert catalog.mock_calls == []


@pytest.mark.parametrize("name", ["table", "schema.table", "catalog.schema.table", ""])
def test_bad_identifier_components(name):
    cfg = config()
    cfg["table"]["name"] = name
    if name == "table":
        assert preparation.target_ref(cfg, "iceberg") == ("gold", "table")
    else:
        with pytest.raises(ValueError):
            preparation.target_ref(cfg, "iceberg")
