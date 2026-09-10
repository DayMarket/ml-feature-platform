"""Проверить точную привязку событий к DQ календаря без Airflow и источников."""

from datetime import datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
runtime = import_module("layers.silver.event_code.demand_event_calendar.v1.job.runtime")
CONFIG = yaml.safe_load((ROOT / "layers/silver/event_code/demand_event_calendar/v1/config.yaml").read_text())
CALENDAR = yaml.safe_load((ROOT / CONFIG["inputs"]["calendar_config"]).read_text())


def reference():
    return runtime.scheduled_calendar_reference(CONFIG, CALENDAR,
                                                 "2026-09-08T03:10:00Z", "2026-09-09T03:10:00Z")


def checked():
    ref = reference()
    return {"dq_status": "passed", "dag_id": ref["dag_id"], "run_id": ref["run_id"],
            "receipt": {"status": "written", "source_manifest_id": ref["run_id"]}}


def test_calendar_logical_date_is_not_run_id_suffix():
    ref = reference()
    assert ref["logical_date"] == "2026-09-08T03:00:00+00:00"
    assert ref["run_id"] == "scheduled__2026-09-09T03:00:00+00:00"
    assert ref["dag_id"] == CALENDAR["dag"]["id"]


@pytest.mark.parametrize("value", ["2026-09-08T03:10:00", "2026-09-08T03:10:00+00:00",
                                  "2026-09-08T03:10:00Z", "2026-09-08 03:10:00+00:00",
                                  "2026-09-08 03:10:00", "2026-09-08T08:10:00+05:00"])
def test_airflow_timestamp_formats(value):
    assert runtime.utc_timestamp(value) == datetime(2026, 9, 8, 3, 10, tzinfo=timezone.utc)


@pytest.mark.parametrize("value", ["bad", "2026-09-08", None, 1])
def test_invalid_timestamp(value):
    with pytest.raises(ValueError):
        runtime.utc_timestamp(value)


@pytest.mark.parametrize("change", [{"dq_status": "failed"}, {"dq_status": None},
                                   {"dag_id": "other"}, {"run_id": "other"}, {"receipt": None},
                                   {"receipt": {"status": "written", "source_manifest_id": "other"}}])
def test_wrong_upstream_dq_blocks_before_catalog(monkeypatch, change):
    load = Mock()
    catalog = Mock()
    monkeypatch.setattr(runtime, "load_events", load)
    with pytest.raises(ValueError):
        runtime.execute_load(CONFIG, ROOT, "events-run", "regular", reference(),
                             {**checked(), **change}, catalog=catalog)
    load.assert_not_called()
    catalog.table_exists.assert_not_called()


@pytest.mark.parametrize("mode", ["regular", "manual"])
def test_modes_use_same_loader_and_service_preflight(monkeypatch, mode):
    catalog = Mock()
    catalog.name = "iceberg"
    load = Mock(return_value={"rows_written": 10, "snapshot_id": 123})
    monkeypatch.setattr(runtime, "load_events", load)
    query = Mock()
    now = datetime(2026, 9, 9, 3, 10, tzinfo=timezone.utc)
    result = runtime.execute_load(CONFIG, ROOT, "events-run", mode, reference(), checked(),
                                  catalog=catalog, query_records=query, now=now)
    assert catalog.load_table.call_count == 2
    assert load.call_args.kwargs["calendar_receipt"] == checked()["receipt"]
    assert load.call_args.kwargs["ingested_at"] == now
    assert load.call_args.kwargs["query_records"] is query
    assert result["upstream_dq"]["run_id"] == reference()["run_id"]


def test_missing_service_table_blocks(monkeypatch):
    catalog = Mock()
    catalog.name = "iceberg"
    catalog.table_exists.return_value = False
    load = Mock()
    monkeypatch.setattr(runtime, "load_events", load)
    with pytest.raises(ValueError, match="служебной"):
        runtime.execute_load(CONFIG, ROOT, "events-run", "regular", reference(), checked(), catalog=catalog)
    load.assert_not_called()


def test_changed_interval_blocks():
    start = runtime.utc_timestamp("2026-09-08T03:10:00Z")
    with pytest.raises(ValueError):
        runtime.scheduled_calendar_reference(CONFIG, CALENDAR, start, start + timedelta(hours=23))
