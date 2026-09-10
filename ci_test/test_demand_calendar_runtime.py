"""Проверить ручной календарный запуск без Airflow, DWH и записи данных."""

from datetime import datetime, timezone
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from layers.silver.date.demand_calendar.v1.job import runtime  # noqa: E402

CONFIG = ROOT / "layers/silver/date/demand_calendar/v1/config.yaml"


@pytest.mark.parametrize("mode", ["manual", "regular"])
def test_modes_use_same_loader_and_preflight_service_tables(monkeypatch, mode):
    config = yaml.safe_load(CONFIG.read_text())
    catalog = Mock()
    catalog.name = "iceberg"
    load = Mock(return_value={"rows_written": 2191, "snapshot_id": 1})
    monkeypatch.setattr(runtime, "load_calendar", load)
    now = datetime(2026, 9, 8, tzinfo=timezone.utc)
    query = Mock()
    runtime.execute_load(config, ROOT, "run", mode, catalog=catalog, query_dataframe=query, now=now)
    assert catalog.load_table.call_count == 2
    assert load.call_args.kwargs["ingested_at"] == now
    assert load.call_args.kwargs["query_dataframe"] is query
    assert load.call_args.kwargs["source_manifest_id"] == "run"
    query.assert_not_called()


def test_missing_service_table_stops_before_source(monkeypatch):
    config = yaml.safe_load(CONFIG.read_text())
    catalog = Mock()
    catalog.name = "iceberg"
    catalog.table_exists.return_value = False
    load = Mock()
    monkeypatch.setattr(runtime, "load_calendar", load)
    with pytest.raises(ValueError, match="служебной"):
        runtime.execute_load(config, ROOT, "run", "manual", catalog=catalog)
    load.assert_not_called()


def test_invalid_mode_does_not_open_connections():
    with pytest.raises(ValueError):
        runtime.execute_load({}, ROOT, "run", "some_range")
