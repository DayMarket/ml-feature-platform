"""Проверить загрузку событий на настоящем локальном Iceberg/SQLite."""

from copy import deepcopy
from datetime import date, datetime, timezone
from importlib import import_module
import json
from pathlib import Path
import re
import sys
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml

pytest.importorskip("pyiceberg")
pytest.importorskip("sqlalchemy")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.transforms import MonthTransform

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
extraction = import_module("layers.silver.event_code.demand_event_calendar.v1.job.extraction")
writer = import_module("layers.silver.event_code.demand_event_calendar.v1.job.writer")
cal_prep = import_module("layers.silver.date.demand_calendar.v1.job.preparation")
cal_writer = import_module("layers.silver.date.demand_calendar.v1.job.writer")


def entity(relative):
    path = ROOT / relative
    config = yaml.safe_load((path / "config.yaml").read_text())
    kinds = {"DATE": pa.date32(), "INT": pa.int32(), "STRING": pa.large_string(),
             "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us")}
    fields = re.findall(r"^    (\w+) (\w+)( NOT NULL)? COMMENT ",
                        (path / "migrations/create_table.sql").read_text(), re.M)
    return config, pa.schema([pa.field(n, kinds[k], nullable=not bool(required)) for n, k, required in fields])


@pytest.fixture
def env(tmp_path):
    config, schema = entity("layers/silver/event_code/demand_event_calendar/v1")
    cal_config, cal_schema = entity("layers/silver/date/demand_calendar/v1")
    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("silver")
    for cfg, sch in ((config, schema), (cal_config, cal_schema)):
        table = catalog.create_table(extraction.target_ref(cfg, catalog.name), schema=sch)
        with table.update_spec() as update:
            update.add_field("date", MonthTransform(), "date_month")
    cal_table = catalog.load_table(extraction.target_ref(cal_config, catalog.name))
    source = pa.Table.from_pylist([{**dict.fromkeys(cal_prep.SOURCE_FIELDS), "dt": day,
                                   "is_public_holiday": 1, "holiday_name": "holiday"}
                                  for day in (date(2026, 10, 1), date(2026, 11, 11), date(2026, 11, 12))])
    data = cal_prep.prepare_calendar(source, cal_table.schema().as_arrow(), cal_config,
                                     source_manifest_id="calendar-run", ingested_at=datetime(2026, 9, 8, tzinfo=timezone.utc))
    receipt = cal_writer.write_prepared(cal_config, catalog, data)
    yield config, catalog, receipt
    catalog.engine.dispose()


def query_result(title="Sale", finish=None):
    return ([(135, title, "CANCELED", "BIG_SALE", datetime(2026, 11, 10, 19),
              finish or datetime(2026, 11, 12, 19), None, None, None)],
            [("id", "Nullable(Int64)"), *[(n, "Nullable(String)") for n in extraction.PROMO_TEXT],
             *[(n, "Nullable(DateTime64(6, 'UTC'))") for n in extraction.PROMO_TIMES]])


def prepared(env, query=None, receipt=None):
    config, catalog, calendar_receipt = env
    return extraction.extract_prepared(config, catalog, ROOT, calendar_receipt=receipt or calendar_receipt,
                                        source_manifest_id="events-run", ingested_at=datetime(2026, 9, 8, 1, tzinfo=timezone.utc),
                                        query_records=query or Mock(return_value=query_result()))


def test_full_load_and_retry(env):
    config, catalog, receipt = env
    for _ in range(2):
        result = writer.load_events(config, catalog, ROOT, calendar_receipt=receipt,
                                     source_manifest_id="events-run", ingested_at=datetime(2026, 9, 8, 1, tzinfo=timezone.utc),
                                     query_records=Mock(return_value=query_result()))
        assert result["status"] == "written" and result["rows_written"] == 5
        assert result["coverage_report"]["calendar_source"]["receipt"] == receipt
        assert json.loads(json.dumps(result)) == result
    table = catalog.load_table(extraction.target_ref(config, catalog.name))
    assert table.scan().to_arrow().num_rows == 5 and set(table.refs()) == {"main"}


def test_correction_replaces_old_title_and_removed_date(env):
    config, catalog, _ = env
    writer.write_prepared(config, catalog, *prepared(env))
    changed = prepared(env, Mock(return_value=query_result(title="Corrected", finish=datetime(2026, 11, 11, 19))))
    result = writer.write_prepared(config, catalog, *changed)
    assert result["rows_written"] == 4
    rows = catalog.load_table(extraction.target_ref(config, catalog.name)).scan().to_arrow().to_pylist()
    assert [r["event_name"] for r in rows if r["source_kind"] == "marketing_sale"] == ["Corrected"]


@pytest.mark.parametrize("field,value", [("snapshot_id", 123), ("snapshot_id", True),
                                       ("table_uuid", "wrong"), ("rows_written", 1),
                                       ("source_manifest_id", "wrong"), ("date_min", "2020-01-01"),
                                       ("ingested_at", "2026-09-08T02:00:00+00:00"),
                                       ("status", "failed")])
def test_wrong_calendar_receipt_blocks_before_ch(env, field, value):
    receipt = {**env[2], field: value}
    query = Mock(return_value=query_result())
    with pytest.raises(ValueError):
        prepared(env, query=query, receipt=receipt)
    query.assert_not_called()


def test_read_uses_requested_old_snapshot_not_current(env):
    _, catalog, receipt = env
    cfg, _ = entity("layers/silver/date/demand_calendar/v1")
    table = catalog.load_table(extraction.target_ref(cfg, catalog.name))
    old = table.scan().to_arrow()
    table.overwrite(old.slice(0, 1))
    data, report = prepared(env)
    assert data.num_rows == 5
    assert report["calendar_source"]["receipt"]["snapshot_id"] == receipt["snapshot_id"]


def test_empty_source_preserves_previous_snapshot(env):
    config, catalog, receipt = env
    writer.write_prepared(config, catalog, *prepared(env))
    table = catalog.load_table(extraction.target_ref(config, catalog.name))
    previous = table.current_snapshot().snapshot_id
    with pytest.raises(ValueError, match="Пустой реестр"):
        writer.load_events(config, catalog, ROOT, calendar_receipt=receipt,
                             source_manifest_id="empty", ingested_at=datetime.now(timezone.utc),
                             query_records=Mock(return_value=([], query_result()[1])))
    table.refresh()
    assert table.current_snapshot().snapshot_id == previous


@pytest.mark.parametrize("kind", ["empty", "duplicate", "wrong_manifest", "wrong_coverage",
                                  "bad_calendar_id", "invalid_event_code", "outside_interval", "empty_source"])
def test_bad_batch_does_not_commit(env, kind):
    config, catalog, _ = env
    data, report = prepared(env)
    writer.write_prepared(config, catalog, data, report)
    table = catalog.load_table(extraction.target_ref(config, catalog.name))
    previous = table.current_snapshot().snapshot_id
    report = deepcopy(report)
    rows = data.to_pylist()
    if kind == "empty":
        data = data.slice(0, 0)
    elif kind == "duplicate":
        data = pa.concat_tables([data, data])
        report["output_rows"] = data.num_rows
    elif kind == "wrong_manifest":
        report["source_manifest_id"] = "other"
    elif kind == "wrong_coverage":
        report["promo_coverage"][0]["included_days"] += 1
    elif kind == "empty_source":
        report["promo_source_rows"] = 0
    else:
        index = next(i for i, r in enumerate(rows) if r["source_kind"] == "marketing_sale")
        field, value = {"bad_calendar_id": ("calendar_id", "uz_official"),
                        "invalid_event_code": ("event_code", "changed-title"),
                        "outside_interval": ("date", date(2026, 11, 15))}[kind]
        rows[index][field] = value
        data = pa.Table.from_pylist(rows, schema=data.schema)
    with pytest.raises(ValueError):
        writer.write_prepared(config, catalog, data, report)
    table.refresh()
    assert table.current_snapshot().snapshot_id == previous


def test_concurrent_change_during_readback_is_not_ready(env, monkeypatch):
    config, catalog, _ = env
    data, report = prepared(env)
    table = catalog.load_table(extraction.target_ref(config, catalog.name))
    original_scan = table.scan

    def scan(*args, **kwargs):
        assert type(kwargs["snapshot_id"]) is int
        table.append(data.slice(0, 1))
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(table, "scan", scan)
    client = Mock(wraps=catalog)
    client.name = catalog.name
    client.load_table.return_value = table
    with pytest.raises(RuntimeError, match="изменился во время read-back"):
        writer.write_prepared(config, client, data, report)
    actual = catalog.load_table(extraction.target_ref(config, catalog.name))
    assert actual.scan().to_arrow().num_rows == 6
