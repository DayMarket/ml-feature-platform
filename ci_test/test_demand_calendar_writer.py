"""Проверить обычную запись календаря на настоящем локальном Iceberg/SQLite."""

from datetime import date, datetime, timezone
import json
from pathlib import Path
import re
import sys
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml

pytest.importorskip("pyiceberg", reason="Integration: нужен PyIceberg в отдельной тестовой среде")
pytest.importorskip("sqlalchemy")
from pyiceberg.catalog.sql import SqlCatalog

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from layers.silver.date.demand_calendar.v1.job import preparation, writer  # noqa: E402

ENTITY = ROOT / "layers/silver/date/demand_calendar/v1"


@pytest.fixture
def env(tmp_path):
    config = yaml.safe_load((ENTITY / "config.yaml").read_text())
    kinds = {"DATE": pa.date32(), "INT": pa.int32(), "STRING": pa.large_string(),
             "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us")}
    fields = re.findall(r"^    (\w+) (\w+)( NOT NULL)? COMMENT ",
                        (ENTITY / "migrations/create_table.sql").read_text(), re.M)
    schema = pa.schema([pa.field(n, kinds[k], nullable=not bool(required))
                        for n, k, required in fields])
    catalog = SqlCatalog("iceberg", uri=f"sqlite:///{tmp_path}/catalog.db",
                         warehouse=(tmp_path / "warehouse").as_uri())
    catalog.create_namespace("silver")
    identifier = preparation.target_ref(config, catalog.name)
    from pyiceberg.transforms import MonthTransform
    table = catalog.create_table(identifier, schema=schema)
    # ID назначает Iceberg, а не Arrow: добавляем months(date) по имени.
    with table.update_spec() as update:
        update.add_field("date", MonthTransform(), "date_month")
    yield config, catalog, table.schema().as_arrow()
    catalog.engine.dispose()


def batch(config, schema, days=(date(2026, 8, 31), date(2026, 9, 1)), *, name="Original"):
    rows = []
    for day in days:
        row = {key: None for key in preparation.SOURCE_FIELDS}
        row.update(dt=day, month=day.month, day_of_week_iso=day.isoweekday(), holiday_name=name)
        rows.append(row)
    return preparation.prepare_calendar(
        pa.Table.from_pylist(rows), schema, config,
        source_manifest_id="capture-1", ingested_at=datetime(2026, 9, 8, tzinfo=timezone.utc))


def test_write_retry_has_no_duplicates_or_tags(env):
    config, catalog, schema = env
    data = batch(config, schema)
    first = writer.write_prepared(config, catalog, data)
    second = writer.write_prepared(config, catalog, data)
    for result in (first, second):
        assert result["status"] == "written"
        assert result["calendar_id"] == "uz_official"
        assert result["rows_written"] == 2
        assert result["source_manifest_id"] == "capture-1"
        assert result["ingested_at"] == "2026-09-08T00:00:00.000000+00:00"
        assert result["date_min"] == "2026-08-31"
        assert result["date_max"] == "2026-09-01"
        assert isinstance(result["snapshot_id"], int)
        assert json.loads(json.dumps(result)) == result
    table = catalog.load_table(preparation.target_ref(config, catalog.name))
    assert table.current_snapshot().snapshot_id == second["snapshot_id"]
    assert table.scan().to_arrow().num_rows == 2
    assert set(table.refs()) == {"main"}
    assert table.scan().to_arrow()["is_working_day"].null_count == 2


def test_correction_replaces_whole_date_keyed_calendar(env):
    config, catalog, schema = env
    original = batch(config, schema)
    writer.write_prepared(config, catalog, original)
    table = catalog.load_table(preparation.target_ref(config, catalog.name))
    other = original.set_column(original.schema.get_field_index("calendar_id"),
                                original.schema.field("calendar_id"),
                                pa.array(["another_calendar"] * 2, type=schema.field("calendar_id").type))
    table.append(other)
    replacement = batch(config, schema, (date(2026, 9, 1),), name="Corrected")
    writer.write_prepared(config, catalog, replacement)
    rows = catalog.load_table(preparation.target_ref(config, catalog.name)).scan().to_arrow().to_pylist()
    assert len(rows) == 1
    own = [r for r in rows if r["calendar_id"] == "uz_official"]
    assert len(own) == 1 and own[0]["holiday_name"] == "Corrected"
    assert own[0]["date"] == date(2026, 9, 1)


@pytest.mark.parametrize("kind", ["empty", "duplicate", "other", "null_key", "bad_month",
                                  "bad_weekday", "blank_manifest", "mixed_manifest"])
def test_bad_batch_leaves_previous_data_unchanged(env, kind):
    config, catalog, schema = env
    data = batch(config, schema)
    writer.write_prepared(config, catalog, data)
    table = catalog.load_table(preparation.target_ref(config, catalog.name))
    before = table.current_snapshot().snapshot_id
    if kind == "empty":
        invalid = data.slice(0, 0)
    elif kind == "duplicate":
        invalid = pa.concat_tables([data, data])
    else:
        column, values = {
            "other": ("calendar_id", ["other", "other"]),
            "null_key": ("date", [None, date(2026, 9, 1)]),
            "bad_month": ("month", [13, 9]),
            "bad_weekday": ("day_of_week_iso", [0, 2]),
            "blank_manifest": ("source_manifest_id", [" ", " "]),
            "mixed_manifest": ("source_manifest_id", ["a", "b"]),
        }[kind]
        invalid = data.set_column(schema.get_field_index(column), schema.field(column),
                                  pa.array(values, type=schema.field(column).type))
    with pytest.raises(ValueError):
        writer.write_prepared(config, catalog, invalid)
    table.refresh()
    assert table.current_snapshot().snapshot_id == before
    assert table.scan().to_arrow().num_rows == 2


def test_missing_table_is_not_created(env):
    config, catalog, schema = env
    data = batch(config, schema)
    config["table"]["name"] = "feature_platform_absent"
    with pytest.raises(ValueError, match="миграцию"):
        writer.write_prepared(config, catalog, data)
    assert not catalog.table_exists(("silver", "feature_platform_absent"))


def test_schema_mismatch_does_not_write(env):
    config, catalog, schema = env
    with pytest.raises(ValueError, match="схеме"):
        writer.write_prepared(config, catalog, batch(config, schema).drop(["year"]))


def test_postwrite_mismatch_is_failure_not_ready(env):
    config, _, schema = env
    data = batch(config, schema)
    table = Mock()
    table.schema.return_value.as_arrow.return_value = schema
    table.scan.return_value.to_arrow.return_value = data.slice(0, 1)
    catalog = Mock()
    catalog.name = "iceberg"
    catalog.load_table.return_value = table
    with pytest.raises(RuntimeError, match="не совпал"):
        writer.write_prepared(config, catalog, data)
    table.overwrite.assert_called_once()


@pytest.mark.parametrize("phase", ["during_scan", "after_scan"])
def test_concurrent_commit_is_not_reported_as_checked_current(env, monkeypatch, phase):
    config, catalog, schema = env
    data = batch(config, schema)
    table = catalog.load_table(preparation.target_ref(config, catalog.name))
    other = data.set_column(
        schema.get_field_index("calendar_id"), schema.field("calendar_id"),
        pa.array(["another_calendar"] * 2, type=schema.field("calendar_id").type),
    )
    original_scan = table.scan
    original_refresh = table.refresh
    scans = []

    def scan(*args, **kwargs):
        scans.append(kwargs)
        if phase == "during_scan":
            table.append(other)
        return original_scan(*args, **kwargs)

    def refresh():
        if phase == "after_scan":
            table.append(other)
        return original_refresh()

    monkeypatch.setattr(table, "scan", scan)
    monkeypatch.setattr(table, "refresh", refresh)
    client = Mock(wraps=catalog)
    client.name = catalog.name
    client.load_table.return_value = table
    with pytest.raises(RuntimeError, match="изменился во время read-back"):
        writer.write_prepared(config, client, data)
    assert len(scans) == 1 and isinstance(scans[0]["snapshot_id"], int)
    # Ошибка не откатывает ни наш commit, ни конкурентную запись.
    actual = catalog.load_table(preparation.target_ref(config, catalog.name))
    assert actual.scan().to_arrow().num_rows == 4
    assert set(actual.refs()) == {"main"}


def test_receipt_preserves_utc_microseconds_and_full_range(env):
    config, catalog, schema = env
    data = batch(config, schema, (date(2021, 1, 1), date(2027, 12, 31)))
    moment = datetime(2026, 9, 8, 4, 30, 1, 123456)
    data = data.set_column(
        schema.get_field_index("ingested_at"), schema.field("ingested_at"),
        pa.array([moment] * 2, type=schema.field("ingested_at").type),
    )
    result = writer.write_prepared(config, catalog, data)
    assert result["ingested_at"] == "2026-09-08T04:30:01.123456+00:00"
    assert result["date_min"] == "2021-01-01"
    assert result["date_max"] == "2027-12-31"
