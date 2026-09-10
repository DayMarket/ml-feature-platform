"""Проверить нормализацию событий на локальных данных без сервисов и Airflow."""

from datetime import date, datetime, timezone
import importlib.util
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/silver/event_code/demand_event_calendar/v1"
spec = importlib.util.spec_from_file_location("event_preparation", ENTITY / "job/preparation.py")
prep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prep)


@pytest.fixture
def schema():
    kinds = {"DATE": pa.date32(), "STRING": pa.string(), "TIMESTAMP": pa.timestamp("us")}
    sql = (ENTITY / "migrations/create_table.sql").read_text()
    return pa.schema([pa.field(name, kinds[kind], nullable=not bool(required))
                      for name, kind, required in re.findall(
                          r"^    (\w+) (\w+)( NOT NULL)? COMMENT ", sql, re.M)])


@pytest.fixture
def calendar():
    return pa.table({
        "date": [date(2026, 11, day) for day in (10, 11, 12, 14)],
        "calendar_id": ["uz_official"] * 4,
        "is_public_holiday": [False, True, None, True],
        "holiday_name": ["Не праздник", "", None, None],
    })


def promos(start="2026-11-11T00:00:00+05:00", finish="2026-11-12T00:00:00+05:00", **updates):
    row = {"id": 135, "title": "11/11", "status": "CREATED", "type": "BIG_SALE",
           "started_at": datetime.fromisoformat(start), "finished_at": datetime.fromisoformat(finish),
           "announced_at": None, "created_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
           "updated_at": None}
    row.update(updates)
    return pa.Table.from_pylist([row])


def prepare(calendar, source, schema, **kwargs):
    return prep.prepare_events(calendar, source, schema,
                               source_manifest_id=kwargs.get("source_manifest_id", "capture-1"),
                               ingested_at=kwargs.get("ingested_at", datetime(2026, 9, 8, tzinfo=timezone.utc)))


@pytest.mark.parametrize("start,finish,days", [
    ("2026-11-11T00:00:00+05:00", "2026-11-12T00:00:00+05:00", [11]),
    ("2026-11-11T10:00:00+05:00", "2026-11-12T15:00:00+05:00", [11, 12]),
    ("2026-11-10T19:00:00Z", "2026-11-11T19:00:00Z", [11]),
    ("2026-11-10T18:59:59.999999Z", "2026-11-10T19:00:00Z", [10]),
    ("2026-11-11T19:00:00Z", "2026-11-11T19:00:00.000001Z", [12]),
])
def test_half_open_business_days(calendar, schema, start, finish, days):
    table, report = prepare(calendar, promos(start, finish), schema)
    rows = [row for row in table.to_pylist() if row["source_kind"] == "marketing_sale"]
    assert [row["date"].day for row in rows] == days
    assert report["promo_coverage"][0]["uncovered_days"] == 0


@pytest.mark.parametrize("status,kind", [("CREATED", "BIG_SALE"), ("CANCELED", "BIG_SALE"),
                                         ("CANCELED", "TODAY_DEALS"), ("NEW_STATUS", "NEW_TYPE"),
                                         (None, None)])
def test_raw_values_and_parquet_parity(calendar, schema, tmp_path, status, kind):
    table, report = prepare(calendar, promos(status=status, type=kind, title="  title  "), schema)
    rows = table.to_pylist()
    promo = next(row for row in rows if row["source_kind"] == "marketing_sale")
    assert promo["source_status"] == status and promo["source_type"] == kind
    assert promo["event_name"] == "  title  " and promo["calendar_id"] is None
    assert promo["event_code"] == "marketing_sale:135"
    assert promo["source_announced_at"] is None and promo["source_created_at"] is not None
    assert promo["source_started_at"] == datetime(2026, 11, 10, 19)
    holidays = [row for row in rows if row["source_kind"] == "calendar"]
    assert [row["event_name"] for row in holidays] == ["", None]
    assert report["calendar_unknown_holiday_dates"] == ["2026-11-12"]
    assert table.schema == schema
    pq.write_table(table, tmp_path / "events.parquet")
    assert pq.read_table(tmp_path / "events.parquet").equals(table)


def test_coverage_gaps_clipping_and_outside_are_reported(calendar, schema):
    source = promos("2026-11-09T00:00:00Z", "2026-11-16T00:00:00Z")
    table, report = prepare(calendar, source, schema)
    assert set(table["date"].to_pylist()) == set(calendar["date"].to_pylist())
    assert report["calendar_missing_dates"] == 1
    assert report["promo_coverage"] == [{"source_event_id": "135", "interval_days": 8,
                                          "included_days": 4, "uncovered_days": 4, "coverage": "partial"}]
    table, report = prepare(calendar, promos("2027-01-01T00:00:00Z", "2027-01-02T00:00:00Z"), schema)
    assert report["promo_coverage"][0]["coverage"] == "outside"
    assert all(kind == "calendar" for kind in table["source_kind"].to_pylist())


@pytest.mark.parametrize("change", [
    {"id": None}, {"id": 0}, {"id": -1}, {"id": 1.5}, {"id": "135"},
    {"started_at": None}, {"finished_at": None},
    {"started_at": datetime(2026, 11, 11)},
    {"announced_at": datetime(2026, 10, 1)},
    {"started_at": datetime(2026, 11, 13, tzinfo=timezone.utc)},
    {"finished_at": datetime.fromisoformat("2026-11-11T00:00:00+05:00")},
    {"status": 1},
])
def test_bad_promo_blocks_without_coercion(calendar, schema, change):
    with pytest.raises(ValueError):
        prepare(calendar, promos(**change), schema)


def test_duplicates_and_missing_columns_block(calendar, schema):
    source = promos()
    for source in (pa.concat_tables([source, source]), source.drop(["status"]),
                   source.append_column("extra", pa.array([1]))):
        with pytest.raises(ValueError):
            prepare(calendar, source, schema)
    with pytest.raises(ValueError, match="Повтор даты"):
        prepare(pa.concat_tables([calendar, calendar]), promos(), schema)


@pytest.mark.parametrize("field,values", [("date", ["2026-11-11"] * 4),
                                         ("calendar_id", [None] * 4),
                                         ("calendar_id", ["other"] * 4),
                                         ("is_public_holiday", [0, 1, None, 1]),
                                         ("holiday_name", [1, 2, 3, 4])])
def test_bad_calendar_types_and_identity_block(calendar, schema, field, values):
    calendar = calendar.set_column(calendar.column_names.index(field), field, pa.array(values))
    with pytest.raises(ValueError):
        prepare(calendar, promos(), schema)


def test_empty_calendar_blocks_but_empty_promo_remains_explicit(calendar, schema):
    with pytest.raises(ValueError, match="Пустой календарь"):
        prepare(calendar.slice(0, 0), promos(), schema)
    table, report = prepare(calendar, promos().slice(0, 0), schema)
    assert report["promo_source_rows"] == 0 and table.num_rows == 2
    assert "ready" not in report


def test_invalid_lineage_and_schema_block(calendar, schema):
    for kwargs in ({"source_manifest_id": ""}, {"ingested_at": datetime(2026, 9, 8)}):
        with pytest.raises(ValueError):
            prepare(calendar, promos(), schema, **kwargs)
    for bad in (schema.remove(0), schema.set(0, pa.field("date", pa.date32(), nullable=True)),
                schema.set(8, pa.field("source_started_at", pa.timestamp("ns")))):
        with pytest.raises(ValueError):
            prepare(calendar, promos(), bad)


def test_input_order_does_not_change_output_or_report(calendar, schema):
    source = pa.concat_tables([promos(), promos(id=136, title="second")])
    first, first_report = prepare(calendar, source, schema)
    second, second_report = prepare(calendar.take([3, 1, 2, 0]), source.take([1, 0]), schema)
    assert first.equals(second) and first_report == second_report


def test_utc_zoned_target_preserves_instants(calendar, schema):
    schema = pa.schema([pa.field(field.name, pa.timestamp("us", "UTC"), nullable=field.nullable)
                        if pa.types.is_timestamp(field.type) else field for field in schema])
    table, _ = prepare(calendar, promos(), schema)
    assert table.schema == schema
    assert table["ingested_at"][0].as_py().utcoffset().total_seconds() == 0


@pytest.mark.parametrize("days,start,finish", [
    ([date(2024, 2, 28), date(2024, 2, 29)], "2024-02-28T00:00:00+05:00", "2024-03-01T00:00:00+05:00"),
    ([date(2026, 12, 31), date(2027, 1, 1)], "2026-12-31T00:00:00+05:00", "2027-01-02T00:00:00+05:00"),
])
def test_leap_day_and_year_boundary(schema, days, start, finish):
    calendar = pa.table({"date": days, "calendar_id": ["uz_official"] * 2,
                         "is_public_holiday": [False] * 2, "holiday_name": [None] * 2})
    table, _ = prepare(calendar, promos(start, finish), schema)
    assert table["date"].to_pylist() == days


def test_empty_event_result_keeps_schema(calendar, schema):
    calendar = calendar.set_column(2, "is_public_holiday", pa.array([False] * 4))
    table, report = prepare(calendar, promos().slice(0, 0), schema)
    assert table.num_rows == report["output_rows"] == 0 and table.schema == schema


def test_submicrosecond_source_is_not_silently_truncated(calendar, schema):
    source = promos()
    source = source.set_column(4, "started_at", source["started_at"].cast(pa.timestamp("ns", "UTC")))
    with pytest.raises(ValueError, match="точностью"):
        prepare(calendar, source, schema)
