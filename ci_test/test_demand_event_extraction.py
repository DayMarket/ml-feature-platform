"""Проверить метаданные реестра и preflight без источников данных."""

from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
import sys
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
ext = import_module("layers.silver.event_code.demand_event_calendar.v1.job.extraction")


@pytest.fixture
def config():
    return yaml.safe_load((ROOT / "layers/silver/event_code/demand_event_calendar/v1/config.yaml").read_text())


def records():
    return ([(135, " Title ", "CANCELED", "TODAY_DEALS", datetime(2026, 11, 10, 19),
              datetime(2026, 11, 11, 19), None, None, None)],
            [("id", "Nullable(Int64)"), *[(name, "Nullable(String)") for name in ext.PROMO_TEXT],
             *[(name, "Nullable(DateTime64(6, 'UTC'))") for name in ext.PROMO_TIMES]])


def test_exact_sql_has_no_status_or_date_filters(config):
    sql = ext.source_sql(config)
    assert "FROM silver.b2b_marketing_sale ORDER BY id" in sql
    assert all(word not in sql for word in ("WHERE", "LIMIT", "DISTINCT", "FINAL"))
    assert all(f"toTimeZone({name}, 'UTC') AS {name}" in sql for name in ext.PROMO_TIMES)


def test_native_metadata_preserves_null_and_declared_utc():
    rows, types = records()
    result = ext.source_from_records(rows, types).to_pylist()[0]
    assert result["started_at"] == datetime(2026, 11, 10, 19, tzinfo=timezone.utc)
    assert result["announced_at"] is None
    assert result["status"] == "CANCELED" and result["title"] == " Title "
    assert isinstance(result["id"], int)


@pytest.mark.parametrize("kind", ["DateTime('UTC')", "DateTime64(3, 'UTC')",
                                 "LowCardinality(Nullable(DateTime64(6, 'UTC')))"])
def test_known_timestamp_metadata(kind):
    rows, types = records()
    types[4] = ("started_at", kind)
    assert ext.source_from_records(rows, types)["started_at"].type.tz == "UTC"


@pytest.mark.parametrize("column,kind,value", [
    ("id", "Nullable(Float64)", 135.), ("id", "Nullable(Int64)", 135.),
    ("id", "Nullable(Int64)", True), ("status", "Nullable(String)", 1),
    ("status", "String", None), ("started_at", "DateTime", datetime(2026, 11, 11)),
    ("started_at", "DateTime('Asia/Tashkent')", datetime(2026, 11, 11)),
    ("started_at", "DateTime64(9, 'UTC')", datetime(2026, 11, 11)),
    ("started_at", "DateTime('UTC')", "2026-11-11"),
    ("started_at", "DateTime('UTC')", None),
])
def test_bad_source_types_fail(column, kind, value):
    rows, types = records()
    index = next(i for i, pair in enumerate(types) if pair[0] == column)
    row = list(rows[0])
    row[index] = value
    types[index] = (column, kind)
    with pytest.raises(ValueError):
        ext.source_from_records([row], types)


def test_missing_duplicate_and_short_metadata_fail():
    rows, types = records()
    for invalid_rows, invalid_types in ((rows, types[:-1]), (rows, types + [types[0]]),
                                        ([rows[0][:-1]], types), (rows, types[:-1] + [types[0]])):
        with pytest.raises(ValueError):
            ext.source_from_records(invalid_rows, invalid_types)


def test_source_order_does_not_change_projection():
    rows, types = records()
    assert ext.source_from_records([tuple(reversed(rows[0]))], list(reversed(types))).equals(
        ext.source_from_records(rows, types))


@pytest.mark.parametrize("part,value", [("catalog", "wrong"), ("schema", "iceberg.silver"),
                                       ("name", "silver.events"), ("name", ""), ("name", "a; DROP")])
def test_identifiers_are_strict(config, part, value):
    config["table"][part] = value
    with pytest.raises(ValueError):
        ext.target_ref(config, "iceberg")


def test_missing_output_blocks_before_source(config):
    catalog, query = Mock(), Mock()
    catalog.name = "iceberg"
    catalog.table_exists.return_value = False
    with pytest.raises(ValueError, match="миграции"):
        ext.extract_prepared(config, catalog, ROOT, calendar_receipt={}, source_manifest_id="run",
                             ingested_at=datetime.now(timezone.utc), query_records=query)
    catalog.table_exists.assert_called_once_with(("silver", "feature_platform_demand_event_calendar"))
    query.assert_not_called()
