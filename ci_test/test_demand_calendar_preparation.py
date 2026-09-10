"""Проверить observed calendar до записи, без Airflow/CH/Iceberg сервисов."""

from datetime import date, datetime, timezone
import importlib.util
from pathlib import Path
import re
from unittest.mock import Mock

import pyarrow as pa
import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/silver/date/demand_calendar/v1"
spec = importlib.util.spec_from_file_location("calendar_preparation", ENTITY / "job/preparation.py")
prep = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prep)


@pytest.fixture
def config():
    return yaml.safe_load((ENTITY / "config.yaml").read_text())


@pytest.fixture
def schema():
    types = {"DATE": pa.date32(), "INT": pa.int32(), "STRING": pa.string(),
             "BOOLEAN": pa.bool_(), "TIMESTAMP": pa.timestamp("us")}
    sql = (ENTITY / "migrations/create_table.sql").read_text()
    fields = re.findall(r"^    (\w+) (\w+)( NOT NULL)? COMMENT ", sql, re.M)
    return pa.schema([pa.field(name, types[kind], nullable=not bool(required))
                      for name, kind, required in fields])


@pytest.fixture
def source():
    # Пропущенный 02.09 нельзя достроить, пустое название не превращается в NULL.
    rows = [{name: None for name in prep.SOURCE_FIELDS} for _ in range(2)]
    rows[0].update(dt=date(2026, 9, 1), year=2026, month=9, day=1,
                   holiday_name="", is_working_day=0, is_public_holiday=1)
    rows[1].update(dt=date(2026, 9, 3), holiday_name=None)
    return pa.Table.from_pylist(rows)


def prepare(source, schema, config, **kwargs):
    return prep.prepare_calendar(source, schema, config, source_manifest_id="capture-1",
                                 ingested_at=kwargs.get("ingested_at", datetime(2026, 9, 8, tzinfo=timezone.utc)))


def test_preserves_all_rows_nulls_empty_names_and_no_date_generation(source, schema, config, tmp_path):
    import pyarrow.parquet as pq

    result = prepare(source, schema, config)
    assert result.schema == schema
    assert result["date"].to_pylist() == [date(2026, 9, 1), date(2026, 9, 3)]
    assert result["calendar_id"].to_pylist() == ["uz_official"] * 2
    assert result["holiday_name"].to_pylist() == ["", None]
    assert result["is_working_day"].to_pylist() == [False, None]
    path = tmp_path / "calendar.parquet"
    pq.write_table(result, path)
    assert pq.read_table(path).equals(result)


@pytest.mark.parametrize("field,values", [
    ("is_working_day", [2, None]), ("is_public_holiday", [-1, 0]),
    ("is_weekend", ["false", "true"]), ("is_weekend", [0.0, 1.0]),
    ("year", [2026.0, None]), ("holiday_name", [42, None]),
    ("dt", ["2026-09-01", "2026-09-03"]),
    ("dt", [date(2026, 9, 1), None]),
    ("dt", [date(2026, 9, 1), date(2026, 9, 1)]),
])
def test_invalid_values_do_not_get_coerced(source, schema, config, field, values):
    altered = source.set_column(source.column_names.index(field), field, pa.array(values))
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        prepare(altered, schema, config)


def test_empty_source_fails(source, schema, config):
    with pytest.raises(ValueError, match="Пустой"):
        prepare(source.slice(0, 0), schema, config)


def test_extra_source_column_fails(source, schema, config):
    with pytest.raises(ValueError, match="15"):
        prepare(source.append_column("extra", pa.array([1, 2])), schema, config)


def test_naive_capture_time_fails(source, schema, config):
    with pytest.raises(ValueError, match="зону"):
        prepare(source, schema, config, ingested_at=datetime(2026, 9, 8))


@pytest.mark.parametrize("part,value", [("name", "silver.calendar"), ("name", ""),
                                       ("schema", "iceberg.silver"), ("catalog", "other")])
def test_malformed_identifier_fails(config, part, value):
    config["table"][part] = value
    with pytest.raises(ValueError):
        prep.target_ref(config, "iceberg")


def test_wrong_schema_blocks_before_source_read(schema, config):
    table = Mock()
    table.schema.return_value.as_arrow.return_value = schema.remove(0)
    catalog = Mock()
    catalog.name = "iceberg"
    catalog.load_table.return_value = table
    reader = Mock()
    with pytest.raises(ValueError, match="18"):
        prep.extract_prepared(config, catalog, source_manifest_id="capture-1",
                              ingested_at=datetime.now(timezone.utc), query_dataframe=reader)
    reader.assert_not_called()


def test_missing_table_blocks_before_source_read(config):
    catalog = Mock()
    catalog.name = "iceberg"
    catalog.table_exists.return_value = False
    reader = Mock()
    with pytest.raises(ValueError, match="миграции"):
        prep.extract_prepared(config, catalog, source_manifest_id="capture-1",
                              ingested_at=datetime.now(timezone.utc), query_dataframe=reader)
    reader.assert_not_called()


def test_extract_uses_two_part_identifier_without_writing(source, schema, config):
    table = Mock()
    table.schema.return_value.as_arrow.return_value = schema
    catalog = Mock()
    catalog.name = "iceberg"
    catalog.load_table.return_value = table
    # Arrow-backed pandas сохраняет nullable integer, без float coercion в fixture.
    import pandas as pd
    reader = Mock(return_value=source.to_pandas(types_mapper=pd.ArrowDtype))
    result = prep.extract_prepared(config, catalog, source_manifest_id="capture-1",
                                  ingested_at=datetime.now(timezone.utc), query_dataframe=reader)
    catalog.load_table.assert_called_once_with(("silver", "feature_platform_demand_calendar"))
    assert result.num_rows == 2
    assert "WHERE" not in reader.call_args.args[0]
    assert "LIMIT" not in reader.call_args.args[0]
    table.overwrite.assert_not_called()
    table.append.assert_not_called()


def driver_result(source):
    metadata = []
    for name in prep.SOURCE_FIELDS:
        kind = ("Date" if name == "dt" else "Nullable(UInt8)" if name in prep.FLAGS
                else "Nullable(Int64)" if name in prep.NUMBERS else "Nullable(String)")
        metadata.append((name, kind))
    rows = [tuple(row[name] for name in prep.SOURCE_FIELDS) for row in source.to_pylist()]
    return rows, metadata


def test_native_driver_records_keep_date_integer_and_null(source, schema, config, tmp_path):
    import pyarrow.parquet as pq

    raw = prep.source_from_records(*driver_result(source))
    assert raw["dt"].type == pa.date32()
    assert raw["year"].type == pa.int64()
    assert raw["year"].to_pylist() == [2026, None]
    prepared = prepare(raw, schema, config)
    pq.write_table(prepared, tmp_path / "native.parquet")
    assert pq.read_table(tmp_path / "native.parquet").equals(prepared)
    assert prepared["holiday_name"].to_pylist() == ["", None]


@pytest.mark.parametrize("field,kind,value", [
    ("dt", "DateTime", datetime(2026, 9, 1)),
    ("dt", "Date", datetime(2026, 9, 1)),
    ("dt", "Date", None),
    ("year", "Nullable(Float64)", 2026.0),
    ("year", "Nullable(Int64)", 2026.0),
    ("year", "Nullable(Int64)", True),
    ("holiday_name", "Nullable(String)", 12),
    ("is_weekend", "Nullable(UInt8)", "0"),
])
def test_native_driver_does_not_coerce_schema_drift(source, field, kind, value):
    rows, metadata = driver_result(source)
    index = prep.SOURCE_FIELDS.index(field)
    metadata[index] = (field, kind)
    altered = list(rows[0])
    altered[index] = value
    rows[0] = tuple(altered)
    with pytest.raises(ValueError):
        prep.source_from_records(rows, metadata)


def test_native_driver_rejects_incomplete_response(source):
    rows, metadata = driver_result(source)
    with pytest.raises(ValueError, match="15"):
        prep.source_from_records(rows, metadata[:-1])
    with pytest.raises(ValueError, match="Число"):
        prep.source_from_records([rows[0][:-1]], metadata)


def test_native_driver_supports_bool_and_low_cardinality(source, schema, config):
    rows, metadata = driver_result(source)
    index = prep.SOURCE_FIELDS.index("is_working_day")
    metadata[index] = ("is_working_day", "Nullable(Bool)")
    values = list(rows[0])
    values[index] = False
    rows[0] = tuple(values)
    index = prep.SOURCE_FIELDS.index("holiday_name")
    metadata[index] = ("holiday_name", "LowCardinality(Nullable(String))")
    result = prepare(prep.source_from_records(rows, metadata), schema, config)
    assert result["is_working_day"].to_pylist() == [False, None]


def test_production_extract_disables_numpy_and_uses_native_rows(source, schema, config, monkeypatch):
    import sys
    from types import ModuleType
    from unittest.mock import MagicMock

    table = Mock()
    table.schema.return_value.as_arrow.return_value = schema
    catalog = Mock()
    catalog.name = "iceberg"
    catalog.load_table.return_value = table
    client = MagicMock()
    client.__enter__.return_value = client
    client.execute.return_value = driver_result(source)
    hook = Mock()
    hook.return_value.get_conn.return_value = client
    module_name = "airflow_commons.hooks.clickhouse_hook"
    fake = ModuleType(module_name)
    fake.ClickHouseHook = hook
    monkeypatch.setitem(sys.modules, module_name, fake)
    result = prep.extract_prepared(config, catalog, source_manifest_id="capture-native",
                                  ingested_at=datetime(2026, 9, 8, tzinfo=timezone.utc))
    hook.assert_called_once_with(clickhouse_conn_id="clickhouse_dwh_team_logistics", use_numpy=False)
    client.execute.assert_called_once_with(prep.source_sql(config), with_column_types=True)
    client.query_dataframe.assert_not_called()
    client.__exit__.assert_called_once()
    assert result["date"].type == pa.date32()
    assert result["year"].to_pylist() == [2026, None]
    table.overwrite.assert_not_called()
