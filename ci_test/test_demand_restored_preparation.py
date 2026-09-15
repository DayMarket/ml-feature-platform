"""Точная копия E3 без внешних подключений и общие helpers restored-тестов."""

from datetime import date, datetime, timezone
from decimal import Decimal
from importlib import import_module
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PATHS = {
    "restored": "layers/silver/sku_id_estimate_kind/demand_restored_daily/v1",
}
DAY = date(2026, 9, 6)
CAPTURE = datetime(2026, 9, 9, 4, tzinfo=timezone.utc)


def module(kind, name):
    return import_module(PATHS[kind].replace("/", ".") + ".job." + name)


def config(kind):
    return yaml.safe_load((ROOT / PATHS[kind] / "config.yaml").read_text())


def schema(kind):
    types = {"DATE": pa.date32(), "BIGINT": pa.int64(), "TINYINT": pa.int8(), "INT": pa.int32(),
             "DOUBLE": pa.float64(), "STRING": pa.string(), "TIMESTAMP": pa.timestamp("us"),
             "DECIMAL(38,0)": pa.decimal128(38, 0)}
    ddl = (ROOT / PATHS[kind] / "migrations/create_table.sql").read_text()
    fields = re.findall(r"^    (\w+) (DATE|BIGINT|TINYINT|INT|DOUBLE|STRING|TIMESTAMP|DECIMAL\(38,0\))( NOT NULL)? COMMENT", ddl, re.M)
    assert len(fields) == {"restored": 31}[kind]
    return pa.schema([pa.field(n, types[t], nullable=not required) for n, t, required in fields])


def utc_timestamps(value):
    return pa.schema([
        pa.field(
            field.name,
            pa.timestamp("us", "UTC") if pa.types.is_timestamp(field.type) else field.type,
            nullable=field.nullable,
        )
        for field in value
    ])


def test_restored_accepts_iceberg_utc_timestamps():
    target = utc_timestamps(schema("restored"))
    result = module("restored", "preparation").prepare_batch(
        raw("restored"),
        target,
        selected=selected(),
        run=run(),
        manifest="capture-1",
        version="v1",
        ingested_at=CAPTURE,
    )
    assert result.schema == target


def raw(kind, changes=None):
    prep, target = module(kind, "preparation"), schema(kind)
    columns = [
        n for n in target.names if n not in {*prep.LINEAGE, "source_manifest_id", "source_contract_version", "ingested_at"}
    ]
    values = {}
    for name in columns:
        dtype = target.field(name).type
        if name == "date":
            value = DAY
        elif name == "prediction_date":
            value = date(2026, 9, 8)
        elif name == "settled_at":
            value = None
        elif name == "source_updated_at":
            value = CAPTURE
        elif name == "sku_id":
            value = 1
        elif name == "seller_id":
            value = 7
        elif name == "seller_key":
            value = "seller:7"
        elif name == "run_id":
            value = "e3-selected"
        elif name == "estimate_kind":
            value = "provisional"
        elif name == "quality_status":
            value = "ok"
        elif name == "unavailable_reason":
            value = ""
        elif pa.types.is_string(dtype):
            value = "UZS" if name == "currency_code" else "v1"
        elif pa.types.is_decimal(dtype):
            value = Decimal("10")
        elif name.endswith("_usd"):
            value = 1.0
        elif pa.types.is_floating(dtype):
            value = 1.0
        else:
            value = 1
        values[name] = value
    values.update(changes or {})
    return pa.Table.from_arrays([
        pa.array([values[n]], type=pa.timestamp("us", "UTC") if n == "source_updated_at" else target.field(n).type)
        for n in columns
    ], names=columns)


def selected():
    return module("restored", "query").selection(run_id="e3-selected", prediction_date=date(2026, 9, 8),
                                               start=DAY, end=date(2026, 9, 7))


def run():
    return {"run_id": "e3-selected", "prediction_date": date(2026, 9, 8), "state_version": 3,
            "stage": "e3", "status": "validated", "model_version": "m", "code_version": "sha",
            "catalog_version": "cat", "input_manifest": '{"inputs":1}', "output_manifest": '{"outputs":1}',
            "finished_at": CAPTURE, "published_at": None}


def prepare(kind, source=None, **overrides):
    args = {"manifest": "capture-1", "version": "v1", "ingested_at": CAPTURE}
    args.update({"selected": selected(), "run": run()})
    args.update(overrides)
    return module(kind, "preparation").prepare_batch(
        raw(kind) if source is None else source, schema(kind), **args)


@pytest.mark.parametrize("kind", list(PATHS))
def test_parquet_round_trip_and_required_schema(kind, tmp_path):
    result = prepare(kind)
    target = tmp_path / f"{kind}.parquet"
    pq.write_table(result, target)
    assert pq.read_table(target).equals(result)
    module(kind, "preparation").validate_schema(schema(kind))


@pytest.mark.parametrize("status", ["running", "written", "failed", "success"])
def test_e3_only_validated_or_published_run(status):
    with pytest.raises(ValueError, match="проверен"):
        prepare("restored", run=run() | {"status": status})


def test_e3_unavailable_nulls_are_not_discarded():
    changes = {"quality_status": "unavailable", "unavailable_reason": "no_rate",
               "lost_units": None, "demand_units": None, "potential_units": None}
    result = prepare("restored", raw("restored", changes)).to_pylist()[0]
    assert result["quality_status"] == "unavailable" and result["lost_units"] is None


@pytest.mark.parametrize("field,value", [("run_id", "other"), ("stage", "e2"), ("state_version", 0),
                                       ("output_manifest", "{}"), ("input_manifest", "[]")])
def test_e3_bad_passport_blocks(field, value):
    with pytest.raises(ValueError):
        prepare("restored", run=run() | {field: value})


@pytest.mark.parametrize("kind", list(PATHS))
def test_duplicate_source_rows_block(kind):
    source = raw(kind)
    with pytest.raises(ValueError, match="порядок|дубликат"):
        prepare(kind, pa.concat_tables([source, source]))


def test_e3_query_uses_native_params_and_never_latest_or_quality_filter():
    sql = module("restored", "query").source_query(config("restored"))
    assert "FINAL" in sql and "PREWHERE prediction_date = %(prediction_date)s AND run_id = %(run_id)s" in sql
    assert "event_date < %(end)s" in sql and "quality_status =" not in sql
    assert "latest" not in sql and "argMax" not in sql
    assert config("restored")["dag"]["schedule"] is None


@pytest.mark.parametrize("run_id", ["latest", "current", "", None])
def test_e3_selector_requires_exact_run(run_id):
    with pytest.raises(ValueError):
        module("restored", "query").selection(
            run_id=run_id, prediction_date=date(2026, 9, 8), start=DAY, end=date(2026, 9, 7))


def wire(source, kind):
    columns = []
    rows = source.to_pylist()
    for field in source.schema:
        if pa.types.is_date(field.type):
            dtype = "Date"
        elif pa.types.is_integer(field.type):
            dtype = "Int64"
        elif pa.types.is_decimal(field.type):
            dtype = "Decimal(38, 0)"
        elif pa.types.is_floating(field.type):
            dtype = "Float64"
        elif pa.types.is_timestamp(field.type):
            dtype = "DateTime64(6, 'UTC')"
        else:
            dtype = "String"
        if source[field.name].null_count:
            dtype = f"Nullable({dtype})"
        columns.append((field.name, dtype))
    return [[row[name] for name, _ in columns] for row in rows], columns
