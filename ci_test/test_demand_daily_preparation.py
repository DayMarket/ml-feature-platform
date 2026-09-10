"""Типы raw/USD, seller grain и точная копия E3 без внешних подключений."""

from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from importlib import import_module
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PATHS = {
    "sales": "layers/silver/sku_id/demand_sales_daily/v1",
    "finance": "layers/silver/sku_id_seller_key/demand_finance_daily/v1",
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
    assert len(fields) == {"sales": 38, "finance": 60, "restored": 31}[kind]
    return pa.schema([pa.field(n, types[t], nullable=not required) for n, t, required in fields])


def fx():
    return {"date": DAY, "fx_rate_date": DAY, "fx_rate_uzs_per_usd": 10.0,
            "fx_rate_source": "exact_date", "fx_captured_at": CAPTURE}


def raw(kind, changes=None):
    prep, target = module(kind, "preparation"), schema(kind)
    columns = prep.RAW_COLUMNS if kind != "restored" else [
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
            value = Decimal("-20") if kind == "finance" else Decimal("10")
        elif name.endswith("_usd"):
            value = -2.0 if kind == "finance" else 1.0
        elif pa.types.is_floating(dtype):
            value = 1.0
        else:
            value = -2 if kind == "finance" and name.startswith("finance_") else 1
        if kind == "sales" and name.rsplit("_", 1)[-1] in ("fbs", "dbs", "other", "unknown"):
            value = Decimal(0) if pa.types.is_decimal(dtype) else 0
        if kind == "sales" and name.endswith(("_fbs_usd", "_dbs_usd", "_other_usd", "_unknown_usd")):
            value = 0.0
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
    args.update({"selected": selected(), "run": run()} if kind == "restored" else {"day": DAY, "fx": fx()})
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


def test_finance_signed_raw_and_usd_survive_without_clipping():
    result = prepare("finance").to_pylist()[0]
    assert result["finance_units_net"] == -2
    assert result["finance_gmv_net"] == Decimal("-20")
    assert result["finance_gmv_net_usd"] == -2.0


@pytest.mark.parametrize("seller,key", [(None, "unknown"), (12, "seller:12")])
def test_finance_unknown_seller_is_preserved(seller, key):
    row = prepare("finance", raw("finance", {"seller_id": seller, "seller_key": key})).to_pylist()[0]
    assert row["seller_id"] == seller and row["seller_key"] == key


@pytest.mark.parametrize("seller,key", [(0, "unknown"), (-7, "seller:-7"), (7, "unknown"), (None, "seller:7")])
def test_finance_invalid_attribution_blocks(seller, key):
    with pytest.raises(ValueError):
        prepare("finance", raw("finance", {"seller_id": seller, "seller_key": key}))


@pytest.mark.parametrize("kind,field", [("sales", "sales_gmv"), ("finance", "finance_gmv_net")])
def test_no_float_to_raw_decimal_coercion(kind, field):
    source = raw(kind)
    source = source.set_column(source.column_names.index(field), field, pa.array([0.1]))
    with pytest.raises(ValueError, match="округлять"):
        prepare(kind, source)


def test_sales_exact_channel_sum_above_decimal_context_precision():
    huge = Decimal("12345678901234567890123456789012345678")
    source = raw("sales", {"sales_gmv": huge, "sales_gmv_fbo": huge,
                           "sales_gmv_usd": float(huge)/10, "sales_gmv_fbo_usd": float(huge)/10})
    with localcontext() as context:
        context.prec = 6
        result = prepare("sales", source)
    assert result["sales_gmv"][0].as_py() == huge


def test_sales_channel_mismatch_and_unknown_day_are_not_zero():
    with pytest.raises(ValueError, match="Каналы"):
        prepare("sales", raw("sales", {"sales_gmv_dbs": Decimal(2), "sales_gmv_dbs_usd": 0.2}))
    with pytest.raises(ValueError, match="разложения"):
        prepare("sales", raw("sales", {"sales_gmv_dbs": None, "sales_gmv_dbs_usd": None}))


@pytest.mark.parametrize("kind", ["sales", "finance"])
def test_unavailable_rate_keeps_raw_and_requires_null_usd(kind):
    changes = {n + "_usd": None for n in module(kind, "preparation").MONEY}
    receipt = fx() | {"fx_rate_date": None, "fx_rate_uzs_per_usd": None, "fx_rate_source": "unavailable"}
    result = prepare(kind, raw(kind, changes), fx=receipt)
    assert result[module(kind, "preparation").MONEY[0]][0].as_py() is not None
    with pytest.raises(ValueError, match="USD"):
        prepare(kind, fx=receipt)


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


def test_finance_sql_uses_event_date_without_cohort_filters_or_final():
    sql = module("finance", "query").source_query(config("finance"), DAY, fx_available=True)
    assert "WHERE dt = toDate('2026-09-06')" in sql
    assert "date_created" not in sql and "FINAL" not in sql and "JOIN" not in sql
    assert "GROUP BY sku_id, seller_key, seller_id" in sql
    assert "sum(toDecimal128(net_gmv, 0))" in sql
    assert "seller_id > 0" in sql and "nullIf(seller_id, 0)" in sql
    assert sql.count("daily_uzs_to_usd(date, ") == 20


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
