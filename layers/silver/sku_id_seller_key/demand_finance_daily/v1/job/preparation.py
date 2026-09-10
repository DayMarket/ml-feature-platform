"""Подготовить дневные суммы без потери точности и проверить сохранённый курс."""

from datetime import date, datetime, timezone
from decimal import Decimal
import math
import re
from zoneinfo import ZoneInfo

import pyarrow as pa

MONEY = ("finance_gmv_generated","finance_gmv_assembled","finance_gmv_delivered","finance_gmv_completed","finance_gmv_returned","finance_gmv_net","finance_gmv_generated_without_promo","finance_gmv_delivered_without_promo","finance_gmv_completed_without_promo","finance_gmv_returned_without_promo","finance_gmv_net_without_promo","finance_promocodes_generated","finance_seller_discount_generated","finance_discount_generated","finance_promocodes_completed","finance_seller_discount_completed","finance_discount_completed","finance_promocodes_returned","finance_seller_discount_returned","finance_discount_returned",)
QUANTITIES = ("finance_units_generated","finance_units_assembled","finance_units_delivered","finance_units_completed","finance_units_returned","finance_units_net","finance_returned_units_current_period","finance_returned_units_previous_period",)
TECH = ("fx_rate_date","fx_rate_uzs_per_usd","fx_rate_source","fx_captured_at","source_manifest_id","source_contract_version","ingested_at",)
RAW_COLUMNS = ("date","sku_id","seller_key","seller_id","finance_units_generated","finance_units_assembled","finance_units_delivered","finance_units_completed","finance_units_returned","finance_units_net","finance_returned_units_current_period","finance_returned_units_previous_period","finance_gmv_generated","finance_gmv_assembled","finance_gmv_delivered","finance_gmv_completed","finance_gmv_returned","finance_gmv_net","finance_gmv_generated_without_promo","finance_gmv_delivered_without_promo","finance_gmv_completed_without_promo","finance_gmv_returned_without_promo","finance_gmv_net_without_promo","finance_promocodes_generated","finance_seller_discount_generated","finance_discount_generated","finance_promocodes_completed","finance_seller_discount_completed","finance_discount_completed","finance_promocodes_returned","finance_seller_discount_returned","finance_discount_returned","finance_gmv_generated_usd","finance_gmv_assembled_usd","finance_gmv_delivered_usd","finance_gmv_completed_usd","finance_gmv_returned_usd","finance_gmv_net_usd","finance_gmv_generated_without_promo_usd","finance_gmv_delivered_without_promo_usd","finance_gmv_completed_without_promo_usd","finance_gmv_returned_without_promo_usd","finance_gmv_net_without_promo_usd","finance_promocodes_generated_usd","finance_seller_discount_generated_usd","finance_discount_generated_usd","finance_promocodes_completed_usd","finance_seller_discount_completed_usd","finance_discount_completed_usd","finance_promocodes_returned_usd","finance_seller_discount_returned_usd","finance_discount_returned_usd","source_rows",)
REQUIRED = ("date","sku_id","seller_key","source_rows","fx_rate_source","fx_captured_at","source_manifest_id","source_contract_version","ingested_at",)

def target_ref(config, catalog_name):
    table = config['table']
    for key in ('catalog', 'schema', 'name'):
        if not isinstance(table.get(key), str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', table[key]):
            raise ValueError(f'Неверная компонента table.{key}')
    if table['catalog'] != catalog_name:
        raise ValueError('Другой Iceberg каталог')
    return table['schema'], table['name']


def validate_schema(schema):
    expected = {
        "date": pa.date32(),
        "sku_id": pa.int64(),
        "seller_key": pa.string(),
        "seller_id": pa.int64(),
        "finance_units_generated": pa.int64(),
        "finance_units_assembled": pa.int64(),
        "finance_units_delivered": pa.int64(),
        "finance_units_completed": pa.int64(),
        "finance_units_returned": pa.int64(),
        "finance_units_net": pa.int64(),
        "finance_returned_units_current_period": pa.int64(),
        "finance_returned_units_previous_period": pa.int64(),
        "finance_gmv_generated": pa.decimal128(38, 0),
        "finance_gmv_assembled": pa.decimal128(38, 0),
        "finance_gmv_delivered": pa.decimal128(38, 0),
        "finance_gmv_completed": pa.decimal128(38, 0),
        "finance_gmv_returned": pa.decimal128(38, 0),
        "finance_gmv_net": pa.decimal128(38, 0),
        "finance_gmv_generated_without_promo": pa.decimal128(38, 0),
        "finance_gmv_delivered_without_promo": pa.decimal128(38, 0),
        "finance_gmv_completed_without_promo": pa.decimal128(38, 0),
        "finance_gmv_returned_without_promo": pa.decimal128(38, 0),
        "finance_gmv_net_without_promo": pa.decimal128(38, 0),
        "finance_promocodes_generated": pa.decimal128(38, 0),
        "finance_seller_discount_generated": pa.decimal128(38, 0),
        "finance_discount_generated": pa.decimal128(38, 0),
        "finance_promocodes_completed": pa.decimal128(38, 0),
        "finance_seller_discount_completed": pa.decimal128(38, 0),
        "finance_discount_completed": pa.decimal128(38, 0),
        "finance_promocodes_returned": pa.decimal128(38, 0),
        "finance_seller_discount_returned": pa.decimal128(38, 0),
        "finance_discount_returned": pa.decimal128(38, 0),
        "finance_gmv_generated_usd": pa.float64(),
        "finance_gmv_assembled_usd": pa.float64(),
        "finance_gmv_delivered_usd": pa.float64(),
        "finance_gmv_completed_usd": pa.float64(),
        "finance_gmv_returned_usd": pa.float64(),
        "finance_gmv_net_usd": pa.float64(),
        "finance_gmv_generated_without_promo_usd": pa.float64(),
        "finance_gmv_delivered_without_promo_usd": pa.float64(),
        "finance_gmv_completed_without_promo_usd": pa.float64(),
        "finance_gmv_returned_without_promo_usd": pa.float64(),
        "finance_gmv_net_without_promo_usd": pa.float64(),
        "finance_promocodes_generated_usd": pa.float64(),
        "finance_seller_discount_generated_usd": pa.float64(),
        "finance_discount_generated_usd": pa.float64(),
        "finance_promocodes_completed_usd": pa.float64(),
        "finance_seller_discount_completed_usd": pa.float64(),
        "finance_discount_completed_usd": pa.float64(),
        "finance_promocodes_returned_usd": pa.float64(),
        "finance_seller_discount_returned_usd": pa.float64(),
        "finance_discount_returned_usd": pa.float64(),
        "source_rows": pa.int64(),
        "fx_rate_date": pa.date32(),
        "fx_rate_uzs_per_usd": pa.float64(),
        "fx_rate_source": pa.string(),
        "fx_captured_at": pa.timestamp('us'),
        "source_manifest_id": pa.string(),
        "source_contract_version": pa.string(),
        "ingested_at": pa.timestamp('us'),
    }
    if len(schema.names) != len(expected) or set(schema.names) != set(expected):
        raise ValueError("Колонки не совпадают с миграцией")
    for name, kind in expected.items():
        field = schema.field(name)
        if field.type != kind and not (pa.types.is_string(kind) and pa.types.is_large_string(field.type)):
            raise ValueError(f"Несовместимый тип {name}: {field.type}")
        if name in REQUIRED and field.nullable:
            raise ValueError(f"{name} должен быть required")


def utc_naive(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('Нужен timezone-aware capture')
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def validate_fx(fx, day):
    if set(fx) != {'date', 'fx_rate_date', 'fx_rate_uzs_per_usd', 'fx_rate_source', 'fx_captured_at'}:
        raise ValueError('Неверная схема FX receipt')
    if type(day) is not date or fx['date'] != day:
        raise ValueError('FX receipt другого дня')
    captured = utc_naive(fx['fx_captured_at'])
    source, rate, rate_date = fx['fx_rate_source'], fx['fx_rate_uzs_per_usd'], fx['fx_rate_date']
    if source == 'unavailable':
        if rate is not None or rate_date is not None:
            raise ValueError('unavailable требует NULL ставки и даты')
    else:
        if source not in {'exact_date', 'latest_available'} or type(rate_date) is not date:
            raise ValueError('Неверное происхождение курса')
        if isinstance(rate, bool) or not isinstance(rate, (float, int)) or not math.isfinite(rate) or rate <= 0:
            raise ValueError('Нужна конечная положительная ставка')
        if source == 'exact_date' and rate_date != day:
            raise ValueError('exact_date другого дня')
        if rate_date > fx['fx_captured_at'].astimezone(ZoneInfo('Asia/Tashkent')).date():
            raise ValueError('Курс позже захвата')
    return captured


def prepare_batch(raw, schema, *, day, fx, manifest, version, ingested_at):
    validate_schema(schema)
    captured, fx_time = utc_naive(ingested_at), validate_fx(fx, day)
    if fx_time > captured:
        raise ValueError("FX capture позже ingestion")
    if any(not isinstance(v, str) or not v.strip() for v in (manifest, version)):
        raise ValueError("Нужны manifest/version")
    if (not isinstance(raw, pa.Table) or len(raw.column_names) != len(RAW_COLUMNS)
            or set(raw.column_names) != set(RAW_COLUMNS)):
        raise ValueError("Неверный набор source колонок")
    arrays = {}
    for name in RAW_COLUMNS:
        source, desired = raw[name], schema.field(name).type
        exact_integer = (pa.types.is_integer(source.type)
                         or (pa.types.is_decimal(source.type) and source.type.scale == 0))
        if (pa.types.is_integer(desired) or pa.types.is_decimal(desired)) and not (
                exact_integer or pa.types.is_null(source.type)):
            raise ValueError(f"Нельзя округлять {name} из float/строки")
        if pa.types.is_date(desired) and not pa.types.is_date(source.type):
            raise ValueError(f"Нужна исходная DATE {name}")
        if name == "source_updated_at" and (
                not pa.types.is_timestamp(source.type) or source.type.tz != "UTC"):
            raise ValueError("source_updated_at должен быть явно UTC")
        arrays[name] = source.cast(desired, safe=True)
    metadata = {**{k: fx[k] for k in TECH if k in fx}, "fx_captured_at": fx_time,
                "source_manifest_id": manifest, "source_contract_version": version, "ingested_at": captured}
    for name in TECH:
        arrays[name] = pa.array([metadata[name]] * raw.num_rows, type=schema.field(name).type)
    result = pa.Table.from_arrays([arrays[field.name] for field in schema], schema=schema)
    return validate_batch(result, schema, day=day)


def validate_batch(batch, schema, *, day):
    validate_schema(schema)
    if type(day) is not date or not isinstance(batch, pa.Table) or not batch.schema.equals(schema, check_metadata=False):
        raise ValueError("Неверная DATE или схема готовой порции")
    for name in REQUIRED:
        if batch[name].null_count:
            raise ValueError(f"NULL в required {name}")
    previous = None
    for row in batch.to_pylist():
        key = (row["sku_id"], row["seller_key"])
        if row["date"] != day or row["sku_id"] <= 0 or (previous is not None and key <= previous):
            raise ValueError("Неверный день, SKU или порядок ключей")
        previous = key
        seller = row["seller_id"]
        if seller is not None and seller <= 0:
            raise ValueError("Неверный seller_id, source zero должен стать NULL")
        expected_key = "unknown" if seller is None else f"seller:{seller}"
        if row["seller_key"] != expected_key or row["source_rows"] <= 0:
            raise ValueError("Неверная атрибуция продавца либо source count")

        fx = {"date": day, **{n: row[n] for n in TECH[:4]}}
        fx["fx_captured_at"] = row["fx_captured_at"].replace(tzinfo=timezone.utc)
        validate_fx(fx, day)
        if row["fx_captured_at"] > row["ingested_at"]:
            raise ValueError("FX capture позже ingestion")
        for name in MONEY:
            raw_value, actual = row[name], row[name + "_usd"]
            if raw_value is not None and (not isinstance(raw_value, Decimal) or not raw_value.is_finite()):
                raise ValueError("Исходная сумма не точный Decimal")
            if raw_value is None or row["fx_rate_source"] == "unavailable":
                if actual is not None:
                    raise ValueError("USD без исходной суммы или курса")
            else:
                expected = float(raw_value) / row["fx_rate_uzs_per_usd"]
                if actual is None or not math.isfinite(actual) or not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12):
                    raise ValueError("USD не совпадает с сохранённым курсом")
    return batch
