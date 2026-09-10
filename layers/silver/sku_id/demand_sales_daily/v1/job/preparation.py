"""Подготовить дневные суммы без потери точности и проверить сохранённый курс."""

from datetime import date, datetime, timezone
from decimal import Decimal
import math
import re
from zoneinfo import ZoneInfo

import pyarrow as pa

MONEY = ("sales_gmv","sales_payment_value","sales_full_value","sales_seller_promo_value","sales_marketplace_promo_value","sales_gmv_fbo","sales_gmv_fbs","sales_gmv_dbs","sales_gmv_other","sales_gmv_unknown",)
QUANTITIES = ("sales_units","sales_order_items","sales_orders","sales_units_fbo","sales_units_fbs","sales_units_dbs","sales_units_other","sales_units_unknown",)
TECH = ("fx_rate_date","fx_rate_uzs_per_usd","fx_rate_source","fx_captured_at","source_manifest_id","source_contract_version","ingested_at",)
RAW_COLUMNS = ("date","sku_id","sales_units","sales_order_items","sales_orders","sales_gmv","sales_payment_value","sales_full_value","sales_seller_promo_value","sales_marketplace_promo_value","sales_gmv_fbo","sales_gmv_fbs","sales_gmv_dbs","sales_gmv_other","sales_gmv_unknown","sales_units_fbo","sales_units_fbs","sales_units_dbs","sales_units_other","sales_units_unknown","sales_gmv_usd","sales_payment_value_usd","sales_full_value_usd","sales_seller_promo_value_usd","sales_marketplace_promo_value_usd","sales_gmv_fbo_usd","sales_gmv_fbs_usd","sales_gmv_dbs_usd","sales_gmv_other_usd","sales_gmv_unknown_usd","source_updated_at",)
REQUIRED = ("date","sku_id","source_updated_at","fx_rate_source","fx_captured_at","source_manifest_id","source_contract_version","ingested_at",)

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
        "sales_units": pa.int64(),
        "sales_order_items": pa.int64(),
        "sales_orders": pa.int64(),
        "sales_gmv": pa.decimal128(38, 0),
        "sales_payment_value": pa.decimal128(38, 0),
        "sales_full_value": pa.decimal128(38, 0),
        "sales_seller_promo_value": pa.decimal128(38, 0),
        "sales_marketplace_promo_value": pa.decimal128(38, 0),
        "sales_gmv_fbo": pa.decimal128(38, 0),
        "sales_gmv_fbs": pa.decimal128(38, 0),
        "sales_gmv_dbs": pa.decimal128(38, 0),
        "sales_gmv_other": pa.decimal128(38, 0),
        "sales_gmv_unknown": pa.decimal128(38, 0),
        "sales_units_fbo": pa.int64(),
        "sales_units_fbs": pa.int64(),
        "sales_units_dbs": pa.int64(),
        "sales_units_other": pa.int64(),
        "sales_units_unknown": pa.int64(),
        "sales_gmv_usd": pa.float64(),
        "sales_payment_value_usd": pa.float64(),
        "sales_full_value_usd": pa.float64(),
        "sales_seller_promo_value_usd": pa.float64(),
        "sales_marketplace_promo_value_usd": pa.float64(),
        "sales_gmv_fbo_usd": pa.float64(),
        "sales_gmv_fbs_usd": pa.float64(),
        "sales_gmv_dbs_usd": pa.float64(),
        "sales_gmv_other_usd": pa.float64(),
        "sales_gmv_unknown_usd": pa.float64(),
        "source_updated_at": pa.timestamp('us'),
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
        key = (row["sku_id"],)
        if row["date"] != day or row["sku_id"] <= 0 or (previous is not None and key <= previous):
            raise ValueError("Неверный день, SKU или порядок ключей")
        previous = key
        for name in QUANTITIES + tuple(n for n in MONEY if n != "sales_marketplace_promo_value"):
            if row[name] is not None and row[name] < 0:
                raise ValueError(f"Отрицательный {name}")
        channels = ("fbo", "fbs", "dbs", "other", "unknown")
        for total, prefix in (("sales_units", "sales_units_"), ("sales_gmv", "sales_gmv_")):
            parts = [row[prefix + c] for c in channels]
            if any(v is None for v in parts) or row[total] is None:
                raise ValueError("Нет полного канального разложения")
            # Decimal-сложение через integer сохраняет все 38 знаков независимо от global context.
            if sum(int(v) for v in parts) != int(row[total]):
                raise ValueError("Каналы не совпадают с общей суммой")

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
