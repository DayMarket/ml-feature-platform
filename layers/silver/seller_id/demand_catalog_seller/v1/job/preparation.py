"""Подготовить полный текущий seller-master каталог без окон и исторической атрибуции."""

from datetime import datetime, timezone
import re
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

SOURCE_FIELDS = ("seller_id", "source_master_seller_id", "is_1p", "seller_registered_at")


def target_ref(config, catalog_name):
    table = config["table"]
    if any(not isinstance(table.get(k), str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table[k])
           for k in ("catalog", "schema", "name")):
        raise ValueError("Нужны отдельные компоненты Iceberg identifier")
    if table["catalog"] != catalog_name:
        raise ValueError("Другой Iceberg каталог")
    return table["schema"], table["name"]


def source_sql(config):
    source = config["source"]
    if (source.get("database"), source.get("table")) != ("marts", "sellers_info"):
        raise ValueError("Источник не совпадает с согласованным marts.sellers_info")
    return ("SELECT seller_id, master_seller_id AS source_master_seller_id, is_1p, "
            "toDateTime64(toTimeZone(registration_date, 'UTC'), 6, 'UTC') AS seller_registered_at "
            "FROM `marts`.`sellers_info` ORDER BY seller_id "
            "SETTINGS max_threads=1, max_execution_time=300")


def expected_schema():
    fields = [("date", pa.date32(), False), ("seller_id", pa.int64(), False),
              ("source_master_seller_id", pa.string(), True), ("master_seller_id", pa.string(), True),
              ("seller_mapping_status", pa.string(), False), ("has_master", pa.bool_(), True),
              ("is_1p", pa.bool_(), True), ("seller_registered_at", pa.timestamp("us"), True),
              ("catalog_version", pa.string(), False), ("source_contract_version", pa.string(), False),
              ("source_manifest_id", pa.string(), False), ("ingested_at", pa.timestamp("us"), False)]
    return pa.schema([pa.field(name, kind, nullable=nullable) for name, kind, nullable in fields])


def validate_schema(schema):
    expected = expected_schema()
    if len(schema) != len(expected) or set(schema.names) != set(expected.names):
        raise ValueError("Catalog seller требует 12 согласованных полей")
    for field in expected:
        found = schema.field(field.name)
        same = found.type == field.type or pa.types.is_string(field.type) and pa.types.is_large_string(found.type)
        if not same or found.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable catalog seller: {field.name}")


def prepare_catalog(source, schema, *, expected_source_rows, catalog_version, source_manifest_id,
                    source_contract_version, ingested_at):
    """expected_source_rows приходит из независимого source count, не из длины выгрузки."""
    validate_schema(schema)
    if (type(expected_source_rows) is not int or expected_source_rows <= 0
            or not isinstance(source, pa.Table) or source.num_rows != expected_source_rows):
        raise ValueError("Нужен непустой полный захват, совпадающий с source count")
    if len(source.column_names) != len(SOURCE_FIELDS) or set(source.column_names) != set(SOURCE_FIELDS):
        raise ValueError("Нужны ровно четыре исходных поля seller")
    if any(not isinstance(v, str) or not v.strip() for v in (catalog_version, source_manifest_id, source_contract_version)):
        raise ValueError("Нужны версии каталога/контракта и manifest")
    if not isinstance(ingested_at, datetime) or ingested_at.utcoffset() is None:
        raise ValueError("Нужно aware время захвата")
    ids, raw_master, flags, registered = (source[name] for name in SOURCE_FIELDS)
    if not pa.types.is_integer(ids.type) or ids.null_count:
        raise ValueError("seller_id должен быть целым без NULL")
    ids = ids.cast(pa.int64(), safe=True)
    if pc.min(ids).as_py() <= 0 or pc.count_distinct(ids).as_py() != source.num_rows:
        raise ValueError("Неверные или повторные seller_id")
    if not (pa.types.is_string(raw_master.type) or pa.types.is_large_string(raw_master.type)):
        raise ValueError("Master должен быть исходной строкой/NULL")
    if not pa.types.is_boolean(flags.type):
        raise ValueError("is_1p должен быть исходным Bool/NULL")
    if registered.type != pa.timestamp("us", "UTC"):
        raise ValueError("Регистрация должна быть явно UTC в микросекундах")
    masters, statuses, has_master = [], [], []
    actual_masters, fallback_ids = set(), set()
    for seller_id, raw in zip(ids.to_pylist(), raw_master.to_pylist(), strict=True):
        if raw is None:
            masters.append(None)
            statuses.append("unavailable")
            has_master.append(None)
        elif raw.strip():
            value = raw.strip()
            actual_masters.add(value)
            masters.append(value)
            statuses.append("matched")
            has_master.append(True)
        else:
            value = str(seller_id)
            fallback_ids.add(value)
            masters.append(value)
            statuses.append("unmatched")
            has_master.append(False)
    if actual_masters & fallback_ids:
        raise ValueError("Fallback seller_id конфликтует с реальным master")
    moment = ingested_at.astimezone(timezone.utc).replace(tzinfo=None)
    values = {"seller_id": ids, "source_master_seller_id": raw_master,
              "master_seller_id": masters, "seller_mapping_status": statuses,
              "has_master": has_master, "is_1p": flags,
              "seller_registered_at": registered.cast(pa.timestamp("us"), safe=True)}
    constants = {"date": ingested_at.astimezone(ZoneInfo("Asia/Tashkent")).date(),
                 "catalog_version": catalog_version, "source_contract_version": source_contract_version,
                 "source_manifest_id": source_manifest_id, "ingested_at": moment}
    arrays = []
    for field in schema:
        value = [constants[field.name]] * source.num_rows if field.name in constants else values[field.name]
        arrays.append(value.cast(field.type, safe=True) if isinstance(value, pa.ChunkedArray)
                      else pa.array(value, type=field.type, safe=True))
    result = pa.Table.from_arrays(arrays, schema=schema)
    result.validate(full=True)
    return result.sort_by([("seller_id", "ascending")])
