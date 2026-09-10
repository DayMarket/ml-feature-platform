"""Свернуть BIG_SALE до дня и соединить с точным официальным календарём."""

from collections import defaultdict
from datetime import datetime, timezone
import re

import pyarrow as pa

CALENDAR_FIELDS = (
    "date", "calendar_id", "year", "quarter", "month", "month_name_en", "month_abbr_en",
    "day", "day_of_week_iso", "day_name_en", "day_abbr_en", "iso_week", "is_weekend",
    "is_public_holiday", "holiday_name", "is_working_day",
)
FLAGS = ("big_sale_created", "big_sale_canceled", "big_sale_unknown_status")
EXTRA_FIELDS = (*FLAGS, "big_sale_event_count", "calendar_coverage_status",
                "promotion_coverage_status", "calendar_snapshot_id", "events_snapshot_id",
                "calendar_source_manifest_id", "events_source_manifest_id",
                "source_manifest_id", "ingested_at")
NUMBERS = {"year", "quarter", "month", "day", "day_of_week_iso", "iso_week"}
BOOLEANS = {"is_weekend", "is_public_holiday", "is_working_day", *FLAGS}
LONGS = {"big_sale_event_count", "calendar_snapshot_id", "events_snapshot_id"}
REQUIRED = {"date", "calendar_id", *set(EXTRA_FIELDS) - set(FLAGS)}


def target_ref(config, catalog_name):
    table = config["table"]
    for key in ("catalog", "schema", "name"):
        if not isinstance(table[key], str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table[key]):
            raise ValueError(f"table.{key}: нужен отдельный компонент identifier")
    if table["catalog"] != catalog_name:
        raise ValueError("Загружен другой Iceberg catalog")
    return table["schema"], table["name"]


def validate_schema(schema):
    if tuple(schema.names) != (*CALENDAR_FIELDS, *EXTRA_FIELDS):
        raise ValueError("Gold calendar: схема должна содержать 28 согласованных полей")
    for field in schema:
        dtype = field.type
        if field.name == "date":
            valid = dtype == pa.date32()
        elif field.name in NUMBERS:
            valid = dtype == pa.int32()
        elif field.name in LONGS:
            valid = dtype == pa.int64()
        elif field.name in BOOLEANS:
            valid = dtype == pa.bool_()
        elif field.name == "ingested_at":
            valid = pa.types.is_timestamp(dtype) and dtype.unit == "us" and dtype.tz in (None, "UTC")
        else:
            valid = pa.types.is_string(dtype) or pa.types.is_large_string(dtype)
        if not valid or field.nullable != (field.name not in REQUIRED):
            raise ValueError(f"Несовместимое поле gold calendar: {field.name}")


def prepare(calendar, events, schema, *, calendar_receipt, events_receipt, run_id, ingested_at):
    validate_schema(schema)
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Нужен run_id сборки")
    if not isinstance(ingested_at, datetime) or ingested_at.utcoffset() is None:
        raise ValueError("Нужен aware ingested_at")
    if not calendar.num_rows or calendar["date"].type != pa.date32() or calendar["date"].null_count:
        raise ValueError("Нужен непустой DATE календарь без NULL ключей")
    days = calendar["date"].to_pylist()
    if len(set(days)) != len(days) or set(calendar["calendar_id"].to_pylist()) != {"uz_official"}:
        raise ValueError("Повтор даты или другой источник календаря")
    # Проверка типов исходных полей не допускает скрытого преобразования флагов/чисел.
    for name in CALENDAR_FIELDS:
        source, target = calendar.schema.field(name).type, schema.field(name).type
        text = (pa.types.is_string(source) or pa.types.is_large_string(source)) and (
            pa.types.is_string(target) or pa.types.is_large_string(target))
        if source != target and not text:
            raise ValueError(f"Несовместимое поле входного календаря: {name}")
    if events["date"].type != pa.date32():
        raise ValueError("event.date должен быть DATE")
    seen, by_day = set(), defaultdict(list)
    day_set = set(days)
    for row in events.to_pylist():
        key = row["date"], row["event_code"]
        if key in seen or key[0] not in day_set or not isinstance(key[1], str) or not key[1]:
            raise ValueError("Повтор/неверный ключ события или дата вне календаря")
        seen.add(key)
        if row["source_kind"] not in ("calendar", "marketing_sale"):
            raise ValueError("Неизвестный source_kind")
        if row["source_kind"] == "marketing_sale" and row["source_type"] == "BIG_SALE":
            by_day[row["date"]].append(row["source_status"])
    captured = ingested_at.astimezone(timezone.utc)
    if schema.field("ingested_at").type.tz is None:
        captured = captured.replace(tzinfo=None)
    result = []
    for row in calendar.select(CALENDAR_FIELDS).to_pylist():
        statuses = by_day[row["date"]]
        values = ("CREATED" in statuses, "CANCELED" in statuses,
                  any(value not in ("CREATED", "CANCELED") for value in statuses))
        row.update(zip(FLAGS, values if statuses else (None, None, None)))
        row.update(big_sale_event_count=len(statuses), calendar_coverage_status="source_row_present",
                   promotion_coverage_status="registry_rows_present" if statuses else "no_registry_rows",
                   calendar_snapshot_id=calendar_receipt["snapshot_id"],
                   events_snapshot_id=events_receipt["snapshot_id"],
                   calendar_source_manifest_id=calendar_receipt["source_manifest_id"],
                   events_source_manifest_id=events_receipt["source_manifest_id"],
                   source_manifest_id=run_id, ingested_at=captured)
        result.append(row)
    return pa.Table.from_pylist(result, schema=schema).sort_by([("date", "ascending")])
