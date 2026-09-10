"""Развернуть праздники и акции по исходным датам календаря без записи в Iceberg."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pyarrow as pa


CALENDAR_FIELDS = ("date", "calendar_id", "is_public_holiday", "holiday_name")
PROMO_TEXT = ("title", "status", "type")
PROMO_TIMES = ("started_at", "finished_at", "announced_at", "created_at", "updated_at")
PROMO_FIELDS = ("id", *PROMO_TEXT, *PROMO_TIMES)
OUTPUT_FIELDS = (
    "date", "event_code", "source_kind", "calendar_id", "source_event_id", "event_name",
    "source_status", "source_type", *("source_" + name for name in PROMO_TIMES),
    "source_manifest_id", "ingested_at",
)
BUSINESS_ZONE = ZoneInfo("Asia/Tashkent")


def validate_target_schema(schema: pa.Schema) -> None:
    """Не ослаблять физический контракт 15 полей и обязательность lineage."""
    if tuple(schema.names) != OUTPUT_FIELDS:
        raise ValueError("Схема event_calendar не совпадает с контрактом 15 полей")
    required = {"date", "event_code", "source_kind", "source_event_id",
                "source_manifest_id", "ingested_at"}
    for field in schema:
        dtype = field.type
        if field.name == "date":
            valid = dtype == pa.date32()
        elif field.name.endswith("_at"):
            valid = (pa.types.is_timestamp(dtype) and dtype.unit == "us"
                     and dtype.tz in (None, "UTC"))
        else:
            valid = pa.types.is_string(dtype) or pa.types.is_large_string(dtype)
        if not valid or field.nullable != (field.name not in required):
            raise ValueError(f"Несовместимое поле event_calendar: {field.name}")


def _source_types(calendar: pa.Table, promos: pa.Table) -> None:
    for table, fields in ((calendar, CALENDAR_FIELDS), (promos, PROMO_FIELDS)):
        if (len(set(table.column_names)) != len(table.column_names)
                or not set(fields).issubset(table.column_names)):
            raise ValueError("Нет обязательных исходных полей или повторяются имена колонок")
    if set(promos.column_names) != set(PROMO_FIELDS):
        raise ValueError("Реестр должен содержать ровно объявленную проекцию полей")
    if calendar["date"].type != pa.date32() or calendar["date"].null_count:
        raise ValueError("calendar.date должен быть DATE без NULL")
    if not (pa.types.is_boolean(calendar["is_public_holiday"].type)
            or pa.types.is_null(calendar["is_public_holiday"].type)):
        raise ValueError("is_public_holiday должен быть BOOLEAN или NULL")
    for table, fields in ((calendar, ("calendar_id", "holiday_name")), (promos, PROMO_TEXT)):
        for name in fields:
            dtype = table[name].type
            if not (pa.types.is_string(dtype) or pa.types.is_large_string(dtype)
                    or pa.types.is_null(dtype)):
                raise ValueError(f"{name}: требуется строка или NULL")
    if not pa.types.is_integer(promos["id"].type):
        raise ValueError("id акции должен быть целым исходным идентификатором")
    for name in PROMO_TIMES:
        dtype = promos[name].type
        if pa.types.is_null(dtype):
            continue
        if not (pa.types.is_timestamp(dtype) and dtype.tz and dtype.unit in ("s", "ms", "us")):
            raise ValueError(f"{name}: нужен timestamp с явной зоной и точностью не выше us")


def _stored_timestamp(value: datetime | None, schema: pa.Schema, field: str):
    if value is None:
        return None
    utc = value.astimezone(timezone.utc)
    return utc if schema.field(field).type.tz else utc.replace(tzinfo=None)


def prepare_events(calendar: pa.Table, promos: pa.Table, schema: pa.Schema, *,
                   source_manifest_id: str, ingested_at: datetime) -> tuple[pa.Table, dict]:
    """Вернуть типизированные события и отчёт покрытия, не объявляя источник ready.

    Вызывающий загрузчик закрепляет календарный snapshot после upstream DQ и реестр
    в manifest. Наивная зона источника не угадывается. Пустые/обратные интервалы
    и дубли id блокируют подготовку; обрезка по календарю отражается в отчёте.
    """
    validate_target_schema(schema)
    _source_types(calendar, promos)
    if not isinstance(source_manifest_id, str) or not source_manifest_id.strip():
        raise ValueError("Нужен source_manifest_id конкретного захвата обоих источников")
    if not isinstance(ingested_at, datetime) or ingested_at.utcoffset() is None:
        raise ValueError("ingested_at должен содержать часовую зону")
    if not calendar.num_rows:
        raise ValueError("Пустой календарь не подтверждает покрытие")
    calendar_rows = calendar.select(CALENDAR_FIELDS).to_pylist()
    days = sorted(row["date"] for row in calendar_rows)
    if len(set(days)) != len(days):
        raise ValueError("Повтор даты в источнике calendar")
    if any(row["calendar_id"] != "uz_official" for row in calendar_rows):
        raise ValueError("Ожидается только calendar_id=uz_official")
    promo_rows = promos.select(PROMO_FIELDS).to_pylist()
    ids = [row["id"] for row in promo_rows]
    if any(value is None or value <= 0 for value in ids):
        raise ValueError("NULL/неположительный id акции")
    if len(set(ids)) != len(ids):
        raise ValueError("Повтор id в реестре: нужен однозначный срез, не drop_duplicates")
    result_rows = []
    report = {
        "interval_rule": "[started_at,finished_at)", "business_timezone": "Asia/Tashkent",
        "source_manifest_id": source_manifest_id,
        "calendar_rows": len(days), "calendar_date_min": days[0].isoformat(),
        "calendar_date_max": days[-1].isoformat(),
        "calendar_missing_dates": (days[-1] - days[0]).days + 1 - len(days),
        "calendar_unknown_holiday_dates": [], "promo_source_rows": len(promo_rows),
        "promo_coverage": [],
    }

    def base_row():
        return {**dict.fromkeys(OUTPUT_FIELDS), "source_manifest_id": source_manifest_id,
                "ingested_at": _stored_timestamp(ingested_at, schema, "ingested_at")}

    for row in calendar_rows:
        if row["is_public_holiday"] is None:
            report["calendar_unknown_holiday_dates"].append(row["date"].isoformat())
        if row["is_public_holiday"] is not True:
            continue
        source_id = row["date"].isoformat()
        result_rows.append({**base_row(), "date": row["date"], "source_kind": "calendar",
                            "calendar_id": "uz_official", "source_event_id": source_id,
                            "event_code": f"calendar:uz_official:{source_id}",
                            "event_name": row["holiday_name"]})

    for row in promo_rows:
        start, finish = row["started_at"], row["finished_at"]
        if (start is None or finish is None
                or finish.astimezone(timezone.utc) <= start.astimezone(timezone.utc)):
            raise ValueError(f"Акция {row['id']}: отсутствующий/пустой/обратный интервал")
        first = start.astimezone(BUSINESS_ZONE).date()
        last = (finish.astimezone(timezone.utc) - timedelta(microseconds=1)).astimezone(BUSINESS_ZONE).date()
        selected = days[bisect_left(days, first):bisect_right(days, last)]
        expected = (last - first).days + 1
        report["promo_coverage"].append({
            "source_event_id": str(row["id"]), "interval_days": expected,
            "included_days": len(selected), "uncovered_days": expected - len(selected),
            "coverage": "complete" if len(selected) == expected else "partial" if selected else "outside",
        })
        values = {**base_row(), "source_kind": "marketing_sale",
                  "source_event_id": str(row["id"]), "event_code": f"marketing_sale:{row['id']}",
                  "event_name": row["title"], "source_status": row["status"], "source_type": row["type"]}
        for name in PROMO_TIMES:
            values["source_" + name] = _stored_timestamp(row[name], schema, "source_" + name)
        result_rows.extend({**values, "date": day} for day in selected)
    result = pa.Table.from_pylist(result_rows, schema=schema)
    result.validate(full=True)
    report["output_rows"] = result.num_rows
    report["calendar_unknown_holiday_dates"].sort()
    report["promo_coverage"].sort(key=lambda item: item["source_event_id"])
    return result.sort_by([("date", "ascending"), ("event_code", "ascending")]), report
