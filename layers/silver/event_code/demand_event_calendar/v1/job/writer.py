"""Заменить полный календарь событий и проверить точный Iceberg snapshot без tags."""

from collections import Counter
from datetime import datetime, time, timedelta, timezone

import pyarrow as pa

from .extraction import extract_prepared, target_ref
from .preparation import BUSINESS_ZONE, PROMO_TIMES, validate_target_schema


def validate_batch(batch, schema, report):
    """Проверить схему, ключи, границы, conditional NULL и счётчики захвата до записи."""
    validate_target_schema(schema)
    if not batch.schema.equals(schema, check_metadata=False):
        raise ValueError("Arrow batch не соответствует схеме загруженной Iceberg таблицы")
    if not batch.num_rows:
        raise ValueError("Пустой результат не разрешает удаление всего календаря событий")
    for field in schema:
        if not field.nullable and batch[field.name].null_count:
            raise ValueError(f"NULL в обязательном поле: {field.name}")
    if type(report.get("promo_source_rows")) is not int or report["promo_source_rows"] <= 0:
        raise ValueError("Пустой/непроверенный реестр акций не разрешает запись")
    if report.get("output_rows") != batch.num_rows:
        raise ValueError("Число событий не совпадает с отчётом подготовки")
    if (report.get("interval_rule"), report.get("business_timezone")) != (
            "[started_at,finished_at)", "Asia/Tashkent"):
        raise ValueError("Отчёт использует другой временной контракт")
    manifest = report.get("source_manifest_id")
    if not isinstance(manifest, str) or not manifest.strip():
        raise ValueError("Нет source_manifest_id отчёта")
    if set(batch["source_manifest_id"].to_pylist()) != {manifest}:
        raise ValueError("Manifest batch не совпадает с отчётом")
    if len(set(batch["ingested_at"].to_pylist())) != 1:
        raise ValueError("Один захват должен иметь одно время ingested_at")
    seen, counts, attributes = set(), Counter(), {}
    for row in batch.to_pylist():
        key = row["date"], row["event_code"]
        if key in seen:
            raise ValueError("Повтор ключа (date,event_code)")
        seen.add(key)
        if row["source_kind"] == "calendar":
            expected = f"calendar:uz_official:{row['date'].isoformat()}"
            if (row["calendar_id"] != "uz_official" or row["source_event_id"] != row["date"].isoformat()
                    or row["event_code"] != expected):
                raise ValueError("Нарушена идентичность календарного праздника")
            if any(row["source_" + name] is not None for name in (*PROMO_TIMES, "status", "type")):
                raise ValueError("У праздника не может быть атрибутов маркетинговой акции")
        elif row["source_kind"] == "marketing_sale":
            source_id = row["source_event_id"]
            if (not source_id.isascii() or not source_id.isdecimal() or int(source_id) <= 0
                    or str(int(source_id)) != source_id or row["event_code"] != "marketing_sale:" + source_id
                    or row["calendar_id"] is not None):
                raise ValueError("Нарушена идентичность акции")
            start, finish = row["source_started_at"], row["source_finished_at"]
            if start is None or finish is None:
                raise ValueError("Нет границ акции")
            start = start.replace(tzinfo=timezone.utc) if start.utcoffset() is None else start
            finish = finish.replace(tzinfo=timezone.utc) if finish.utcoffset() is None else finish
            day_start = datetime.combine(row["date"], time(), BUSINESS_ZONE)
            day_end = day_start + timedelta(days=1)
            if finish <= start or not (start < day_end and finish > day_start):
                raise ValueError("День вне полуоткрытого интервала акции")
            signature = tuple(row[name] for name in batch.column_names if name != "date")
            if source_id in attributes and attributes[source_id] != signature:
                raise ValueError("Разные исходные атрибуты одной акции")
            attributes[source_id] = signature
            counts[source_id] += 1
        else:
            raise ValueError("Неизвестный source_kind")
    coverage = report.get("promo_coverage", [])
    if len(coverage) != report["promo_source_rows"]:
        raise ValueError("Неполный отчёт по исходным акциям")
    ids = [item["source_event_id"] for item in coverage]
    if len(set(ids)) != len(ids) or set(counts) - set(ids):
        raise ValueError("Набор акций не совпадает с отчётом")
    for item in coverage:
        if (item["included_days"] != counts[item["source_event_id"]]
                or item["included_days"] + item["uncovered_days"] != item["interval_days"]
                or item["uncovered_days"] < 0):
            raise ValueError("Число дней акции не совпадает с отчётом")
    return batch.sort_by([("date", "ascending"), ("event_code", "ascending")])


def write_prepared(config, catalog, batch: pa.Table, report: dict) -> dict:
    """Атомарно заменить весь принадлежащий энтити справочник; результат ещё не DQ-ready."""
    from pyiceberg.expressions import AlwaysTrue

    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет Iceberg таблицы {identifier}: сначала применить миграцию")
    table = catalog.load_table(identifier)
    prepared = validate_batch(batch, table.schema().as_arrow(), report)
    # Заменяем справочник целиком: исчезнувшая акция могла остаться в другом месяце.
    table.overwrite(prepared, overwrite_filter=AlwaysTrue())
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("После записи событий нет Iceberg snapshot")
    snapshot_id = snapshot.snapshot_id
    actual = table.scan(snapshot_id=snapshot_id).to_arrow().sort_by(
        [("date", "ascending"), ("event_code", "ascending")])
    if not actual.equals(prepared, check_metadata=False):
        raise RuntimeError("Записанные события не совпали с полным подготовленным захватом")
    table.refresh()
    current = table.current_snapshot()
    if current is None or current.snapshot_id != snapshot_id:
        raise RuntimeError("Iceberg snapshot событий изменился во время read-back")
    moment = actual["ingested_at"][0].as_py()
    if moment.utcoffset() is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return {
        "status": "written", "rows_written": actual.num_rows,
        "snapshot_id": snapshot_id, "table_uuid": str(table.metadata.table_uuid),
        "source_manifest_id": report["source_manifest_id"],
        "ingested_at": moment.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        "date_min": actual["date"][0].as_py().isoformat(),
        "date_max": actual["date"][-1].as_py().isoformat(),
        "coverage_report": report,
    }


def load_events(config, catalog, repo_root, *, calendar_receipt,
                source_manifest_id, ingested_at, query_records=None):
    """Общий полный загрузчик для scheduled и manual запусков."""
    batch, report = extract_prepared(config, catalog, repo_root,
                                     calendar_receipt=calendar_receipt,
                                     source_manifest_id=source_manifest_id,
                                     ingested_at=ingested_at, query_records=query_records)
    return write_prepared(config, catalog, batch, report)
