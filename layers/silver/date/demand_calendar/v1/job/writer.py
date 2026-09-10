"""Заменить полный uz_official обычной Iceberg записью, без tags и публикации release."""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc

from .preparation import extract_prepared, target_ref, validate_target_schema


def validate_batch(batch: pa.Table, schema: pa.Schema, calendar_id: str) -> pa.Table:
    """Проверить область записи, required-поля и полный ключ до изменения таблицы."""
    validate_target_schema(schema)
    if not batch.schema.equals(schema, check_metadata=False):
        raise ValueError("Arrow batch не соответствует схеме загруженной Iceberg таблицы")
    if batch.num_rows == 0:
        raise ValueError("Пустой захват календаря не разрешает удаление существующих данных")
    if calendar_id != "uz_official":
        raise ValueError("Поддерживается только согласованный календарь uz_official")
    for field in schema:
        if not field.nullable and batch[field.name].null_count:
            raise ValueError(f"NULL в обязательном поле: {field.name}")
    if not pc.all(pc.equal(batch["calendar_id"], calendar_id)).as_py():
        raise ValueError("Batch содержит другой calendar_id")
    if pc.count_distinct(batch["date"]).as_py() != batch.num_rows:
        raise ValueError("Повтор ключа date")
    manifests = batch["source_manifest_id"].to_pylist()
    if any(not value.strip() for value in manifests) or len(set(manifests)) != 1:
        raise ValueError("Один полный захват должен иметь один непустой source_manifest_id")
    if pc.count_distinct(batch["ingested_at"]).as_py() != 1:
        raise ValueError("Один захват должен иметь одно время ingested_at")
    # Инварианты исходного календаря: неизвестные значения остаются NULL.
    for name, low, high in (("month", 1, 12), ("day_of_week_iso", 1, 7)):
        invalid = pc.or_(pc.less(batch[name], low), pc.greater(batch[name], high))
        if pc.any(pc.fill_null(invalid, False)).as_py():
            raise ValueError(f"Невалидное значение {name}")
    return batch.sort_by([("date", "ascending")])


def write_prepared(config, catalog, batch: pa.Table) -> dict:
    """Перезаписать весь выбранный календарь и сверить строки с подготовленным batch."""
    from pyiceberg.expressions import AlwaysTrue

    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет Iceberg таблицы {identifier}: сначала применить миграцию")
    table = catalog.load_table(identifier)
    calendar_id = config["source"]["calendar_id"]
    prepared = validate_batch(batch, table.schema().as_arrow(), calendar_id)
    scope = AlwaysTrue()
    # Не overwrite_partitions: исчезнувшая дата могла быть в отдельном старом месяце.
    table.overwrite(prepared, overwrite_filter=scope)
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("После записи календаря отсутствует Iceberg snapshot")
    snapshot_id = snapshot.snapshot_id
    actual = table.scan(snapshot_id=snapshot_id, row_filter=scope).to_arrow().sort_by(
        [("date", "ascending")]
    )
    if not actual.equals(prepared, check_metadata=False):
        raise RuntimeError("Записанный календарь не совпал с полным подготовленным захватом")
    # Read-back проверил наш commit; current мог смениться во время чтения.
    table.refresh()
    current = table.current_snapshot()
    if current is None or current.snapshot_id != snapshot_id:
        raise RuntimeError("Iceberg snapshot календаря изменился во время read-back")
    moment = actual["ingested_at"][0].as_py()
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    # Только описание проверенной записи. Оно не заменяет DQ или блокировку write + DQ.
    return {
        "status": "written",
        "calendar_id": calendar_id,
        "rows_written": actual.num_rows,
        "snapshot_id": snapshot_id,
        "table_uuid": str(table.metadata.table_uuid),
        "source_manifest_id": actual["source_manifest_id"][0].as_py(),
        "ingested_at": moment.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        "date_min": actual["date"][0].as_py().isoformat(),
        "date_max": actual["date"][-1].as_py().isoformat(),
    }


def load_calendar(config, catalog, *, source_manifest_id: str,
                  ingested_at: datetime, query_dataframe=None) -> dict:
    """Общая загрузка полного справочника для scheduled и manual запусков."""
    batch = extract_prepared(config, catalog, source_manifest_id=source_manifest_id,
                             ingested_at=ingested_at, query_dataframe=query_dataframe)
    return write_prepared(config, catalog, batch)
