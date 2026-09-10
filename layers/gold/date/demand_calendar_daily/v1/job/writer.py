"""Заменить полный gold-календарь и сверить точный записанный snapshot."""

from datetime import timezone

from .preparation import FLAGS, REQUIRED, target_ref, validate_schema


def validate_batch(batch, schema):
    validate_schema(schema)
    if not batch.schema.equals(schema, check_metadata=False) or not batch.num_rows:
        raise ValueError("Нужен непустой batch по схеме target")
    for name in REQUIRED:
        if batch[name].null_count:
            raise ValueError(f"NULL в обязательном поле {name}")
    days = batch["date"].to_pylist()
    if len(set(days)) != len(days):
        raise ValueError("Повтор ключа date")
    for name in ("calendar_snapshot_id", "events_snapshot_id"):
        values = batch[name].to_pylist()
        if len(set(values)) != 1 or values[0] <= 0:
            raise ValueError(f"Неверная версия {name}")
    for name in ("source_manifest_id", "calendar_source_manifest_id", "events_source_manifest_id"):
        values = batch[name].to_pylist()
        if len(set(values)) != 1 or not values[0].strip():
            raise ValueError(f"Неверный manifest {name}")
    if len(set(batch["ingested_at"].to_pylist())) != 1:
        raise ValueError("Смешанные времена захвата")
    for row in batch.to_pylist():
        if row["calendar_id"] != "uz_official" or row["calendar_coverage_status"] != "source_row_present":
            raise ValueError("Неверный источник календаря")
        count = row["big_sale_event_count"]
        flags = [row[name] for name in FLAGS]
        if count < 0 or (count == 0 and (flags != [None] * 3 or row["promotion_coverage_status"] != "no_registry_rows")):
            raise ValueError("Неверные флаги отсутствующих записей BIG_SALE")
        if count > 0 and (None in flags or not any(flags) or sum(flags) > count
                          or row["promotion_coverage_status"] != "registry_rows_present"):
            raise ValueError("Неверные флаги записей BIG_SALE")
    return batch.sort_by([("date", "ascending")])


def write_prepared(config, catalog, batch):
    from pyiceberg.expressions import AlwaysTrue

    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier}: сначала применить миграцию")
    table = catalog.load_table(identifier)
    prepared = validate_batch(batch, table.schema().as_arrow())
    table.overwrite(prepared, overwrite_filter=AlwaysTrue())
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("Нет snapshot после записи gold")
    actual = table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().sort_by([("date", "ascending")])
    if not actual.equals(prepared, check_metadata=False):
        raise RuntimeError("Read-back gold не совпал с подготовленным календарём")
    table.refresh()
    current = table.current_snapshot()
    if current is None or current.snapshot_id != snapshot.snapshot_id:
        raise RuntimeError("Snapshot изменился во время read-back")
    moment = actual["ingested_at"][0].as_py()
    if moment.utcoffset() is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return {"status": "written", "rows_written": actual.num_rows,
            "table_uuid": str(table.metadata.table_uuid), "snapshot_id": snapshot.snapshot_id,
            "source_manifest_id": actual["source_manifest_id"][0].as_py(),
            "ingested_at": moment.astimezone(timezone.utc).isoformat(timespec="microseconds"),
            "date_min": actual["date"][0].as_py().isoformat(),
            "date_max": actual["date"][-1].as_py().isoformat()}
