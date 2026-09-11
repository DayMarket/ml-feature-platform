"""Атомарно заменить полный SKU-каталог после проверки всех входных captures."""

from datetime import timezone

from .preparation import prepare_catalog, target_ref, validate_schema


def preflight(config, catalog):
    from pyiceberg.transforms import IdentityTransform

    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier} в {type(catalog).__name__}: сначала миграции")
    table = catalog.load_table(identifier)
    validate_schema(table.schema().as_arrow())
    fields = table.spec().fields
    if (len(fields) != 1 or fields[0].source_id != table.schema().find_field("date").field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError("SKU-каталог требует identity partition по date")
    return table


def write_catalog(config, catalog, source, *, categories, goldens, active_links, counts, seller, bound_seller,
                  source_manifest_id, ingested_at, verify_source, expected_metadata_location=None):
    """verify_source перепроверяет source captures и exact seller DQ непосредственно перед commit."""
    from pyiceberg.expressions import AlwaysTrue

    if not callable(verify_source):
        raise ValueError("Нужна повторная проверка source captures и exact seller DQ")
    if expected_metadata_location is not None and (not isinstance(expected_metadata_location, str)
                                                   or not expected_metadata_location.strip()):
        raise ValueError("Нужна точная target metadata location")
    table = preflight(config, catalog)
    if expected_metadata_location is not None and table.metadata_location != expected_metadata_location:
        raise ValueError("SKU target изменился во время извлечения источников")
    prepared, audit = prepare_catalog(source, table.schema().as_arrow(), categories=categories, goldens=goldens,
        active_links=active_links, counts=counts, seller=seller, bound_seller=bound_seller,
        source_manifest_id=source_manifest_id, source_contract_version=config["source"]["contract_version"],
        ingested_at=ingested_at)
    properties = {name: prepared[name][0].as_py()
                  for name in ("catalog_version", "source_manifest_id", "source_contract_version")}
    properties["catalog_seller_snapshot_id"] = str(bound_seller["receipt"]["snapshot_id"])
    properties["ingested_at"] = ingested_at.astimezone(timezone.utc).isoformat(timespec="microseconds")
    with table.transaction() as transaction:
        transaction.overwrite(prepared, overwrite_filter=AlwaysTrue(), snapshot_properties=properties)
        if verify_source() is not True:
            raise ValueError("Source captures/exact seller DQ изменились или не подтверждены перед commit")
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("Нет snapshot после записи SKU-каталога")
    actual = table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().sort_by([("sku_id", "ascending")])
    if not actual.equals(prepared, check_metadata=False):
        raise RuntimeError("SKU read-back не совпал с полным подготовленным каталогом")
    table.refresh()
    current = table.current_snapshot()
    if current is None or current.snapshot_id != snapshot.snapshot_id:
        raise RuntimeError("SKU snapshot изменился во время read-back")
    day = prepared["date"][0].as_py().isoformat()
    return {"status": "written", "rows_written": actual.num_rows, "snapshot_id": snapshot.snapshot_id,
            "table_uuid": str(table.metadata.table_uuid), "date_min": day, "date_max": day,
            **properties, "catalog_seller_snapshot_id": bound_seller["receipt"]["snapshot_id"],
            "source_audit": audit}
