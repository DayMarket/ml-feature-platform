"""Атомарно заменить полный seller-каталог и сверить точный записанный snapshot."""

from datetime import timezone

import pyarrow as pa

from .preparation import SOURCE_FIELDS, prepare_catalog, target_ref, validate_schema


def validate_batch(batch, schema, *, expected_source_rows, source_contract_version):
    """Повторно проверить provenance и вывод master из сохранённых исходных полей."""
    validate_schema(schema)
    if not isinstance(batch, pa.Table) or not batch.schema.equals(schema, check_metadata=False):
        raise ValueError("Arrow batch не соответствует схеме Iceberg seller-каталога")
    if type(expected_source_rows) is not int or expected_source_rows <= 0 or batch.num_rows != expected_source_rows:
        raise ValueError("Нужен непустой полный каталог, совпадающий с независимым source count")
    batch.validate(full=True)
    for field in schema:
        if not field.nullable and batch[field.name].null_count:
            raise ValueError(f"NULL в обязательном поле {field.name}")
    constants = {}
    for name in ("date", "catalog_version", "source_contract_version", "source_manifest_id", "ingested_at"):
        values = batch[name].unique().to_pylist()
        if len(values) != 1:
            raise ValueError(f"Смешанные значения {name} внутри полного каталога")
        constants[name] = values[0]
    if (not isinstance(source_contract_version, str) or not source_contract_version.strip()
            or constants["source_contract_version"] != source_contract_version):
        raise ValueError("Версия source contract не совпала с config")
    raw = batch.select(SOURCE_FIELDS)
    raw = raw.set_column(raw.schema.get_field_index("seller_registered_at"), "seller_registered_at",
                         raw["seller_registered_at"].cast(pa.timestamp("us", "UTC"), safe=True))
    expected = prepare_catalog(
        raw, schema, expected_source_rows=expected_source_rows,
        catalog_version=constants["catalog_version"], source_manifest_id=constants["source_manifest_id"],
        source_contract_version=source_contract_version,
        ingested_at=constants["ingested_at"].replace(tzinfo=timezone.utc),
    )
    ordered = batch.sort_by([("seller_id", "ascending")])
    if not ordered.equals(expected, check_metadata=False):
        raise ValueError("Дата захвата или master-поля не соответствуют исходным данным")
    return ordered


def preflight(config, catalog):
    """Проверить существующую целевую схему до извлечения полного источника."""
    from pyiceberg.transforms import IdentityTransform

    identifier = target_ref(config, catalog.name)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {config['table']['catalog']}.{identifier[0]}.{identifier[1]} "
                         f"в {type(catalog).__name__}: сначала применить миграцию")
    table = catalog.load_table(identifier)
    validate_schema(table.schema().as_arrow())
    fields = table.spec().fields
    if (len(fields) != 1 or fields[0].source_id != table.schema().find_field("date").field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError("Catalog seller требует identity partition по date")
    return table


def write_prepared(config, catalog, batch, *, expected_source_rows, verify_source,
                   expected_metadata_location=None):
    """Receipt written не заменяет DQ; verify_source обязателен перед атомарным commit."""
    from pyiceberg.expressions import AlwaysTrue

    if not callable(verify_source):
        raise ValueError("Нужна повторная проверка source capture перед commit")
    if expected_metadata_location is not None and (not isinstance(expected_metadata_location, str)
                                                   or not expected_metadata_location.strip()):
        raise ValueError("Нужна точная metadata location исходного target preflight")
    table = preflight(config, catalog)
    if expected_metadata_location is not None and table.metadata_location != expected_metadata_location:
        raise ValueError("Целевая таблица изменилась во время извлечения источника")
    prepared = validate_batch(batch, table.schema().as_arrow(), expected_source_rows=expected_source_rows,
                              source_contract_version=config["source"]["contract_version"])
    properties = {name: prepared[name][0].as_py()
                  for name in ("catalog_version", "source_manifest_id", "source_contract_version")}
    properties["ingested_at"] = prepared["ingested_at"][0].as_py().replace(tzinfo=timezone.utc).isoformat(
        timespec="microseconds")
    # Замена всей таблицы удаляет и старые партиции исчезнувших продавцов.
    with table.transaction() as transaction:
        transaction.overwrite(prepared, overwrite_filter=AlwaysTrue(), snapshot_properties=properties)
        if verify_source() is not True:
            raise ValueError("Source capture изменился или не подтверждён перед commit")
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("Нет snapshot после записи seller-каталога")
    actual = table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().sort_by([("seller_id", "ascending")])
    if not actual.equals(prepared, check_metadata=False):
        raise RuntimeError("Записанный seller-каталог не совпал с полным подготовленным захватом")
    table.refresh()
    current = table.current_snapshot()
    if current is None or current.snapshot_id != snapshot.snapshot_id:
        raise RuntimeError("Snapshot seller-каталога изменился во время read-back")
    day = prepared["date"][0].as_py().isoformat()
    return {"status": "written", "rows_written": actual.num_rows,
            "snapshot_id": snapshot.snapshot_id, "table_uuid": str(table.metadata.table_uuid),
            "date_min": day, "date_max": day, **properties}
