"""Атомарно заменить полное дерево и сверить все строки записанного snapshot."""

from datetime import date, timezone
import re

import pyarrow as pa

from .preparation import LEVELS, target_ref, validate_schema

SORT = [("level_code", "ascending"), ("node_id", "ascending")]


def validate_batch(batch, schema, *, capture_date, catalog_version, source_snapshot_id,
                   expected_nodes, source_contract_version):
    """Проверить структуру дерева; полноту относительно SKU доказывает source reader."""
    validate_schema(schema)
    if not isinstance(batch, pa.Table) or not batch.schema.equals(schema, check_metadata=False):
        raise ValueError("Arrow batch не соответствует схеме Iceberg tree")
    if type(capture_date) is not date or any(type(value) is not int or not 0 < value <= 2**63 - 1
                                           for value in (source_snapshot_id, expected_nodes)):
        raise ValueError("Нужны source date, положительные snapshot ID и node count")
    if batch.num_rows != expected_nodes:
        raise ValueError("Дерево не совпало с ожидаемым node count")
    if any(not isinstance(value, str) or not value.strip() for value in (catalog_version, source_contract_version)):
        raise ValueError("Нужны версии каталога и source contract")
    batch.validate(full=True)
    for field in schema:
        if not field.nullable and batch[field.name].null_count:
            raise ValueError(f"NULL в обязательном поле {field.name}")
    constants = {}
    for name in ("date", "catalog_version", "catalog_sku_snapshot_id", "source_contract_version",
                 "source_manifest_id", "ingested_at"):
        values = batch[name].unique().to_pylist()
        if len(values) != 1:
            raise ValueError(f"Смешанные значения {name} внутри полного дерева")
        constants[name] = values[0]
    if (constants["date"] != capture_date or constants["catalog_version"] != catalog_version
            or constants["catalog_sku_snapshot_id"] != source_snapshot_id
            or constants["source_contract_version"] != source_contract_version):
        raise ValueError("Дерево не соответствует metadata выбранного source capture/контракта")
    if not constants["source_manifest_id"].strip():
        raise ValueError("Нужен source manifest")
    nodes = {}
    for row in batch.select(["node_id", "level", "level_code", "parent_id", "is_passthrough"]).to_pylist():
        node, code = row["node_id"], row["level_code"]
        if node in nodes:
            raise ValueError("Повтор node_id")
        if not 0 <= code < len(LEVELS) or row["level"] != LEVELS[code]:
            raise ValueError("Уровень не соответствует level_code")
        if code == 0:
            if node != "market" or row["parent_id"] is not None or row["is_passthrough"]:
                raise ValueError("Неверный корень дерева")
            number = None
        else:
            match = re.fullmatch(rf"{LEVELS[code]}:([1-9][0-9]*)", node)
            if match is None or int(match[1]) > 2**63 - 1:
                raise ValueError("Невалидный canonical node_id")
            number = int(match[1])
        nodes[node] = (code, row["parent_id"], row["is_passthrough"], number)
    if "market" not in nodes:
        raise ValueError("Нет корня market")
    for node, (code, parent, passthrough, number) in nodes.items():
        if code == 0:
            continue
        if parent not in nodes or nodes[parent][0] != code - 1:
            raise ValueError("Родитель должен существовать на предыдущем уровне")
        if passthrough != (code > 1 and number == nodes[parent][3]):
            raise ValueError("is_passthrough не соответствует category ID родителя")
    # Каждый материализованный узел должен входить хотя бы в один полный SKU-путь.
    visited = set()
    for leaf, values in nodes.items():
        if values[0] == len(LEVELS) - 1:
            node = leaf
            while node is not None:
                visited.add(node)
                node = nodes[node][1]
    if visited != set(nodes):
        raise ValueError("Есть узлы без полного пути до leaf")
    return batch.sort_by(SORT)


def preflight(config, catalog):
    """Проверить существующую схему/partition до чтения SKU-среза."""
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
        raise ValueError("Catalog tree требует identity partition по date")
    return table


def write_prepared(config, catalog, batch, *, capture_date, catalog_version, source_snapshot_id,
                   expected_nodes, verify_source, expected_metadata_location=None):
    """Обязательная проверка exact SKU input перед commit; written ещё не passed DQ."""
    from pyiceberg.expressions import AlwaysTrue

    if not callable(verify_source):
        raise ValueError("Нужна повторная проверка source snapshot перед commit")
    if expected_metadata_location is not None and (not isinstance(expected_metadata_location, str)
                                                   or not expected_metadata_location.strip()):
        raise ValueError("Нужна точная metadata location исходного target preflight")
    table = preflight(config, catalog)
    if expected_metadata_location is not None and table.metadata_location != expected_metadata_location:
        raise ValueError("Целевая таблица изменилась во время чтения SKU-среза")
    prepared = validate_batch(batch, table.schema().as_arrow(), capture_date=capture_date,
                              catalog_version=catalog_version, source_snapshot_id=source_snapshot_id,
                              expected_nodes=expected_nodes, source_contract_version=config["source"]["contract_version"])
    properties = {name: prepared[name][0].as_py()
                  for name in ("catalog_version", "source_manifest_id", "source_contract_version")}
    properties["catalog_sku_snapshot_id"] = str(source_snapshot_id)
    properties["ingested_at"] = prepared["ingested_at"][0].as_py().replace(tzinfo=timezone.utc).isoformat(
        timespec="microseconds")
    with table.transaction() as transaction:
        transaction.overwrite(prepared, overwrite_filter=AlwaysTrue(), snapshot_properties=properties)
        if verify_source() is not True:
            raise ValueError("Source snapshot изменился или не подтверждён перед commit")
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("Нет snapshot после записи дерева")
    actual = table.scan(snapshot_id=snapshot.snapshot_id).to_arrow().sort_by(SORT)
    if not actual.equals(prepared, check_metadata=False):
        raise RuntimeError("Записанное дерево не совпало с полным подготовленным срезом")
    table.refresh()
    current = table.current_snapshot()
    if current is None or current.snapshot_id != snapshot.snapshot_id:
        raise RuntimeError("Snapshot дерева изменился во время read-back")
    day = capture_date.isoformat()
    return {"status": "written", "rows_written": actual.num_rows,
            "snapshot_id": snapshot.snapshot_id, "table_uuid": str(table.metadata.table_uuid),
            "date_min": day, "date_max": day, **properties,
            "catalog_sku_snapshot_id": source_snapshot_id}
