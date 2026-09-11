"""Построить полное дерево из нормализованных путей одного проверяемого SKU-среза."""

from datetime import date, datetime, timezone
from hashlib import sha1
import re

import pyarrow as pa

LEVELS = ("market", "l1", "l2", "l3", "l4", "l5", "leaf")
INPUT_COLUMNS = ("date", "sku_id", "catalog_version", "category_path_status", *LEVELS)


def target_ref(config, catalog_name):
    table = config["table"]
    if any(not isinstance(table.get(key), str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table[key])
           for key in ("catalog", "schema", "name")):
        raise ValueError("Нужны отдельные компоненты Iceberg identifier")
    if table["catalog"] != catalog_name:
        raise ValueError("Другой Iceberg каталог")
    return table["schema"], table["name"]


def expected_schema():
    fields = [("date", pa.date32(), False), ("level", pa.string(), False), ("node_id", pa.string(), False),
              ("level_code", pa.int32(), False), ("parent_id", pa.string(), True),
              ("is_passthrough", pa.bool_(), False), ("catalog_version", pa.string(), False),
              ("catalog_sku_snapshot_id", pa.int64(), False), ("source_contract_version", pa.string(), False),
              ("source_manifest_id", pa.string(), False), ("ingested_at", pa.timestamp("us"), False)]
    return pa.schema([pa.field(name, kind, nullable=nullable) for name, kind, nullable in fields])


def validate_schema(schema):
    expected = expected_schema()
    if len(schema) != len(expected) or set(schema.names) != set(expected.names):
        raise ValueError("Tree требует 11 согласованных полей")
    for field in expected:
        actual = schema.field(field.name)
        same = actual.type == field.type or pa.types.is_string(field.type) and pa.types.is_large_string(actual.type)
        if not same or actual.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable tree.{field.name}")


def prepare_tree(batches, schema, *, capture_date, catalog_version, source_snapshot_id,
                 expected_source_rows, source_manifest_id, source_contract_version, ingested_at):
    """Не подтверждает DQ snapshot: binding точного input выполняет runtime."""
    validate_schema(schema)
    if type(capture_date) is not date or any(type(v) is not int or not 0 < v <= 2**63 - 1
                                           for v in (source_snapshot_id, expected_source_rows)):
        raise ValueError("Нужны source date, положительные snapshot ID и count")
    if any(not isinstance(value, str) or not value.strip()
           for value in (catalog_version, source_manifest_id, source_contract_version)):
        raise ValueError("Нужны версии и source manifest")
    if not isinstance(ingested_at, datetime) or ingested_at.utcoffset() is None:
        raise ValueError("Нужно aware время материализации")
    nodes, paths = {}, set()
    seen, missing, previous = 0, 0, 0
    stream = iter(batches)
    try:
        for batch in stream:
            if not isinstance(batch, pa.Table) or not set(INPUT_COLUMNS).issubset(batch.column_names):
                raise ValueError("Неполный состав полей исходного SKU-каталога")
            if len(batch.column_names) != len(set(batch.column_names)):
                raise ValueError("Повтор исходной колонки")
            for name in INPUT_COLUMNS:
                kind = batch.schema.field(name).type
                valid = (kind == pa.date32() if name == "date" else kind == pa.int64() if name == "sku_id"
                         else pa.types.is_string(kind) or pa.types.is_large_string(kind))
                if not valid:
                    raise ValueError(f"Неверный тип input.{name}")
            for row in batch.select(INPUT_COLUMNS).to_pylist():
                sku = row["sku_id"]
                if sku is None or sku <= previous:
                    raise ValueError("SKU должны быть положительными и строго упорядоченными без повторов")
                previous = sku
                seen += 1
                if seen > expected_source_rows:
                    raise ValueError("SKU больше source count")
                if row["date"] != capture_date or row["catalog_version"] != catalog_version:
                    raise ValueError("Смешанные source capture/version")
                path = tuple(row[level] for level in LEVELS)
                if row["category_path_status"] == "missing":
                    if any(value is not None for value in path):
                        raise ValueError("Missing category не должна иметь нормализованный путь")
                    missing += 1
                    continue
                if row["category_path_status"] != "valid":
                    raise ValueError("Конфликт или неизвестный статус пути")
                if path in paths:
                    continue
                if path[0] != "market":
                    raise ValueError("Нужен единственный корень market")
                ids = [None]
                for level, node in zip(LEVELS[1:], path[1:], strict=True):
                    match = re.fullmatch(rf"{level}:([1-9][0-9]*)", node or "")
                    if match is None or int(match[1]) > 2**63 - 1:
                        raise ValueError("Невалидный canonical node_id")
                    ids.append(int(match[1]))
                for code, (level, node) in enumerate(zip(LEVELS, path, strict=True)):
                    parent = None if code == 0 else path[code - 1]
                    passthrough = code > 1 and ids[code] == ids[code - 1]
                    attrs = (level, code, parent, passthrough)
                    if node in nodes and nodes[node] != attrs:
                        raise ValueError("Один узел имеет нескольких родителей или противоречивые атрибуты")
                    nodes[node] = attrs
                paths.add(path)
        if seen != expected_source_rows:
            raise ValueError("Неполный поток исходного SKU-каталога")
        if not nodes:
            raise ValueError("Нет валидных категорий, пустой tree не публикуется")
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()
    constants = dict(date=capture_date, catalog_version=catalog_version, catalog_sku_snapshot_id=source_snapshot_id,
                     source_manifest_id=source_manifest_id, source_contract_version=source_contract_version,
                     ingested_at=ingested_at.astimezone(timezone.utc).replace(tzinfo=None))
    rows = [{**constants, "node_id": node, "level": values[0], "level_code": values[1],
             "parent_id": values[2], "is_passthrough": values[3]} for node, values in nodes.items()]
    rows.sort(key=lambda row: (row["level_code"], row["node_id"]))
    output = pa.Table.from_pylist(rows, schema=schema)
    pairs = "|".join(f"{row['node_id']}>{row['parent_id']}" for row in rows)
    audit = {"source_sku_rows": seen, "missing_category_sku": missing, "valid_category_sku": seen - missing,
             "n_nodes": len(rows), "nodes_by_level": {level: sum(row["level"] == level for row in rows) for level in LEVELS},
             "edges_sha1": sha1(pairs.encode()).hexdigest()}
    return output, audit
