"""Читать полный SKU snapshot порциями Trino с проверкой capture на каждой строке."""

from contextlib import closing
from datetime import date, datetime
from pathlib import Path
import re

import pyarrow as pa

from dq.config import trino_catalog_alias
from dq.tests import quote_identifier

from .inputs import READ_COLUMNS
from .preparation import target_ref


def table_ref(source, repo_root):
    target_ref(source, source["table"]["catalog"])
    table = source["table"]
    alias = trino_catalog_alias(Path(repo_root), table["catalog"])
    return ".".join(quote_identifier(value) for value in (alias, table["schema"], table["name"]))


def source_sql(source, repo_root, bound):
    snapshot_id = bound["receipt"].get("snapshot_id")
    if type(snapshot_id) is not int or not 0 < snapshot_id <= 2**63 - 1:
        raise ValueError("Нужен точный snapshot ID")
    columns = ", ".join(quote_identifier(name) for name in READ_COLUMNS)
    return f'SELECT {columns} FROM {table_ref(source, repo_root)} FOR VERSION AS OF {snapshot_id} ORDER BY "sku_id"'


def validate_description(description, schema):
    types = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint", pa.bool_(): "boolean",
             pa.timestamp("us"): "timestamp(6)", pa.float64(): "double"}
    if not isinstance(description, (list, tuple)) or len(description) != len(schema):
        raise ValueError("Нет полной Trino metadata")
    for item, field in zip(description, schema, strict=True):
        if not isinstance(item, (list, tuple)) or len(item) != 7 or item[0] != field.name or not isinstance(item[1], str):
            raise ValueError("Неверные колонки Trino metadata")
        kind = item[1].lower().replace(" ", "")
        valid = (re.fullmatch(r"varchar(?:\(\d+\))?", kind) is not None
                 if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
                 else types.get(field.type) == kind)
        if not valid:
            raise ValueError(f"Неверный Trino тип {field.name}")


def metadata_query(connection, sql, schema):
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        validate_description(cursor.description, schema)
        records = cursor.fetchmany(1)
        if not isinstance(records, (list, tuple)) or records:
            raise ValueError("Metadata LIMIT 0 должен вернуть пустой результат")


def read_batches(connection, source, repo_root, bound, schema, *, max_batch_rows, max_batch_bytes):
    if any(type(value) is not int or value <= 0 for value in (max_batch_rows, max_batch_bytes)):
        raise ValueError("Нужны положительные лимиты порций")
    projection = pa.schema([schema.field(name) for name in READ_COLUMNS])
    receipt = bound["receipt"]
    expected = receipt["rows_written"]
    if type(expected) is not int or expected <= 0:
        raise ValueError("Нужен положительный source count")
    captured = bound["captured_at"].replace(tzinfo=None)
    previous, seen = 0, 0
    with closing(connection.cursor()) as cursor:
        cursor.execute(source_sql(source, repo_root, bound))
        validate_description(cursor.description, projection)
        while True:
            records = cursor.fetchmany(max_batch_rows)
            if not isinstance(records, (list, tuple)) or len(records) > max_batch_rows:
                raise ValueError("Неверная форма/размер Trino порции")
            if not records:
                break
            rows = []
            for values in records:
                if not isinstance(values, (list, tuple)) or len(values) != len(projection):
                    raise ValueError("Неверная форма SKU строки")
                row = dict(zip(READ_COLUMNS, values, strict=True))
                for field, value in zip(projection, values, strict=True):
                    if value is None:
                        valid = field.nullable
                    elif field.name == "date":
                        valid = type(value) is date
                    elif field.name == "sku_id":
                        valid = type(value) is int and 0 < value <= 2**63 - 1
                    elif field.name == "ingested_at":
                        valid = isinstance(value, datetime) and value.utcoffset() is None
                    else:
                        valid = isinstance(value, str)
                    if not valid:
                        raise ValueError(f"Нельзя неявно преобразовать SKU.{field.name}")
                if row["sku_id"] <= previous or row["date"] != bound["date"]:
                    raise ValueError("Неверный день/порядок/дубли SKU")
                if (any(row[key] != receipt[key] for key in ("catalog_version", "source_manifest_id", "source_contract_version"))
                        or row["ingested_at"] != captured):
                    raise ValueError("SKU строки не соответствуют checked receipt")
                previous = row["sku_id"]
                rows.append(row)
            seen += len(rows)
            if seen > expected:
                raise ValueError("SKU count больше проверенного")
            batch = pa.Table.from_pylist(rows, schema=projection)
            if batch.nbytes > max_batch_bytes:
                raise ValueError("Превышен размер входной Arrow-порции")
            yield batch
        if seen != expected:
            raise ValueError("Неполный поток SKU")
