"""Прочитать полный exact seller snapshot через Trino без latest или фильтра SKU."""

from contextlib import closing
from datetime import date, datetime
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.compute as pc

from dq.config import trino_catalog_alias
from dq.tests import quote_identifier

from .preparation import target_ref


def table_ref(config, repo_root):
    table = config["table"]
    target_ref(config, table["catalog"])
    alias = trino_catalog_alias(Path(repo_root), table["catalog"])
    return ".".join(quote_identifier(value) for value in (alias, table["schema"], table["name"]))


def source_sql(source, repo_root, bound, schema):
    snapshot = bound["receipt"].get("snapshot_id")
    if type(snapshot) is not int or not 0 < snapshot <= 2**63 - 1:
        raise ValueError("Нужен положительный exact seller snapshot ID")
    columns = ", ".join(quote_identifier(name) for name in schema.names)
    return f'SELECT {columns} FROM {table_ref(source, repo_root)} FOR VERSION AS OF {snapshot} ORDER BY "seller_id"'


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
        rows = cursor.fetchmany(1)
        if not isinstance(rows, (list, tuple)) or rows:
            raise ValueError("Metadata LIMIT 0 должен вернуть пустой результат")


def read_seller(source, repo_root, connection, bound, schema, *, max_batch_rows, max_batch_bytes):
    expected = bound["receipt"].get("rows_written")
    if any(type(value) is not int or value <= 0 for value in (expected, max_batch_rows, max_batch_bytes)):
        raise ValueError("Нужны положительные seller count и лимиты порций")
    seen, previous, batches = 0, 0, []
    with closing(connection.cursor()) as cursor:
        cursor.execute(source_sql(source, repo_root, bound, schema))
        validate_description(cursor.description, schema)
        while True:
            records = cursor.fetchmany(max_batch_rows)
            if not isinstance(records, (list, tuple)) or len(records) > max_batch_rows:
                raise ValueError("Неверный размер/форма seller порции")
            if not records:
                break
            if any(not isinstance(row, (list, tuple)) or len(row) != len(schema) for row in records):
                raise ValueError("Неверная форма seller строки")
            arrays = []
            for position, field in enumerate(schema):
                values = [row[position] for row in records]
                for value in values:
                    if value is None:
                        valid = field.nullable
                    elif field.type == pa.date32():
                        valid = type(value) is date
                    elif field.type == pa.int64():
                        valid = type(value) is int and 0 < value <= 2**63 - 1
                    elif field.type == pa.bool_():
                        valid = type(value) is bool
                    elif field.type == pa.timestamp("us"):
                        valid = type(value) is datetime and value.utcoffset() is None
                    else:
                        valid = type(value) is str
                    if not valid:
                        raise ValueError(f"Неверное native seller.{field.name}")
                arrays.append(pa.array(values, type=field.type, safe=True))
            batch = pa.Table.from_arrays(arrays, schema=schema)
            ids = batch["seller_id"]
            if (ids[0].as_py() <= previous or batch.num_rows > 1
                    and pc.any(pc.less_equal(ids.slice(1), ids.slice(0, batch.num_rows - 1))).as_py()):
                raise ValueError("Повтор/порядок seller ID нарушен")
            previous = ids[-1].as_py()
            seen += batch.num_rows
            if seen > expected or batch.nbytes > max_batch_bytes:
                raise ValueError("Seller count/порция превышает лимит")
            batches.append(batch)
        if seen != expected:
            raise ValueError("Неполный seller snapshot")
    return pa.concat_tables(batches)
