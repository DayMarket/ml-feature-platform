"""Читать отсортированные silver-дни порциями Trino из точных Iceberg snapshots."""

from contextlib import closing
from datetime import date, datetime, timezone
from decimal import Decimal
import math
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.compute as pc

from dq.config import trino_catalog_alias
from dq.day_range import capture_time
from dq.tests import quote_identifier

from .preparation import SOURCE_FIELDS, target_ref, validate_source_schema


def snapshot_ref(source, repo_root, version, day):
    target_ref(source, source["table"]["catalog"])
    snapshot = version.get("snapshot_id")
    if type(day) is not date or type(snapshot) is not int or snapshot <= 0:
        raise ValueError("Нужны DATE и точный snapshot входа")
    table = source["table"]
    alias = trino_catalog_alias(Path(repo_root), table["catalog"])
    ref = ".".join(quote_identifier(v) for v in (alias, table["schema"], table["name"]))
    return f"{ref} FOR VERSION AS OF {snapshot}", f'"date" = DATE \'{day.isoformat()}\''


def source_sql(kind, source, repo_root, version, day):
    ref, scope = snapshot_ref(source, repo_root, version, day)
    columns = ", ".join(quote_identifier(name) for name in SOURCE_FIELDS[kind])
    return f'SELECT {columns}\nFROM {ref}\nWHERE {scope}\nORDER BY "sku_id"'


def union_count_sql(sources, repo_root, inputs, day):
    parts = []
    for kind in ("sales", "stock"):
        ref, scope = snapshot_ref(sources[kind], repo_root, inputs[kind], day)
        parts.append(f'SELECT "sku_id" FROM {ref} WHERE {scope}')
    return 'SELECT count(*) AS rows_expected FROM (\n' + '\nUNION\n'.join(parts) + '\n) AS keys'


def read_union_count(connection, sources, repo_root, inputs, day):
    with closing(connection.cursor()) as cursor:
        cursor.execute(union_count_sql(sources, repo_root, inputs, day))
        rows = cursor.fetchmany(2)
        if (not isinstance(rows, (list, tuple)) or len(rows) != 1
                or not isinstance(rows[0], (list, tuple)) or len(rows[0]) != 1
                or type(rows[0][0]) is not int or rows[0][0] <= 0):
            raise ValueError("Нет положительного count объединения")
        total = rows[0][0]
        counts = [inputs[kind]["day_receipt"]["rows_written"] for kind in ("sales", "stock")]
        if not max(counts) <= total <= sum(counts):
            raise ValueError("Count объединения несовместим с проверенными silver counts")
        return total


def trino_type_matches(dtype, value):
    if not isinstance(value, str):
        return False
    value = value.lower().replace(" ", "")
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return re.fullmatch(r"varchar(?:\(\d+\))?", value) is not None
    if pa.types.is_timestamp(dtype):
        return dtype.unit == "us" and dtype.tz is None and value == "timestamp(6)"
    mapping = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint", pa.float64(): "double",
               pa.bool_(): "boolean", pa.decimal128(38, 0): "decimal(38,0)"}
    return mapping.get(dtype) == value


def validate_description(description, schema, columns):
    if not isinstance(description, (list, tuple)) or len(description) != len(columns):
        raise ValueError("Нет полной Trino metadata silver")
    for column, field in zip(columns, description, strict=True):
        if (not isinstance(field, (list, tuple)) or len(field) != 7 or field[0] != column
                or not trino_type_matches(schema.field(column).type, field[1])):
            raise ValueError(f"Неверный Trino тип/порядок {column}")


def exact_value(value, field):
    if value is None:
        if not field.nullable:
            raise ValueError(f"NULL обязательного silver поля {field.name}")
        return value
    dtype = field.type
    if pa.types.is_decimal(dtype):
        valid = isinstance(value, Decimal) and value.is_finite()
    elif pa.types.is_integer(dtype):
        valid = type(value) is int
    elif pa.types.is_floating(dtype):
        valid = type(value) is float and math.isfinite(value)
    elif pa.types.is_timestamp(dtype):
        valid = isinstance(value, datetime) and value.utcoffset() is None
    elif pa.types.is_date(dtype):
        valid = type(value) is date
    elif pa.types.is_boolean(dtype):
        valid = type(value) is bool
    else:
        valid = isinstance(value, str)
    if not valid:
        raise ValueError(f"Нельзя неявно преобразовать значение silver.{field.name}")
    return value


def read_batches(connection, kind, source, repo_root, version, schema, *, day,
                 max_batch_rows, max_batch_bytes):
    """Cursor.close отменяет незавершённый запрос; DBAPI-порция собирается по колонкам."""
    if any(type(v) is not int or v <= 0 for v in (max_batch_rows, max_batch_bytes)):
        raise ValueError("Нужны положительные лимиты входных порций")
    validate_source_schema(kind, schema)
    sql = source_sql(kind, source, repo_root, version, day)
    receipt = version["day_receipt"]
    expected = receipt.get("rows_written")
    if (receipt.get("date") != day.isoformat() or type(expected) is not int or expected <= 0
            or receipt.get("table_uuid") != version.get("table_uuid")):
        raise ValueError("Неверный дневной receipt silver")
    captured = capture_time(receipt).astimezone(timezone.utc).replace(tzinfo=None)
    columns = SOURCE_FIELDS[kind]
    previous, seen = None, 0
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        validate_description(cursor.description, schema, columns)
        while True:
            records = cursor.fetchmany(max_batch_rows)
            if not isinstance(records, (list, tuple)) or len(records) > max_batch_rows:
                raise ValueError("Неверная форма/размер Trino порции")
            if not records:
                break
            if any(not isinstance(row, (list, tuple)) or len(row) != len(columns) for row in records):
                raise ValueError("Неверная форма Trino строки")
            arrays = []
            for name, values in zip(columns, zip(*records), strict=True):
                field = schema.field(name)
                arrays.append(pa.array([exact_value(value, field) for value in values], type=field.type))
            batch = pa.Table.from_arrays(arrays, schema=pa.schema([schema.field(name) for name in columns]))
            ids = batch["sku_id"]
            if (ids.null_count or batch["date"].null_count
                    or pc.any(pc.not_equal(batch["date"], day)).as_py()
                    or pc.min(ids).as_py() <= 0 or previous is not None and ids[0].as_py() <= previous
                    or batch.num_rows > 1 and pc.any(
                        pc.less_equal(ids.slice(1), ids.slice(0, batch.num_rows - 1))).as_py()):
                raise ValueError("Неверный день/порядок/дубли silver SKU")
            for name, value in (
                ("source_manifest_id", receipt["source_manifest_id"]),
                ("source_contract_version", receipt["source_contract_version"]),
                ("ingested_at", captured),
            ):
                if batch[name].null_count or pc.any(pc.not_equal(batch[name], value)).as_py():
                    raise ValueError("Silver строки не соответствуют checked receipt")
            previous = ids[-1].as_py()
            seen += batch.num_rows
            if seen > expected:
                raise ValueError("Silver count больше проверенного")
            if batch.nbytes > max_batch_bytes:
                raise ValueError("Превышен размер входной Arrow-порции")
            yield batch
        if seen != expected:
            raise ValueError("Неполный поток silver")
