"""Читать полный seller-sales день порциями из точного проверенного snapshot."""

from contextlib import closing
from datetime import date, datetime, timezone
from decimal import Decimal
import math
from pathlib import Path
import re

import pyarrow as pa

from dq.config import trino_catalog_alias
from dq.day_range import capture_time
from dq.tests import quote_identifier

from .preparation import target_ref


def snapshot_ref(source, repo_root, version, day):
    target_ref(source, source["table"]["catalog"])
    snapshot = version.get("snapshot_id")
    if type(day) is not date or type(snapshot) is not int or not 0 < snapshot <= 2**63 - 1:
        raise ValueError("Нужны DATE и точный snapshot входа")
    table = source["table"]
    alias = trino_catalog_alias(Path(repo_root), table["catalog"])
    ref = ".".join(quote_identifier(v) for v in (alias, table["schema"], table["name"]))
    return f"{ref} FOR VERSION AS OF {snapshot}", f'"date" = DATE \'{day.isoformat()}\''


def source_sql(source, repo_root, version, schema, day):
    ref, scope = snapshot_ref(source, repo_root, version, day)
    columns = ", ".join(quote_identifier(name) for name in schema.names)
    return f'SELECT {columns}\nFROM {ref}\nWHERE {scope}\nORDER BY "sku_id", "seller_key"'


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


def read_batches(connection, source, repo_root, version, schema, *, day,
                 max_batch_rows, max_batch_bytes):
    """Cursor.close отменяет незавершённый запрос, включая ранний выход consumer."""
    if any(type(v) is not int or v <= 0 for v in (max_batch_rows, max_batch_bytes)):
        raise ValueError("Нужны положительные лимиты входных порций")
    sql = source_sql(source, repo_root, version, schema, day)
    receipt = version["day_receipt"]
    expected = receipt.get("rows_written")
    if (receipt.get("date") != day.isoformat() or type(expected) is not int or expected <= 0
            or receipt.get("table_uuid") != version.get("table_uuid")):
        raise ValueError("Неверный дневной receipt silver")
    captured = capture_time(receipt).astimezone(timezone.utc).replace(tzinfo=None)
    columns = schema.names
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
            output = []
            for values in records:
                if not isinstance(values, (list, tuple)) or len(values) != len(columns):
                    raise ValueError("Неверная форма Trino строки")
                row = {name: exact_value(value, schema.field(name))
                       for name, value in zip(columns, values, strict=True)}
                sku, seller = row["sku_id"], row["seller_key"]
                key = (sku, seller)
                if (row["date"] != day or sku is None or sku <= 0
                        or not isinstance(seller, str) or not seller
                        or (previous is not None and key <= previous)):
                    raise ValueError("Неверный день/порядок/дубли silver SKU")
                if (any(row[key] != receipt[key] for key in ("source_manifest_id", "source_contract_version"))
                        or row["ingested_at"] != captured):
                    raise ValueError("Silver строки не соответствуют checked receipt")
                previous = key
                output.append(row)
            seen += len(output)
            if seen > expected:
                raise ValueError("Silver count больше проверенного")
            batch = pa.Table.from_pylist(output, schema=schema)
            if batch.nbytes > max_batch_bytes:
                raise ValueError("Превышен размер входной Arrow-порции")
            yield batch
        if seen != expected:
            raise ValueError("Неполный поток silver")
