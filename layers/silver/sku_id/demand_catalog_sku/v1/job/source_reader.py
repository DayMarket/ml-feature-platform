"""Прочитать полные raw SKU/category/MDM captures с проверкой native CH типов."""

from datetime import datetime, timedelta
from itertools import chain
import re
from uuid import UUID

import pyarrow as pa
import pyarrow.compute as pc

from .category_paths import CATEGORY_FIELDS
from .preparation import SOURCE_FIELDS
from .query import capture_query, source_ref

FIELDS = {"sku": SOURCE_FIELDS, "category": CATEGORY_FIELDS,
          "golden": ("golden_sku_id", "is_merged", "merged_into"),
          "active_links": ("meta_sku_id", "golden_sku_id", "link_provenance", "meta_present", "meta_source", "source_sku_id")}


def audit_query(config, kind):
    if kind in ("sku", "category"):
        return ("SELECT count(), uniqExact(id), countIf(id = 0 OR id > 9223372036854775807) FROM "
                + source_ref(config, kind) + " SETTINGS max_threads=1, max_execution_time=300")
    if kind == "golden":
        return ("SELECT count(), uniqExact(golden_sku_id), countIf(golden_sku_id = "
                "toUUID('00000000-0000-0000-0000-000000000000') OR is_merged NOT IN (0,1)) FROM "
                + source_ref(config, "golden") + " FINAL SETTINGS max_threads=1, max_execution_time=300")
    if kind == "active_links":
        source_ref(config, "meta_sku")
        dictionary = config["source"]["meta_sku"]
        exists = f"dictHas('{dictionary}', meta_sku_id)"
        return (f"SELECT count(), countIf({exists} AND dictGetString('{dictionary}', 'source', meta_sku_id) = 'uzum'), "
                f"countIf(NOT {exists}) FROM {source_ref(config, 'golden_links')} WHERE deleted_at IS NULL "
                "SETTINGS max_threads=1, max_execution_time=300")
    raise ValueError("Неизвестный вид source audit")


def read_counts(config, client):
    counts = {}
    for kind in FIELDS:
        rows = client.execute(audit_query(config, kind))
        if (not isinstance(rows, (list, tuple)) or len(rows) != 1 or not isinstance(rows[0], (list, tuple))
                or len(rows[0]) != 3 or any(type(value) is not int or not 0 <= value <= 2**63 - 1 for value in rows[0])):
            raise ValueError(f"Неверная форма/count source audit {kind}")
        count, distinct_or_uzum, invalid = rows[0]
        if count <= 0 or invalid != 0:
            raise ValueError(f"Пустой/недопустимый source {kind}; orphan meta блокирует active_links")
        if kind == "active_links":
            if not 0 < distinct_or_uzum <= count:
                raise ValueError("Нет полного положительного Uzum links count")
            counts["uzum_links"] = distinct_or_uzum
        elif distinct_or_uzum != count:
            raise ValueError(f"Повторные ключи source {kind}")
        counts[kind] = count
    return counts


def _column_type(kind, name, raw_type):
    if not isinstance(raw_type, str):
        raise ValueError("Нужны строковые CH-типы")
    nullable, dtype = False, raw_type
    while dtype.startswith(("Nullable(", "LowCardinality(")) and dtype.endswith(")"):
        wrapper, dtype = dtype.split("(", 1)
        nullable |= wrapper == "Nullable"
        dtype = dtype[:-1]
    if kind == "sku":
        required = ("UInt64" if name == "sku_id" else "Int64" if name in SOURCE_FIELDS[1:5]
                    else "datetime" if name == "sku_created_at" else "String")
    elif kind == "category":
        required = "String" if name in ("l1_title", "leaf_title") else "UInt64"
    elif name in ("golden_sku_id", "meta_sku_id", "merged_into"):
        required = "UUID"
    elif name in ("is_merged", "meta_present"):
        required = "UInt8"
    else:
        required = "String"
    valid = bool(re.fullmatch(r"DateTime64\(6,\s*'UTC'\)", dtype)) if required == "datetime" else dtype == required
    if not valid:
        raise ValueError(f"Несовместимый CH type {kind}.{name}: {raw_type}")
    types = {"UInt64": (pa.uint64(), int), "Int64": (pa.int64(), int), "UInt8": (pa.uint8(), int),
             "String": (pa.string(), str), "UUID": (pa.string(), UUID),
             "datetime": (pa.timestamp("us", "UTC"), datetime)}
    return *types[required], nullable


def source_arrow(kind, rows, columns):
    names = FIELDS.get(kind)
    if (names is None or not isinstance(columns, (list, tuple)) or len(columns) != len(names)
            or any(not isinstance(item, (list, tuple)) or len(item) != 2 for item in columns)
            or tuple(item[0] for item in columns) != names):
        raise ValueError(f"Неверные native source колонки {kind}")
    if any(not isinstance(row, (list, tuple)) or len(row) != len(names) for row in rows):
        raise ValueError("Source строка не соответствует header")
    arrays = []
    for position, (name, raw_type) in enumerate(columns):
        dtype, native, nullable = _column_type(kind, name, raw_type)
        values = [row[position] for row in rows]
        for value in values:
            if value is None:
                if not nullable:
                    raise ValueError(f"NULL противоречит source type {name}")
                continue
            if type(value) is not native:
                raise ValueError(f"Неверное native значение {kind}.{name}")
            if native is datetime and value.utcoffset() != timedelta(0):
                raise ValueError("Source timestamp должен явно приходить в UTC")
        if native is UUID:
            values = [str(value) if value is not None else None for value in values]
        arrays.append(pa.array(values, type=dtype, safe=True))
    return pa.Table.from_arrays(arrays, names=names)


def read_metadata(config, client):
    result = {}
    for kind in FIELDS:
        response = client.execute(capture_query(config, kind, metadata_only=True), with_column_types=True)
        if (not isinstance(response, (list, tuple)) or len(response) != 2
                or not isinstance(response[0], (list, tuple)) or response[0]):
            raise ValueError("Source LIMIT 0 должен вернуть только metadata")
        source_arrow(kind, [], response[1])
        result[kind] = list(map(tuple, response[1]))
    return result


def read_batches(config, client, kind, *, expected_rows, columns, max_batch_rows, max_batch_bytes):
    if any(type(value) is not int or value <= 0 for value in (expected_rows, max_batch_rows, max_batch_bytes)):
        raise ValueError("Нужны положительные count и лимиты порций")
    source_arrow(kind, [], columns)
    stream = iter(client.execute_iter(capture_query(config, kind), with_column_types=True,
                                      chunk_size=max_batch_rows,
                                      settings={"max_block_size": max_batch_rows}))
    seen, previous = 0, None
    try:
        first = next(stream, None)
        if not isinstance(first, list) or not first:
            raise ValueError("Source stream не вернул metadata")
        header, rows = first[0], first[1:]
        source_arrow(kind, [], header)
        if list(map(tuple, header)) != list(map(tuple, columns)):
            raise ValueError("Source metadata изменилась после preflight")
        for rows in chain((rows,), stream):
            if not rows:
                continue
            batch = source_arrow(kind, rows, columns)
            if batch.nbytes > max_batch_bytes:
                raise ValueError("Превышен лимит Arrow порции, усечение запрещено")
            if kind in ("sku", "category"):
                ids = batch[FIELDS[kind][0]]
                if ids.null_count or pc.min(ids).as_py() <= 0 or pc.max(ids).as_py() > 2**63 - 1:
                    raise ValueError("Неверный положительный source ID")
                if ((previous is not None and ids[0].as_py() <= previous)
                        or batch.num_rows > 1 and pc.any(pc.less_equal(ids.slice(1), ids.slice(0, batch.num_rows - 1))).as_py()):
                    raise ValueError("Повторный/неупорядоченный source ID")
                previous = ids[-1].as_py()
            seen += batch.num_rows
            if seen > expected_rows:
                raise ValueError("Source строк больше независимого count")
            yield batch
        if seen != expected_rows:
            raise ValueError("Неполный source capture")
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()


def capture_all(config, client, counts, metadata, *, max_batch_rows, max_batch_bytes):
    output = {}
    for kind in FIELDS:
        stream = read_batches(config, client, kind, expected_rows=counts[kind], columns=metadata[kind],
                              max_batch_rows=max_batch_rows, max_batch_bytes=max_batch_bytes)
        try:
            output[kind] = pa.concat_tables(list(stream))
        finally:
            stream.close()
    return output


def verify_captures(config, client, captures, counts, metadata, *, max_batch_rows, max_batch_bytes):
    """Полное повторное сравнение raw содержимого, не только counts; это не snapshot isolation CH."""
    if read_counts(config, client) != counts:
        return False
    for kind in FIELDS:
        stream = read_batches(config, client, kind, expected_rows=counts[kind], columns=metadata[kind],
                              max_batch_rows=max_batch_rows, max_batch_bytes=max_batch_bytes)
        offset = 0
        try:
            for batch in stream:
                if not batch.equals(captures[kind].slice(offset, batch.num_rows), check_metadata=False):
                    return False
                offset += batch.num_rows
        finally:
            stream.close()
    return read_counts(config, client) == counts
