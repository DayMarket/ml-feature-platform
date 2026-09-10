"""Прочитать полный seller-каталог и подтвердить содержимое перед Iceberg commit."""

from datetime import datetime, timezone
from hashlib import sha256
from itertools import chain
import json
import re

import pyarrow as pa

from .preparation import SOURCE_FIELDS, prepare_catalog, source_sql
from .writer import preflight, write_prepared


def audit_sql(config):
    source_sql(config)
    return ("SELECT count() AS rows_count, uniqExact(seller_id) AS unique_sellers, "
            "countIf(seller_id = 0 OR seller_id > 9223372036854775807) AS invalid_ids "
            "FROM `marts`.`sellers_info` SETTINGS max_threads=1, max_execution_time=300")


def read_audit(config, client):
    rows = client.execute(audit_sql(config))
    if (not isinstance(rows, (list, tuple)) or len(rows) != 1
            or not isinstance(rows[0], (list, tuple)) or len(rows[0]) != 3
            or any(type(value) is not int for value in rows[0])):
        raise ValueError("Неверный source count/audit полного seller-каталога")
    count, unique, invalid = rows[0]
    if count <= 0 or count != unique or invalid != 0:
        raise ValueError("Пустой, повторный или недопустимый seller в source audit")
    return count


def source_arrow(rows, columns):
    """Проверить реальные CH-типы, сохранив String/Bool/UTC/NULL без Pandas."""
    if (not isinstance(columns, (list, tuple)) or len(columns) != len(SOURCE_FIELDS)
            or any(not isinstance(item, (list, tuple)) or len(item) != 2 for item in columns)):
        raise ValueError("Нужны метаданные четырёх исходных seller-полей")
    names = [name for name, _ in columns]
    if (any(not isinstance(name, str) for name in names)
            or len(set(names)) != len(SOURCE_FIELDS) or set(names) != set(SOURCE_FIELDS)):
        raise ValueError("Неверные исходные seller-поля")
    if any(not isinstance(row, (list, tuple)) or len(row) != len(names) for row in rows):
        raise ValueError("Строка не совпадает с метаданными CH")
    arrays = []
    for index, (name, kind) in enumerate(columns):
        if not isinstance(kind, str):
            raise ValueError("Нужны строковые CH-типы")
        nullable = False
        while kind.startswith(("Nullable(", "LowCardinality(")) and kind.endswith(")"):
            wrapper, kind = kind.split("(", 1)
            nullable |= wrapper == "Nullable"
            kind = kind[:-1]
        values = [row[index] for row in rows]
        if not nullable and any(value is None for value in values):
            raise ValueError(f"NULL противоречит source type {name}")
        if name == "seller_id" and kind == "UInt64":
            dtype, allowed = pa.uint64(), int
        elif name == "source_master_seller_id" and kind == "String":
            dtype, allowed = pa.string(), str
        elif name == "is_1p" and kind == "Bool":
            dtype, allowed = pa.bool_(), bool
        elif name == "seller_registered_at" and re.fullmatch(r"DateTime64\(6,\s*'UTC'\)", kind):
            dtype, allowed = pa.timestamp("us", "UTC"), datetime
        else:
            raise ValueError(f"Несовместимый source type {name}: {kind}")
        if any(value is not None and type(value) is not allowed for value in values):
            raise ValueError(f"Неверные native значения {name}")
        if name == "seller_registered_at":
            if any(value is not None and value.utcoffset() is None for value in values):
                raise ValueError("Регистрация должна приходить с явно заданной зоной UTC")
        arrays.append(pa.array(values, type=dtype, safe=True))
    return pa.Table.from_arrays(arrays, names=names).select(SOURCE_FIELDS)


def limits(config):
    result = tuple(config["runtime"].get(key) for key in ("max_batch_rows", "max_batch_bytes", "max_catalog_bytes"))
    if any(type(value) is not int or value <= 0 for value in result):
        raise ValueError("Нужны положительные лимиты памяти/порций полного каталога")
    return result


def source_batches(config, client, *, expected_rows, expected_columns=None):
    """Поток ограниченных порций, без усечения каталога при достижении лимита."""
    rows_limit, bytes_limit, catalog_limit = limits(config)
    if type(expected_rows) is not int or expected_rows <= 0:
        raise ValueError("Нужен положительный независимый source count")
    stream = iter(client.execute_iter(source_sql(config), with_column_types=True, chunk_size=rows_limit,
                                      settings={"max_block_size": rows_limit}))
    seen, size, last = 0, 0, None
    try:
        first = next(stream, None)
        if not isinstance(first, list) or not first:
            raise ValueError("Source stream не вернул metadata")
        columns, records = first[0], first[1:]
        # Проверка header также для пустого/оборванного потока.
        source_arrow([], columns)
        if expected_columns is not None and list(map(tuple, columns)) != list(map(tuple, expected_columns)):
            raise ValueError("Source metadata изменилась после preflight")
        for records in chain((records,), stream):
            if not records:
                continue
            batch = source_arrow(records, columns)
            size += batch.nbytes
            if batch.nbytes > bytes_limit or size > catalog_limit:
                raise ValueError("Превышен лимит памяти seller-каталога, усечение запрещено")
            ids = batch["seller_id"].to_pylist()
            if any(value is None or value <= 0 or value > 2**63 - 1 for value in ids):
                raise ValueError("Недопустимый seller_id в потоке")
            if (any(left >= right for left, right in zip(ids, ids[1:]))
                    or last is not None and ids[0] <= last):
                raise ValueError("Повтор или неверный порядок seller_id между порциями")
            last = ids[-1]
            seen += batch.num_rows
            if seen > expected_rows:
                raise ValueError("Строк больше независимого source count")
            yield batch
        if seen != expected_rows:
            raise ValueError("Неполный поток seller-каталога")
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()


def source_digest(raw):
    """Канонический hash отсортированных raw-полей, независимый от размера CH-порций."""
    canonical = raw.select(SOURCE_FIELDS).combine_chunks()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, canonical.schema) as out:
        out.write_table(canonical)
    return sha256(sink.getvalue()).hexdigest()


def utc_now():
    return datetime.now(timezone.utc)


def validate_arguments(config, *, catalog_version, source_manifest_id):
    """Проверить параметры до открытия соединений или чтения источника."""
    limits(config)
    source_sql(config)
    version = config["source"].get("contract_version")
    if any(not isinstance(value, str) or not value.strip()
           for value in (catalog_version, source_manifest_id, version)):
        raise ValueError("Нужны catalog version, source manifest и contract version")


def load_catalog(config, catalog, client, *, catalog_version, source_manifest_id):
    """Захватить текущий каталог; исторические даты не подставляются из параметров DAG."""
    validate_arguments(config, catalog_version=catalog_version, source_manifest_id=source_manifest_id)
    _, _, catalog_limit = limits(config)
    version = config["source"]["contract_version"]
    target = preflight(config, catalog)
    sql = source_sql(config).replace(" SETTINGS ", " LIMIT 0 SETTINGS ")
    result = client.execute(sql, with_column_types=True)
    if not isinstance(result, (list, tuple)) or len(result) != 2 or result[0] != []:
        raise ValueError("Source metadata preflight должен вернуть только типы")
    metadata = source_arrow([], result[1]).schema
    expected = read_audit(config, client)
    captured = utc_now().replace(microsecond=0)
    batches = source_batches(config, client, expected_rows=expected, expected_columns=result[1])
    try:
        raw = pa.concat_tables(list(batches))
    finally:
        batches.close()
    if raw.schema != metadata:
        raise ValueError("Source schema изменилась после metadata preflight")
    prepared = prepare_catalog(raw, target.schema().as_arrow(), expected_source_rows=expected,
                               catalog_version=catalog_version, source_manifest_id=source_manifest_id,
                               source_contract_version=version, ingested_at=captured)
    if prepared.nbytes > catalog_limit:
        raise ValueError("Подготовленный каталог превышает лимит памяти")
    digest = source_digest(raw)

    def verify():
        if read_audit(config, client) != expected:
            return False
        repeated = source_batches(config, client, expected_rows=expected, expected_columns=result[1])
        offset = 0
        try:
            for batch in repeated:
                if not batch.equals(raw.slice(offset, batch.num_rows), check_metadata=False):
                    return False
                offset += batch.num_rows
        finally:
            repeated.close()
        return read_audit(config, client) == expected

    receipt = write_prepared(config, catalog, prepared, expected_source_rows=expected, verify_source=verify,
                             expected_metadata_location=target.metadata_location)
    receipt["source_audit"] = {
        "source": f"{config['source']['database']}.{config['source']['table']}",
        "rows_count": expected, "content_sha256": digest,
        "source_columns": [list(item) for item in result[1]],
        "query_sha256": sha256(source_sql(config).encode()).hexdigest(),
        "verified_at": utc_now().isoformat(timespec="microseconds"),
    }
    # XCom/immutable package используют JSON receipt, не native Arrow объекты.
    json.dumps(receipt)
    return receipt
