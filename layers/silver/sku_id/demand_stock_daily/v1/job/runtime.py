"""Порционно загрузить разреженное наличие одного EOD-дня."""

from datetime import date, datetime, timezone
from itertools import chain
import re

import pyarrow as pa

from .preparation import RAW_COLUMNS, prepare_batch, target_ref, validate_schema
from .query import count_query, source_query, source_ref
from .writer import write_day


def source_arrow(rows, columns):
    names = [name for name, _ in columns]
    if tuple(names) != RAW_COLUMNS or any(len(row) != len(names) for row in rows):
        raise ValueError("Неверная схема ClickHouse source")
    arrays = []
    for index, (name, declared_type) in enumerate(columns):
        kind = declared_type
        nullable = False
        while kind.startswith(("Nullable(", "LowCardinality(")) and kind.endswith(")"):
            wrapper, kind = kind.split("(", 1)
            nullable = nullable or wrapper == "Nullable"
            kind = kind[:-1]
        values = [row[index] for row in rows]
        if not nullable and any(value is None for value in values):
            raise ValueError(f"NULL противоречит типу источника {name}")
        if kind in ("Date", "Date32"):
            dtype = pa.date32()
            invalid = any(value is not None and type(value) is not date for value in values)
        elif re.fullmatch(r"U?Int(8|16|32|64)", kind):
            dtype = pa.uint64() if kind.startswith("U") else pa.int64()
            invalid = any(value is not None and type(value) is not int for value in values)
        else:
            raise ValueError(f"Не поддержан source type {name}: {declared_type}")
        if invalid:
            raise ValueError(f"Неверные значения source {name}")
        arrays.append(pa.array(values, type=dtype))
    return pa.Table.from_arrays(arrays, names=names)


def read_audit(config, client, day):
    """Оперативные статусы не заменяют проверку текущего source-множества."""
    rows = client.execute(count_query(config, day))
    if (
        not isinstance(rows, (list, tuple))
        or len(rows) != 1
        or not isinstance(rows[0], (list, tuple))
        or len(rows[0]) != 6
    ):
        raise ValueError("Неверный ответ source проверки")
    total, keys, invalid_keys, invalid_values, key_hash, updated_at = rows[0]
    if any(type(value) is not int or value < 0 for value in rows[0][:5]):
        raise ValueError("Source проверки требуют целые неотрицательные счётчики")
    if total <= 0 or keys != total or invalid_keys or invalid_values or not isinstance(updated_at, datetime):
        raise ValueError(f"Source day {day}: пустой день, неуникальный ключ или недопустимые значения")
    return total, keys, invalid_keys, invalid_values, key_hash, updated_at


def read_count(config, client, day):
    return read_audit(config, client, day)[0]


def load_day(config, catalog, client, *, day, manifest, require_source_ready=None):
    """Проверить источник, записать sparse-день и повторить его сигнатуру до commit."""
    from pyiceberg.transforms import IdentityTransform

    if type(day) is not date or (require_source_ready is not None and not callable(require_source_ready)):
        raise ValueError("Нужны DATE и корректный coordination hook")
    version = config["source"]["contract_version"]
    if any(not isinstance(value, str) or not value.strip() for value in (manifest, version)):
        raise ValueError("Нужны manifest/version")
    limit = config["runtime"]["max_batch_rows"]
    max_bytes = config["runtime"]["max_batch_bytes"]
    if any(type(value) is not int or value <= 0 for value in (limit, max_bytes)):
        raise ValueError("Неверные лимиты порций")
    identifier = target_ref(config, catalog.name)
    source_ref(config)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier}: сначала миграции")
    table = catalog.load_table(identifier)
    schema = table.schema().as_arrow()
    validate_schema(schema)
    fields = table.spec().fields
    if (
        len(fields) != 1
        or fields[0].source_id != table.schema().find_field("date").field_id
        or not isinstance(fields[0].transform, IdentityTransform)
    ):
        raise ValueError("Нужен identity partition по date")
    if require_source_ready is not None and require_source_ready(day) is not True:
        raise ValueError("Upstream не готов")
    audit = read_audit(config, client, day)
    captured = datetime.now(timezone.utc)
    sql = source_query(config, day)

    def batches():
        stream = iter(
            client.execute_iter(
                sql,
                with_column_types=True,
                chunk_size=limit,
                settings={"max_block_size": limit},
            )
        )
        try:
            first = next(stream, None)
            if not isinstance(first, list) or not first:
                raise ValueError("Нет метаданных streaming query")
            columns, records = first[0], first[1:]
            source_arrow([], columns)
            for records in chain((records,), stream):
                if not records:
                    continue
                yield prepare_batch(
                    source_arrow(records, columns),
                    schema,
                    day=day,
                    manifest=manifest,
                    version=version,
                    ingested_at=captured,
                )
        finally:
            close = getattr(stream, "close", None)
            if callable(close):
                close()

    def verify():
        if require_source_ready is not None and require_source_ready(day) is not True:
            return False
        return read_audit(config, client, day) == audit

    stream = batches()
    try:
        return write_day(
            config,
            catalog,
            stream,
            day=day,
            expected_rows=audit[0],
            manifest=manifest,
            version=version,
            ingested_at=captured,
            verify_source=verify,
        )
    finally:
        stream.close()
