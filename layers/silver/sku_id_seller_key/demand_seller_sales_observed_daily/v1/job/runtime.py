"""Порционно загрузить день seller-когорты продаж после сверки source/FX."""

from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from itertools import chain
import json
import re

import pyarrow as pa

from .preparation import RAW_COLUMNS, prepare_batch, target_ref, validate_fx, validate_schema
from .query import TOTAL_COLUMNS, coverage_totals_query, fx_query, source_query, source_ref, coverage_query
from .writer import write_day


def source_arrow(rows, columns):
    names = [name for name, _ in columns]
    if len(names) != len(RAW_COLUMNS) or set(names) != set(RAW_COLUMNS):
        raise ValueError("Неверная схема ClickHouse source")
    if any(len(row) != len(names) for row in rows):
        raise ValueError("Строка не совпадает с метаданными CH")
    arrays = []
    for index, (name, kind) in enumerate(columns):
        nullable = False
        while kind.startswith(("Nullable(", "LowCardinality(")) and kind.endswith(")"):
            wrapper, kind = kind.split("(", 1)
            nullable |= wrapper == "Nullable"
            kind = kind[:-1]
        values = [row[index] for row in rows]
        if not nullable and any(v is None for v in values):
            raise ValueError(f"NULL противоречит типу источника {name}")
        if kind in ("Date", "Date32"):
            dtype, valid = pa.date32(), lambda v: type(v) is date
        elif re.fullmatch(r"U?Int(8|16|32|64)", kind):
            dtype = pa.uint64() if kind.startswith("U") else pa.int64()
            def valid(v):
                return type(v) is int
        elif kind in ("Float32", "Float64"):
            dtype, valid = pa.float64(), lambda v: type(v) in (int, float)
        elif re.fullmatch(r"Decimal\(38,\s*0\)", kind):
            dtype = pa.decimal128(38, 0)
            def valid(v):
                return isinstance(v, Decimal) and v.is_finite() and v == int(v)
        elif kind == "String":
            dtype, valid = pa.string(), lambda v: isinstance(v, str)
        elif kind == "DateTime64(6, 'UTC')":
            dtype = pa.timestamp("us", "UTC")
            def valid(v):
                return isinstance(v, datetime) and v.utcoffset() is not None
        else:
            raise ValueError(f"Не поддержан source type {name}: {kind}")
        if any(v is not None and not valid(v) for v in values):
            raise ValueError(f"Неверные значения source {name}")
        arrays.append(pa.array(values, type=dtype))
    return pa.Table.from_arrays(arrays, names=names)


def exact_total(value):
    if type(value) is int:
        return value
    if isinstance(value, Decimal) and value.is_finite() and value == int(value):
        return int(value)
    raise ValueError("Source total должен быть точным целым, не float или NULL")


def read_fx(client, day):
    rows = client.execute(fx_query(day))
    if len(rows) != 1 or len(rows[0]) != 5:
        raise ValueError("Нужна одна строка FX receipt")
    result = dict(zip(("date", "fx_rate_date", "fx_rate_uzs_per_usd", "fx_rate_source", "fx_captured_at"), rows[0]))
    validate_fx(result, day)
    return result


def read_coverage(config, client, day):
    rows = client.execute(coverage_totals_query(config, day))
    if len(rows) != 1 or len(rows[0]) != len(TOTAL_COLUMNS) + 2:
        raise ValueError("Неверная схема source coverage")
    total, *values = rows[0]
    if type(total) is not int or total <= 0:
        raise ValueError("Пустой source day не доказывает известный ноль")
    updated = values.pop()
    if not isinstance(updated, datetime) or updated.utcoffset() is None:
        raise ValueError("Нужно timezone-aware source_updated_at")
    return (total, *(exact_total(value) for value in values), updated)


def audit_source(config, client, day, coverage):
    rows = client.execute(coverage_query(config, day))
    if len(rows) != 1 or len(rows[0]) != 6:
        raise ValueError("Неверный source audit")
    count, unique, keys, invalid_sellers, units, gmv = rows[0]
    if any(type(v) is not int for v in (count, unique, keys, invalid_sellers, units)):
        raise ValueError("Неверные типы source audit")
    expected = dict(zip(TOTAL_COLUMNS, coverage[1:-1]))
    if (invalid_sellers != 0 or count != unique or keys != coverage[0] or count != expected["sales_order_items"]
            or units != expected["sales_units"] or exact_total(gmv) != expected["sales_gmv"]):
        raise ValueError("Неуникальная позиция либо изменившийся source day")


def utc_now():
    return datetime.now(timezone.utc)


def source_signature(coverage, fx):
    """Связать контрольные суммы и применённый курс, исключая время повторного захвата."""
    values = [v.astimezone(timezone.utc).isoformat() if isinstance(v, datetime) else str(v)
              for v in coverage]
    rate = fx['fx_rate_uzs_per_usd']
    payload = {'version': 1, 'coverage': values, 'fx_source': fx['fx_rate_source'],
               'fx_date': fx['fx_rate_date'].isoformat() if fx['fx_rate_date'] else None,
               'fx_rate': float(rate).hex() if rate is not None else None}
    return sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def load_day(config, catalog, client, *, day, manifest, require_source_ready=None,
             expected_source_signature=None):
    """Проверить source данные; необязательный hook только координирует запуск."""
    from pyiceberg.transforms import IdentityTransform

    if type(day) is not date or (require_source_ready is not None and not callable(require_source_ready)):
        raise ValueError("Нужны DATE и корректный coordination hook")
    version = config["source"]["contract_version"]
    if any(not isinstance(v, str) or not v.strip() for v in (manifest, version)):
        raise ValueError("Нужны manifest/version")
    limit = config["runtime"]["max_batch_rows"]
    max_bytes = config["runtime"]["max_batch_bytes"]
    if any(type(v) is not int or v <= 0 for v in (limit, max_bytes)):
        raise ValueError("Неверные лимиты порций")
    identifier = target_ref(config, catalog.name)
    source_ref(config)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier}: сначала миграции")
    table = catalog.load_table(identifier)
    initial_target = table.metadata_location
    schema = table.schema().as_arrow()
    validate_schema(schema)
    fields = table.spec().fields
    if (len(fields) != 1 or fields[0].source_id != table.schema().find_field("date").field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError("Нужен identity partition по date")
    if require_source_ready is not None and require_source_ready(day) is not True:
        raise ValueError("Upstream не готов")
    coverage = read_coverage(config, client, day)
    audit_source(config, client, day, coverage)
    receipt = read_fx(client, day)
    signature = source_signature(coverage, receipt)
    if expected_source_signature is not None and signature != expected_source_signature:
        raise ValueError("Source/FX изменился после проверки диапазона")
    captured = utc_now()
    sql = source_query(config, day, fx_available=receipt["fx_rate_source"] != "unavailable")
    seen = [0] * (len(TOTAL_COLUMNS) + 1)
    updated = None

    def batches():
        nonlocal updated
        stream = iter(client.execute_iter(sql, with_column_types=True, chunk_size=limit,
                                          settings={"max_block_size": limit}))
        try:
            first = next(stream, None)
            if not isinstance(first, list) or not first:
                raise ValueError("Нет метаданных streaming query")
            columns, records = first[0], first[1:]
            for records in chain((records,), stream):
                if not records:
                    continue
                batch = prepare_batch(source_arrow(records, columns), schema, day=day, fx=receipt,
                                      manifest=manifest, version=version, ingested_at=captured)
                seen[0] += batch.num_rows
                for index, name in enumerate(TOTAL_COLUMNS, 1):
                    # Python int не округляет Decimal(38,0) при суммировании порций.
                    seen[index] += sum(exact_total(v) for v in batch[name].to_pylist())
                latest = max(batch["source_updated_at"].to_pylist()).replace(tzinfo=timezone.utc)
                updated = latest if updated is None else max(updated, latest)
                yield batch
        finally:
            close = getattr(stream, "close", None)
            if callable(close):
                close()

    def verify():
        if tuple(seen) + (updated,) != coverage:
            raise ValueError("Выгруженные суммы не совпали с source coverage")
        if require_source_ready is not None and require_source_ready(day) is not True:
            return False
        if read_coverage(config, client, day) != coverage:
            return False
        audit_source(config, client, day, coverage)
        current = read_fx(client, day)
        return all(current[k] == receipt[k] for k in receipt if k != "fx_captured_at")

    stream = batches()
    try:
        return write_day(config, catalog, stream, day=day, expected_rows=coverage[0],
                         manifest=manifest, version=version, ingested_at=captured, verify_source=verify,
                         source_signature=signature, expected_metadata_location=initial_target)
    finally:
        stream.close()
