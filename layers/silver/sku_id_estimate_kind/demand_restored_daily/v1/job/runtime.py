"""Прочитать точный удерживаемый E3-run и атомарно перенести один полный день."""

from datetime import date, datetime, timedelta, timezone
from itertools import chain
import re

import pyarrow as pa

from .manifest import RAW_COLUMNS, day_manifest
from .preparation import prepare_batch, target_ref, validate_run, validate_schema
from .query import registry_query, selection, source_query, source_ref
from .writer import write_day

REGISTRY_COLUMNS = (
    "run_id", "prediction_date", "state_version", "stage", "status", "model_version",
    "code_version", "catalog_version", "input_manifest", "output_manifest", "finished_at", "published_at",
)


def read_run(config, client, selected):
    rows, columns = client.execute(registry_query(config), params=selected, with_column_types=True)
    names = [name for name, _ in columns]
    if names != list(REGISTRY_COLUMNS) or len(rows) != 1 or len(rows[0]) != len(names):
        raise ValueError("Нужен один точный паспорт E3 с полной схемой")
    result = dict(zip(names, rows[0]))
    validate_run(result, selected)
    return result


def source_arrow(rows, columns):
    names = [name for name, _ in columns]
    if len(names) != len(RAW_COLUMNS) or set(names) != set(RAW_COLUMNS):
        raise ValueError("Неверная схема E3 source")
    if any(len(row) != len(names) for row in rows):
        raise ValueError("Неверная длина source строки")
    arrays = []
    for index, (name, kind) in enumerate(columns):
        nullable = False
        while kind.startswith(("Nullable(", "LowCardinality(")) and kind.endswith(")"):
            wrapper, kind = kind.split("(", 1)
            nullable |= wrapper == "Nullable"
            kind = kind[:-1]
        values = [row[index] for row in rows]
        if not nullable and any(value is None for value in values):
            raise ValueError(f"NULL противоречит source type {name}")
        if kind in ("Date", "Date32"):
            dtype, valid = pa.date32(), lambda v: type(v) is date
        elif re.fullmatch(r"U?Int(8|16|32|64)", kind):
            dtype = pa.uint64() if kind.startswith("U") else pa.int64()
            def valid(v):
                return type(v) is int
        elif kind == "Float64":
            dtype, valid = pa.float64(), lambda v: type(v) is float
        elif kind == "String":
            dtype, valid = pa.string(), lambda v: isinstance(v, str)
        elif kind == "DateTime64(6, 'UTC')":
            dtype = pa.timestamp("us", "UTC")
            def valid(v):
                return isinstance(v, datetime) and v.utcoffset() is not None
        else:
            raise ValueError(f"Неподдержанный source type {name}: {kind}")
        if any(v is not None and not valid(v) for v in values):
            raise ValueError(f"Неверное исходное значение {name}")
        arrays.append(pa.array(values, type=dtype))
    return pa.Table.from_arrays(arrays, names=names)


def utc_now():
    return datetime.now(timezone.utc)


def preflight_target(config, catalog, manifest):
    """Проверить target и локальные настройки до чтения CH."""
    from pyiceberg.transforms import IdentityTransform

    version = config["source"]["contract_version"]
    if any(not isinstance(v, str) or not v.strip() for v in (manifest, version)):
        raise ValueError("Нет manifest/version переноса")
    limit = config["runtime"]["max_batch_rows"]
    if any(type(v) is not int or v <= 0 for v in (limit, config["runtime"]["max_batch_bytes"])):
        raise ValueError("Неверные лимиты порций")
    identifier = target_ref(config, catalog.name)
    source_ref(config)
    source_ref(config, registry=True)
    if not catalog.table_exists(identifier):
        raise ValueError(f"Нет таблицы {identifier}: сначала миграции")
    table = catalog.load_table(identifier)
    schema = table.schema().as_arrow()
    validate_schema(schema)
    fields = table.spec().fields
    if (len(fields) != 1 or fields[0].source_id != table.schema().find_field("date").field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError("Нужен identity partition по date")
    return table


def load_day(config, catalog, client, *, selected, day, manifest, require_run_held, expected_run=None):
    """Callback проверяет реальный hold; наличие строк/status его не заменяет."""
    selection(**selected)
    if type(day) is not date or not selected["start"] <= day < selected["end"]:
        raise ValueError("День вне запроса")
    if not callable(require_run_held):
        raise ValueError("Нужна проверка удержания точного E3-run")
    table = preflight_target(config, catalog, manifest)
    schema = table.schema().as_arrow()
    version = config["source"]["contract_version"]
    limit = config["runtime"]["max_batch_rows"]
    passport = read_run(config, client, selected)
    if expected_run is not None and passport != expected_run:
        raise ValueError("Паспорт E3 изменился после подготовки диапазона")
    day_manifest(config, selected, passport, day)
    if require_run_held(selected, passport) is not True:
        raise ValueError("E3-run не удерживается от очистки")
    captured = utc_now()
    params = selected | {"start": day, "end": day + timedelta(days=1)}

    def batches():
        stream = iter(client.execute_iter(source_query(config), params=params, with_column_types=True,
                                          chunk_size=limit, settings={"max_block_size": limit}))
        try:
            first = next(stream, None)
            if not isinstance(first, list) or not first:
                raise ValueError("Нет метаданных source потока")
            columns, records = first[0], first[1:]
            for records in chain((records,), stream):
                if not records:
                    continue
                yield prepare_batch(source_arrow(records, columns), schema, selected=selected,
                                    run=passport, manifest=manifest, version=version, ingested_at=captured)
        finally:
            close = getattr(stream, "close", None)
            if callable(close):
                close()

    def verify():
        current = read_run(config, client, selected)
        return current == passport and require_run_held(selected, current) is True

    stream = batches()
    try:
        return write_day(config, catalog, stream, day=day, selected=selected, run=passport,
                         manifest=manifest, version=version, ingested_at=captured, verify_source=verify)
    finally:
        stream.close()
