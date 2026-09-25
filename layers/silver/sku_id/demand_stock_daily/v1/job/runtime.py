"""Перезаписать дневные партиции EOD-наличия из ClickHouse в Iceberg."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import date, datetime, timedelta, timezone
import logging

import pyarrow as pa

from .query import source_query

logger = logging.getLogger("airflow.task")
BATCH_ROWS = 100_000


def day_range(start: str, end: str, *, today: date | None = None) -> list[date]:
    """Включительный диапазон завершённых UTC-дней `[start, end]`."""
    try:
        first, last = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    except ValueError as error:
        raise ValueError(f"start/end должны быть датами YYYY-MM-DD: {start!r}, {end!r}") from error
    today = today or datetime.now(timezone.utc).date()
    if first > last:
        raise ValueError(f"start {first} позже end {last}")
    if last >= today:
        raise ValueError(f"end {last} должен быть раньше текущего UTC-дня {today}")
    return [first + timedelta(days=offset) for offset in range((last - first).days + 1)]


def to_arrow(values, dtype: pa.DataType) -> pa.Array:
    if pa.types.is_timestamp(dtype):
        # ClickHouse отдаёт aware datetime; в Iceberg хранится UTC без зоны.
        return pa.array(values, type=pa.timestamp(dtype.unit, "UTC")).cast(dtype)
    return pa.array(values, type=dtype)


def rows_to_arrow(names: list[str], rows: list, schema: pa.Schema, constants: dict) -> pa.Table:
    """Порция строк → Arrow по схеме Iceberg; недостающие колонки берутся из constants."""
    columns = dict(zip(names, zip(*rows))) if rows else {name: () for name in names}
    arrays = []
    for field in schema:
        if field.name in columns:
            arrays.append(to_arrow(columns[field.name], field.type))
        elif field.name in constants:
            arrays.append(to_arrow([constants[field.name]] * len(rows), field.type))
        else:
            raise ValueError(f"Запрос не вернул колонку {field.name}")
    return pa.Table.from_arrays(arrays, schema=schema)


def clickhouse_arrow(client, sql: str, schema: pa.Schema, constants: dict) -> pa.Table:
    """Прочитать результат порциями BATCH_ROWS, чтобы не держать все строки Python-объектами."""
    logger.info("ClickHouse query:\n%s", sql)
    stream = client.execute_iter(
        sql, with_column_types=True, chunk_size=BATCH_ROWS, settings={"max_block_size": BATCH_ROWS}
    )
    names, parts = None, []
    for chunk in stream:
        if names is None:
            names, chunk = [name for name, _ in chunk[0]], chunk[1:]
        if chunk:
            parts.append(rows_to_arrow(names, chunk, schema, constants))
    if names is None:
        raise ValueError("ClickHouse не вернул метаданные колонок")
    return pa.concat_tables(parts) if parts else schema.empty_table()


def load_table(config: dict, catalog=None):
    from dq.results_writer import load_results_catalog

    table = config["table"]
    catalog = catalog or load_results_catalog(table["catalog"])
    return catalog.load_table((table["schema"], table["name"]))


def write_day(table, data: pa.Table, day: date) -> None:
    """Атомарно заменить одну партицию `date`; пустой день не перезаписывается."""
    from pyiceberg.expressions import EqualTo

    from dq.results_writer import run_iceberg_commit_with_retry

    if data.num_rows == 0:
        raise ValueError(f"{day}: источник вернул 0 строк, партиция не перезаписана")

    def commit() -> None:
        table.refresh()
        table.overwrite(data, overwrite_filter=EqualTo("date", day.isoformat()))

    run_iceberg_commit_with_retry(commit, f"overwrite {table.name()} date={day}")
    logger.info("%s: записано %d строк", day, data.num_rows)


def load_range(config: dict, start: str, end: str, *, run_id: str, client=None, catalog=None) -> list[str]:
    days = day_range(start, end)
    table = load_table(config, catalog)
    schema = table.schema().as_arrow()
    constants = {
        "source_manifest_id": run_id,
        "source_contract_version": config["source"]["contract_version"],
        "ingested_at": datetime.now(timezone.utc).replace(microsecond=0),
    }
    with ExitStack() as stack:
        if client is None:
            from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

            hook = ClickHouseHook(clickhouse_conn_id=config["source"]["conn_id"], use_numpy=False)
            client = stack.enter_context(hook.get_conn())
        for day in days:
            write_day(table, clickhouse_arrow(client, source_query(config, day), schema, constants), day)
    return [day.isoformat() for day in days]
