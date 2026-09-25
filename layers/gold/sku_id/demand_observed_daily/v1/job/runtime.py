"""Перезаписать дневные партиции observed-панели FULL JOIN-ом в Trino."""

from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path

import pyarrow as pa

from .query import counts_query, source_query, versioned

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


def trino_arrow(connection, sql: str, schema: pa.Schema, constants: dict) -> pa.Table:
    logger.info("Trino query:\n%s", sql)
    parts = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        names = [column[0] for column in cursor.description]
        while rows := cursor.fetchmany(BATCH_ROWS):
            parts.append(rows_to_arrow(names, rows, schema, constants))
    return pa.concat_tables(parts) if parts else schema.empty_table()


def quote(*parts: str) -> str:
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


def table_ref(repo_root: str, table_config: dict) -> str:
    from dq.config import trino_catalog_alias

    alias = trino_catalog_alias(Path(repo_root), table_config["catalog"])
    return quote(alias, table_config["schema"], table_config["name"])


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


def pinned_input(config: dict, repo_root: str, catalog) -> tuple[str, dict]:
    """Текущий snapshot входа: все дни одного запуска читают одну версию."""
    table = load_table(config, catalog)
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise ValueError(f"Входная таблица {table.name()} пуста")
    return versioned(table_ref(repo_root, config["table"]), snapshot.snapshot_id), {
        "snapshot_id": snapshot.snapshot_id,
        "table_uuid": str(table.metadata.table_uuid),
    }


def load_range(config: dict, sources: dict, repo_root: str, start: str, end: str, *,
               run_id: str, connection=None, catalog=None) -> list[str]:
    from dq.results_writer import load_results_catalog

    days = day_range(start, end)
    catalog = catalog or load_results_catalog(config["table"]["catalog"])
    table = load_table(config, catalog)
    schema = table.schema().as_arrow()
    sales, sales_meta = pinned_input(sources["sales"], repo_root, catalog)
    stock, stock_meta = pinned_input(sources["stock"], repo_root, catalog)
    constants = {
        "sales_snapshot_id": sales_meta["snapshot_id"],
        "sales_table_uuid": sales_meta["table_uuid"],
        "stock_snapshot_id": stock_meta["snapshot_id"],
        "stock_table_uuid": stock_meta["table_uuid"],
        "source_manifest_id": run_id,
        "source_contract_version": config["source"]["contract_version"],
        "ingested_at": datetime.now(timezone.utc).replace(microsecond=0),
    }
    if connection is None:
        from airflow.providers.trino.hooks.trino import TrinoHook

        connection = TrinoHook(trino_conn_id=config["source"]["conn_id"]).get_conn()
    with closing(connection):
        for day in days:
            with closing(connection.cursor()) as cursor:
                cursor.execute(counts_query(sales, stock, day))
                sales_rows, stock_rows = cursor.fetchall()[0]
            # Пустая stock-партиция означала бы «всё не в наличии» — такой день не пишем.
            if not sales_rows or not stock_rows:
                raise ValueError(f"{day}: нет входных строк (sales={sales_rows}, stock={stock_rows})")
            write_day(table, trino_arrow(connection, source_query(sales, stock, day), schema, constants), day)
    return [day.isoformat() for day in days]
