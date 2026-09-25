"""Полностью заменить календарь копией ClickHouse silver.calendar."""

from __future__ import annotations

from datetime import datetime, timezone
import logging

import pyarrow as pa

logger = logging.getLogger("airflow.task")

SOURCE_SQL = """SELECT
    dt AS date,
    CAST(year AS Nullable(Int32)) AS year,
    CAST(quarter AS Nullable(Int32)) AS quarter,
    CAST(month AS Nullable(Int32)) AS month,
    month_name_en,
    month_abbr_en,
    CAST(day AS Nullable(Int32)) AS day,
    CAST(day_of_week_iso AS Nullable(Int32)) AS day_of_week_iso,
    day_name_en,
    day_abbr_en,
    CAST(iso_week AS Nullable(Int32)) AS iso_week,
    CAST(is_weekend AS Nullable(Bool)) AS is_weekend,
    CAST(is_public_holiday AS Nullable(Bool)) AS is_public_holiday,
    holiday_name,
    CAST(is_working_day AS Nullable(Bool)) AS is_working_day
FROM silver.calendar
ORDER BY dt"""


def capture_time() -> datetime:
    # Секунды: DQ снапшотной партиции сравнивает ingested_at с литералом без микросекунд.
    return datetime.now(timezone.utc).replace(microsecond=0)


def records_to_arrow(rows, columns, schema: pa.Schema, constants: dict) -> pa.Table:
    names = [name for name, _ in columns]
    values = dict(zip(names, zip(*rows))) if rows else {name: () for name in names}
    arrays = []
    for field in schema:
        column = values.get(field.name)
        if column is None:
            if field.name not in constants:
                raise ValueError(f"Запрос не вернул колонку {field.name}")
            column = [constants[field.name]] * len(rows)
        if pa.types.is_timestamp(field.type):
            arrays.append(pa.array(column, type=pa.timestamp(field.type.unit, "UTC")).cast(field.type))
        else:
            arrays.append(pa.array(column, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def load_table(config: dict, catalog=None):
    from dq.results_writer import load_results_catalog

    table = config["table"]
    catalog = catalog or load_results_catalog(table["catalog"])
    return catalog.load_table((table["schema"], table["name"]))


def replace_table(table, data: pa.Table) -> None:
    """Атомарно заменить всё содержимое; пустой захват не удаляет прежние данные."""
    from dq.results_writer import run_iceberg_commit_with_retry

    if data.num_rows == 0:
        raise ValueError(f"{table.name()}: пустой захват, таблица не перезаписана")

    def commit() -> None:
        table.refresh()
        table.overwrite(data)

    run_iceberg_commit_with_retry(commit, f"replace {table.name()}")
    logger.info("%s: записано %d строк", table.name(), data.num_rows)


def load(config: dict, *, run_id: str, client=None, catalog=None) -> dict:
    table = load_table(config, catalog)
    captured = capture_time()
    constants = {
        "calendar_id": config["source"]["calendar_id"],
        "source_manifest_id": run_id,
        "ingested_at": captured,
    }
    if client is None:
        from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

        hook = ClickHouseHook(clickhouse_conn_id=config["source"]["clickhouse_conn_id"], use_numpy=False)
        with hook.get_conn() as connection:
            rows, columns = connection.execute(SOURCE_SQL, with_column_types=True)
    else:
        rows, columns = client.execute(SOURCE_SQL, with_column_types=True)
    replace_table(table, records_to_arrow(rows, columns, table.schema().as_arrow(), constants))
    return {"ingested_at": captured.strftime("%Y-%m-%d %H:%M:%S"), "rows": len(rows)}
