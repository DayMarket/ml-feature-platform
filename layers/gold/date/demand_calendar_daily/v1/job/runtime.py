"""Полностью заменить gold-календарь: официальный календарь + BIG_SALE по дням."""

from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import logging
from pathlib import Path

import pyarrow as pa

logger = logging.getLogger("airflow.task")

CALENDAR_COLUMNS = (
    "date", "calendar_id", "year", "quarter", "month", "month_name_en", "month_abbr_en",
    "day", "day_of_week_iso", "day_name_en", "day_abbr_en", "iso_week", "is_weekend",
    "is_public_holiday", "holiday_name", "is_working_day",
)


def source_query(calendar: str, events: str) -> str:
    """Сетка дат равна календарю; флаги BIG_SALE NULL, если акций на дату нет."""
    columns = ",\n    ".join([
        *(f'c."{name}"' for name in CALENDAR_COLUMNS),
        "IF(e.events > 0, e.created) AS big_sale_created",
        "IF(e.events > 0, e.canceled) AS big_sale_canceled",
        "IF(e.events > 0, e.unknown_status) AS big_sale_unknown_status",
        "COALESCE(e.events, 0) AS big_sale_event_count",
        "'source_row_present' AS calendar_coverage_status",
        "IF(e.events > 0, 'registry_rows_present', 'no_registry_rows') AS promotion_coverage_status",
        "c.source_manifest_id AS calendar_source_manifest_id",
        f"(SELECT max(source_manifest_id) FROM {events}) AS events_source_manifest_id",
    ])
    return f"""SELECT
    {columns}
FROM {calendar} AS c
LEFT JOIN (
    SELECT
        "date",
        count(DISTINCT event_code) AS events,
        bool_or(source_status = 'CREATED') AS created,
        bool_or(source_status = 'CANCELED') AS canceled,
        bool_or(source_status IS NULL OR source_status NOT IN ('CREATED', 'CANCELED')) AS unknown_status
    FROM {events}
    WHERE source_kind = 'marketing_sale' AND source_type = 'BIG_SALE'
    GROUP BY "date"
) AS e ON c."date" = e."date"
ORDER BY c."date\""""


def quote(*parts: str) -> str:
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


def rows_to_arrow(names, rows, schema: pa.Schema, constants: dict) -> pa.Table:
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


def pinned_input(table_config: dict, repo_root: str, catalog) -> tuple[str, int]:
    """Trino-ссылка на текущий snapshot входа и его id."""
    from dq.config import trino_catalog_alias

    table = catalog.load_table((table_config["schema"], table_config["name"]))
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise ValueError(f"Входная таблица {table.name()} пуста")
    alias = trino_catalog_alias(Path(repo_root), table_config["catalog"])
    ref = quote(alias, table_config["schema"], table_config["name"])
    return f"{ref} FOR VERSION AS OF {snapshot.snapshot_id}", snapshot.snapshot_id


def replace_table(table, data: pa.Table) -> None:
    """Атомарно заменить всё содержимое; пустой результат не удаляет прежние данные."""
    from dq.results_writer import run_iceberg_commit_with_retry

    if data.num_rows == 0:
        raise ValueError(f"{table.name()}: пустой результат, таблица не перезаписана")

    def commit() -> None:
        table.refresh()
        table.overwrite(data)

    run_iceberg_commit_with_retry(commit, f"replace {table.name()}")
    logger.info("%s: записано %d строк", table.name(), data.num_rows)


def load(config: dict, sources: dict, repo_root: str, *, run_id: str,
         connection=None, catalog=None) -> dict:
    from dq.results_writer import load_results_catalog

    catalog = catalog or load_results_catalog(config["table"]["catalog"])
    table = catalog.load_table((config["table"]["schema"], config["table"]["name"]))
    calendar, calendar_snapshot = pinned_input(sources["calendar"]["table"], repo_root, catalog)
    events, events_snapshot = pinned_input(sources["events"]["table"], repo_root, catalog)
    captured = datetime.now(timezone.utc).replace(microsecond=0)
    constants = {
        "calendar_snapshot_id": calendar_snapshot,
        "events_snapshot_id": events_snapshot,
        "source_manifest_id": run_id,
        "ingested_at": captured,
    }
    if connection is None:
        from airflow.providers.trino.hooks.trino import TrinoHook

        connection = TrinoHook(trino_conn_id=config["inputs"]["trino_conn_id"]).get_conn()
    sql = source_query(calendar, events)
    logger.info("Trino query:\n%s", sql)
    with closing(connection), closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
    replace_table(table, rows_to_arrow(names, rows, table.schema().as_arrow(), constants))
    return {"ingested_at": captured.strftime("%Y-%m-%d %H:%M:%S"), "rows": len(rows)}
