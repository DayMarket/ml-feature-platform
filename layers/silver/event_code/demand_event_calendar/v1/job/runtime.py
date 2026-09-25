"""Полностью заменить справочник событий: праздники календаря и акции реестра."""

from __future__ import annotations

from datetime import datetime, timezone
import logging

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger("airflow.task")

CALENDAR_ID = "uz_official"
TIMES = ("started_at", "finished_at", "announced_at", "created_at", "updated_at")

# Акция разворачивается в дни Asia/Tashkent, пересекающие [started_at, finished_at).
PROMO_SQL = """SELECT
    day AS date,
    concat('marketing_sale:', toString(id)) AS event_code,
    'marketing_sale' AS source_kind,
    toString(id) AS source_event_id,
    title AS event_name,
    status AS source_status,
    type AS source_type,
    {times}
FROM silver.b2b_marketing_sale
ARRAY JOIN arrayMap(
    i -> addDays(toDate(assumeNotNull(started_at), 'Asia/Tashkent'), i),
    range(toUInt32(dateDiff(
        'day',
        toDate(assumeNotNull(started_at), 'Asia/Tashkent'),
        toDate(toDateTime64(assumeNotNull(finished_at), 6) - toIntervalMicrosecond(1), 'Asia/Tashkent')
    ) + 1))
) AS day
WHERE started_at IS NOT NULL AND finished_at > started_at
ORDER BY date, event_code""".format(times=",\n    ".join(
    f"toDateTime64(toTimeZone({name}, 'UTC'), 6, 'UTC') AS source_{name}" for name in TIMES
))

# Пустой реестр, повтор id или некорректный интервал блокируют запись.
CHECK_SQL = """SELECT
    count() AS promos,
    count() - uniqExact(id) AS duplicate_ids,
    countIf(id <= 0) AS invalid_ids,
    countIf(started_at IS NULL OR finished_at IS NULL OR finished_at <= started_at) AS invalid_intervals
FROM silver.b2b_marketing_sale"""


def capture_time() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def records_to_arrow(rows, columns, schema: pa.Schema, constants: dict) -> pa.Table:
    names = [name for name, _ in columns]
    values = dict(zip(names, zip(*rows))) if rows else {name: () for name in names}
    arrays = []
    for field in schema:
        column = values.get(field.name)
        if column is None:
            column = [constants.get(field.name)] * len(rows)
        if pa.types.is_timestamp(field.type):
            arrays.append(pa.array(column, type=pa.timestamp(field.type.unit, "UTC")).cast(field.type))
        else:
            arrays.append(pa.array(column, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def load_table(table_config: dict, catalog):
    return catalog.load_table((table_config["schema"], table_config["name"]))


def holiday_events(calendar: pa.Table, schema: pa.Schema, constants: dict) -> pa.Table:
    """Строка на каждую дату календаря с is_public_holiday = TRUE."""
    holidays = calendar.filter(pc.fill_null(calendar["is_public_holiday"], False))
    ids = pc.cast(holidays["date"], pa.string())
    columns = {
        "date": holidays["date"],
        "event_code": pc.binary_join_element_wise(f"calendar:{CALENDAR_ID}:", ids, ""),
        "source_kind": pa.repeat("calendar", holidays.num_rows),
        "calendar_id": pa.repeat(CALENDAR_ID, holidays.num_rows),
        "source_event_id": ids,
        "event_name": holidays["holiday_name"],
    }
    arrays = []
    for field in schema:
        if field.name in columns:
            arrays.append(pc.cast(columns[field.name], field.type))
        else:
            arrays.append(pa.array([constants.get(field.name)] * holidays.num_rows, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


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


def load(config: dict, calendar_config: dict, *, run_id: str, client=None, catalog=None) -> dict:
    from dq.results_writer import load_results_catalog

    catalog = catalog or load_results_catalog(config["table"]["catalog"])
    table = load_table(config["table"], catalog)
    schema = table.schema().as_arrow()
    captured = capture_time()
    constants = {"source_manifest_id": run_id, "ingested_at": captured}

    calendar = load_table(calendar_config["table"], catalog).scan(
        selected_fields=("date", "is_public_holiday", "holiday_name")
    ).to_arrow()
    if calendar.num_rows == 0:
        raise ValueError("Календарь пуст: события не перезаписаны")

    def fetch(connection):
        (promos, duplicates, invalid_ids, invalid_intervals), = connection.execute(CHECK_SQL)
        if not promos or duplicates or invalid_ids or invalid_intervals:
            raise ValueError(
                f"Реестр акций некорректен: rows={promos}, duplicate_ids={duplicates}, "
                f"invalid_ids={invalid_ids}, invalid_intervals={invalid_intervals}"
            )
        return connection.execute(PROMO_SQL, with_column_types=True)

    if client is None:
        from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

        hook = ClickHouseHook(clickhouse_conn_id=config["source"]["clickhouse_conn_id"], use_numpy=False)
        with hook.get_conn() as connection:
            rows, columns = fetch(connection)
    else:
        rows, columns = fetch(client)
    promos = records_to_arrow(rows, columns, schema, constants)
    # Дни акций ограничены покрытием календаря.
    promos = promos.filter(pc.is_in(promos["date"], value_set=calendar["date"]))
    events = pa.concat_tables([holiday_events(calendar, schema, constants), promos])
    replace_table(table, events)
    return {"ingested_at": captured.strftime("%Y-%m-%d %H:%M:%S"), "rows": events.num_rows}
