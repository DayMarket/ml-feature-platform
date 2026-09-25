"""Полностью заменить текущий seller-master каталог из marts.sellers_info."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger("airflow.task")


def source_query(config: dict) -> str:
    """Статус master-связи: непустой master — matched, пустой — unmatched, NULL — unavailable."""
    source = config["source"]
    return f"""SELECT
    toInt64(sid) AS seller_id,
    raw_master AS source_master_seller_id,
    multiIf(isNull(raw_master), NULL, trimBoth(raw_master) != '', trimBoth(raw_master), toString(sid)) AS master_seller_id,
    multiIf(isNull(raw_master), 'unavailable', trimBoth(raw_master) != '', 'matched', 'unmatched') AS seller_mapping_status,
    CAST(if(isNull(raw_master), NULL, trimBoth(raw_master) != '') AS Nullable(Bool)) AS has_master,
    raw_is_1p AS is_1p,
    toDateTime64(toTimeZone(registration_date, 'UTC'), 6, 'UTC') AS seller_registered_at
FROM (
    SELECT seller_id AS sid, master_seller_id AS raw_master, is_1p AS raw_is_1p, registration_date
    FROM {source['database']}.{source['table']}
)
ORDER BY seller_id
SETTINGS max_threads = 1, max_execution_time = 600"""


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


def check_fallback_collisions(data: pa.Table) -> None:
    """seller_id, подставленный как master, не должен совпадать с реальным master."""
    status = data["seller_mapping_status"]
    real = pc.filter(data["master_seller_id"], pc.equal(status, "matched"))
    fallback = pc.filter(data["master_seller_id"], pc.equal(status, "unmatched"))
    collisions = pc.sum(pc.is_in(fallback, value_set=pc.unique(real))).as_py() or 0
    if collisions:
        raise ValueError(f"{collisions} fallback master_seller_id совпадают с реальными master")


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
    from dq.results_writer import load_results_catalog

    table_config = config["table"]
    catalog = catalog or load_results_catalog(table_config["catalog"])
    table = catalog.load_table((table_config["schema"], table_config["name"]))
    captured = datetime.now(timezone.utc).replace(microsecond=0)
    constants = {
        "date": captured.astimezone(ZoneInfo("Asia/Tashkent")).date(),
        # SKU и tree наследуют catalog_version этого захвата.
        "catalog_version": f"catalog:{run_id}",
        "source_contract_version": config["source"]["contract_version"],
        "source_manifest_id": run_id,
        "ingested_at": captured,
    }
    sql = source_query(config)
    logger.info("ClickHouse query:\n%s", sql)
    if client is None:
        from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

        hook = ClickHouseHook(clickhouse_conn_id=config["source"]["conn_id"], use_numpy=False)
        with hook.get_conn() as connection:
            rows, columns = connection.execute(sql, with_column_types=True)
    else:
        rows, columns = client.execute(sql, with_column_types=True)
    data = records_to_arrow(rows, columns, table.schema().as_arrow(), constants)
    del rows
    check_fallback_collisions(data)
    replace_table(table, data)
    return {"ingested_at": captured.strftime("%Y-%m-%d %H:%M:%S"), "rows": data.num_rows}
