"""Полностью заменить дерево категорий, построенное из текущего SKU-каталога."""

from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import logging
from pathlib import Path

import pyarrow as pa

logger = logging.getLogger("airflow.task")


def source_query(sku: str) -> str:
    """Уникальные рёбра market → l1 → … → leaf из валидных путей SKU.

    Passthrough — узел L2+ повторяет категорию родителя (выравнивание глубины).
    Узел с несколькими родителями даст повтор ключа и будет отклонён DQ.
    """
    return f"""WITH paths AS (
    SELECT DISTINCT market, l1, l2, l3, l4, l5, leaf, "date", catalog_version
    FROM {sku}
    WHERE category_path_status = 'valid'
)
SELECT DISTINCT
    p."date",
    t.level,
    t.node_id,
    t.level_code,
    t.parent_id,
    t.level_code >= 2 AND split_part(t.node_id, ':', 2) = split_part(t.parent_id, ':', 2) AS is_passthrough,
    p.catalog_version
FROM paths AS p
CROSS JOIN UNNEST(
    ARRAY['market', 'l1', 'l2', 'l3', 'l4', 'l5', 'leaf'],
    ARRAY[0, 1, 2, 3, 4, 5, 6],
    ARRAY[p.market, p.l1, p.l2, p.l3, p.l4, p.l5, p.leaf],
    ARRAY[CAST(NULL AS VARCHAR), p.market, p.l1, p.l2, p.l3, p.l4, p.l5]
) AS t(level, level_code, node_id, parent_id)
ORDER BY t.level_code, t.node_id"""


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


def load(config: dict, sku_config: dict, repo_root: str, *, run_id: str,
         connection=None, catalog=None) -> dict:
    from dq.config import trino_catalog_alias
    from dq.results_writer import load_results_catalog

    table_config = config["table"]
    catalog = catalog or load_results_catalog(table_config["catalog"])
    table = catalog.load_table((table_config["schema"], table_config["name"]))
    source_config = sku_config["table"]
    snapshot = catalog.load_table((source_config["schema"], source_config["name"])).current_snapshot()
    if snapshot is None:
        raise ValueError("SKU-каталог пуст")
    alias = trino_catalog_alias(Path(repo_root), source_config["catalog"])
    sku = f"{quote(alias, source_config['schema'], source_config['name'])} FOR VERSION AS OF {snapshot.snapshot_id}"
    captured = datetime.now(timezone.utc).replace(microsecond=0)
    constants = {
        "catalog_sku_snapshot_id": snapshot.snapshot_id,
        "source_contract_version": config["source"]["contract_version"],
        "source_manifest_id": run_id,
        "ingested_at": captured,
    }
    if connection is None:
        from airflow.providers.trino.hooks.trino import TrinoHook

        connection = TrinoHook(trino_conn_id=config["source"]["conn_id"]).get_conn()
    sql = source_query(sku)
    logger.info("Trino query:\n%s", sql)
    with closing(connection), closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
    replace_table(table, rows_to_arrow(names, rows, table.schema().as_arrow(), constants))
    return {"ingested_at": captured.strftime("%Y-%m-%d %H:%M:%S"), "rows": len(rows)}
