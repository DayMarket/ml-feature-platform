"""Полностью заменить текущий SKU-каталог: dict.sku + категории + MDM + seller-каталог."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc

from .mapping import mark_category_conflicts, resolve_sku_links
from .query import category_query, golden_query, links_query, sku_query

logger = logging.getLogger("airflow.task")
BATCH_ROWS = 100_000
SELLER_FIELDS = (
    "source_master_seller_id", "master_seller_id", "seller_mapping_status",
    "has_master", "is_1p", "seller_registered_at",
)


def to_arrow(values, dtype: pa.DataType | None) -> pa.Array:
    if dtype is not None and pa.types.is_timestamp(dtype):
        return pa.array(values, type=pa.timestamp(dtype.unit, "UTC")).cast(dtype)
    return pa.array(values, type=dtype)


def clickhouse_arrow(client, sql: str, types: dict) -> pa.Table:
    """Прочитать запрос порциями; types задаёт Arrow-тип колонок (None — вывести)."""
    logger.info("ClickHouse query:\n%s", sql)
    stream = client.execute_iter(
        sql, with_column_types=True, chunk_size=BATCH_ROWS, settings={"max_block_size": BATCH_ROWS}
    )
    names, parts = None, []
    for chunk in stream:
        if names is None:
            names, chunk = [name for name, _ in chunk[0]], chunk[1:]
        if chunk:
            columns = list(zip(*chunk))
            parts.append(pa.table({
                name: to_arrow(column, types.get(name)) for name, column in zip(names, columns)
            }))
    if names is None:
        raise ValueError("ClickHouse не вернул метаданные колонок")
    if not parts:
        return pa.table({name: pa.array([], type=types.get(name) or pa.null()) for name in names})
    return pa.concat_tables(parts, promote_options="permissive")


def lookup(keys: pa.ChunkedArray, table: pa.Table, key: str, columns) -> dict:
    """LEFT JOIN по уникальному ключу через index_in."""
    positions = pc.index_in(keys, value_set=table[key])
    return {name: pc.take(table[name], positions) for name in columns}


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


def build_catalog(schema: pa.Schema, sku: pa.Table, categories: pa.Table, golden: pa.Table,
                  links: pa.Table, seller: pa.Table, constants: dict) -> pa.Table:
    types = {field.name: field.type for field in schema}
    categories = mark_category_conflicts(categories)
    mapping = resolve_sku_links(links, golden)
    values = {name: sku[name] for name in sku.column_names}
    values.update(lookup(sku["category_id"], categories, "category_id",
                         [name for name in categories.column_names if name != "category_id"]))
    values.update(lookup(sku["sku_id"], mapping, "sku_id", ["golden_sku_id", "golden_mapping_status"]))
    values.update(lookup(sku["seller_id"], seller, "seller_id", SELLER_FIELDS))
    values["category_path_status"] = pc.fill_null(values["category_path_status"], "missing")
    values["golden_mapping_status"] = pc.fill_null(values["golden_mapping_status"], "unmatched")
    values["seller_mapping_status"] = pc.fill_null(values["seller_mapping_status"], "unavailable")
    status = values["golden_mapping_status"]
    sku_unit = pc.binary_join_element_wise("s:", pc.cast(sku["sku_id"], pa.string()), "")
    golden_unit = pc.binary_join_element_wise("g:", values["golden_sku_id"], "")
    values["unit_id"] = pc.case_when(
        pc.make_struct(pc.equal(status, "matched"), pc.is_in(status, value_set=pa.array(["unmatched", "conflict"], type=status.type))),
        golden_unit, sku_unit, pa.scalar(None, pa.string()),
    )
    arrays = []
    for field in schema:
        if field.name in constants:
            arrays.append(pa.repeat(pa.scalar(constants[field.name], type=field.type), sku.num_rows))
        else:
            arrays.append(pc.cast(values[field.name], types[field.name]))
    return pa.Table.from_arrays(arrays, schema=schema)


def load(config: dict, seller_config: dict, *, run_id: str, client=None, catalog=None) -> dict:
    from dq.results_writer import load_results_catalog

    table_config = config["table"]
    catalog = catalog or load_results_catalog(table_config["catalog"])
    table = catalog.load_table((table_config["schema"], table_config["name"]))
    schema = table.schema().as_arrow()
    types = {field.name: field.type for field in schema}

    seller_table = catalog.load_table((seller_config["table"]["schema"], seller_config["table"]["name"]))
    seller_snapshot = seller_table.current_snapshot()
    if seller_snapshot is None:
        raise ValueError("Seller-каталог пуст")
    seller = seller_table.scan(
        snapshot_id=seller_snapshot.snapshot_id,
        selected_fields=("seller_id", "catalog_version", *SELLER_FIELDS),
    ).to_arrow()
    versions = pc.unique(seller["catalog_version"]).to_pylist()
    if len(versions) != 1:
        raise ValueError(f"Seller-каталог содержит {len(versions)} catalog_version")

    captured = datetime.now(timezone.utc).replace(microsecond=0)
    constants = {
        "date": captured.astimezone(ZoneInfo("Asia/Tashkent")).date(),
        "catalog_version": versions[0],
        "catalog_seller_snapshot_id": seller_snapshot.snapshot_id,
        "source_contract_version": config["source"]["contract_version"],
        "source_manifest_id": run_id,
        "ingested_at": captured,
    }

    def read(connection):
        return (
            clickhouse_arrow(connection, sku_query(config), types),
            clickhouse_arrow(connection, category_query(config), types),
            clickhouse_arrow(connection, golden_query(config),
                             {"golden_sku_id": pa.string(), "is_merged": pa.int8(), "merged_into": pa.string()}),
            clickhouse_arrow(connection, links_query(config),
                             {"sku_id": pa.int64(), "golden_sku_id": pa.string()}),
        )

    if client is None:
        from airflow_commons.hooks.clickhouse_hook import ClickHouseHook

        hook = ClickHouseHook(clickhouse_conn_id=config["source"]["conn_id"], use_numpy=False)
        with hook.get_conn() as connection:
            sku, categories, golden, links = read(connection)
    else:
        sku, categories, golden, links = read(client)
    data = build_catalog(schema, sku, categories, golden, links, seller, constants)
    del sku, categories, golden, links
    replace_table(table, data)
    return {"ingested_at": captured.strftime("%Y-%m-%d %H:%M:%S"), "rows": data.num_rows}
