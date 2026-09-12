"""Подключить seller capture через Airflow Connections после проверки схем DQ/stats."""

from contextlib import ExitStack, closing
import logging
from pathlib import Path
import re

import pyarrow as pa
import yaml

from dq.config import load_dq_settings, trino_catalog_alias
from dq.results_writer import load_results_catalog
from dq.tests import quote_identifier
from feature_stats.config import load_feature_stats_settings

from .preparation import source_sql, target_ref
from .runtime import load_catalog, source_arrow, validate_arguments
from .writer import preflight as target_preflight

logger = logging.getLogger("airflow.task")


def connection_ids(config):
    dq = load_dq_settings(config)
    stats = load_feature_stats_settings(config)
    if dq.warmup_days != 0:
        raise ValueError("Полная замена seller-каталога требует dq.warmup_days: 0")
    if (dq.scope != "partition" or dq.partition_column != "ingested_at"
            or dq.partition_granularity != "timestamp" or not stats.enabled):
        raise ValueError("Catalog seller требует DQ и stats точного времени захвата")
    names = (config["source"].get("conn_id"), config["dq"].get("trino_conn_id"),
             config.get("feature_stats", {}).get("trino_conn_id"))
    if any(not isinstance(name, str) or not name.strip() or name != name.strip() for name in names):
        raise ValueError("Нужны явные согласованные CH/DQ/stats connection IDs")
    if (dq.trino_conn_id, stats.trino_conn_id) != names[1:]:
        raise ValueError("Connection IDs не совпали с DQ/stats settings")
    return names[0], tuple(dict.fromkeys(names[1:]))


def service_schema(entity_path):
    """Прочитать простые поля DQ/stats DDL, отклонить неизвестную форму миграции."""
    ddl = (Path(entity_path) / "migrations/create_table.sql").read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE IF NOT EXISTS \{target_table\}\s*\((.*?)\n\)\s*USING iceberg", ddl, re.S)
    if body is None:
        raise ValueError("Неподдержанная форма service migration")
    types = {"DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "STRING": pa.string(),
             "INT": pa.int32(), "BIGINT": pa.int64(), "DOUBLE": pa.float64(), "BOOLEAN": pa.bool_()}
    fields = []
    for line in body.group(1).splitlines():
        if not line.strip():
            continue
        column = re.fullmatch(r"\s*([a-z_][a-z0-9_]*)\s+([A-Z]+)( NOT NULL)? COMMENT '(?:''|[^'])*',?\s*", line)
        if column is None or column[2] not in types:
            raise ValueError("Неподдержанная колонка service migration")
        fields.append(pa.field(column[1], types[column[2]], nullable=not bool(column[3])))
    if not fields or len({field.name for field in fields}) != len(fields):
        raise ValueError("Пустая/неоднозначная service schema")
    return pa.schema(fields)


def validate_service_schema(actual, expected):
    if len(actual) != len(expected) or set(actual.names) != set(expected.names):
        raise ValueError("Service schema не соответствует миграции")
    for field in expected:
        found = actual.field(field.name)
        same_type = found.type == field.type or pa.types.is_string(field.type) and pa.types.is_large_string(found.type)
        if not same_type or found.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable service.{field.name}")


def metadata_query(connection, sql, schema):
    types = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint",
             pa.float64(): "double", pa.bool_(): "boolean", pa.timestamp("us"): "timestamp(6)"}
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        columns = cursor.description
        if not isinstance(columns, (list, tuple)) or len(columns) != len(schema):
            raise ValueError("Неверная metadata Trino")
        for description, field in zip(columns, schema, strict=True):
            if not isinstance(description, (list, tuple)) or len(description) < 2 or description[0] != field.name:
                raise ValueError("Неверные колонки Trino metadata")
            kind = str(description[1]).lower().replace(" ", "")
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                valid = re.fullmatch(r"varchar(?:\(\d+\))?", kind) is not None
            else:
                valid = field.type in types and kind == types[field.type]
            if not valid:
                raise ValueError(f"Неверный тип Trino {field.name}: {kind}")
        rows = cursor.fetchmany(1)
        if not isinstance(rows, (list, tuple)) or rows:
            raise ValueError("Metadata LIMIT 0 должен вернуть пустой результат")


def preflight(config, repo_root, catalog, client, connections):
    _, names = connection_ids(config)
    if not isinstance(connections, dict) or set(connections) != set(names):
        raise ValueError("Нужны все настроенные Trino connections")
    target = target_preflight(config, catalog)
    stats = load_feature_stats_settings(config)
    if set(stats.exclude_columns) - set(target.schema().column_names):
        raise ValueError("Неизвестные feature_stats.exclude_columns")
    entries = [(config, target)]
    for relative in ("dq/results/config.yaml", "feature_stats/results/config.yaml"):
        path = Path(repo_root) / relative
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        identifier = target_ref(cfg, catalog.name)
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет служебной таблицы {identifier}: сначала миграции")
        table = catalog.load_table(identifier)
        validate_service_schema(table.schema().as_arrow(), service_schema(path.parent))
        entries.append((cfg, table))
    for cfg, table in entries:
        meta = cfg["table"]
        alias = trino_catalog_alias(Path(repo_root), meta["catalog"])
        ref = ".".join(quote_identifier(v) for v in (alias, meta["schema"], meta["name"]))
        for connection in connections.values():
            metadata_query(connection, f"SELECT * FROM {ref} LIMIT 0", table.schema().as_arrow())
    result = client.execute(source_sql(config).replace(" SETTINGS ", " LIMIT 0 SETTINGS "), with_column_types=True)
    if not isinstance(result, (list, tuple)) or len(result) != 2 or result[0] != []:
        raise ValueError("Source metadata preflight должен вернуть только типы")
    source_arrow([], result[1])
    logger.info("Seller preflight: catalog=%s, namespace=%s, table=%s, service_tables=%d",
                catalog.name, config["table"]["schema"], config["table"]["name"], len(entries) - 1)


def execute_capture(config, repo_root, *, catalog_version, source_manifest_id,
                    catalog=None, client=None, connections=None):
    """Открытые здесь CH/Trino закрываются при успехе и ошибке; переданные — у caller."""
    validate_arguments(config, catalog_version=catalog_version, source_manifest_id=source_manifest_id)
    source, trino_ids = connection_ids(config)
    target_ref(config, config["table"].get("catalog"))
    with ExitStack() as stack:
        if catalog is None:
            catalog = load_results_catalog(config["table"]["catalog"])
        if client is None:
            from airflow_commons.hooks.clickhouse_hook import ClickHouseHook
            client = stack.enter_context(ClickHouseHook(clickhouse_conn_id=source, use_numpy=False).get_conn())
        if connections is None:
            from airflow.providers.trino.hooks.trino import TrinoHook
            connections = {}
            for name in trino_ids:
                connection = TrinoHook(trino_conn_id=name).get_conn()
                stack.callback(connection.close)
                connections[name] = connection
        preflight(config, repo_root, catalog, client, connections)
        return load_catalog(config, catalog, client, catalog_version=catalog_version,
                            source_manifest_id=source_manifest_id)
