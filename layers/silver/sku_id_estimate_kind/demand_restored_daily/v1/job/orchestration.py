"""Подключить CH/Trino и проверить схемы перед переносом удерживаемого E3-run."""

from contextlib import ExitStack, closing
from datetime import datetime, timezone
import logging
from pathlib import Path
import re

import pyarrow as pa
import yaml

from dq.config import load_dq_settings, trino_catalog_alias
from dq.day_range import validate_settings
from dq.results_writer import load_results_catalog
from dq.tests import quote_identifier
from feature_stats.day_range import validate_range_settings

from .preparation import prepare_batch, target_ref
from .query import source_schema_query
from .ranges import load_range, validate_request
from .runtime import preflight_target, read_run, source_arrow

logger = logging.getLogger("airflow.task")


def connection_ids(config):
    source = config["source"].get("conn_id")
    dq = load_dq_settings(config)
    validate_settings(dq)
    stats = validate_range_settings(config)
    names = (source, dq.trino_conn_id, stats.trino_conn_id)
    if any(not isinstance(name, str) or not name.strip() for name in names):
        raise ValueError("Нужны подтверждённые CH/DQ/stats connection IDs")
    return source, tuple(dict.fromkeys(names[1:]))


def service_schema(entity_path):
    """Читать простые колонки текущей DQ/stats миграции, неизвестный DDL отклонять."""
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


def same_type(actual, expected):
    return actual == expected or pa.types.is_string(expected) and pa.types.is_large_string(actual)


def validate_service_schema(actual, expected):
    if len(actual) != len(expected) or set(actual.names) != set(expected.names):
        raise ValueError("Service schema не соответствует миграции")
    for field in expected:
        found = actual.field(field.name)
        if not same_type(found.type, field.type) or found.nullable != field.nullable:
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


def preflight(config, repo_root, catalog, client, connections, selected, manifest):
    _, names = connection_ids(config)
    if not isinstance(connections, dict) or set(connections) != set(names):
        raise ValueError("Нужны все настроенные Trino connections")
    target = preflight_target(config, catalog, manifest)
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
    result = client.execute(source_schema_query(config), params=selected, with_column_types=True)
    if not isinstance(result, (list, tuple)) or len(result) != 2 or result[0] != []:
        raise ValueError("Metadata preflight CH должен вернуть только схему")
    raw = source_arrow([], result[1])
    passport = read_run(config, client, selected)
    prepare_batch(raw, target.schema().as_arrow(), selected=selected, run=passport, manifest=manifest,
                  version=config["source"]["contract_version"], ingested_at=datetime.now(timezone.utc))
    logger.info("E3 preflight: catalog=%s, namespace=%s, table=%s, tables=%d",
                catalog.name, config["table"]["schema"], config["table"]["name"], len(entries))
    return True


def execute_owner_range(config, repo_root, request, *, catalog=None, client=None, connections=None):
    """Owner использует реальную проверку immutable-run, не внешний always-True callback."""
    from .source_guard import ImmutableRunGuard

    selected = validate_request(config, request)[0]
    source, _ = connection_ids(config)
    with ExitStack() as stack:
        if client is None:
            from airflow_commons.hooks.clickhouse_hook import ClickHouseHook
            client = stack.enter_context(ClickHouseHook(clickhouse_conn_id=source, use_numpy=False).get_conn())
        guard = ImmutableRunGuard(config, client)
        guard(selected, read_run(config, client, selected))
        return execute_range(config, repo_root, request, require_run_held=guard,
                             catalog=catalog, client=client, connections=connections)


def execute_range(config, repo_root, request, *, require_run_held, catalog=None, client=None, connections=None):
    """Не создаёт и не снимает hold; обязательная проверка передаётся producer integration."""
    selections = validate_request(config, request)
    if not callable(require_run_held):
        raise ValueError("Нужна реальная проверка удержания E3-run")
    source, trino_ids = connection_ids(config)
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
        def checked(cfg, cat, source_client):
            return preflight(cfg, repo_root, cat, source_client, connections, selections[0], request["request_id"])
        return load_range(config, catalog, client, request, preflight=checked, require_run_held=require_run_held)
