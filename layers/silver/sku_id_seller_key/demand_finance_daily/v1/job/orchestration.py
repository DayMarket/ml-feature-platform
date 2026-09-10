"""Подключить диапазонный writer через Airflow Connections после preflight."""

from contextlib import ExitStack
from datetime import datetime, timezone
import logging
from pathlib import Path

import yaml

from dq.config import load_dq_settings, trino_catalog_alias
from dq.day_range import validate_settings
from dq.results_writer import load_results_catalog
from dq.tests import quote_identifier
from feature_stats.day_range import validate_range_settings

from .preparation import prepare_batch, target_ref, validate_schema
from .query import source_query, source_ref
from .ranges import load_range, validate_request
from .runtime import read_fx, source_arrow
from .service_schema import service_schema, validate_service_schema

logger = logging.getLogger("airflow.task")


def connection_ids(config):
    source = config["source"].get("conn_id")
    if not isinstance(source, str) or not source.strip():
        raise ValueError("Нужен подтверждённый source conn_id")
    settings = load_dq_settings(config)
    validate_settings(settings)
    stats = validate_range_settings(config)
    return source, tuple(dict.fromkeys((settings.trino_conn_id, stats.trino_conn_id)))


def source_schema_query(config, day):
    sql = source_query(config, day, fx_available=True)
    marker = "\nSETTINGS "
    if sql.count(marker) != 1:
        raise ValueError("Неизвестная форма source SQL для metadata preflight")
    return sql.replace(marker, "\nLIMIT 0\nSETTINGS ")


def preflight(config, repo_root, catalog, client, queries, day):
    """Проверить каталог, доступность таблиц через Trino и source output schema."""
    from pyiceberg.transforms import IdentityTransform

    _, trino_ids = connection_ids(config)
    if set(queries) != set(trino_ids) or any(not callable(q) for q in queries.values()):
        raise ValueError("Нужны проверки каждого настроенного Trino connection")
    source_ref(config)
    table_configs = [config["table"]]
    service_schemas = []
    for relative in ("dq/results/config.yaml", "feature_stats/results/config.yaml"):
        table_configs.append(yaml.safe_load((Path(repo_root) / relative).read_text())["table"])
        service_schemas.append(service_schema((Path(repo_root) / relative).parent))
    tables = []
    for item in table_configs:
        identifier = target_ref({"table": item}, catalog.name)
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет таблицы {item['catalog']}.{item['schema']}.{item['name']}: сначала миграции")
        tables.append(catalog.load_table(identifier))
    for table, expected in zip(tables[1:], service_schemas, strict=True):
        validate_service_schema(table.schema().as_arrow(), expected)
    target = tables[0]
    schema = target.schema().as_arrow()
    validate_schema(schema)
    fields = target.spec().fields
    if (len(fields) != 1 or fields[0].source_id != target.schema().find_field("date").field_id
            or not isinstance(fields[0].transform, IdentityTransform)):
        raise ValueError("Нужен identity partition по date")
    for key in ("max_batch_rows", "max_batch_bytes"):
        if type(config["runtime"][key]) is not int or config["runtime"][key] <= 0:
            raise ValueError("Неверные лимиты порций")
    for item in table_configs:
        alias = trino_catalog_alias(Path(repo_root), item["catalog"])
        ref = ".".join(quote_identifier(v) for v in (alias, item["schema"], item["name"]))
        for query in queries.values():
            rows = query(f"SELECT * FROM {ref} LIMIT 0")
            if not isinstance(rows, (list, tuple)) or rows:
                raise ValueError("Metadata preflight Trino должен вернуть пустой результат")
    result = client.execute(source_schema_query(config, day), with_column_types=True)
    if not isinstance(result, (list, tuple)) or len(result) != 2 or result[0] != []:
        raise ValueError("Metadata preflight CH должен вернуть только схему")
    raw = source_arrow([], result[1])
    fx = read_fx(client, day)
    prepare_batch(raw, schema, day=day, fx=fx, manifest="metadata-preflight",
                  version=config["source"]["contract_version"], ingested_at=datetime.now(timezone.utc))
    logger.info("Preflight: catalog=%s, namespace=%s, table=%s, service_tables=%d",
                catalog.name, config["table"]["schema"], config["table"]["name"], len(tables) - 1)
    return True


def execute_range(config, repo_root, request, *, catalog=None, client=None, queries=None):
    """Исполнить подготовленный запрос; результат written ещё не означает DQ passed."""
    days = validate_request(config, request)
    source_conn, trino_ids = connection_ids(config)
    with ExitStack() as stack:
        if catalog is None:
            catalog = load_results_catalog(config["table"]["catalog"])
        if client is None:
            from airflow_commons.hooks.clickhouse_hook import ClickHouseHook
            client = stack.enter_context(ClickHouseHook(clickhouse_conn_id=source_conn, use_numpy=False).get_conn())
        if queries is None:
            from airflow.providers.trino.hooks.trino import TrinoHook
            queries = {name: TrinoHook(trino_conn_id=name).get_records for name in trino_ids}
        def checked(_config, _catalog, _client):
            return preflight(_config, repo_root, _catalog, _client, queries, days[0])
        return load_range(config, catalog, client, request, preflight=checked)


def prepare_request(config, repo_root, *, run_id, mode, interval_start, interval_end,
                    history_start, catalog=None, query=None):
    """Построить фиксированное штатное окно или явный ручной диапазон."""
    from .ranges import build_request

    arguments = dict(run_id=run_id, mode=mode, interval_start=interval_start,
                     interval_end=interval_end, history_start=history_start)
    connection_ids(config)
    return build_request(config, **arguments)
