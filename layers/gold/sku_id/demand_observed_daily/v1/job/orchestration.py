"""Открыть Airflow Connections и проверить все таблицы перед gold range load."""

from contextlib import ExitStack, closing
from datetime import date, datetime, timezone
from decimal import Decimal
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

from .inputs import bind_inputs, preflight_inputs, source_configs
from .planning import checked_references, execute_request as execute_planned, validate_request
from .preparation import SOURCE_FIELDS, same_type, target_ref
from .reader import exact_value, source_sql, validate_description

logger = logging.getLogger("airflow.task")
NATIVE_PROBE_SQL = """SELECT
CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)) AS raw_amount,
DATE '2026-09-01' AS source_date,
CAST(TIMESTAMP '2026-09-09 04:00:00.123456' AS TIMESTAMP(6)) AS captured,
CAST(NULL AS DOUBLE) AS unknown_usd,
CAST(1.25 AS DOUBLE) AS known_usd,
BIGINT '0' AS raw_zero"""


def connection_ids(config):
    source = config["source"].get("conn_id")
    dq = load_dq_settings(config)
    validate_settings(dq)
    stats = validate_range_settings(config)
    names = (source, dq.trino_conn_id, stats.trino_conn_id)
    if any(not isinstance(name, str) or not name.strip() for name in names):
        raise ValueError("Нужны подтверждённые Trino connection IDs source/DQ/stats")
    return tuple(dict.fromkeys(names))


def read_checked(task_instance, references):
    """Читать именно результат проверок данных, не operational task status."""
    refs = checked_references(references)
    checked = {}
    for kind, ref in refs.items():
        payload = task_instance.xcom_pull(dag_id=ref["dag_id"], task_ids="dq", run_id=ref["run_id"],
                                         include_prior_dates=False)
        if (not isinstance(payload, dict) or payload.get("dq_status") != "passed"
                or any(payload.get(key) != ref[key] for key in ("dag_id", "run_id"))):
            raise ValueError(f"Нет passed DQ payload точного run {kind}, latest запрещён")
        checked[kind] = payload
    return checked


def native_probe(connection):
    schema = pa.schema([pa.field("raw_amount", pa.decimal128(38, 0)), pa.field("source_date", pa.date32()),
                        pa.field("captured", pa.timestamp("us")), pa.field("unknown_usd", pa.float64()),
                        pa.field("known_usd", pa.float64()), pa.field("raw_zero", pa.int64())])
    with closing(connection.cursor()) as cursor:
        cursor.execute(NATIVE_PROBE_SQL)
        validate_description(cursor.description, schema, schema.names)
        rows = cursor.fetchmany(2)
        if (not isinstance(rows, (list, tuple)) or len(rows) != 1
                or not isinstance(rows[0], (list, tuple)) or len(rows[0]) != len(schema)):
            raise ValueError("Неверный ответ Trino native type probe")
        values = [exact_value(value, field) for value, field in zip(rows[0], schema, strict=True)]
        if values != [Decimal("12345678901234567890123456789012345678"), date(2026, 9, 1),
                      datetime(2026, 9, 9, 4, 0, 0, 123456), None, 1.25, 0]:
            raise ValueError("Trino connection не сохраняет Decimal/NULL/timestamp")


def metadata_query(connection, sql, *, schema=None, columns=None):
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        if schema is not None:
            validate_description(cursor.description, schema, columns)
        rows = cursor.fetchmany(1)
        if not isinstance(rows, (list, tuple)) or rows:
            raise ValueError("Metadata LIMIT 0 должен вернуть пустой результат")


def service_schema(entity_path):
    """Прочитать простые колонки текущего create_table DQ/stats; незнакомый DDL — отказ."""
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
    if set(actual.names) != set(expected.names) or len(actual) != len(expected):
        raise ValueError("Service schema не соответствует миграции")
    for field in expected:
        found = actual.field(field.name)
        if not same_type(found.type, field.type) or found.nullable != field.nullable:
            raise ValueError(f"Неверный тип/nullable service.{field.name}")


def preflight(config, repo_root, catalog, connections, sources, bound, days):
    names = connection_ids(config)
    if not isinstance(connections, dict) or set(connections) != set(names):
        raise ValueError("Нужны все настроенные source/DQ/stats connections")
    tables = preflight_inputs(config, sources, catalog, bound)
    configs = {**sources, "output": config}
    for kind, path in (("dq", "dq/results/config.yaml"), ("stats", "feature_stats/results/config.yaml")):
        cfg = yaml.safe_load((Path(repo_root) / path).read_text(encoding="utf-8"))
        identifier = target_ref(cfg, catalog.name)
        if not catalog.table_exists(identifier):
            raise ValueError(f"Нет служебной таблицы {identifier}: сначала миграции")
        tables[kind] = catalog.load_table(identifier)
        validate_service_schema(tables[kind].schema().as_arrow(), service_schema((Path(repo_root) / path).parent))
        configs[kind] = cfg
    native_probe(connections[config["source"]["conn_id"]])
    for kind, cfg in configs.items():
        table = cfg["table"]
        alias = trino_catalog_alias(Path(repo_root), table["catalog"])
        ref = ".".join(quote_identifier(v) for v in (alias, table["schema"], table["name"]))
        for connection in connections.values():
            schema = tables[kind].schema().as_arrow() if kind in ("dq", "stats") else None
            metadata_query(connection, f"SELECT * FROM {ref} LIMIT 0", schema=schema,
                           columns=schema.names if schema is not None else None)
    for kind, source in sources.items():
        sql = source_sql(kind, source, repo_root, bound[kind], days[0]) + "\nLIMIT 0"
        metadata_query(connections[config["source"]["conn_id"]], sql,
                       schema=tables[kind].schema().as_arrow(), columns=SOURCE_FIELDS[kind])
    logger.info("Gold preflight: catalog=%s, namespace=%s, table=%s, tables=%d, connections=%d",
                catalog.name, config["table"]["schema"], config["table"]["name"], len(tables), len(names))
    return True


def execute_request(config, repo_root, request, *, task_instance=None, fetch_checked=None,
                    catalog=None, connections=None, ingested_at=None):
    """Открытые здесь connections закрываются при любом исходе; внешние остаются caller."""
    days = validate_request(config, request)
    names = connection_ids(config)
    captured = datetime.now(timezone.utc) if ingested_at is None else ingested_at
    if (not isinstance(captured, datetime) or captured.utcoffset() is None
            or days[-1] >= captured.astimezone(timezone.utc).date()):
        raise ValueError("Нужны aware capture и завершённые дни")
    sources = source_configs(config, repo_root)
    references = checked_references(request["references"])
    if any(ref["dag_id"] != sources[kind]["dag"]["id"] for kind, ref in references.items()):
        raise ValueError("Неверные silver DQ владельцы")
    if fetch_checked is None:
        if task_instance is None:
            from airflow.sdk import get_current_context
            task_instance = get_current_context()["ti"]
        def fetch_checked(refs):
            return read_checked(task_instance, refs)
    if not callable(fetch_checked):
        raise ValueError("Нужно чтение exact upstream DQ")
    initial = bind_inputs(sources, references, fetch_checked(references), days=days, captured_at=captured)

    def unchanged(refs):
        checked = fetch_checked(refs)
        if bind_inputs(sources, refs, checked, days=days, captured_at=captured) != initial:
            raise ValueError("Upstream DQ изменился после connection preflight")
        return checked

    with ExitStack() as stack:
        if catalog is None:
            catalog = load_results_catalog(config["table"]["catalog"])
        if connections is None:
            from airflow.providers.trino.hooks.trino import TrinoHook
            connections = {}
            for name in names:
                connection = TrinoHook(trino_conn_id=name).get_conn()
                stack.callback(connection.close)
                connections[name] = connection
        preflight(config, repo_root, catalog, connections, sources, initial, days)
        return execute_planned(config, repo_root, catalog, connections[config["source"]["conn_id"]],
                               request, fetch_checked=unchanged, ingested_at=captured)


def prepare_request(config, repo_root, *, catalog=None, query=None, **arguments):
    """Построить ручной план либо зафиксировать состояние regular-выхода."""
    from .planning import prepare_request as prepare_plan, validate_arguments

    validate_arguments(config, repo_root, arguments)
    connection_ids(config)
    if arguments["mode"] == "manual":
        return prepare_plan(config, repo_root, catalog=None, query=None, **arguments)
    if catalog is None:
        catalog = load_results_catalog(config["table"]["catalog"])
    return prepare_plan(config, repo_root, catalog=catalog, query=None, **arguments)
