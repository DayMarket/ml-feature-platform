"""Проверить схемы/подключения seller bridge без production Airflow/Trino/CH."""

from copy import deepcopy
from types import ModuleType, SimpleNamespace
import sys

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_catalog_seller_runtime import Source, ROOT, env as env, target
from layers.silver.seller_id.demand_catalog_seller.v1.job import orchestration as bridge


def description(schema):
    types = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint",
             pa.float64(): "double", pa.bool_(): "boolean", pa.timestamp("us"): "timestamp(6)"}
    return [(field.name, "varchar" if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
             else types[field.type], None, None, None, None, None) for field in schema]


class Client(Source):
    def __init__(self):
        super().__init__()
        self.connection_closed = False
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.connection_closed = True


class Connection:
    def __init__(self, schemas):
        self.schemas, self.failure, self.cursors, self.closed = schemas, None, [], False
    def cursor(self):
        cursor = SimpleNamespace(description=None, closed=False)
        def execute(sql):
            assert sql.startswith('SELECT * FROM "dwh-iceberg".') and sql.endswith("LIMIT 0")
            cursor.sql = sql
            schema = next(schema for name, schema in self.schemas.items() if f'"{name}"' in sql)
            cursor.description = description(schema)
            if self.failure == "unreadable":
                raise RuntimeError("service unreadable")
            if self.failure == "missing_column":
                cursor.description.pop()
            if self.failure == "wrong_type":
                cursor.description[0] = (schema.names[0], "varchar", None, None, None, None, None)
        def close():
            cursor.closed = True
        cursor.execute, cursor.close = execute, close
        cursor.fetchmany = lambda _: [[1]] if self.failure == "nonempty" else []
        self.cursors.append(cursor)
        return cursor
    def close(self):
        self.closed = True


@pytest.fixture
def prepared_env(env):
    schemas = {env[0]["table"]["name"]: target(env).schema().as_arrow()}
    for relative in ("dq/results", "feature_stats/results"):
        meta = yaml.safe_load((ROOT / relative / "config.yaml").read_text())["table"]
        schema = bridge.service_schema(ROOT / relative)
        table = env[1].create_table((meta["schema"], meta["name"]), schema=schema)
        schemas[meta["name"]] = table.schema().as_arrow()
    return env, Client(), Connection(schemas)


def execute(prepared_env, **kwargs):
    destination, client, connection = prepared_env
    options = dict(catalog_version="catalog-1", source_manifest_id="capture-1", catalog=destination[1],
                   client=client, connections={"trino_search": connection}) | kwargs
    return bridge.execute_capture(destination[0], ROOT, **options)


def test_all_service_checks_before_capture_and_no_close_for_caller_connections(prepared_env):
    written = execute(prepared_env)
    dest, client, connection = prepared_env
    assert written["status"] == "written" and "dq_status" not in written
    assert client.streams == client.closed == 2
    assert len(connection.cursors) == 3 and all(cursor.closed for cursor in connection.cursors)
    assert not connection.closed and not client.connection_closed
    assert target(dest).scan().count() == 3


@pytest.mark.parametrize("failure", ["unreadable", "missing_column", "wrong_type", "nonempty"])
def test_bad_trino_preflight_never_reads_source(prepared_env, failure):
    dest, client, connection = prepared_env
    connection.failure = failure
    with pytest.raises((ValueError, RuntimeError)):
        execute(prepared_env)
    assert client.queries == [] and client.streams == 0
    assert target(dest).current_snapshot() is None
    assert all(cursor.closed for cursor in connection.cursors)


@pytest.mark.parametrize("service", ["dq/results", "feature_stats/results"])
@pytest.mark.parametrize("failure", ["missing_table", "missing_field", "wrong_nullable"])
def test_service_physical_schema_must_match_migration(prepared_env, service, failure):
    dest, client, connection = prepared_env
    meta = yaml.safe_load((ROOT / service / "config.yaml").read_text())["table"]
    identifier = meta["schema"], meta["name"]
    original = dest[1].load_table(identifier).schema().as_arrow()
    if failure == "missing_table":
        dest[1].drop_table(identifier)
    elif failure == "missing_field":
        with dest[1].load_table(identifier).update_schema() as update:
            update.delete_column(original.names[-1])
    else:
        dest[1].drop_table(identifier)
        fields = [pa.field(field.name, field.type, nullable=False if index == 0 else field.nullable)
                  for index, field in enumerate(original)]
        dest[1].create_table(identifier, schema=pa.schema(fields))
    with pytest.raises(ValueError):
        execute(prepared_env)
    assert client.queries == [] and connection.cursors == []
    assert target(dest).current_snapshot() is None


def test_invalid_source_type_stops_before_stream(prepared_env):
    dest, client, connection = prepared_env
    client.columns[0] = ("seller_id", "Float64")
    with pytest.raises(ValueError, match="source type"):
        execute(prepared_env)
    assert client.streams == 0 and len(client.queries) == 1
    assert target(dest).current_snapshot() is None


@pytest.mark.parametrize("section,key,value", [
    ("source", "conn_id", None), ("dq", "trino_conn_id", None),
    ("feature_stats", "trino_conn_id", ""), ("source", "conn_id", " spaced "),
    ("dq", "scope", "table"),
    ("feature_stats", "enabled", False), ("dq", "warmup_days", 1), ("dq", "warmup_days", 3)])
def test_invalid_contract_stops_before_connections(prepared_env, monkeypatch, section, key, value):
    prepared_env[0][0][section][key] = value
    monkeypatch.setattr(bridge, "load_results_catalog", lambda _: pytest.fail("Не открывать catalog"))
    with pytest.raises(ValueError):
        execute(prepared_env, catalog=None, client=None, connections=None)
    assert not prepared_env[1].queries


def test_invalid_capture_arguments_stop_before_connections(prepared_env, monkeypatch):
    monkeypatch.setattr(bridge, "load_results_catalog", lambda _: pytest.fail("Не открывать catalog"))
    with pytest.raises(ValueError, match="catalog version"):
        execute(prepared_env, catalog_version="", catalog=None, client=None, connections=None)


def test_invalid_stats_exclude_stops_before_source(prepared_env):
    prepared_env[0][0]["feature_stats"]["exclude_columns"] = ["missing"]
    with pytest.raises(ValueError, match="exclude_columns"):
        execute(prepared_env)
    assert not prepared_env[1].queries


@pytest.mark.parametrize("failure", [None, "source_type", "source_stream", "trino_open", "second_trino_open"])
def test_own_connections_close_on_all_paths(prepared_env, monkeypatch, failure):
    dest, client, connection = prepared_env
    if failure == "source_type":
        client.columns[0] = ("seller_id", "Float64")
    if failure == "source_stream":
        client.fail_on_repeat = True
    if failure == "second_trino_open":
        dest[0]["feature_stats"]["trino_conn_id"] = "trino_recsys"
    ch, trino = ModuleType("airflow_commons.hooks.clickhouse_hook"), ModuleType("airflow.providers.trino.hooks.trino")
    calls = []
    class CHHook:
        def __init__(self, **kwargs):
            calls.append(kwargs)
        def get_conn(self):
            return client
    class TrinoHook:
        def __init__(self, trino_conn_id):
            self.name = trino_conn_id
            calls.append(trino_conn_id)
        def get_conn(self):
            if failure == "trino_open" or self.name == "trino_recsys":
                raise RuntimeError("trino unavailable")
            return connection
    ch.ClickHouseHook, trino.TrinoHook = CHHook, TrinoHook
    monkeypatch.setitem(sys.modules, ch.__name__, ch)
    monkeypatch.setitem(sys.modules, trino.__name__, trino)
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            execute(prepared_env, client=None, connections=None)
    else:
        execute(prepared_env, client=None, connections=None)
    assert calls[:2] == [{"clickhouse_conn_id": "clickhouse_dwh_team_logistics", "use_numpy": False}, "trino_search"]
    assert client.connection_closed and connection.closed == (failure != "trino_open")
    assert all(cursor.closed for cursor in connection.cursors)


def test_missing_connection_mapping_fails_preflight(prepared_env):
    with pytest.raises(ValueError, match="все настроенные"):
        execute(prepared_env, connections={})
    assert not prepared_env[1].queries


def test_current_catalog_has_no_numeric_nonkey_feature(prepared_env):
    from feature_stats.config import load_feature_stats_settings
    from feature_stats.task import build_stats_context
    from feature_stats.runner import run_feature_stats

    dest, _, _ = prepared_env
    cfg = dest[0]
    settings = load_feature_stats_settings(cfg)
    context = build_stats_context(cfg, ROOT, "2026-09-09T04:00:00+00:00")
    metadata = [(row[0], row[1]) for row in description(target(dest).schema().as_arrow())]
    calls = []
    def query(sql):
        assert "information_schema" in sql
        calls.append(sql)
        return metadata
    assert run_feature_stats(settings, context, query) == []
    assert len(calls) == 1 and settings.enabled is True


def test_type_and_nullability_validation_is_strict():
    expected = pa.schema([pa.field("counter", pa.int64(), nullable=False)])
    for actual in (pa.schema([pa.field("counter", pa.float64(), nullable=False)]),
                   pa.schema([pa.field("counter", pa.int64(), nullable=True)])):
        with pytest.raises(ValueError, match="тип/nullable"):
            bridge.validate_service_schema(actual, expected)


def test_config_connection_lists_do_not_require_extra_trino_when_shared(prepared_env):
    cfg = deepcopy(prepared_env[0][0])
    assert bridge.connection_ids(cfg) == ("clickhouse_dwh_team_logistics", ("trino_search",))
