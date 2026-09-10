"""E3 bridge: preflight до записи, обязательный hold и закрытие собственных клиентов."""

from datetime import timedelta
from types import ModuleType, SimpleNamespace
import sys

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_daily_preparation import CAPTURE, ROOT, module
from ci_test.test_demand_restored_runtime import Client as SourceClient, loader as loader
from ci_test.test_demand_restored_writer import target as target
from ci_test.test_demand_sales_finance_runtime import wire

BRIDGE = module("restored", "orchestration")
RANGES = module("restored", "ranges")


def description(schema):
    types = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint",
             pa.float64(): "double", pa.bool_(): "boolean", pa.timestamp("us"): "timestamp(6)"}
    return [(field.name, "varchar" if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
             else types[field.type], None, None, None, None, None) for field in schema]


class Client(SourceClient):
    def __init__(self, config, data):
        super().__init__(config, data)
        self.metadata_calls = 0
        self.connection_closed = False
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.connection_closed = True
    def execute(self, sql, *, params, with_column_types):
        if sql == module("restored", "query").source_schema_query(self.config):
            self.metadata_calls += 1
            assert params == self.data[0] and with_column_types
            columns = wire(self.data[2][0], "restored")[1]
            if self.failure == "source_type":
                columns = [(name, "Int64" if name == "estimate_kind" else kind) for name, kind in columns]
            elif self.failure == "source_missing":
                columns.pop()
            return ([[1]] if self.failure == "source_nonempty" else []), columns
        return super().execute(sql, params=params, with_column_types=with_column_types)


class Connection:
    def __init__(self, schemas):
        self.schemas, self.failure, self.cursors, self.closed = schemas, None, [], False
    def cursor(self):
        cursor = SimpleNamespace(description=None, closed=False)
        def execute(sql):
            assert sql.endswith("LIMIT 0") and sql.startswith('SELECT * FROM "dwh-iceberg".')
            schema = next(value for key, value in self.schemas.items() if f'"{key}"' in sql)
            cursor.description = description(schema)
            if self.failure == "unreadable":
                raise RuntimeError("service unreadable")
            if self.failure == "metadata":
                cursor.description.pop()
        def close():
            cursor.closed = True
        cursor.execute, cursor.close = execute, close
        cursor.fetchmany = lambda size: [[1]] if self.failure == "nonempty" else []
        self.cursors.append(cursor)
        return cursor
    def close(self):
        self.closed = True


@pytest.fixture
def bridge_env(loader, monkeypatch):
    destination, source = loader
    cfg, catalog, table = destination
    schemas = {cfg["table"]["name"]: table.schema().as_arrow()}
    for path in ("dq/results", "feature_stats/results"):
        meta = yaml.safe_load((ROOT / path / "config.yaml").read_text())["table"]
        schema = BRIDGE.service_schema(ROOT / path)
        catalog.create_table((meta["schema"], meta["name"]), schema=schema)
        schemas[meta["name"]] = schema
    monkeypatch.setattr(BRIDGE, "datetime", SimpleNamespace(now=lambda _: CAPTURE + timedelta(seconds=1)))
    request = RANGES.build_request(cfg, copy_id="exact-copy", selections=[source.data[0]])
    return destination, Client(cfg, source.data), Connection(schemas), request


def execute(env, **changes):
    destination, client, connection, request = env
    options = dict(require_run_held=lambda *args: True, catalog=destination[1], client=client,
                   connections={"trino_search": connection}) | changes
    return BRIDGE.execute_range(destination[0], ROOT, request, **options)


def test_bridge_copies_exact_run_after_service_and_source_preflight(bridge_env):
    written = execute(bridge_env)
    destination, client, connection, _ = bridge_env
    assert written["status"] == "written" and "dq_status" not in written
    assert client.metadata_calls == 1 and client.streamed and client.closed
    assert len(connection.cursors) == 3 and all(c.closed for c in connection.cursors)
    assert not connection.closed and not client.connection_closed
    assert destination[2].refresh().scan().count() == 2


@pytest.mark.parametrize("failure", ["source_type", "source_missing", "source_nonempty", "status",
                                    "metadata", "nonempty", "unreadable", "missing_service", "service_type"])
def test_failed_preflight_never_streams_or_writes(bridge_env, failure):
    destination, client, connection, _ = bridge_env
    client.failure = connection.failure = failure
    if failure == "missing_service":
        meta = yaml.safe_load((ROOT / "dq/results/config.yaml").read_text())["table"]
        destination[1].drop_table((meta["schema"], meta["name"]))
    elif failure == "service_type":
        meta = yaml.safe_load((ROOT / "dq/results/config.yaml").read_text())["table"]
        table = destination[1].load_table((meta["schema"], meta["name"]))
        with table.update_schema() as schema:
            schema.delete_column("status")
    with pytest.raises((ValueError, RuntimeError)):
        execute(bridge_env)
    assert not client.streamed and destination[2].refresh().current_snapshot() is None
    assert all(cursor.closed for cursor in connection.cursors)


def test_missing_hold_callback_rejected_before_connections(bridge_env, monkeypatch):
    monkeypatch.setattr(BRIDGE, "load_results_catalog", lambda *a: pytest.fail("Не открывать catalog"))
    with pytest.raises(ValueError, match="удержания"):
        execute(bridge_env, require_run_held=None, catalog=None, client=None, connections=None)
    assert bridge_env[1].metadata_calls == 0 and bridge_env[2].cursors == []


def test_false_hold_blocks_result_stream_even_when_metadata_is_readable(bridge_env):
    with pytest.raises(ValueError, match="удерживается"):
        execute(bridge_env, require_run_held=lambda *args: False)
    assert not bridge_env[1].streamed and bridge_env[0][2].refresh().current_snapshot() is None


@pytest.mark.parametrize("failure", [None, "source_type", "stream", "trino_open"])
def test_owned_clients_close_on_success_and_failure(bridge_env, monkeypatch, failure):
    _, client, connection, _ = bridge_env
    client.failure = failure
    ch = ModuleType("airflow_commons.hooks.clickhouse_hook")
    trino = ModuleType("airflow.providers.trino.hooks.trino")
    calls = []
    class CHHook:
        def __init__(self, **kwargs):
            calls.append(kwargs)
        def get_conn(self):
            return client
    class TrinoHook:
        def __init__(self, trino_conn_id):
            calls.append(trino_conn_id)
        def get_conn(self):
            if failure == "trino_open":
                raise RuntimeError("trino unavailable")
            return connection
    ch.ClickHouseHook, trino.TrinoHook = CHHook, TrinoHook
    monkeypatch.setitem(sys.modules, ch.__name__, ch)
    monkeypatch.setitem(sys.modules, trino.__name__, trino)
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            execute(bridge_env, client=None, connections=None)
    else:
        execute(bridge_env, client=None, connections=None)
    assert calls == [{"clickhouse_conn_id": "clickhouse_dwh_team_logistics", "use_numpy": False}, "trino_search"]
    assert client.connection_closed and connection.closed == (failure != "trino_open")
    assert all(cursor.closed for cursor in connection.cursors)


def test_changed_request_fails_before_catalog(bridge_env, monkeypatch):
    bridge_env[3]["dates"] = []
    monkeypatch.setattr(BRIDGE, "load_results_catalog", lambda *a: pytest.fail("Не открывать catalog"))
    with pytest.raises(ValueError):
        execute(bridge_env, catalog=None)
    assert not bridge_env[1].streamed


def test_source_preflight_preserves_parameter_binding(bridge_env):
    query = module("restored", "query")
    sql = query.source_schema_query(bridge_env[0][0])
    assert "%(run_id)s" in sql and "%(prediction_date)s" in sql and "%(start)s" in sql and "%(end)s" in sql
    assert "LIMIT 0\nSETTINGS " in sql
    assert "FINAL\nPREWHERE" in sql and "toString(estimate_kind)" in sql
