"""Gold Airflow bridge: exact XCom, preflight и владение соединениями без сети."""

from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from importlib import import_module
import sys
from types import ModuleType, SimpleNamespace

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_observed_preparation import DAY, NOW, ROOT
from ci_test.test_demand_observed_reader import description
from ci_test.test_demand_observed_runtime import setup as setup, SourceConnection
from ci_test.test_demand_observed_writer import env as env
from ci_test.test_demand_observed_planning import PLAN

BRIDGE = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.orchestration")


class Connection(SourceConnection):
    def __init__(self, tables):
        super().__init__(tables)
        self.closed = False
    def close(self):
        self.closed = True
    def cursor(self):
        cursor = super().cursor()
        execute = cursor.execute
        def routed(sql):
            if sql == BRIDGE.NATIVE_PROBE_SQL:
                cursor.sql = sql
                schema = pa.schema([pa.field("raw_amount", pa.decimal128(38, 0)), pa.field("source_date", pa.date32()),
                    pa.field("captured", pa.timestamp("us")), pa.field("unknown_usd", pa.float64()),
                    pa.field("known_usd", pa.float64()), pa.field("raw_zero", pa.int64())])
                cursor.description = description(schema, schema.names)
                cursor.rows = [[Decimal("12345678901234567890123456789012345678"), date(2026, 9, 1),
                                datetime(2026, 9, 9, 4, 0, 0, 123456), None, 1.25, 0]]
                if self.failure == "native":
                    cursor.rows[0][0] = "12345678901234567890123456789012345678"
            elif sql.endswith("LIMIT 0"):
                if self.failure == "service" and "feature_platform_dq_results" in sql:
                    raise RuntimeError("service inaccessible")
                execute(sql)
                for path in ("dq/results", "feature_stats/results"):
                    cfg = yaml.safe_load((ROOT / path / "config.yaml").read_text())["table"]
                    if cfg["name"] in sql:
                        schema = BRIDGE.service_schema(ROOT / path)
                        cursor.description = description(schema, schema.names)
                        if self.failure == "service_trino_schema":
                            cursor.description.pop()
                cursor.rows = [] if self.failure != "nonempty" else [[1]]
                if self.failure == "schema" and "FOR VERSION AS OF" in sql:
                    cursor.description.pop()
            else:
                execute(sql)
        cursor.execute = routed
        return cursor


@pytest.fixture
def bridge_env(setup):
    config, catalog, gold = setup[0]
    for path in ("dq/results/config.yaml", "feature_stats/results/config.yaml"):
        cfg = yaml.safe_load((ROOT / path).read_text())["table"]
        catalog.create_table((cfg["schema"], cfg["name"]), schema=BRIDGE.service_schema((ROOT / path).parent))
    refs = {kind: dict(ref, logical_date=NOW.isoformat()) for kind, ref in setup[2].items()}
    request = PLAN.build_request(config, run_id="gold-run", mode="manual", history_start=DAY,
        interval_start=DAY.isoformat() + "T00:00:00Z", interval_end="2026-09-02T00:00:00Z",
        references=refs)
    return setup, request, Connection(setup[4].tables)


def execute(bridge_env, **options):
    setup, request, connection = bridge_env
    args = {"catalog": setup[0][1], "connections": {"trino_search": connection},
            "fetch_checked": lambda _: deepcopy(setup[3]), "ingested_at": NOW} | options
    return BRIDGE.execute_request(setup[0][0], ROOT, request, **args)


def test_preflight_and_range_use_same_source_connection(bridge_env):
    result = execute(bridge_env)
    _, request, connection = bridge_env
    assert result["status"] == "written" and result["request_id"] == request["request_id"]
    assert result["day_receipts"][0]["rows_written"] == 4 and "dq_status" not in result
    assert not connection.closed and all(cursor.closed for cursor in connection.cursors)
    queries = [cursor.sql for cursor in connection.cursors]
    assert queries[0] == BRIDGE.NATIVE_PROBE_SQL
    assert sum(sql.endswith("LIMIT 0") for sql in queries) == 7
    assert sum("FOR VERSION AS OF" in sql and sql.endswith("LIMIT 0") for sql in queries) == 2
    assert any('"dwh-iceberg"."silver"."feature_platform_dq_results"' in sql for sql in queries)


@pytest.mark.parametrize("failure", ["native", "service", "nonempty", "schema", "missing_service",
                                     "service_schema", "service_trino_schema"])
def test_failed_preflight_never_writes_gold(bridge_env, failure):
    setup, _, connection = bridge_env
    connection.failure = failure
    if failure == "missing_service":
        cfg = yaml.safe_load((ROOT / "feature_stats/results/config.yaml").read_text())["table"]
        setup[0][1].drop_table((cfg["schema"], cfg["name"]))
    elif failure == "service_schema":
        cfg = yaml.safe_load((ROOT / "dq/results/config.yaml").read_text())["table"]
        table = setup[0][1].load_table((cfg["schema"], cfg["name"]))
        with table.update_schema() as schema:
            schema.delete_column("status")
    with pytest.raises((ValueError, RuntimeError)):
        execute(bridge_env)
    assert setup[0][2].refresh().current_snapshot() is None
    assert not connection.closed and all(cursor.closed for cursor in connection.cursors)
    assert all("AS rows_expected" not in (cursor.sql or "") for cursor in connection.cursors)


def task_instance(bridge_env, change=None):
    setup, _, _ = bridge_env
    calls = []
    def pull(**kwargs):
        calls.append(kwargs)
        kind = next(kind for kind, ref in setup[2].items() if ref["dag_id"] == kwargs["dag_id"])
        assert kwargs == {"dag_id": setup[2][kind]["dag_id"], "run_id": setup[2][kind]["run_id"],
                          "task_ids": "dq", "include_prior_dates": False}
        result = deepcopy(setup[3][kind])
        if change == "missing":
            return None
        if change == "other_run":
            result["run_id"] = "latest-but-wrong"
        if change == "written":
            result["dq_status"] = "written"
        return result
    return SimpleNamespace(xcom_pull=pull), calls


def test_exact_xcom_transport_is_rechecked_before_commit(bridge_env):
    ti, calls = task_instance(bridge_env)
    result = execute(bridge_env, fetch_checked=None, task_instance=ti)
    assert result["status"] == "written"
    assert len(calls) >= 6 and all(c["task_ids"] == "dq" for c in calls)


@pytest.mark.parametrize("change", ["missing", "other_run", "written"])
def test_no_fallback_from_missing_or_wrong_xcom(bridge_env, change):
    ti, _ = task_instance(bridge_env, change)
    with pytest.raises(ValueError, match="latest"):
        execute(bridge_env, fetch_checked=None, task_instance=ti)
    assert bridge_env[2].cursors == []


def install_hook(monkeypatch, connection):
    module = ModuleType("airflow.providers.trino.hooks.trino")
    calls = []
    class Hook:
        def __init__(self, trino_conn_id):
            calls.append(trino_conn_id)
        def get_conn(self):
            return connection
    module.TrinoHook = Hook
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return calls


@pytest.mark.parametrize("failure", [None, "native", "stream"])
def test_owned_connections_close_on_success_and_failure(bridge_env, monkeypatch, failure):
    connection = bridge_env[2]
    connection.failure = failure
    calls = install_hook(monkeypatch, connection)
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            execute(bridge_env, connections=None)
    else:
        execute(bridge_env, connections=None)
    assert calls == ["trino_search"] and connection.closed
    assert all(cursor.closed for cursor in connection.cursors)


def test_shared_hive_catalog_loader_is_used(bridge_env, monkeypatch):
    calls = []
    def load(name):
        calls.append(name)
        return bridge_env[0][0][1]
    monkeypatch.setattr(BRIDGE, "load_results_catalog", load)
    execute(bridge_env, catalog=None)
    assert calls == ["iceberg"]


def test_invalid_plan_never_opens_connections(bridge_env, monkeypatch):
    bridge_env[1]["dates"] = []
    calls = install_hook(monkeypatch, bridge_env[2])
    with pytest.raises(ValueError):
        execute(bridge_env, connections=None)
    assert calls == [] and bridge_env[2].cursors == []


def test_second_connection_failure_closes_first(bridge_env, monkeypatch):
    setup, request, connection = bridge_env
    setup[0][0]["dq"]["trino_conn_id"] = "test-second-connection"
    request["config_digest"] = PLAN.digest(setup[0][0])
    request["request_id"] = PLAN.digest({k: v for k, v in request.items() if k != "request_id"})
    module = ModuleType("airflow.providers.trino.hooks.trino")
    class Hook:
        def __init__(self, trino_conn_id):
            self.name = trino_conn_id
        def get_conn(self):
            if self.name != "trino_search":
                raise RuntimeError("second connection failed")
            return connection
    module.TrinoHook = Hook
    monkeypatch.setitem(sys.modules, module.__name__, module)
    with pytest.raises(RuntimeError, match="second connection"):
        execute(bridge_env, connections=None)
    assert connection.closed and connection.cursors == []


@pytest.mark.parametrize("change", ["missing", "type", "nullable", "extra"])
def test_service_schema_contract_rejects_drift(change):
    expected = BRIDGE.service_schema(ROOT / "dq/results")
    actual = expected
    index = expected.get_field_index("status")
    if change == "missing":
        actual = expected.remove(index)
    elif change == "type":
        actual = expected.set(index, pa.field("status", pa.int64()))
    elif change == "nullable":
        actual = expected.set(index, pa.field("status", pa.string(), nullable=False))
    else:
        actual = expected.append(pa.field("unexpected", pa.string()))
    with pytest.raises(ValueError):
        BRIDGE.validate_service_schema(actual, expected)


def test_unknown_service_ddl_is_not_silently_partially_parsed(monkeypatch):
    from pathlib import Path
    original = Path.read_text
    def read(path, *args, **kwargs):
        text = original(path, *args, **kwargs)
        return text.replace("status STRING", "status ARRAY<STRING>") if str(path).endswith("dq/results/migrations/create_table.sql") else text
    monkeypatch.setattr(Path, "read_text", read)
    with pytest.raises(ValueError, match="колонка service"):
        BRIDGE.service_schema(ROOT / "dq/results")
