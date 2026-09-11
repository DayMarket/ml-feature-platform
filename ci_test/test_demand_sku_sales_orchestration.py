"""SKU Connections bridge: только Trino, exact XCom и строгий preflight до записи."""

from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from importlib import import_module
import sys
from types import ModuleType, SimpleNamespace

import pyarrow as pa
import pytest
import yaml

from ci_test.test_demand_sku_sales_from_fp import case as case, env as env, ROOT, CAPTURE, DAY, Connection
from ci_test.test_demand_sku_sales_planning import PLAN, FIRST, STOP, arguments

BRIDGE = import_module("layers.silver.sku_id.demand_sales_daily.v1.job.seller_orchestration")


def description(schema):
    types = {pa.date32(): "date", pa.int32(): "integer", pa.int64(): "bigint", pa.bool_(): "boolean",
             pa.decimal128(38, 0): "decimal(38,0)", pa.timestamp("us"): "timestamp(6)", pa.float64(): "double"}
    return [(f.name, types.get(f.type, "varchar"), None, None, None, None, f.nullable) for f in schema]


class BridgeConnection(Connection):
    def __init__(self, batch, schemas):
        super().__init__(batch)
        self.schemas = schemas
        self.closed, self.failure = False, None
    def close(self):
        self.closed = True
    def cursor(self):
        cursor = super().cursor()
        execute = cursor.execute
        def routed(sql):
            if sql == BRIDGE.NATIVE_PROBE_SQL:
                self.queries.append(sql)
                cursor.offset, cursor.is_count = 0, False
                schema = pa.schema([pa.field("raw_amount", pa.decimal128(38, 0)), pa.field("source_date", pa.date32()),
                    pa.field("captured", pa.timestamp("us")), pa.field("unknown_usd", pa.float64()),
                    pa.field("known_usd", pa.float64()), pa.field("raw_zero", pa.int64())])
                cursor.description = description(schema)
                cursor.rows = [[Decimal("12345678901234567890123456789012345678"), date(2026, 9, 1),
                                datetime(2026, 9, 9, 4, 0, 0, 123456), None, 1.25, 0]]
                if self.failure == "native":
                    cursor.rows[0][0] = str(cursor.rows[0][0])
            elif sql.endswith("LIMIT 0"):
                self.queries.append(sql)
                cursor.offset, cursor.is_count = 0, False
                schema = next(value for key, value in self.schemas.items() if f'"{key}"' in sql)
                cursor.description = description(schema)
                cursor.rows = [] if self.failure != "nonempty" else [[1]]
                if self.failure == "service" and "feature_platform_dq_results" in sql:
                    raise RuntimeError("service inaccessible")
                if self.failure == "schema" and "FOR VERSION AS OF" in sql:
                    cursor.description.pop()
            else:
                execute(sql)
        cursor.execute = routed
        return cursor


@pytest.fixture
def bridge_case(case):
    cfg, catalog, target, ref, checked, connection = case
    schemas = {cfg["table"]["name"]: target.schema().as_arrow()}
    source, _ = BRIDGE.source_config(cfg, ROOT)
    schemas[source["table"]["name"]] = connection.batch.schema
    for path in ("dq/results", "feature_stats/results"):
        table = yaml.safe_load((ROOT / path / "config.yaml").read_text())["table"]
        schema = BRIDGE.service_schema(ROOT / path)
        catalog.create_table((table["schema"], table["name"]), schema=schema)
        schemas[table["name"]] = schema
    request = PLAN.build_request(cfg, run_id="sku-run", mode="manual", history_start=DAY,
        interval_start=DAY.isoformat() + "T00:00:00Z", interval_end=(DAY + timedelta(days=1)).isoformat() + "T00:00:00Z",
        reference=dict(ref, logical_date=CAPTURE.isoformat()))
    return case, request, BridgeConnection(connection.batch, schemas)


def execute(bridge_case, **options):
    case, request, connection = bridge_case
    args = dict(catalog=case[1], connections={"trino_search": connection},
                fetch_checked=lambda _: deepcopy(case[4]), ingested_at=CAPTURE) | options
    return BRIDGE.execute_request(case[0], ROOT, request, **args)


def test_one_trino_connection_preflight_and_write(bridge_case):
    result = execute(bridge_case)
    connection = bridge_case[2]
    assert result["status"] == "written" and "dq_status" not in result
    assert result["day_receipts"][0]["rows_written"] == 1
    assert not connection.closed and all(c.closed for c in connection.cursors)
    assert connection.queries[0] == BRIDGE.NATIVE_PROBE_SQL
    assert sum(sql.endswith("LIMIT 0") for sql in connection.queries) == 5
    assert any('"dwh-iceberg"."silver"."feature_platform_dq_results"' in sql for sql in connection.queries)


@pytest.mark.parametrize("failure", ["native", "nonempty", "service", "schema", "missing_service", "service_schema"])
def test_preflight_failure_never_writes(bridge_case, failure):
    case, _, connection = bridge_case
    connection.failure = failure
    table = yaml.safe_load((ROOT / "dq/results/config.yaml").read_text())["table"]
    identifier = (table["schema"], table["name"])
    if failure == "missing_service":
        case[1].drop_table(identifier)
    elif failure == "service_schema":
        with case[1].load_table(identifier).update_schema() as update:
            update.delete_column("status")
    with pytest.raises((ValueError, RuntimeError)):
        execute(bridge_case)
    assert case[2].refresh().current_snapshot() is None
    assert all(c.closed for c in connection.cursors)
    assert not any(sql.startswith('SELECT count(DISTINCT') for sql in connection.queries)


@pytest.mark.parametrize("change", [None, "missing", "run", "written"])
def test_exact_xcom_no_latest(bridge_case, change):
    case, _, connection = bridge_case
    calls = []
    def pull(**kwargs):
        calls.append(kwargs)
        assert kwargs == {"dag_id": case[3]["dag_id"], "run_id": case[3]["run_id"],
                          "task_ids": "dq", "include_prior_dates": False}
        payload = deepcopy(case[4])
        if change == "missing":
            return None
        if change == "run":
            payload["run_id"] = "another"
        if change == "written":
            payload["dq_status"] = "written"
        return payload
    ti = SimpleNamespace(xcom_pull=pull)
    if change:
        with pytest.raises(ValueError, match="latest"):
            execute(bridge_case, fetch_checked=None, task_instance=ti)
        assert connection.cursors == []
    else:
        execute(bridge_case, fetch_checked=None, task_instance=ti)
        assert len(calls) >= 4


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
def test_owned_connection_closes_on_success_and_failure(bridge_case, monkeypatch, failure):
    connection = bridge_case[2]
    connection.failure = failure
    connection.fail_fetch = failure == "stream"
    calls = install_hook(monkeypatch, connection)
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            execute(bridge_case, connections=None)
    else:
        execute(bridge_case, connections=None)
    assert calls == ["trino_search"] and connection.closed
    assert all(c.closed for c in connection.cursors)


def test_manual_plan_opens_no_connections(case, monkeypatch):
    monkeypatch.setattr(BRIDGE, "load_results_catalog", lambda _: pytest.fail("No catalog for manual plan"))
    plan = BRIDGE.prepare_request(case[0], ROOT, catalog=None, query=None,
        **arguments(case, mode="manual", interval_start="2026-07-01T00:00:00Z"))
    assert plan["output_state"] is None and len(plan["dates"]) == (STOP - FIRST).days


def test_bad_reference_prevents_catalog_and_connection(case, monkeypatch):
    args = arguments(case)
    args["reference"]["dag_id"] = "wrong"
    monkeypatch.setattr(BRIDGE, "load_results_catalog", lambda _: pytest.fail("No catalog for bad owner"))
    with pytest.raises(ValueError, match="владельцев"):
        BRIDGE.prepare_request(case[0], ROOT, **args)
