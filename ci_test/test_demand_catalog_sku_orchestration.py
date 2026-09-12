"""Проверить exact XCom и владение CH/Trino connections без Airflow сервера."""

from copy import deepcopy
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from ci_test.test_demand_catalog_sku_inputs import env as input_env  # noqa: F401
from ci_test.test_demand_catalog_sku_runtime import env as env  # noqa: F401
from ci_test.test_demand_catalog_sku_preparation import NOW, ROOT
from layers.silver.sku_id.demand_catalog_sku.v1.job import orchestration as bridge


def execute(env, **options):
    args = dict(reference=env["reference"], source_manifest_id="sku-capture", ingested_at=NOW,
                catalog=env["catalog"], client=env["client"], connection=env["connection"],
                get_checked=lambda _: env["checked"])
    return bridge.execute_capture(env["cfg"], ROOT, **(args | options))


def test_exact_xcom_and_caller_owned_connections(env):
    ti = Mock()
    ti.xcom_pull.return_value = env["checked"]
    env["client"].disconnect = Mock()
    env["connection"].close = Mock()
    assert execute(env, get_checked=None, task_instance=ti)["status"] == "written"
    assert ti.xcom_pull.call_count == 3
    for call in ti.xcom_pull.call_args_list:
        assert call.kwargs == dict(dag_id=env["reference"]["dag_id"], run_id=env["reference"]["run_id"],
                                   task_ids="dq", include_prior_dates=False)
    env["client"].disconnect.assert_not_called()
    env["connection"].close.assert_not_called()


@pytest.mark.parametrize("failure", [None, "open_trino", "dq", "source"])
def test_own_connections_closed_and_confirmed_hooks_used(env, monkeypatch, failure):
    closed = []
    class ManagedClient:
        def __enter__(self):
            return env["client"]
        def __exit__(self, *args):
            closed.append("CH")
    ch_module = ModuleType("airflow_commons.hooks.clickhouse_hook")
    ch_hook = Mock()
    ch_hook.return_value.get_conn.return_value = ManagedClient()
    ch_module.ClickHouseHook = ch_hook
    monkeypatch.setitem(sys.modules, ch_module.__name__, ch_module)
    trino_module = ModuleType("airflow.providers.trino.hooks.trino")
    trino_hook = Mock()
    if failure == "open_trino":
        trino_hook.return_value.get_conn.side_effect = RuntimeError("connection unavailable")
    else:
        trino_hook.return_value.get_conn.return_value = env["connection"]
    trino_module.TrinoHook = trino_hook
    monkeypatch.setitem(sys.modules, trino_module.__name__, trino_module)
    env["connection"].close = Mock()
    loader = Mock(return_value=env["catalog"])
    monkeypatch.setattr(bridge, "load_results_catalog", loader)
    if failure == "dq":
        env["checked"]["dq_status"] = "failed"
    if failure == "source":
        env["client"].rows["active_links"][0][3] = 0
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            execute(env, catalog=None, client=None, connection=None)
    else:
        execute(env, catalog=None, client=None, connection=None)
    loader.assert_called_once_with("iceberg")
    ch_hook.assert_called_once_with(clickhouse_conn_id="clickhouse_dwh_team_logistics", use_numpy=False)
    trino_hook.assert_called_once_with(trino_conn_id="trino_search")
    assert closed == ["CH"]
    assert env["connection"].close.call_count == (failure != "open_trino")


@pytest.mark.parametrize("reference", [None, {}, {"dag_id": "wrong", "run_id": "r"}])
def test_bad_reference_fails_before_catalog_or_xcom(env, monkeypatch, reference):
    loader, getter = Mock(), Mock()
    monkeypatch.setattr(bridge, "load_results_catalog", loader)
    with pytest.raises(ValueError):
        execute(env, catalog=None, reference=reference, get_checked=getter)
    loader.assert_not_called()
    getter.assert_not_called()


@pytest.mark.parametrize("block,value", [("source", ""), ("dq", " trino_search"), ("feature_stats", "trino_recsys")])
def test_invalid_connections_rejected(env, block, value):
    cfg = deepcopy(env["cfg"])
    cfg[block]["conn_id" if block == "source" else "trino_conn_id"] = value
    with pytest.raises(ValueError):
        bridge.connection_ids(cfg)


@pytest.mark.parametrize("warmup_days", [1, 3])
def test_warmup_rejected_before_connections_or_xcom(env, monkeypatch, warmup_days):
    env["cfg"]["dq"]["warmup_days"] = warmup_days
    loader, getter = Mock(), Mock()
    monkeypatch.setattr(bridge, "load_results_catalog", loader)
    with pytest.raises(ValueError, match="warmup_days"):
        execute(env, catalog=None, client=None, connection=None, get_checked=getter)
    loader.assert_not_called()
    getter.assert_not_called()


def test_failed_or_different_xcom_cannot_be_latest_fallback(env):
    ti = Mock()
    for value in (None, {**env["checked"], "run_id": "other"}, {**env["checked"], "dq_status": "failed"}):
        ti.xcom_pull.return_value = value
        with pytest.raises(ValueError, match="latest запрещён"):
            bridge.read_checked(ti, env["src"], env["reference"])
