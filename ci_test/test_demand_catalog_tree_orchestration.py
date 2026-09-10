"""Проверить exact-run XCom и владение connection tree bridge без Airflow сервера."""

from copy import deepcopy
from datetime import timedelta
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from ci_test.test_demand_catalog_tree import NOW, ROOT
from ci_test.test_demand_catalog_tree_runtime import Connection, full as full  # noqa: F401
from ci_test.test_demand_catalog_tree_writer import env as tree_env  # noqa: F401
from layers.silver.level_node_id.demand_catalog_tree.v1.job import orchestration as bridge


def execute(state, **options):
    target, _, _, reference, checked = state
    return bridge.execute_capture(target[0], ROOT, reference=options.pop("reference", reference),
                                  source_manifest_id="tree-run", ingested_at=NOW + timedelta(minutes=1),
                                  catalog=options.pop("catalog", target[1]),
                                  get_checked=options.pop("get_checked", lambda _: checked), **options)


def test_exact_xcom_never_uses_prior_date(full):
    _, source, _, ref, checked = full
    ti = Mock()
    ti.xcom_pull.return_value = checked
    assert bridge.read_checked(ti, source, ref) == checked
    ti.xcom_pull.assert_called_once_with(dag_id=ref["dag_id"], task_ids="dq", run_id=ref["run_id"], include_prior_dates=False)
    for payload in (None, {**checked, "run_id": "other"}, {**checked, "dq_status": "failed"}):
        ti.xcom_pull.return_value = payload
        with pytest.raises(ValueError, match="latest запрещён"):
            bridge.read_checked(ti, source, ref)


def test_external_connection_is_not_closed_and_task_xcom_is_rechecked(full):
    connection = Connection(full[0][1])
    connection.close = Mock()
    ti = Mock()
    ti.xcom_pull.return_value = full[4]
    result = execute(full, connection=connection, get_checked=None, task_instance=ti)
    assert result["status"] == "written" and ti.xcom_pull.call_count == 2
    connection.close.assert_not_called()
    assert all(cursor.closed for cursor in connection.cursors)


@pytest.mark.parametrize("failure", [None, "dq", "stream", "open"])
def test_own_connection_closed_on_success_and_failure(full, monkeypatch, failure):
    connection = Connection(full[0][1])
    connection.close = Mock()
    module = ModuleType("airflow.providers.trino.hooks.trino")
    hook = Mock()
    if failure == "open":
        hook.return_value.get_conn.side_effect = RuntimeError("connection unavailable")
    else:
        hook.return_value.get_conn.return_value = connection
    module.TrinoHook = hook
    monkeypatch.setitem(sys.modules, module.__name__, module)
    loader = Mock(return_value=full[0][1])
    monkeypatch.setattr(bridge, "load_results_catalog", loader)
    if failure == "stream":
        connection.transform = lambda _: []
    getter = (lambda _: None) if failure == "dq" else lambda _: full[4]
    if failure:
        with pytest.raises((ValueError, RuntimeError)):
            execute(full, catalog=None, get_checked=getter)
    else:
        execute(full, catalog=None, get_checked=getter)
    loader.assert_called_once_with("iceberg")
    hook.assert_called_once_with(trino_conn_id="trino_search")
    assert connection.close.call_count == (failure != "open")
    assert all(cursor.closed for cursor in connection.cursors)


@pytest.mark.parametrize("ref", [{"dag_id": "wrong", "run_id": "r"}, {"dag_id": "x"}, None])
def test_bad_reference_before_catalog_or_xcom(full, monkeypatch, ref):
    loader, getter = Mock(), Mock()
    monkeypatch.setattr(bridge, "load_results_catalog", loader)
    with pytest.raises(ValueError, match="DAG/run"):
        execute(full, catalog=None, reference=ref, get_checked=getter)
    loader.assert_not_called()
    getter.assert_not_called()


@pytest.mark.parametrize("block,value", [("source", ""), ("dq", " other "), ("feature_stats", "trino_recsys")])
def test_unapproved_connection_combination_rejected(full, block, value):
    config = deepcopy(full[0][0])
    config[block]["conn_id" if block == "source" else "trino_conn_id"] = value
    with pytest.raises(ValueError):
        bridge.connection_id(config)


def test_invalid_stats_exclusion_fails_before_scan(full):
    full[0][0]["feature_stats"]["exclude_columns"] = ["missing"]
    connection = Connection(full[0][1])
    with pytest.raises(ValueError, match="exclude_columns"):
        execute(full, connection=connection)
    assert not connection.queries


@pytest.mark.parametrize("warmup_days", [1, 3])
def test_warmup_rejected_before_connections_or_xcom(full, monkeypatch, warmup_days):
    full[0][0]["dq"]["warmup_days"] = warmup_days
    loader, getter = Mock(), Mock()
    monkeypatch.setattr(bridge, "load_results_catalog", loader)
    with pytest.raises(ValueError, match="warmup_days"):
        execute(full, catalog=None, get_checked=getter)
    loader.assert_not_called()
    getter.assert_not_called()
