"""Настоящий owner bridge, source guard и terminal проверки на локальном Iceberg."""

from copy import deepcopy
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from ci_test.test_demand_restored_orchestration import BRIDGE, ROOT, bridge_env, loader, target  # noqa: F401
from ci_test.test_demand_restored_source_guard import guard, policy_bundle


def sealed_env(env):
    destination, client, connection, request = env
    _, selected, passport, metadata = policy_bundle()
    actual = deepcopy(client.data[1])
    document = json.loads(actual["output_manifest"])
    policy = json.loads(passport["output_manifest"])["fp_source_policy"]
    policy.update(run_id=client.data[0]["run_id"], prediction_date=client.data[0]["prediction_date"].isoformat(),
                  producer_code_version=actual["code_version"], daily_manifest_sha256=guard.digest(document["fp_daily_copy"]))
    document["fp_source_policy"] = policy
    actual["output_manifest"] = json.dumps(document)
    client.data = client.data[0], actual, client.data[2]
    original = client.execute
    def execute(sql, **kwargs):
        if sql in {guard.TABLES_SQL, guard.MUTATIONS_SQL}:
            return metadata.execute(sql, **kwargs)
        return original(sql, **kwargs)
    client.execute = execute
    return metadata


def run_owner(env):
    destination, client, connection, request = env
    return BRIDGE.execute_owner_range(destination[0], ROOT, request, catalog=destination[1],
                                      client=client, connections={"trino_search": connection})


def test_legacy_passport_cannot_reach_result_stream(bridge_env):  # noqa: F811
    with pytest.raises(ValueError, match="fp_source_policy"):
        run_owner(bridge_env)
    assert not bridge_env[1].streamed
    assert bridge_env[0][2].refresh().current_snapshot() is None


def test_owner_copies_sealed_run_and_resumes_with_real_guard(bridge_env):  # noqa: F811
    metadata = sealed_env(bridge_env)
    result = run_owner(bridge_env)
    resumed = run_owner(bridge_env)
    assert result["status"] == "written" and result["snapshot_id"] == resumed["snapshot_id"]
    assert len(metadata.calls) > 4


@pytest.mark.parametrize("change", [False, True])
def test_terminal_source_guard_covers_whole_task_and_closes_client(bridge_env, monkeypatch, change):  # noqa: F811
    sealed_env(bridge_env)
    written = run_owner(bridge_env)
    destination, client, _, request = bridge_env
    hook = ModuleType("airflow_commons.hooks.clickhouse_hook")
    class Hook:
        def __init__(self, **kwargs):
            assert kwargs == {"clickhouse_conn_id": destination[0]["source"]["conn_id"], "use_numpy": False}
        def get_conn(self):
            return client
    hook.ClickHouseHook = Hook
    monkeypatch.setitem(sys.modules, hook.__name__, hook)
    def pull(*, task_ids, include_prior_dates):
        assert include_prior_dates is False
        return request if task_ids == "prepare_request" else written
    def task():
        with guard.checked_output(destination[0], {"ti": SimpleNamespace(xcom_pull=pull)}):
            if change:
                client.data[1]["state_version"] += 1
    if change:
        with pytest.raises(ValueError, match="изменились"):
            task()
    else:
        task()
    assert client.connection_closed
