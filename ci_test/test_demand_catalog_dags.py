"""Полный capture каталогов выполняет один owner DAG в scheduled/manual режимах."""

import ast
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PATHS = [
    "layers/silver/seller_id/demand_catalog_seller/v1",
    "layers/silver/sku_id/demand_catalog_sku/v1",
    "layers/silver/level_node_id/demand_catalog_tree/v1",
]
NOW = datetime(2026, 9, 9, 4, tzinfo=timezone.utc)


@pytest.fixture(params=PATHS)
def entity(request):
    path = ROOT / request.param
    config = yaml.safe_load((path / "config.yaml").read_text())
    source = None
    if config.get("inputs"):
        source = yaml.safe_load((ROOT / next(iter(config["inputs"].values()))).read_text())
    module = import_module(request.param.replace("/", ".") + ".job.requests")
    return path, config, source, module


def capture(entity, conf, run_type, run_id):
    _, config, source, module = entity
    return module.capture_request(
        config,
        source,
        conf,
        run_id=run_id,
        run_type=run_type,
        logical_date="2026-09-08T04:00:00Z",
        interval_start="2026-09-08T04:00:00Z",
        interval_end=NOW,
    )


def reference(entity):
    source = entity[2]
    if source is None:
        return None
    return {
        "dag_id": source["dag"]["id"],
        "run_id": "source-exact",
        "logical_date": NOW.isoformat(),
    }


def test_scheduled_capture_uses_same_interval(entity):
    run_id = "scheduled__2026-09-09T04:00:00+00:00"
    result = capture(entity, {}, "scheduled", run_id)
    assert result["mode"] == "regular"
    assert result["source_manifest_id"] == run_id
    assert result["catalog_version"] == capture(entity, {}, "scheduled", run_id)["catalog_version"]


def test_manual_capture_uses_same_owner_and_exact_reference(entity):
    exact = reference(entity)
    conf = {"mode": "manual"} | ({"reference": exact} if exact else {})
    result = capture(entity, conf, "manual", "manual-run")
    assert result["mode"] == "manual"
    assert result["source_manifest_id"] == "manual-run"
    assert result["reference"] == exact


@pytest.mark.parametrize("conf,run_type", [({}, "manual"), ({"mode": "manual"}, "scheduled")])
def test_mode_must_match_run_type(entity, conf, run_type):
    if run_type == "manual" and entity[2] is not None:
        conf = conf | {"reference": reference(entity)}
    with pytest.raises(ValueError):
        capture(entity, conf, run_type, "manual-run")


def test_manual_dependent_catalog_requires_exact_reference(entity):
    if entity[2] is None:
        pytest.skip("Seller catalog не имеет upstream owner")
    with pytest.raises(ValueError):
        capture(entity, {"mode": "manual"}, "manual", "manual-run")


def test_capture_factories_are_guarded(entity):
    path, config, _, _ = entity
    tree = ast.parse((path / "dag.py").read_text())
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"build_dq_task", "build_feature_stats_task"}
    ]
    assert len(calls) == 2
    for call in calls:
        keywords = {keyword.arg: keyword.value for keyword in call.keywords}
        expected = ({"receipt_task_id", "task_guard"}
                    if call.func.id == "build_dq_task" else {"task_guard"})
        assert set(keywords) == expected
        assert keywords["task_guard"].id == "owner_guard"
    assert config["dq"]["scope"] == "partition"
    assert config["dq"]["partition_granularity"] == "timestamp"
    assert config["feature_stats"]["enabled"] is True
