"""Сохранить range wiring и общий бюджет seller owner без импорта Airflow в CI."""

import ast
from importlib import import_module
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/gold/sku_id/demand_observed_daily/v1"


def test_range_factories_share_receipt_guard_and_date():
    tree = ast.parse((ENTITY / "dag.py").read_text())
    factories = {node.func.id: node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name)
                 and node.func.id in {"build_dq_task", "build_feature_stats_task"}}
    assert len(factories) == 2
    for name, call in factories.items():
        values = {kw.arg: kw.value for kw in call.keywords}
        assert ast.literal_eval(values["range_receipt_task_id"]) == "write_observed"
        assert values["range_guard"].id == "owner_guard"
        if name == "build_feature_stats_task":
            assert values["range_timeout_seconds"].id == "MAX_RUN_SECONDS"
    cfg = yaml.safe_load((ENTITY / "config.yaml").read_text())
    template = '{{ ti.xcom_pull(task_ids="write_observed")["dates"][-1] }}'
    assert cfg["dq"]["partition_date_template"] == cfg["feature_stats"]["partition_date_template"] == template
    assert cfg["runtime"]["run_timeout_seconds"] == {"regular": 21600, "manual": 604800}


def test_prepare_and_write_have_whole_task_guard():
    tree = ast.parse((ENTITY / "dag.py").read_text())
    owner = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "observed_dag")
    tasks = {n.name: n for n in owner.body if isinstance(n, ast.FunctionDef)}
    assert set(tasks) == {"prepare_references", "prepare_request", "write_observed"}
    for function in tasks.values():
        guards = [n for n in ast.walk(function) if isinstance(n, ast.With)]
        assert len(guards) == 1
        assert guards[0].items[0].context_expr.func.id == "owner_guard"
        assert isinstance(guards[0].body[-1], ast.Return)


def source_configs():
    cfg = yaml.safe_load((ENTITY / "config.yaml").read_text())
    return cfg, {kind: yaml.safe_load((ROOT / cfg["inputs"][kind + "_config"]).read_text())
                 for kind in ("sales", "stock")}


def resolve(cfg, sources, **changes):
    requests = import_module("layers.gold.sku_id.demand_observed_daily.v1.job.requests")
    arguments = dict(conf={}, run_type="scheduled", run_id="scheduled__2026-09-09T05:00:00+00:00",
                     logical_date="2026-09-08T05:00:00Z", interval_start="2026-09-08T05:00:00Z",
                     interval_end="2026-09-09T05:00:00Z")
    return requests.resolve_references(cfg, sources, **(arguments | changes))


def test_scheduled_both_silver_previous_hour():
    cfg, sources = source_configs()
    assert resolve(cfg, sources) == {kind: {"dag_id": source["dag"]["id"],
        "run_id": "scheduled__2026-09-09T04:00:00+00:00", "logical_date": "2026-09-08T04:00:00+00:00"}
        for kind, source in sources.items()}


@pytest.mark.parametrize("kind", ["sales", "stock"])
@pytest.mark.parametrize("field,value", [("schedule", "0 3 * * *"), ("start_date", "2026-09-09T00:00:00Z")])
def test_source_schedule_changes_block(kind, field, value):
    cfg, sources = source_configs()
    sources[kind]["dag"][field] = value
    with pytest.raises(ValueError, match="интервал"):
        resolve(cfg, sources)


@pytest.mark.parametrize("change", [{"run_id": "wrong"}, {"logical_date": "2026-09-09T05:00:00Z"},
    {"conf": {"references": {}}}, {"run_type": "manual"}, {"conf": {"mode": "manual"}}])
def test_invalid_schedule_or_missing_explicit_references(change):
    with pytest.raises(ValueError):
        resolve(*source_configs(), **change)


def test_manual_references_are_exact_and_not_latest():
    cfg, sources = source_configs()
    refs = resolve(cfg, sources)
    refs["sales"]["run_id"], refs["stock"]["run_id"] = "sales-manual", "stock-manual"
    assert resolve(cfg, sources, conf={"mode": "manual", "references": refs}, run_type="manual") == refs
    refs["stock"]["dag_id"] = "other"
    with pytest.raises(ValueError, match="владельцев"):
        resolve(cfg, sources, conf={"mode": "manual", "references": refs}, run_type="manual")
