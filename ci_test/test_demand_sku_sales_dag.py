"""Сохранить range wiring и общий бюджет seller owner без импорта Airflow в CI."""

import ast
from importlib import import_module
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/silver/sku_id/demand_sales_daily/v1"


def test_range_factories_share_receipt_guard_and_date():
    tree = ast.parse((ENTITY / "dag.py").read_text())
    factories = {node.func.id: node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name)
                 and node.func.id in {"build_dq_task", "build_feature_stats_task"}}
    assert len(factories) == 2
    for name, call in factories.items():
        values = {kw.arg: kw.value for kw in call.keywords}
        assert ast.literal_eval(values["range_receipt_task_id"]) == "write_range"
        assert values["range_guard"].id == "owner_guard"
        if name == "build_feature_stats_task":
            assert values["range_timeout_seconds"].id == "MAX_RUN_SECONDS"
    cfg = yaml.safe_load((ENTITY / "config.yaml").read_text())
    template = '{{ ti.xcom_pull(task_ids="write_range")["dates"][-1] }}'
    assert cfg["dq"]["partition_date_template"] == cfg["feature_stats"]["partition_date_template"] == template
    assert cfg["runtime"]["run_timeout_seconds"] == {"regular": 21600, "manual": 604800}


def test_prepare_and_write_have_whole_task_guard():
    tree = ast.parse((ENTITY / "dag.py").read_text())
    owner = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "sku_sales_dag")
    tasks = {n.name: n for n in owner.body if isinstance(n, ast.FunctionDef)}
    assert set(tasks) == {"prepare_reference", "prepare_request", "write_range"}
    for function in tasks.values():
        guards = [n for n in ast.walk(function) if isinstance(n, ast.With)]
        assert len(guards) == 1
        assert guards[0].items[0].context_expr.func.id == "owner_guard"
        assert isinstance(guards[0].body[-1], ast.Return)


def resolve(cfg, source, **changes):
    requests = import_module("layers.silver.sku_id.demand_sales_daily.v1.job.seller_requests")
    kwargs = dict(conf={}, run_type="scheduled", run_id="scheduled__2026-09-09T04:00:00+00:00",
                  logical_date="2026-09-08T04:00:00Z", interval_start="2026-09-08T04:00:00Z",
                  interval_end="2026-09-09T04:00:00Z")
    return requests.resolve_reference(cfg, source, **(kwargs | changes))


def configs():
    cfg = yaml.safe_load((ENTITY / "config.yaml").read_text())
    source = yaml.safe_load((ROOT / cfg["inputs"]["seller_config"]).read_text())
    return cfg, source


def test_scheduled_reference_exact_same_interval():
    cfg, source = configs()
    assert resolve(cfg, source) == {"dag_id": source["dag"]["id"],
        "run_id": "scheduled__2026-09-09T04:00:00+00:00", "logical_date": "2026-09-08T04:00:00+00:00"}


@pytest.mark.parametrize("changes", [
    {"run_id": "scheduled__2026-09-08T04:00:00+00:00"},
    {"logical_date": "2026-09-09T04:00:00Z"},
    {"interval_start": "2026-09-07T04:00:00Z"},
    {"conf": {"reference": {}}}, {"conf": {"mode": "manual"}},
    {"conf": "invalid"}, {"run_type": "manual"},
])
def test_schedule_or_missing_manual_reference_fails(changes):
    with pytest.raises(ValueError):
        resolve(*configs(), **changes)


@pytest.mark.parametrize("field,value", [("schedule", "0 5 * * *"), ("start_date", "2026-09-09T00:00:00Z")])
def test_changed_source_timetable_fails(field, value):
    cfg, source = configs()
    source["dag"][field] = value
    with pytest.raises(ValueError, match="интервал"):
        resolve(cfg, source)


def test_manual_reference_explicit_and_correct_owner():
    cfg, source = configs()
    ref = {"dag_id": source["dag"]["id"], "run_id": "manual-exact", "logical_date": "2026-09-09T05:00:00Z"}
    assert resolve(cfg, source, run_type="manual", conf={"mode": "manual", "reference": ref})["run_id"] == "manual-exact"
    ref["dag_id"] = "foreign"
    with pytest.raises(ValueError, match="владельца"):
        resolve(cfg, source, run_type="manual", conf={"mode": "manual", "reference": ref})


def test_source_sensor_uses_standard_sensor_and_dag_budget():
    tree = ast.parse((ENTITY / "dag.py").read_text())
    assert not any(isinstance(node, ast.ClassDef) for node in tree.body)
    sensor = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "ExternalTaskSensor"
    )
    values = {keyword.arg: keyword.value for keyword in sensor.keywords}
    assert values["timeout"].id == "MAX_RUN_SECONDS"
    decorator = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "dag"
    )
    dag_values = {keyword.arg: keyword.value for keyword in decorator.keywords}
    timeout = dag_values["dagrun_timeout"]
    assert timeout.func.id == "timedelta"
    assert timeout.keywords[0].arg == "seconds"
    assert timeout.keywords[0].value.id == "MAX_RUN_SECONDS"
