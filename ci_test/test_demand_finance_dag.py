"""Сохранить range wiring и общий бюджет seller owner без импорта Airflow в CI."""

import ast
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ENTITY = ROOT / "layers/silver/sku_id_seller_key/demand_finance_daily/v1"


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
    owner = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "finance_dag")
    tasks = {n.name: n for n in owner.body if isinstance(n, ast.FunctionDef)}
    assert set(tasks) == {"prepare_request", "write_range"}
    for function in tasks.values():
        guards = [n for n in ast.walk(function) if isinstance(n, ast.With)]
        assert len(guards) == 1
        assert guards[0].items[0].context_expr.func.id == "owner_guard"
        assert isinstance(guards[0].body[-1], ast.Return)
