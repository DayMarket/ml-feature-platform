import ast
from pathlib import Path

import yaml

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")
DAG_PATH = ENTITY_DIR / "dag.py"


def dag_source():
    return DAG_PATH.read_text(encoding="utf-8")


def test_dag_id_matches_the_entity_path():
    assert 'dag_id="feature-platform.datasets.search.ranking_logs.v1"' in dag_source()


def test_dag_builds_dq_and_feature_stats_tasks():
    tree = ast.parse(dag_source())
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "build_dq_task" in called
    assert "build_feature_stats_task" in called


def test_dq_and_stats_are_terminal_and_parallel():
    source = dag_source()
    assert ">> [dq_task, stats_task]" in source
    assert "dq_task >>" not in source
    assert "stats_task >>" not in source


def test_dag_partition_template_matches_the_config():
    source = dag_source()
    config = yaml.safe_load((ENTITY_DIR / "config.yaml").read_text(encoding="utf-8"))
    template = config["dq"]["partition_date_template"]
    # Шаблон в DAG'е и в конфиге обязан быть одним и тем же: иначе dq посчитает
    # тесты по другой партиции, чем записал джоб.
    assert template in source


def test_dag_is_paused_on_creation_and_single_run():
    source = dag_source()
    assert "is_paused_upon_creation=True" in source
    assert "max_active_runs=1" in source


def test_dag_is_tagged_as_dataset():
    assert '"dataset"' in dag_source()
