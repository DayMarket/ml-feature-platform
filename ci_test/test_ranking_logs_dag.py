import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

ENTITY_DIR = Path("datasets/search/ranking_logs/v1")
DAG_PATH = ENTITY_DIR / "dag.py"

FEEDBACK_DAG_ID = "feature-platform.layers.gold.sku_group_id.feedback_sku_group_id"


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


# --- AGENTS.md line 44: sensor on the owning DAG's dq task for
# feature_platform_sku_group_feedback_base_stats -----------------------------


def _external_task_sensor_calls(tree):
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "ExternalTaskSensor"
    ]


def _keyword_value(call, name):
    for keyword in call.keywords:
        if keyword.arg == name:
            return ast.literal_eval(keyword.value)
    return None


def test_dag_declares_a_sensor_on_the_feedback_dq_task():
    tree = ast.parse(dag_source())
    sensors = _external_task_sensor_calls(tree)
    assert len(sensors) == 1, "expected exactly one ExternalTaskSensor (feedback_sku_group_id.dq)"


def test_sensor_waits_on_feedback_dag_dq_task():
    tree = ast.parse(dag_source())
    (sensor,) = _external_task_sensor_calls(tree)
    assert _keyword_value(sensor, "external_dag_id") == FEEDBACK_DAG_ID
    assert _keyword_value(sensor, "external_task_id") == "dq"


def test_sensor_is_upstream_of_collect_dataset():
    source = dag_source()
    assert "wait_for_sku_group_feedback >> collect_dataset >> [dq_task, stats_task]" in source


def _extract_function_source(function_name):
    tree = ast.parse(dag_source())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return ast.get_source_segment(dag_source(), node)
    raise AssertionError(f"{function_name} not found in {DAG_PATH}")


class _PendulumLikeDateTime:
    """Minimal stand-in for pendulum.DateTime covering the chain the DAG uses.

    Airflow/pendulum are not installed in this environment (ci_test's other
    dag.py tests never import dag.py either — they work off source text/AST),
    so _feedback_dq_logical_date's arithmetic is exercised by pulling its
    source out of dag.py with ast and exec'ing it against this stand-in
    rather than importing pendulum.
    """

    def __init__(self, dt):
        self._dt = dt

    def in_timezone(self, tz):
        assert tz == "UTC"
        return _PendulumLikeDateTime(self._dt.astimezone(timezone.utc))

    def add(self, **kwargs):
        return _PendulumLikeDateTime(self._dt + timedelta(**kwargs))

    def replace(self, **kwargs):
        return _PendulumLikeDateTime(self._dt.replace(**kwargs))

    def __eq__(self, other):
        if isinstance(other, _PendulumLikeDateTime):
            return self._dt == other._dt
        return NotImplemented


def _load_feedback_dq_logical_date():
    source = _extract_function_source("_feedback_dq_logical_date")
    namespace = {}
    exec(compile(source, str(DAG_PATH), "exec"), namespace)
    return namespace["_feedback_dq_logical_date"]


def test_feedback_dq_logical_date_maps_window_open_to_that_weeks_saturday_0310():
    feedback_dq_logical_date = _load_feedback_dq_logical_date()
    # Наше окно открывается в воскресенье 12:00 UTC 2026-09-06; последняя
    # собираемая дата в нём — суббота 2026-09-12. feedback_sku_group_id пишет
    # партицию в 03:10 UTC того дня.
    window_open = _PendulumLikeDateTime(datetime(2026, 9, 6, 12, 0, 0, tzinfo=timezone.utc))
    expected = _PendulumLikeDateTime(datetime(2026, 9, 12, 3, 10, 0, tzinfo=timezone.utc))
    assert feedback_dq_logical_date(window_open) == expected
