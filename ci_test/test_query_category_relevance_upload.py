import ast
import importlib.util
import json
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UPLOAD_ROOT = ROOT / "upload" / "query_category_relevance_upload" / "v1"
FACTORY_PATH = UPLOAD_ROOT / "config" / "factory.py"
PRODUCER_DAG_PATH = (
    ROOT
    / "layers/gold/category_id_query_text/query_category_relevance_expanded/v1/dag.py"
)
PRODUCER_DAG_ID = (
    "feature-platform.layers.gold.category_id_query_text."
    "query_category_relevance_expanded"
)


def _load_factory():
    airflow_module = types.ModuleType("airflow")
    airflow_sdk_module = types.ModuleType("airflow.sdk")
    airflow_sdk_module.BaseHook = object
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module

    spec = importlib.util.spec_from_file_location(
        "test_query_category_relevance_upload_factory",
        FACTORY_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _producer_task_ids() -> set[str]:
    tree = ast.parse(PRODUCER_DAG_PATH.read_text(encoding="utf-8"))
    constants = {
        target.id: node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    task_ids = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != "task_id":
                continue
            if isinstance(keyword.value, ast.Constant):
                task_ids.add(keyword.value.value)
            elif isinstance(keyword.value, ast.Name):
                task_ids.add(constants[keyword.value.id])
    return task_ids


def test_upload_waits_for_producer_materialize_task():
    factory = _load_factory()

    components = factory.get_upload_components()

    assert len(components) == 1
    assert components[0]["models"] == ["search_unified_model_clusters"]
    assert components[0]["dependencies"] == [
        {
            "task_id": (
                "wait_for_search_unified_model_clusters_"
                "feature_platform_query_category_relevance_expanded"
            ),
            "external_dag_id": PRODUCER_DAG_ID,
            "external_task_id": "dq",
            "execution_delta_minutes": 30,
        }
    ]


def test_sensor_task_exists_in_producer_dag():
    """Переименование таски в DAG'е витрины подвесило бы сенсор upload'а."""
    factory = _load_factory()
    producer_source = PRODUCER_DAG_PATH.read_text(encoding="utf-8")

    for dependency in factory.get_upload_components()[0]["dependencies"]:
        if dependency["external_task_id"] == "dq":
            assert "build_dq_task(" in producer_source, dependency
        else:
            assert dependency["external_task_id"] in _producer_task_ids(), dependency


def test_upload_reads_run_date_partition_of_expanded_table():
    config = json.loads((UPLOAD_ROOT / "config.yaml").read_text(encoding="utf-8"))

    (feature_group,) = config["feature_groups"]
    source = feature_group["source"]
    assert source["table"] == "feature_platform_query_category_relevance_expanded"
    # Без read_mode upload читает партицию date = run_date: в ней уже одна строка на пару.
    assert "read_mode" not in source
    assert "dq_waiver_reason" not in source
    assert feature_group["features"] == ["relevance"]


def test_delta_matches_schedules():
    """Upload в 04:00 UTC ждёт прогон расширенной витрины в 03:30 UTC той же даты."""
    import yaml

    upload_config = json.loads((UPLOAD_ROOT / "config.yaml").read_text(encoding="utf-8"))
    producer_config = yaml.safe_load(
        (PRODUCER_DAG_PATH.parent / "config.yaml").read_text(encoding="utf-8")
    )
    def minutes_of_day(cron: str) -> int:
        minute, hour = cron.split()[:2]
        return int(hour) * 60 + int(minute)

    delta = upload_config["feature_groups"][0]["source"][
        "dependency_execution_delta_minutes"
    ]
    assert (
        minutes_of_day(upload_config["dag"]["schedule"])
        - minutes_of_day(producer_config["dag"]["schedule"])
        == delta
    )
    assert upload_config["dag"]["group_tag"] == producer_config["dag"]["group_tag"]


def main() -> int:
    test_upload_waits_for_producer_materialize_task()
    test_sensor_task_exists_in_producer_dag()
    test_upload_reads_run_date_partition_of_expanded_table()
    test_delta_matches_schedules()
    print("Query category relevance upload tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
