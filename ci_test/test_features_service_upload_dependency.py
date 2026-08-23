import importlib.util
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FACTORY_PATH = ROOT / "upload" / "features_service_upload" / "v1" / "config" / "factory.py"


def _load_factory():
    airflow_module = types.ModuleType("airflow")
    airflow_sdk_module = types.ModuleType("airflow.sdk")
    airflow_sdk_module.BaseHook = object
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module

    spec = importlib.util.spec_from_file_location(
        "test_features_service_upload_factory",
        FACTORY_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_cold_start_upload_waits_for_producer_task():
    factory = _load_factory()

    components = factory.get_upload_components()
    dependencies = components[0]["dependencies"]
    cold_start_dependency = next(
        dependency
        for dependency in dependencies
        if dependency["external_dag_id"] == "spark.pyspark_feature_store_dag"
    )

    assert cold_start_dependency == {
        "task_id": (
            "wait_for_search_ranking_main_"
            "cold_start_boosted_pw_convs_query_atc_order_90"
        ),
        "external_dag_id": "spark.pyspark_feature_store_dag",
        "external_task_id": "fetch_boosted_conversions_etl",
        "execution_delta_minutes": 240,
    }
