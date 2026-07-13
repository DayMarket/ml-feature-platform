import importlib.util
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FACTORY_PATH = (
    ROOT
    / "upload"
    / "dynamic_pricing_inference_upload"
    / "v1"
    / "config"
    / "factory.py"
)
PRODUCER_DAG_ID = (
    "feature-platform.layers.gold.calculated_at_sku_group_id_promotion_id."
    "dynamic_pricing_sku_group_price_features"
)


def _load_factory():
    airflow_module = types.ModuleType("airflow")
    airflow_sdk_module = types.ModuleType("airflow.sdk")
    airflow_sdk_module.BaseHook = object
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module

    spec = importlib.util.spec_from_file_location(
        "test_dynamic_pricing_upload_factory",
        FACTORY_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_dynamic_pricing_upload_waits_for_price_producer_dag():
    factory = _load_factory()

    components = factory.get_upload_components()

    assert len(components) == 1
    dependencies = components[0]["dependencies"]
    assert dependencies == [
        {
            "task_id": (
                "wait_for_dynamic_pricing_inference_"
                "feature_platform_dynamic_pricing_sku_group_price_features"
            ),
            "external_dag_id": PRODUCER_DAG_ID,
            "execution_delta_minutes": 0,
        }
    ]
