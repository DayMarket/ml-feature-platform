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


REPOSITORY_SOURCE_DAG_IDS = {
    "feature-platform.layers.gold.query_sku_group_id."
    "sku_group_query_atc_order_features.v2",
    "feature-platform.layers.gold.sku_group_id.sku_group_search_conversion_features.v2",
    "feature-platform.layers.gold.sku_group_id.sku_group_stock_features",
    "feature-platform.layers.gold.sku_group_id.sku_group_price_features",
    "feature-platform.layers.gold.query.search_query_atc_features",
    "feature-platform.layers.gold.sku_group_id.feedback_sku_group_id",
}


def test_repository_sources_wait_for_the_dq_task():
    """Каждая gold-таблица аплоада ждётся по своей таске dq, а не по всему DAG'у."""
    factory = _load_factory()

    dependencies = factory.get_upload_components()[0]["dependencies"]
    waited = {
        dependency["external_dag_id"]
        for dependency in dependencies
        if dependency["external_task_id"] == "dq"
    }
    assert waited == REPOSITORY_SOURCE_DAG_IDS, waited

    for dependency in dependencies:
        assert not dependency["external_dag_id"].startswith("dbt.source."), dependency


def main() -> int:
    test_cold_start_upload_waits_for_producer_task()
    test_repository_sources_wait_for_the_dq_task()
    print("Features service upload dependency tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
