"""Assemble gold location forecast features after all silver DQ DAGs succeed."""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
import yaml
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(ENTITY_DIR, "..", "..", "..", ".."))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")

SILVER_CONFIG_PATHS = {
    "geo": os.path.join(
        REPO_ROOT, "layers", "silver", "geo_geointellect_features", "v1", "config.yaml"
    ),
    "dp": os.path.join(
        REPO_ROOT, "layers", "silver", "dp_neighbor_order_features", "v1", "config.yaml"
    ),
    "act": os.path.join(
        REPO_ROOT, "layers", "silver", "geo_user_activity_features", "v1", "config.yaml"
    ),
    "loc": os.path.join(
        REPO_ROOT, "layers", "silver", "geo_user_location_features", "v1", "config.yaml"
    ),
    "poi": os.path.join(
        REPO_ROOT, "layers", "silver", "geo_yandex_poi_features", "v1", "config.yaml"
    ),
}


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)
SILVER_CONFIGS = {
    alias: _read_config(path) for alias, path in SILVER_CONFIG_PATHS.items()
}


def _load_job_module(filename: str, module_name: str):
    path = os.path.join(JOB_DIR, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _executor_config() -> dict:
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=CONFIG["runtime"]["image"],
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": "16Gi", "cpu": "4"},
                            limits={"memory": "16Gi"},
                        ),
                    )
                ]
            )
        )
    }


def _dq_dag_id(config: dict) -> str:
    table = config["table"]
    return (
        f"dbt.source.trino.ml_feature_platform_{table['schema']}."
        f"{table['name']}.dq"
    )


def get_dag_default_args() -> dict:
    return {
        "owner": CONFIG["dag"]["owner"],
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "max_retry_delay": timedelta(minutes=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": send_oncall_notification(
            team=CONFIG["alerts"]["team"],
            oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
            severity=CONFIG["alerts"]["severity"],
        ),
    }


@dag(
    default_args=get_dag_default_args(),
    dag_id=CONFIG["dag"]["id"],
    max_active_runs=1,
    tags=["feature-platform", CONFIG["dag"]["group_tag"], "operations", "gold", "geo", "h3", "forecast"],
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def location_h3_forecast_features_dag() -> None:
    silver_dq_sensors = [
        ExternalTaskSensor(
            task_id=f"wait_for_{alias}_dq",
            external_dag_id=_dq_dag_id(silver_config),
            allowed_states=["success"],
            failed_states=["failed"],
            check_existence=True,
            execution_delta=timedelta(0),
            mode="reschedule",
            poke_interval=60,
            timeout=3 * 60 * 60,
        )
        for alias, silver_config in SILVER_CONFIGS.items()
    ]

    @task(executor_config=_executor_config())
    def assemble_gold(partition_value: str) -> None:
        runtime = _load_job_module("runtime.py", "location_h3_forecast_runtime")
        build = _load_job_module("build.py", "location_h3_forecast_build")

        output_config = runtime.load_config(CONFIG_PATH)
        output_ref = runtime.table_ref(output_config)
        input_refs = {
            alias: runtime.table_ref(runtime.load_config(path))
            for alias, path in SILVER_CONFIG_PATHS.items()
        }
        mismatched_catalogs = {
            ref.catalog for ref in input_refs.values() if ref.catalog != output_ref.catalog
        }
        if mismatched_catalogs:
            raise ValueError(
                "Gold and silver configs must use one Iceberg catalog; "
                f"gold={output_ref.catalog!r}, silver={sorted(mismatched_catalogs)!r}"
            )

        catalog = runtime.get_iceberg_catalog(output_ref)
        output_table = runtime.preflight_table(catalog, output_ref)
        input_tables = {
            alias: runtime.preflight_table(catalog, ref)
            for alias, ref in input_refs.items()
        }
        partition_date = runtime.parse_partition_date(partition_value)

        def read_partition(alias, day):
            return runtime.read_iceberg_date(input_tables[alias], day)

        frame = build.build_gold(read_partition, partition_date)
        runtime.write_daily_snapshot(output_table, frame, partition_date)

    gold_task = assemble_gold(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )
    silver_dq_sensors >> gold_task


dag = location_h3_forecast_features_dag()
