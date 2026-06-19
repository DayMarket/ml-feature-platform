"""Materialize the geo_geointellect_features silver Iceberg table from ClickHouse."""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
import yaml
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")

with open(CONFIG_PATH, encoding="utf-8") as config_stream:
    CONFIG = yaml.safe_load(config_stream)


def _load_module(filename: str, module_name: str):
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
    tags=["feature-platform", "operations", "silver", "h3", "geo", "geointellect"],
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def geo_geointellect_features_dag() -> None:
    @task(executor_config=_executor_config())
    def materialize(partition_value: str) -> None:
        runtime = _load_module("runtime.py", "geo_geointellect_features_runtime")
        query = _load_module("query.py", "geo_geointellect_features_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        catalog = runtime.get_iceberg_catalog(ref)

        # Resolve the migrated output before running the expensive source query.
        table = runtime.preflight_table(catalog, ref)
        partition_date = runtime.parse_partition_date(partition_value)
        sql, params = query.build_query(partition_date.isoformat())
        frame = runtime.query_clickhouse(
            config["source"]["clickhouse_conn_id"],
            sql,
            params,
        )
        runtime.write_daily_snapshot(table, frame, partition_date)

    materialize(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )


dag = geo_geointellect_features_dag()
