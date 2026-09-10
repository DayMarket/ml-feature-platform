"""Опубликовать последнюю партицию sku_buyout_features в PostgreSQL сервиса невыкупов."""

import importlib.util
import json
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

UPLOAD_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(UPLOAD_DIR, "..", ".."))

CONFIG_PATH = os.path.join(UPLOAD_DIR, "config.yaml")
with open(CONFIG_PATH, encoding="utf-8") as config_stream:
    CONFIG = json.load(config_stream)

FEATURE_GROUP = CONFIG["feature_groups"][0]
SOURCE = FEATURE_GROUP["source"]
SINK = CONFIG["sink"]

SOURCE_ENTITY_DIR = os.path.join(
    REPO_ROOT, "layers", "gold", "sku_id", "sku_buyout_features", "v1"
)
SOURCE_CONFIG_PATH = os.path.join(SOURCE_ENTITY_DIR, "config.yaml")


def _load_module(path: str, module_name: str):
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
                            requests={"memory": "8Gi", "cpu": "2"},
                            limits={"memory": "8Gi"},
                        ),
                    )
                ]
            )
        )
    }


default_args = {
    "owner": CONFIG["dag"]["owner"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_oncall_notification(
        team=CONFIG["alerts"]["team"],
        oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
        severity=CONFIG["alerts"]["severity"],
    ),
}


@dag(
    default_args=default_args,
    dag_id=CONFIG["dag"]["id"],
    max_active_runs=1,
    tags=[
        "feature-platform",
        "buyout-features",
        CONFIG["dag"]["team"],
        "upload",
        "postgres",
    ],
    dagrun_timeout=timedelta(hours=4),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(cron=CONFIG["dag"]["schedule"], timezone="UTC"),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def buyout_sku_postgres_upload() -> None:
    wait_for_gold_dq = ExternalTaskSensor(
        task_id="wait_for_sku_buyout_features_dq",
        external_dag_id=SOURCE["dependency_dag_id"],
        external_task_id=SOURCE["dependency_task_id"],
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        execution_delta=timedelta(
            minutes=SOURCE["dependency_execution_delta_minutes"]
        ),
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )

    @task(executor_config=_executor_config())
    def publish_to_postgres(interval_end_value: str) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        runtime = _load_module(
            os.path.join(SOURCE_ENTITY_DIR, "job", "runtime.py"),
            "sku_buyout_features_runtime",
        )
        job = _load_module(
            os.path.join(UPLOAD_DIR, "job", "upload_postgres.py"),
            "buyout_sku_upload_postgres",
        )

        ref = runtime.table_ref(runtime.load_config(SOURCE_CONFIG_PATH))
        catalog = runtime.get_iceberg_catalog(ref)
        iceberg_table = runtime.preflight_table(catalog, ref)

        # Та же партиция, что записала витрина: дата конца интервала минус сутки.
        partition_date = runtime.previous_utc_date(interval_end_value)
        target_table = f'{SINK["schema"]}.{SINK["table"]}'
        connection = PostgresHook(
            postgres_conn_id=SINK["connection_id"],
            schema=SINK["schema"],
        ).get_conn()

        job.publish(
            iceberg_table,
            partition_date,
            tuple(FEATURE_GROUP["features"]),
            connection,
            target_table,
            pendulum.now("UTC"),
        )

    wait_for_gold_dq >> publish_to_postgres(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )


dag = buyout_sku_postgres_upload()
