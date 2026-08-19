"""Project the silver city CPI snapshot into the online gold table after its DQ DAG."""

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
REPO_ROOT = os.path.abspath(os.path.join(ENTITY_DIR, "..", "..", "..", "..", ".."))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")

SILVER_CONFIG_PATH = os.path.join(
    REPO_ROOT,
    "layers",
    "silver",
    "city_id_dimensional_group",
    "delivery_cpi_city_features",
    "v1",
    "config.yaml",
)

# Silver пишется в 03:00 UTC, проекция стартует в 06:00 UTC. Дельта считает
# logical date DQ-DAG-а равной logical date DAG-производителя; после появления
# dbt-DQ DAG-а её нужно сверить с его реальным расписанием.
SILVER_DQ_EXECUTION_DELTA = timedelta(hours=3)


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)
SILVER_CONFIG = _read_config(SILVER_CONFIG_PATH)


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
                            requests={"memory": "4Gi", "cpu": "2"},
                            limits={"memory": "4Gi"},
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
    tags=[
        "feature-platform",
        CONFIG["dag"]["group_tag"],
        CONFIG["dag"]["team"],
        "gold",
        "buyout",
        "delivery",
        "cpi",
        "online",
    ],
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def buyout_online_city_features_dag() -> None:
    wait_for_silver_dq = ExternalTaskSensor(
        task_id="wait_for_delivery_cpi_city_dq",
        external_dag_id=_dq_dag_id(SILVER_CONFIG),
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        execution_delta=SILVER_DQ_EXECUTION_DELTA,
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )

    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module("runtime.py", "buyout_online_city_features_runtime")
        query = _load_module("query.py", "buyout_online_city_features_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        silver_ref = runtime.table_ref(runtime.load_config(SILVER_CONFIG_PATH))
        if silver_ref.catalog != ref.catalog:
            raise ValueError(
                "Source and output configs must use one Iceberg catalog; "
                f"output={ref.catalog!r}, silver={silver_ref.catalog!r}"
            )

        catalog = runtime.get_iceberg_catalog(ref)
        # Resolve both migrated tables before reading the source partition.
        table = runtime.preflight_table(catalog, ref)
        runtime.preflight_table(catalog, silver_ref)
        # Партиция совпадает с партицией silver-витрины: дата конца интервала.
        partition_date = runtime.parse_partition_date(interval_end_value)
        sql = query.build_query(partition_date, runtime.trino_table_name(silver_ref))
        frame = runtime.query_trino(config["source"]["trino_conn_id"], sql)
        runtime.require_non_empty(frame, partition_date)
        runtime.write_daily_snapshot(table, frame, partition_date)

    gold_task = materialize(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )
    wait_for_silver_dq >> gold_task


dag = buyout_online_city_features_dag()
