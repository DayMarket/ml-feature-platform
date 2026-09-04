"""Write daily product price facts to Iceberg."""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
import yaml
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from kubernetes.client import models as k8s

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")
REPO_ROOT = os.path.abspath(
    os.path.join(ENTITY_DIR, "..", "..", "..", "..", "..")
)
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

DQ_PARTITION_DATE = (
    '{{ data_interval_end.in_timezone("Asia/Tashkent").strftime('
    '"%Y-%m-%d") }}'
)

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
                            requests={"memory": "4Gi", "cpu": "1"},
                            limits={"memory": "4Gi"},
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
    }


@dag(
    default_args=get_dag_default_args(),
    dag_id=CONFIG["dag"]["id"],
    max_active_runs=1,
    tags=[
        "feature-platform",
        CONFIG["dag"]["group_tag"],
        CONFIG["dag"]["team"],
        "silver",
        "product",
        "prices",
    ],
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=CONFIG["dag"]["catchup"],
)
def product_prices_daily_dag() -> None:
    wait_for_quantity_eod = ExternalTaskSensor(
        task_id="wait_for_quantity_eod",
        external_dag_id="dwh_core.quantity_eod",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(hours=19),
    )

    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module("runtime.py", "product_prices_daily_runtime")
        query = _load_module("query.py", "product_prices_daily_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        catalog = runtime.get_iceberg_catalog(ref)

        table = runtime.preflight_table(catalog, ref)
        dt = runtime.calculation_tashkent_dt(interval_end_value)
        source_date = dt.date() - timedelta(days=1)
        conn_id = config["source"]["trino_conn_id"]

        metrics = runtime.query_trino(
            conn_id,
            query.build_source_metrics_query(source_date),
        )
        runtime.validate_source_metrics(metrics, source_date)

        frames = runtime.iter_trino_batches(
            conn_id=conn_id,
            sql=query.build_query(dt, source_date),
            batch_size=config["runtime"]["query_batch_rows"],
        )
        runtime.write_daily_price_batches(table, frames, dt)

    interval_end_value = (
        '{{ data_interval_end.in_timezone("UTC").isoformat() }}'
    )
    materialize_prices = materialize(interval_end_value)
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(
        DQ_PARTITION_DATE
    )
    wait_for_quantity_eod >> materialize_prices
    materialize_prices >> [dq_task, stats_task]


dag = product_prices_daily_dag()
