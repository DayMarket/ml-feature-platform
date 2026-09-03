"""Write daily SKU CM2 inputs to Iceberg."""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
import yaml
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
    '{{ data_interval_end.in_timezone("Asia/Tashkent").strftime("%Y-%m-%d") }}'
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
    resources = CONFIG["runtime"]["resources"]
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=CONFIG["runtime"]["image"],
                        resources=k8s.V1ResourceRequirements(
                            requests={
                                "memory": resources["memory"],
                                "cpu": resources["cpu"],
                            },
                            limits={"memory": resources["memory"]},
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
        "sku",
        "cm2",
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
def sku_cm2_inputs_daily_dag() -> None:
    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module("runtime.py", "sku_cm2_inputs_daily_runtime")
        query = _load_module("query.py", "sku_cm2_inputs_daily_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        catalog = runtime.get_iceberg_catalog(ref)
        table = runtime.preflight_table(catalog, ref)

        interval_end = runtime.parse_airflow_timestamp(interval_end_value)
        dt = runtime.tashkent_dt(interval_end)
        source = config["source"]
        query_args = {
            "dt": dt,
            "interval_end": interval_end,
            "sku_table": source["sku_table"],
            "prices_table": source["prices_table"],
            "commission_table": source["commission_table"],
            "orders_table": source["orders_table"],
        }

        frame = runtime.query_trino(
            source["trino_conn_id"],
            query.build_query(**query_args),
        )
        runtime.write_inputs(
            table=table,
            frame=frame,
            dt=dt,
        )

    interval_end_value = (
        '{{ data_interval_end.in_timezone("UTC").isoformat() }}'
    )
    materialize_task = materialize(interval_end_value)
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(
        DQ_PARTITION_DATE
    )
    materialize_task >> [dq_task, stats_task]


dag = sku_cm2_inputs_daily_dag()
