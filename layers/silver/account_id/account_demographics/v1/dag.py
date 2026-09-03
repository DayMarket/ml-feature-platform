"""Write daily account demographics to Iceberg."""

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
        "account",
        "demographics",
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
def account_demographics_dag() -> None:
    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module("runtime.py", "account_demographics_runtime")
        query = _load_module("query.py", "account_demographics_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        catalog = runtime.get_iceberg_catalog(ref)

        table = runtime.preflight_table(catalog, ref)
        dt = runtime.dt_from_interval_end(interval_end_value)
        source = config["source"]
        history_table = (
            f'{source["trino_iceberg_catalog"]}.{ref.schema}.{ref.name}'
        )
        frames = runtime.iter_trino_batches(
            conn_id=source["trino_conn_id"],
            sql=query.build_query(
                dt=dt,
                customer_table=source["customer_table"],
                ecosystem_users_table=source["ecosystem_users_table"],
                clickhouse_catalog=source["clickhouse_catalog"],
                geo_events_table=source["geo_events_table"],
                platform_sessions_table=source["platform_sessions_table"],
                city_table=source["city_table"],
                history_table=history_table,
                lookback_days=source["lookback_days"],
                geo_fold_days=source["geo_fold_days"],
            ),
            batch_size=config["runtime"]["query_batch_rows"],
        )
        runtime.write_demographics_batches(table, frames, dt)

    interval_end_value = (
        '{{ data_interval_end.in_timezone("UTC").isoformat() }}'
    )
    materialize_task = materialize(interval_end_value)
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(
        DQ_PARTITION_DATE
    )
    materialize_task >> [dq_task, stats_task]


dag = account_demographics_dag()
