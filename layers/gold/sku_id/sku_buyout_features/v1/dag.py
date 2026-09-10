"""Собрать экономику корзины и выкупаемость на грейне sku_id после DQ источника."""

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
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
DQ_PARTITION_DATE = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
JOB_DIR = os.path.join(ENTITY_DIR, "job")


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)

SOURCE_CONFIG_PATH = os.path.join(REPO_ROOT, CONFIG["source"]["entity_path"], "config.yaml")
SOURCE_DAG_ID = "feature-platform.layers.gold.sku_id.buyout_online_sku_features"
SOURCE_DQ_TASK_ID = "dq"
# Источник пишет партицию в 06:00 UTC, витрина стартует в 07:00 UTC:
# D 07:00 - 1ч = D 06:00 — логическая дата прогона источника за ту же партицию.
SOURCE_DQ_EXECUTION_DELTA = timedelta(hours=1)


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
    tags=[
        "feature-platform",
        CONFIG["dag"]["group_tag"],
        CONFIG["dag"]["team"],
        "gold",
        "buyout",
        "sku",
    ],
    dagrun_timeout=timedelta(hours=4),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def sku_buyout_features_dag() -> None:
    wait_for_source_dq = ExternalTaskSensor(
        task_id="wait_for_buyout_online_sku_dq",
        external_dag_id=SOURCE_DAG_ID,
        external_task_id=SOURCE_DQ_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        execution_delta=SOURCE_DQ_EXECUTION_DELTA,
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )

    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module("runtime.py", "sku_buyout_features_runtime")
        query = _load_module("query.py", "sku_buyout_features_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        source_ref = runtime.table_ref(runtime.load_config(SOURCE_CONFIG_PATH))
        if source_ref.catalog != ref.catalog:
            raise ValueError(
                "Source and output configs must use one Iceberg catalog; "
                f"output={ref.catalog!r}, source={source_ref.catalog!r}"
            )

        catalog = runtime.get_iceberg_catalog(ref)
        # Resolve both migrated tables before running the expensive source query.
        table = runtime.preflight_table(catalog, ref)
        runtime.preflight_table(catalog, source_ref)

        partition_date = runtime.previous_utc_date(interval_end_value)
        source_table = runtime.trino_table_name(source_ref)
        conn_id = config["source"]["trino_conn_id"]

        min_id, max_id = runtime.source_id_bounds(conn_id)
        bounds = runtime.shard_bounds(min_id, max_id, runtime.shard_count(config))

        for index, (lower, upper) in enumerate(bounds):
            sql = query.build_query(partition_date, source_table, lower, upper)
            frame = runtime.query_trino(conn_id, sql)
            if index == 0:
                runtime.require_non_empty(frame, partition_date)
            runtime.write_partition_shard(
                table,
                frame,
                partition_date,
                replace=index == 0,
            )

    gold_task = materialize(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )
    wait_for_source_dq >> gold_task

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: downstream ждёт
    # таску dq, поэтому падение профилей не блокирует потребителей.
    gold_task >> [dq_task, stats_task]


dag = sku_buyout_features_dag()
