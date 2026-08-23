import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

REPO_ROOT = os.path.abspath(os.path.join(DAG_DIR, "..", "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task

CONFIG_PATH = os.path.join(DAG_DIR, "config.yaml")
DQ_PARTITION_DATE = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'

from config.factory import get_dag_settings, get_deployment
from airflow.sdk import dag
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.timetables.interval import CronDataIntervalTimetable

dag_settings = get_dag_settings()

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")

default_args = {
    "owner": dag_settings["owner"],
    "depends_on_past": False,
    "trigger_rule": "all_success",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": send_oncall_notification(
        severity=dag_settings["alert_severity"],
        team=dag_settings["alert_team"],
        oncall_webhook_conn_id=dag_settings["alert_oncall_webhook_conn_id"],
    ),
}


@dag(
    default_args=default_args,
    max_active_runs=1,
    tags=["spark", "feature-platform", dag_settings["team_tag"], "gold", "prices"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable('0 2 * * *', 'UTC'),
    start_date=datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
    dag_id="feature-platform.layers.gold.sku_group_id.sku_group_price_features",
)
def collect_gold_sku_group_price_features():
    wait_for_silver_prices = ExternalTaskSensor(
        task_id="wait_for_silver_sku_group_id_prices",
        # Silver-витрина цен сама считает свой DQ таской dq внутри собственного DAG'а,
        # поэтому ждём её, а не отдельный dbt-DQ-DAG. Расписания 01:00 против 02:00
        # дают прежнюю разницу в час.
        external_dag_id="feature-platform.layers.silver.sku_group_id.sku_group_id_prices",
        external_task_id="dq",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(hours=1),
    )

    collect_features = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=2),
        task_id="getting_sku_group_price_features",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_sku_group_price_features.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    wait_for_silver_prices >> collect_features

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    collect_features >> dq_task

dag = collect_gold_sku_group_price_features()
