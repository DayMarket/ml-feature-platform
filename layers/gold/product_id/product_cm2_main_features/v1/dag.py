"""Materialize Main CM2 product features twice a day."""

import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag
from airflow.timetables.interval import CronDataIntervalTimetable

# from airflow_commons.helpers.oncall import send_oncall_notification

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(ENTITY_DIR, "..", "..", "..", "..", ".."))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, ENTITY_DIR)

from config.factory import get_dag_settings, get_deployment
from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

DQ_PARTITION_TIMESTAMP = (
    '{{ data_interval_end.in_timezone("Asia/Tashkent").strftime("%Y-%m-%d %H:%M:%S") }}'
)
SKU_CM2_INPUTS_DAG_ID = "feature-platform.layers.silver.sku_id.sku_cm2_inputs_daily"

dag_settings = get_dag_settings()
logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")


def _daily_s6_logical_date(logical_date):
    if logical_date.hour == 7:
        return logical_date - timedelta(hours=12)
    if logical_date.hour == 19:
        return logical_date - timedelta(hours=24)
    raise ValueError("product CM2 schedule expects a 07:00 or 19:00 UTC logical date")


default_args = {
    "owner": dag_settings["owner"],
    "depends_on_past": False,
    "trigger_rule": "all_success",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}
# Enable after the new recsys Gold DAG is stable.
# default_args["on_failure_callback"] = send_oncall_notification(
#     severity=dag_settings["alert_severity"],
#     team=dag_settings["alert_team"],
#     oncall_webhook_conn_id=dag_settings["alert_oncall_webhook_conn_id"],
# )


@dag(
    default_args=default_args,
    max_active_runs=1,
    tags=[
        "spark",
        "feature-platform",
        dag_settings["team_tag"],
        dag_settings["group_tag"],
        "gold",
        "product",
        "cm2-main",
        "recsys",
    ],
    dagrun_timeout=timedelta(hours=12),
    is_paused_upon_creation=True,
    catchup=dag_settings["catchup"],
    schedule=CronDataIntervalTimetable(dag_settings["schedule"], "UTC"),
    start_date=pendulum.parse(dag_settings["start_date"]).in_timezone("UTC"),
    dag_id=dag_settings["dag_id"],
)
def collect_gold_product_cm2_main_features():
    wait_for_sku_cm2_inputs_dq = ExternalTaskSensor(
        task_id="wait_for_silver_sku_cm2_inputs_dq",
        external_dag_id=SKU_CM2_INPUTS_DAG_ID,
        external_task_id="dq",
        execution_date_fn=_daily_s6_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
    )
    materialize_task = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_product_cm2_main_features",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(),
        kubernetes_conn_id="spark_k8s",
    )
    dq_task = build_dq_task(
        CONFIG_PATH,
        REPO_ROOT,
        failure_callback_enabled=False,
    )(DQ_PARTITION_TIMESTAMP)
    stats_task = build_feature_stats_task(
        CONFIG_PATH,
        REPO_ROOT,
        failure_callback_enabled=False,
    )(DQ_PARTITION_TIMESTAMP)
    wait_for_sku_cm2_inputs_dq >> materialize_task >> [dq_task, stats_task]


dag = collect_gold_product_cm2_main_features()
