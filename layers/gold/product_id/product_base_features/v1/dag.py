"""Materialize product-base Gold features twice a day."""

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
PRODUCT_METADATA_DAG_ID = "feature-platform.layers.silver.product_id.product_metadata"
PRODUCT_PRICES_DAG_ID = "feature-platform.layers.silver.product_id.product_prices_daily"
SKU_CM2_INPUTS_DAG_ID = "feature-platform.layers.silver.sku_id.sku_cm2_inputs_daily"
ACTION_COUNTS_DAG_ID = (
    "feature-platform.layers.silver.account_id_product_id."
    "account_product_session_action_counts_12h"
)
FEEDBACK_COUNTS_DAG_ID = (
    "feature-platform.layers.silver.product_id.product_feedback_counts_12h"
)
PRODUCT_FEEDBACK_BASE_STATS_DAG_ID = (
    "feature-platform.layers.gold.product_id.feedback_product_id"
)
CATEGORY_GENDER_DAG_ID = (
    "feature-platform.layers.gold.category_id.category_gender_features"
)

dag_settings = get_dag_settings()

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")


def _daily_snapshot_logical_date(logical_date):
    if logical_date.hour == 7:
        return logical_date - timedelta(hours=12)
    if logical_date.hour == 19:
        return logical_date - timedelta(hours=24)
    raise ValueError("product-base schedule expects a 07:00 or 19:00 UTC logical date")


def _feedback_base_stats_logical_date(logical_date):
    if logical_date.hour not in (7, 19):
        raise ValueError(
            "product-base schedule expects a 07:00 or 19:00 UTC logical date"
        )
    return logical_date.replace(hour=3, minute=0, second=0, microsecond=0)


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
        "base-features",
        "recsys",
    ],
    dagrun_timeout=timedelta(hours=12),
    is_paused_upon_creation=True,
    catchup=dag_settings["catchup"],
    schedule=CronDataIntervalTimetable(dag_settings["schedule"], "UTC"),
    start_date=pendulum.parse(dag_settings["start_date"]).in_timezone("UTC"),
    dag_id=dag_settings["dag_id"],
)
def collect_gold_product_base_features():
    wait_for_product_metadata_dq = ExternalTaskSensor(
        task_id="wait_for_silver_product_metadata_dq",
        external_dag_id=PRODUCT_METADATA_DAG_ID,
        external_task_id="dq",
        execution_date_fn=_daily_snapshot_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
    )
    wait_for_product_prices_dq = ExternalTaskSensor(
        task_id="wait_for_silver_product_prices_dq",
        external_dag_id=PRODUCT_PRICES_DAG_ID,
        external_task_id="dq",
        execution_date_fn=_daily_snapshot_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
    )
    wait_for_sku_cm2_inputs_dq = ExternalTaskSensor(
        task_id="wait_for_silver_sku_cm2_inputs_dq",
        external_dag_id=SKU_CM2_INPUTS_DAG_ID,
        external_task_id="dq",
        execution_date_fn=_daily_snapshot_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
    )
    wait_for_action_counts_dq = ExternalTaskSensor(
        task_id="wait_for_silver_account_product_action_counts_dq",
        external_dag_id=ACTION_COUNTS_DAG_ID,
        external_task_id="dq",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
    )
    wait_for_feedback_counts_dq = ExternalTaskSensor(
        task_id="wait_for_silver_product_feedback_counts_dq",
        external_dag_id=FEEDBACK_COUNTS_DAG_ID,
        external_task_id="dq",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
    )
    wait_for_product_feedback_base_stats_dq = ExternalTaskSensor(
        task_id="wait_for_gold_product_feedback_base_stats_dq",
        external_dag_id=PRODUCT_FEEDBACK_BASE_STATS_DAG_ID,
        external_task_id="dq",
        execution_date_fn=_feedback_base_stats_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
    )
    wait_for_category_gender_dq = ExternalTaskSensor(
        task_id="wait_for_gold_category_gender_dq",
        external_dag_id=CATEGORY_GENDER_DAG_ID,
        external_task_id="dq",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
    )

    materialize_task = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_product_base_features",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(),
        kubernetes_conn_id="spark_k8s",
    )
    # Enable callbacks together with the main DAG callback after debugging.
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

    (
        [
            wait_for_product_metadata_dq,
            wait_for_product_prices_dq,
            wait_for_sku_cm2_inputs_dq,
            wait_for_action_counts_dq,
            wait_for_feedback_counts_dq,
            wait_for_product_feedback_base_stats_dq,
            wait_for_category_gender_dq,
        ]
        >> materialize_task
        >> [dq_task, stats_task]
    )


dag = collect_gold_product_base_features()
