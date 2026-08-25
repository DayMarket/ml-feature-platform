import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

REPO_ROOT = os.path.abspath(os.path.join(DAG_DIR, "..", "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

CONFIG_PATH = os.path.join(DAG_DIR, "config.yaml")
DQ_PARTITION_DATE = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'

from config.factory import get_dag_settings, get_deployment
from airflow.sdk import dag
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
    tags=["spark", "feature-platform", dag_settings["team_tag"], "silver", "account-category"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable('0 19 * * *', 'UTC'),
    start_date=pendulum.datetime(2026, 6, 1, 0, 0, 0, tz="Asia/Tashkent"),
    dag_id="feature-platform.layers.silver.account_id_category_id.account_l1_event_w_imps_counts",
)
def collect_silver_account_l1_event_w_imps_counts():
    collect_features = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_account_l1_event_w_imps_counts",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_silver_account_l1_event_w_imps_counts.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: downstream ждёт
    # таску dq, поэтому падение профилей не блокирует потребителей.
    collect_features >> [dq_task, stats_task]


dag = collect_silver_account_l1_event_w_imps_counts()
