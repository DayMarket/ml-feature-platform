import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sdk import dag
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(
    os.path.join(DAG_DIR, "..", "..", "..", "..", "..")
)
CONFIG_PATH = os.path.join(DAG_DIR, "config.yaml")
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, DAG_DIR)

from config.factory import get_dag_settings, get_deployment
from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

DQ_PARTITION_TIMESTAMP = (
    '{{ data_interval_end.in_timezone("Asia/Tashkent").strftime('
    '"%Y-%m-%d %H:%M:%S") }}'
)

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
    tags=[
        "spark",
        "feature-platform",
        dag_settings["team_tag"],
        dag_settings["group_tag"],
        "silver",
        "account",
        "product",
        "actions",
    ],
    dagrun_timeout=timedelta(hours=12),
    is_paused_upon_creation=True,
    catchup=dag_settings["catchup"],
    schedule=CronDataIntervalTimetable(dag_settings["schedule"], "UTC"),
    start_date=pendulum.parse(dag_settings["start_date"]).in_timezone("UTC"),
    dag_id=dag_settings["dag_id"],
)
def collect_silver_account_product_session_action_counts_12h():
    materialize_task = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_account_product_session_action_counts_12h",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(),
        kubernetes_conn_id="spark_k8s",
    )
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(
        DQ_PARTITION_TIMESTAMP
    )
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(
        DQ_PARTITION_TIMESTAMP
    )
    materialize_task >> [dq_task, stats_task]


dag = collect_silver_account_product_session_action_counts_12h()
