import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

from config.factory import get_dag_settings, get_deployment

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
    tags=["spark", "feature-platform", dag_settings["team_tag"], "gold", "feedback"],
    is_paused_upon_creation=True,
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    dag_id="feature_platform_product_feedback_base_stats_gold_dag",
)
def collect_gold_product_feedback_base_stats():
    SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_product_feedback_base_stats",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_product_feedback_base_stats.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )


dag = collect_gold_product_feedback_base_stats()
