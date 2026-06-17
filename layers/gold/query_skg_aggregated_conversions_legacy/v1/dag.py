import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

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
    tags=["spark", "feature-platform", dag_settings["team_tag"], "gold", "legacy", "query-skg"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable('0 3 * * *', 'UTC'),
    start_date=datetime(2026, 6, 10, 0, 0, 0, tzinfo=timezone.utc),
    dag_id="feature_platform_query_skg_aggregated_conversions_legacy_gold_dag",
)
def collect_gold_query_skg_aggregated_conversions_legacy():
    #wait_for_silver_daily_conversions = ExternalTaskSensor(
    #    task_id="wait_for_silver_query_skg_daily_conversions_legacy",
    #    external_dag_id=(
    #        "dbt.source.trino.ml_feature_platform_silver."
    #        "feature_platform_query_skg_daily_conversions_legacy.dq"
    #    ),
    #    allowed_states=["success"],
    #    failed_states=["failed"],
    #    mode="poke",
    #    poke_interval=30,
    #    timeout=6 * 60 * 60,
    #    check_existence=True,
    #    execution_delta=timedelta(hours=2),
    #)

    collect_features = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_query_skg_aggregated_conversions_legacy",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_query_skg_aggregated_conversions_legacy.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    #wait_for_silver_daily_conversions >> 
    collect_features


dag = collect_gold_query_skg_aggregated_conversions_legacy()

