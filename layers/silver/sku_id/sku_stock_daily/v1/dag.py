import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag
from airflow.timetables.interval import CronDataIntervalTimetable

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
    tags=["spark", "feature-platform", dag_settings["team_tag"], "silver", "stock"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable("0 0 * * *", "UTC"),
    start_date=datetime(2026, 6, 10, 0, 0, 0, tzinfo=timezone.utc),
    dag_id="feature_platform_sku_stock_daily_silver_dag",
)
def collect_silver_sku_stock_daily():
    wait_for_sku_eod = ExternalTaskSensor(
        task_id="wait_for_sku_eod",
        external_dag_id="dbt.models.dwh_trino.sku_eod",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(0),
    )

    collect_stock = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_sku_stock_daily",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_silver_sku_stock_daily.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    wait_for_sku_eod >> collect_stock


dag = collect_silver_sku_stock_daily()
