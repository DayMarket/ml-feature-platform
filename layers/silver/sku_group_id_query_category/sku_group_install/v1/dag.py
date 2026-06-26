import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

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

#version 3.3
@dag(
    default_args=default_args,
    max_active_runs=1,
    tags=["spark", "feature-platform", dag_settings["team_tag"], "silver"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable('0 1 * * *', 'UTC'),
    start_date=datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
    dag_id="feature-platform.layers.silver.sku_group_id_query_category.sku_group_install",
)
def collect_silver_sku_group_query_install_stats():

    wait_for_sessions_dq = ExternalTaskSensor(
        task_id="wait_for_sessions_dq",
        external_dag_id="dbt.tests.dbt_clickhouse_dwh.sessions.dq",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
    )

    wait_for_events_dq = ExternalTaskSensor(
        task_id="wait_for_events_dq",
        external_dag_id="dbt.tests.dbt_clickhouse_dwh.events.dq",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
    )

    collect_stats = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_sku_group_query_install_stats",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_silver_sku_group_statistics.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    [wait_for_sessions_dq, wait_for_events_dq] >> collect_stats


dag = collect_silver_sku_group_query_install_stats()
