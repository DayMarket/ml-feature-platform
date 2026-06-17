import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

from config.factory import get_dag_settings, get_deployment, get_source_dependencies
from airflow.sdk import dag
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.timetables.interval import CronDataIntervalTimetable

dag_settings = get_dag_settings()

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")


def _parse_utc_start_date(value: str) -> datetime:
    start_date = datetime.fromisoformat(value)
    if start_date.tzinfo is None:
        return start_date.replace(tzinfo=timezone.utc)
    return start_date.astimezone(timezone.utc)


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
    tags=["spark", "feature-platform", dag_settings["team_tag"], "ranking", "upload"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(dag_settings["schedule"], 'UTC'),
    start_date=_parse_utc_start_date(dag_settings["start_date"]),
    dag_id=dag_settings["dag_id"],
)
def upload_ranking_features():
    sensors = []
    for dependency in get_source_dependencies():
        sensors.append(
            ExternalTaskSensor(
                task_id=dependency["task_id"],
                external_dag_id=dependency["external_dag_id"],
                allowed_states=["success"],
                failed_states=["failed"],
                mode="poke",
                poke_interval=30,
                timeout=6 * 60 * 60,
                check_existence=True,
                execution_delta=timedelta(
                    minutes=dependency["execution_delta_minutes"]
                ),
            )
        )

    upload_features = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="upload_ranking_features",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(".", "upload_ranking_features.yaml"),
        kubernetes_conn_id="spark_k8s",
    )
    sensors >> upload_features


dag = upload_ranking_features()
