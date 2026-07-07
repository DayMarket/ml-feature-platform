import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
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
    tags=[
        "spark",
        "feature-platform",
        dag_settings["team_tag"],
        dag_settings["group_tag"],
        "silver",
        "query",
        "sku-group",
        "dssm",
    ],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(dag_settings["schedule"], "UTC"),
    start_date=pendulum.parse(dag_settings["start_date"]).in_timezone("UTC"),
    catchup=dag_settings["catchup"],
    dag_id=dag_settings["dag_id"],
)
def collect_silver_search_query_sku_group_dssm_scores():
    SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_search_query_sku_group_dssm_scores",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_silver_search_query_sku_group_dssm_scores.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )


dag = collect_silver_search_query_sku_group_dssm_scores()
