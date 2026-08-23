import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

from config.factory import get_dag_settings, get_deployment
from airflow.sdk import dag
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.timetables.interval import CronDataIntervalTimetable

dag_settings = get_dag_settings()

# Пожизненные факты аккаунта читаются из silver-витрины, поэтому ждём её DQ-DAG, а не Spark-DAG.
ACCOUNT_LIFETIME_FACTS_DQ_DAG_ID = (
    "dbt.source.trino.ml_feature_platform_silver."
    "feature_platform_account_lifetime_facts.dq"
)
# Джоб читает партицию пожизненных фактов за дату D, а её пишет silver-запуск, который
# стартовал сутками раньше (02:00 UTC дня D, логическая дата D-1 02:00). Отсюда сутки в дельте.
# Уточняется по факту появления DQ-DAG (см. README).
ACCOUNT_LIFETIME_FACTS_DQ_EXECUTION_DELTA = timedelta(days=1, hours=2)

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
        "gold",
        "account",
        "buyout",
    ],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(dag_settings["schedule"], "UTC"),
    start_date=pendulum.parse(dag_settings["start_date"]).in_timezone("UTC"),
    catchup=False,
    dag_id=dag_settings["dag_id"],
)
def collect_gold_buyout_account_history_features():
    wait_for_account_lifetime_facts = ExternalTaskSensor(
        task_id="wait_for_silver_account_lifetime_facts",
        external_dag_id=ACCOUNT_LIFETIME_FACTS_DQ_DAG_ID,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=60,
        timeout=4 * 60 * 60,
        check_existence=True,
        execution_delta=ACCOUNT_LIFETIME_FACTS_DQ_EXECUTION_DELTA,
    )

    collect_features = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=6),
        task_id="getting_buyout_account_history_features",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_buyout_account_history_features.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    wait_for_account_lifetime_facts >> collect_features


dag = collect_gold_buyout_account_history_features()
