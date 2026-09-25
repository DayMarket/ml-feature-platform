"""Витрина релевантности категории запросу, расширенная всеми формулировками query_id."""

import os
import sys
from datetime import timedelta

import pendulum
import yaml
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

REPO_ROOT = os.path.abspath(os.path.join(DAG_DIR, "..", "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from config.factory import get_dag_settings, get_deployment
from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

CONFIG_PATH = os.path.join(DAG_DIR, "config.yaml")
DQ_PARTITION_DATE = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'

SOURCE_DAG_ID = "feature-platform.layers.gold.category_id_query_text.query_category_relevance"
# У DAG'а исходной витрины нет таски dq: ждём её единственную таску materialize.
SOURCE_TASK_ID = "materialize"
# 03:30 минус 30 минут = 03:00, логическая дата прогона исходной витрины за тот же день.
SOURCE_EXECUTION_DELTA = timedelta(minutes=30)

QUERY_ID_DAG_ID = "feature-platform.layers.gold.query_text_version.search_query_id"
QUERY_ID_TASK_ID = "dq"
# Справочник стартует в 05:00, позже этого DAG'а, поэтому ждём прогон предыдущей даты:
# 03:30 минус 22 ч 30 мин = 05:00 предыдущего дня.
QUERY_ID_EXECUTION_DELTA = timedelta(hours=22, minutes=30)


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)
dag_settings = get_dag_settings()

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
    dag_id=CONFIG["dag"]["id"],
    max_active_runs=1,
    tags=[
        "spark",
        "feature-platform",
        dag_settings["team_tag"],
        CONFIG["dag"]["group_tag"],
        "gold",
        "search",
        "query-id",
    ],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def query_category_relevance_expanded_dag() -> None:
    wait_for_source = ExternalTaskSensor(
        task_id="wait_for_query_category_relevance",
        external_dag_id=SOURCE_DAG_ID,
        external_task_id=SOURCE_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=5 * 60,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=SOURCE_EXECUTION_DELTA,
    )

    wait_for_query_id = ExternalTaskSensor(
        task_id="wait_for_search_query_id_dq",
        external_dag_id=QUERY_ID_DAG_ID,
        external_task_id=QUERY_ID_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=5 * 60,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=QUERY_ID_EXECUTION_DELTA,
    )

    collect = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=3),
        task_id="getting_query_category_relevance_expanded",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_query_category_relevance_expanded.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    [wait_for_source, wait_for_query_id] >> collect

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: upload ждёт таску dq,
    # поэтому падение профилей не блокирует публикацию.
    collect >> [dq_task, stats_task]


dag = query_category_relevance_expanded_dag()
