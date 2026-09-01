import logging
import os
import sys
from datetime import timedelta

import pendulum
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

REPO_ROOT = os.path.abspath(os.path.join(DAG_DIR, "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

CONFIG_PATH = os.path.join(DAG_DIR, "config.yaml")
# Партиция считается от конца недельного интервала: это дата фактического запуска
# DAG'а, её же пишет джоб в collection_date. Значение обязано совпадать с
# dq.partition_date_template и feature_stats.partition_date_template в config.yaml.
DQ_PARTITION_DATE = '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d") }}'

from config.factory import get_dag_settings, get_deployment

dag_settings = get_dag_settings()

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")


def _feedback_dq_logical_date(logical_date, **_):
    """Прогон feedback_sku_group_id, записавший партицию за последний собираемый день.

    Наше окно — [logical_date, logical_date + 7 дней), последняя закрытая дата в
    нём — суббота. feedback_sku_group_id идёт по расписанию 10 3 * * * UTC и пишет
    date = date(data_interval_start), поэтому нужен прогон с логической датой
    «эта суббота, 03:10 UTC». Она позже нашей логической даты, и положительным
    execution_delta не выражается — отсюда execution_date_fn.
    """
    return logical_date.in_timezone("UTC").add(days=6).replace(
        hour=3, minute=10, second=0, microsecond=0
    )


# DAG собирает недельный датасет логов ранжирования для подбора параметров формулы
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
    tags=["spark", "feature-platform", dag_settings["team_tag"], dag_settings["group_tag"], "dataset"],
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(dag_settings["schedule"], "UTC"),
    start_date=pendulum.parse(dag_settings["start_date"]).in_timezone("UTC"),
    dag_id="feature-platform.datasets.search.ranking_logs.v1",
)
def collect_ranking_logs_dataset_v1():
    # AGENTS.md: датасет читает feature_platform_sku_group_feedback_base_stats
    # (owned by feature-platform.layers.gold.sku_group_id.feedback_sku_group_id),
    # поэтому обязан ждать её dq-таску — иначе feedback CTE молча подставит
    # устаревший снапшот вместо честного fail/wait. Остальные три источника
    # (silver.ranking_analytics_events, silver.sku,
    # silver.search_queries_frequency_groups_30d) — upstream DE-таблицы без
    # feature_platform_-префикса, сенсоры на них не нужны.
    wait_for_sku_group_feedback = ExternalTaskSensor(
        task_id="wait_for_sku_group_feedback_dq",
        external_dag_id="feature-platform.layers.gold.sku_group_id.feedback_sku_group_id",
        external_task_id="dq",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_date_fn=_feedback_dq_logical_date,
    )

    collect_dataset = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="collect_ranking_logs_dataset",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_dataset_ranking_logs_v1.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: downstream ждёт
    # таску dq, поэтому падение профилей не блокирует потребителей.
    wait_for_sku_group_feedback >> collect_dataset >> [dq_task, stats_task]


dag = collect_ranking_logs_dataset_v1()
