import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

from config.factory import get_deployment

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")

default_args = {
    "owner": "team:search",
    "depends_on_past": False,
    "trigger_rule": "all_success",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": send_oncall_notification(
        severity="P3",
        team="search",
        oncall_webhook_conn_id="oncall_webhook_search",
    ),
}


@dag(
    default_args=default_args,
    max_active_runs=1,
    tags=["spark", "feature-platform", "team::search", "gold", "orders", "atc"],
    is_paused_upon_creation=True,
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 6, 1, 0, 0, 0),
    dag_id="feature_platform_sku_group_query_atc_order_features_gold_dag",
)
def collect_gold_sku_group_query_atc_order_features():
    wait_for_silver_install_stats = ExternalTaskSensor(
        task_id="wait_for_silver_sku_group_install_stats",
        external_dag_id="feature_platform_sku_group_install_silver_stats_dag",
        external_task_id="getting_sku_group_query_install_stats",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(hours=2),
    )

    wait_for_silver_search_orders = ExternalTaskSensor(
        task_id="wait_for_silver_sku_group_query_search_orders",
        external_dag_id="feature_platform_sku_group_query_search_orders_silver_dag",
        external_task_id="getting_sku_group_query_search_orders",
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(hours=2),
    )

    collect_features = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_sku_group_query_atc_order_features",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_sku_group_query_atc_order_features.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    [wait_for_silver_install_stats, wait_for_silver_search_orders] >> collect_features


dag = collect_gold_sku_group_query_atc_order_features()
