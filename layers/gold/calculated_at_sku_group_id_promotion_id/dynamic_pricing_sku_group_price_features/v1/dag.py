"""Aggregate dynamic-pricing SKU prices to SKU group/promotion."""

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

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, ENTITY_DIR)
REPO_ROOT = os.path.abspath(os.path.join(ENTITY_DIR, "..", "..", "..", "..", ".."))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
SKU_PRICE_CONFIG_PATH = os.path.join(
    REPO_ROOT,
    "layers",
    "gold",
    "calculated_at_sku_id_promotion_id",
    "dynamic_pricing_price_features",
    "v1",
    "config.yaml",
)

from config.factory import get_deployment


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)
SKU_PRICE_CONFIG = _read_config(SKU_PRICE_CONFIG_PATH)


def get_dag_default_args() -> dict:
    return {
        "owner": CONFIG["dag"]["owner"],
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "max_retry_delay": timedelta(minutes=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": send_oncall_notification(
            team=CONFIG["alerts"]["team"],
            oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
            severity=CONFIG["alerts"]["severity"],
        ),
    }


@dag(
    default_args=get_dag_default_args(),
    dag_id=CONFIG["dag"]["id"],
    max_active_runs=1,
    tags=[
        "feature-platform",
        CONFIG["dag"]["group_tag"],
        CONFIG["dag"]["team"],
        "gold",
        "sku-group",
        "dynamic-pricing",
        "prices",
    ],
    dagrun_timeout=timedelta(hours=12),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def dynamic_pricing_sku_group_price_features_dag() -> None:
    wait_for_sku_price_dag = ExternalTaskSensor(
        task_id="wait_for_sku_dynamic_pricing_price_features",
        external_dag_id=SKU_PRICE_CONFIG["dag"]["id"],
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=False,
    )

    aggregate_task = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=10),
        task_id="getting_dynamic_pricing_sku_group_price_features",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_dynamic_pricing_sku_group_price_features.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    wait_for_sku_price_dag >> aggregate_task


dag = dynamic_pricing_sku_group_price_features_dag()
