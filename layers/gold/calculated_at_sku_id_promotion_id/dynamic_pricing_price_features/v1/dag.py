"""Materialize dynamic-pricing final SKU prices to Iceberg."""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
import yaml
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(ENTITY_DIR, "..", "..", "..", "..", ".."))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")
SILVER_CONFIG_PATH = os.path.join(
    REPO_ROOT,
    "layers",
    "silver",
    "sku_id_promotion_id",
    "dynamic_pricing_prices",
    "v1",
    "config.yaml",
)


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)
SILVER_CONFIG = _read_config(SILVER_CONFIG_PATH)


def _load_job_module(filename: str, module_name: str):
    path = os.path.join(JOB_DIR, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _executor_config() -> dict:
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=CONFIG["runtime"]["image"],
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": "60Gi", "cpu": "3"},
                            limits={"memory": "60Gi"},
                        ),
                    )
                ]
            )
        )
    }


def _dq_dag_id(config: dict) -> str:
    table = config["table"]
    return (
        f"dbt.source.trino.ml_feature_platform_{table['schema']}."
        f"{table['name']}.dq"
    )


def _silver_dq_logical_date(logical_date, **_):
    logical_date = pendulum.instance(logical_date).in_timezone("UTC")
    return logical_date.start_of("day").add(hours=1)


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
        "sku",
        "dynamic-pricing",
        "prices",
    ],
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def dynamic_pricing_price_features_dag() -> None:
    wait_for_dynamic_pricing_solution = ExternalTaskSensor(
        task_id="wait_for_dynamic_pricing_solution",
        external_dag_id=CONFIG["dag"]["upstream_dag_id"],
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
        check_existence=True,
    )

    @task(executor_config=_executor_config())
    def materialize(calculated_at_value: str) -> None:
        runtime = _load_job_module("runtime.py", "dynamic_pricing_price_runtime")
        query = _load_job_module("query.py", "dynamic_pricing_price_query")

        output_config = runtime.load_config(CONFIG_PATH)
        silver_config = runtime.load_config(SILVER_CONFIG_PATH)
        output_ref = runtime.table_ref(output_config)
        silver_ref = runtime.table_ref(silver_config)

        catalog = runtime.get_iceberg_catalog(output_ref)
        output_table = runtime.preflight_table(catalog, output_ref)

        calculated_at = runtime.parse_snapshot_timestamp(calculated_at_value)
        history_days = int(output_config["source"]["history_days"])
        silver_table = runtime.trino_table_name(silver_ref)
        promotion_frame = runtime.query_trino(
            output_config["source"]["trino_conn_id"],
            query.build_promotion_ids_query(
                silver_table=silver_table,
                history_days=history_days,
            ),
        )
        promotion_ids = promotion_frame["promotion_id"].tolist()
        promotion_ids.append(query.DEFAULT_PROMOTION_ID)

        for promotion_id in promotion_ids:
            frame = runtime.query_trino(
                output_config["source"]["trino_conn_id"],
                query.build_gold_query(
                    calculated_at=calculated_at,
                    promotion_id=promotion_id,
                    silver_table=silver_table,
                    history_days=history_days,
                ),
            )
            runtime.write_timestamp_promotion_snapshot(
                output_table,
                frame,
                calculated_at,
                promotion_id,
            )

    gold_task = materialize(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )

    [wait_for_dynamic_pricing_solution] >> gold_task


dag = dynamic_pricing_price_features_dag()
