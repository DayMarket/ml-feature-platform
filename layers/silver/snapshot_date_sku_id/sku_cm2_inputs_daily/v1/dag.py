"""Write the daily SKU CM2 input snapshot to Iceberg."""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
import yaml
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")

with open(CONFIG_PATH, encoding="utf-8") as config_stream:
    CONFIG = yaml.safe_load(config_stream)


def _load_module(filename: str, module_name: str):
    path = os.path.join(JOB_DIR, filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _executor_config() -> dict:
    resources = CONFIG["runtime"]["resources"]
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=CONFIG["runtime"]["image"],
                        resources=k8s.V1ResourceRequirements(
                            requests={
                                "memory": resources["memory"],
                                "cpu": resources["cpu"],
                            },
                            limits={"memory": resources["memory"]},
                        ),
                    )
                ]
            )
        )
    }


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
        "silver",
        "sku",
        "cm2",
    ],
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=CONFIG["dag"]["catchup"],
)
def sku_cm2_inputs_daily_dag() -> None:
    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module("runtime.py", "sku_cm2_inputs_daily_runtime")
        query = _load_module("query.py", "sku_cm2_inputs_daily_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        catalog = runtime.get_iceberg_catalog(ref)
        table = runtime.preflight_table(catalog, ref)

        calculated_at = runtime.parse_airflow_timestamp(interval_end_value)
        snapshot_date = runtime.previous_utc_date(calculated_at)
        source = config["source"]
        query_args = {
            "snapshot_date": snapshot_date,
            "calculated_at": calculated_at,
            "sku_table": source["sku_table"],
            "prices_table": source["prices_table"],
            "commission_table": source["commission_table"],
            "commission_column": source["commission_column"],
            "orders_table": source["orders_table"],
            "orders_lookback_days": source["orders_lookback_days"],
            "default_dimensional_group": source["default_dimensional_group"],
        }

        frame = runtime.query_trino(
            source["trino_conn_id"],
            query.build_query(**query_args),
        )
        runtime.write_snapshot(
            table=table,
            frame=frame,
            snapshot_date=snapshot_date,
            allowed_dimensional_groups=source["allowed_dimensional_groups"],
            min_commission_pct=source["min_commission_pct"],
            max_commission_pct=source["max_commission_pct"],
        )

    interval_end_value = (
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )
    materialize(interval_end_value)


dag = sku_cm2_inputs_daily_dag()
