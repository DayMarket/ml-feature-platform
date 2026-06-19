"""Operations feature DAG: location_forecast h3 features (ClickHouse -> Iceberg).

Single daily DAG (00:00 UTC) that materialises five silver feature tables in
parallel from ClickHouse, then assembles the gold model-feature table from them.

This is a ClickHouse-source Airflow/Python pipeline (not Spark): each task reads
through the `clickhouse_dwh_team_logistics` connection and writes Iceberg with
`pyiceberg` via `layers/_common/clickhouse_iceberg.py`. Runs on
`ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2`.

Upstream freshness: the source tables (silver.client_geo_activity_hex_9,
silver.h3_l9_geointellect, silver.organizations_yandex, gold.geo_client_hist,
marts.order_items, dict.delivery_point) are owned by other teams. No
ExternalTaskSensor is wired because their producing DAG ids are not part of this
repo's contract; the 00:00 UTC schedule relies on upstream freshness, matching
the previous inline behaviour. Add sensors once the producing team's DAG ids are
confirmed.
"""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

IMAGE_NAME = "ghcr.io/daymarket/airflow:3.1.8-python3.11-ml-2"

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(DAG_DIR, "..", "..", "..", ".."))
COMMON_DIR = os.path.join(REPO_ROOT, "layers", "_common")

# Silver entities to materialise, in (entity dir name) form. Each exposes
# job/query.py with TABLE_IDENTIFIER, CLICKHOUSE_CONN_ID and build_query().
SILVER_ENTITIES = [
    "dp_neighbor_order_features",
    "geo_user_activity_features",
    "geo_user_location_features",
    "geo_geointellect_features",
    "geo_yandex_poi_features",
]


def _executor_config(image: str = IMAGE_NAME) -> dict:
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=image,
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": "16Gi", "cpu": "4"},
                            limits={"memory": "16Gi"},
                        ),
                    )
                ]
            )
        )
    }


executor_config = _executor_config()


def _load_module(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _materialize_silver(entity: str, partition_value: str) -> None:
    sys.path.insert(0, COMMON_DIR)
    from clickhouse_iceberg import (
        get_iceberg_catalog,
        parse_partition_date,
        query_clickhouse,
        write_daily_snapshot,
    )

    query_path = os.path.join(
        REPO_ROOT, "layers", "silver", entity, "v1", "job", "query.py"
    )
    query = _load_module(query_path, f"query_{entity}")

    partition_date = parse_partition_date(partition_value)
    sql, params = query.build_query(partition_date.isoformat())
    frame = query_clickhouse(query.CLICKHOUSE_CONN_ID, sql, params)

    catalog = get_iceberg_catalog()
    table_identifier = query.TABLE_IDENTIFIER.split(".", 1)[1]  # drop catalog prefix
    write_daily_snapshot(catalog, table_identifier, frame, partition_date)


def get_dag_default_args() -> dict:
    return {
        "owner": "team:operations",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "max_retry_delay": timedelta(minutes=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": send_oncall_notification(
            team="operations-analytics",
            oncall_webhook_conn_id="oncall_webhook_operations",
            severity="P3",
        ),
    }


@dag(
    default_args=get_dag_default_args(),
    dag_id="location_forecast_features_dag",
    max_active_runs=1,
    tags=["feature-platform", "operations", "lastmile", "geo", "h3", "forecast"],
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(cron="0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 6, 19, tz="UTC"),
    catchup=False,
    max_active_tasks=5,
)
def location_forecast_features_dag() -> None:
    """5 ClickHouse -> Iceberg silver tasks, then the gold assembly."""

    @task(executor_config=executor_config)
    def collect_silver(entity: str, partition_value: str) -> str:
        _materialize_silver(entity, partition_value)
        return entity

    @task(executor_config=executor_config)
    def assemble_gold(partition_value: str, _silver_done: list) -> None:
        sys.path.insert(0, COMMON_DIR)
        from clickhouse_iceberg import get_iceberg_catalog, parse_partition_date

        build_path = os.path.join(
            REPO_ROOT,
            "layers",
            "gold",
            "location_h3_forecast_features",
            "v1",
            "job",
            "build.py",
        )
        gold = _load_module(build_path, "gold_build")
        gold.run(get_iceberg_catalog(), parse_partition_date(partition_value))

    partition_value = '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    silver_done = [
        collect_silver.override(task_id=f"collect_{entity}")(entity, partition_value)
        for entity in SILVER_ENTITIES
    ]
    assemble_gold(partition_value, silver_done)


dag = location_forecast_features_dag()
