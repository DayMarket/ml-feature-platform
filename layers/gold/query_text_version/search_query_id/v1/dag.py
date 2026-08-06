"""Append canonical query_id rows for search queries first seen on the interval day."""

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
    "sku_group_id_query_category",
    "sku_group_install",
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
    runtime = CONFIG["runtime"]
    memory = str(runtime.get("memory", "16Gi"))
    cpu = str(runtime.get("cpu", "8"))
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=runtime["image"],
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": memory, "cpu": cpu},
                            limits={"memory": memory, "cpu": cpu},
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
        "query",
        "elasticsearch",
    ],
    dagrun_timeout=timedelta(hours=int(CONFIG["dag"].get("dagrun_timeout_hours", 6))),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def search_query_id_dag() -> None:
    wait_for_silver_install_query = ExternalTaskSensor(
        task_id="wait_for_silver_sku_group_install_query_dq",
        external_dag_id=_dq_dag_id(SILVER_CONFIG),
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
        check_existence=True,
        execution_delta=timedelta(
            hours=int(CONFIG["dag"]["source_dq_execution_delta_hours"])
        ),
    )

    @task(executor_config=_executor_config())
    def materialize(partition_date_value: str, updated_at_value: str) -> None:
        runtime = _load_job_module("runtime.py", "search_query_id_runtime")
        query = _load_job_module("query.py", "search_query_id_query")

        output_config = runtime.load_config(CONFIG_PATH)
        silver_config = runtime.load_config(SILVER_CONFIG_PATH)
        output_ref = runtime.table_ref(output_config)
        silver_ref = runtime.table_ref(silver_config)

        source_config = output_config["source"]
        elastic = runtime.elasticsearch_config(source_config["elasticsearch"])

        catalog = runtime.get_iceberg_catalog(output_ref)
        output_table = runtime.preflight_table(catalog, output_ref)

        partition_date = runtime.parse_partition_date(partition_date_value)
        updated_at = runtime.parse_snapshot_timestamp(updated_at_value)
        version = str(output_config["output"]["version"])

        new_queries_frame = runtime.query_trino(
            source_config["trino_conn_id"],
            query.build_new_queries_query(
                partition_date=partition_date,
                install_query_table=runtime.trino_table_name(silver_ref),
                query_id_table=runtime.trino_table_name(output_ref),
                space=str(source_config["space"]),
                version=version,
            ),
        )

        rows = runtime.build_query_id_rows(
            queries=runtime.extract_queries(new_queries_frame),
            stop_words_pattern=runtime.load_stop_words_pattern(
                ENTITY_DIR,
                str(source_config["stop_words_path"]),
            ),
            elastic=elastic,
            parallel_jobs=int(source_config["elasticsearch"]["parallel_jobs"]),
            timeout_seconds=int(
                source_config["elasticsearch"]["request_timeout_seconds"]
            ),
            retry_count=int(source_config["elasticsearch"]["retry_count"]),
            version=version,
            updated_at=updated_at,
        )

        runtime.append_query_id_rows(
            table=output_table,
            rows=rows,
            version=version,
            write_chunk_size=int(output_config["output"]["write_chunk_size"]),
        )

    gold_task = materialize(
        '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}',
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}',
    )

    wait_for_silver_install_query >> gold_task


dag = search_query_id_dag()
