"""Materialize search query/SKU group Elasticsearch explain features."""

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
REPO_ROOT = os.path.abspath(
    os.path.join(ENTITY_DIR, "..", "..", "..", "..", "..")
)
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")
DATASET_CONFIG_PATH = os.path.join(
    REPO_ROOT,
    "datasets",
    "search",
    "search_ranking",
    "v1",
    "config.yaml",
)


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)
DATASET_CONFIG = _read_config(DATASET_CONFIG_PATH)


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
    cpu = str(runtime.get("cpu", "12"))
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image_pull_policy="Always",
                        image=CONFIG["runtime"]["image"],
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": memory, "cpu": cpu},
                            limits={"memory": memory, "cpu": cpu},
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
        "query",
        "sku-group",
        "elasticsearch",
    ],
    dagrun_timeout=timedelta(hours=6),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def search_query_sku_group_es_features_dag() -> None:
    wait_for_search_ranking_dataset = ExternalTaskSensor(
        task_id="wait_for_search_ranking_dataset",
        external_dag_id="feature-platform.datasets.search.search_ranking.v1",
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        execution_delta=timedelta(hours=2),
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )

    @task(executor_config=_executor_config())
    def materialize(partition_value: str) -> None:
        runtime = _load_job_module("runtime.py", "search_es_features_runtime")
        trino_query = _load_job_module("query.py", "search_es_features_query")
        search = _load_job_module("search.py", "search_es_features_search")

        config = runtime.load_config(CONFIG_PATH)
        dataset_config = runtime.load_config(DATASET_CONFIG_PATH)

        output_ref = runtime.table_ref(config)
        dataset_ref = runtime.table_ref(dataset_config)
        catalog = runtime.get_iceberg_catalog(output_ref)
        output_table = runtime.preflight_table(catalog, output_ref)

        partition_date = runtime.previous_utc_date(partition_value)
        sql = trino_query.build_query(
            partition_date=partition_date,
            dataset_table=runtime.trino_table_name(dataset_ref),
            search_logs_table=config["source"]["search_logs_table"],
        )
        query_groups = runtime.query_trino(config["source"]["trino_conn_id"], sql)
        elastic = runtime.elasticsearch_config(config["source"]["elasticsearch"])
        runtime.write_elasticsearch_features_by_chunks(
            table=output_table,
            query_groups=query_groups,
            partition_date=partition_date,
            elastic=elastic,
            search_module=search,
            fields=config["source"]["elasticsearch"]["fields"],
            size=int(config["source"]["elasticsearch"]["size"]),
            parallel_jobs=int(config["source"]["elasticsearch"]["parallel_jobs"]),
            chunk_size=int(config["source"]["elasticsearch"]["chunk_size"]),
            timeout_seconds=int(
                config["source"]["elasticsearch"]["request_timeout_seconds"]
            ),
            retry_count=int(config["source"]["elasticsearch"]["retry_count"]),
        )

    es_task = materialize(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )

    wait_for_search_ranking_dataset >> es_task


dag = search_query_sku_group_es_features_dag()
