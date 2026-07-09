"""Collect Elasticsearch explain payloads to S3."""

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

try:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")


def _read_config(path: str) -> dict:
    with open(path, encoding="utf-8") as config_stream:
        return yaml.safe_load(config_stream)


CONFIG = _read_config(CONFIG_PATH)


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
    cpu = str(runtime.get("cpu", "10"))
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
        "owner": CONFIG["elasticsearch_collect_dag"]["owner"],
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
    dag_id=CONFIG["elasticsearch_collect_dag"]["id"],
    max_active_runs=1,
    tags=[
        "feature-platform",
        CONFIG["dag"]["group_tag"],
        CONFIG["elasticsearch_collect_dag"]["team"],
        "silver",
        "query",
        "sku-group",
        "elasticsearch",
        "raw-collect",
    ],
    dagrun_timeout=timedelta(
        hours=int(CONFIG["elasticsearch_collect_dag"].get("dagrun_timeout_hours", 72))
    ),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["elasticsearch_collect_dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["elasticsearch_collect_dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def search_query_sku_group_es_elasticsearch_collect_dag() -> None:
    @task(executor_config=_executor_config())
    def collect_raw(partition_value: str, run_id_value: str) -> str:
        runtime = _load_job_module("runtime.py", "search_es_features_raw_runtime")
        trino_query = _load_job_module("query.py", "search_es_features_raw_query")
        search = _load_job_module("search.py", "search_es_features_raw_search")

        config = runtime.load_config(CONFIG_PATH)
        partition_date = runtime.previous_utc_date(partition_value)
        sql = trino_query.build_query(
            partition_date=partition_date,
            clickstream_events_table=config["source"]["clickstream_events_table"],
            search_logs_table=config["source"]["search_logs_table"],
        )
        query_groups = runtime.query_trino(config["source"]["trino_conn_id"], sql)
        elastic = runtime.elasticsearch_config(config["source"]["elasticsearch"])
        storage = runtime.raw_storage_config(config["raw_storage"])
        runtime.write_elasticsearch_raw_to_s3(
            query_groups=query_groups,
            partition_date=partition_date,
            run_id=run_id_value,
            elastic=elastic,
            search_module=search,
            fields=config["source"]["elasticsearch"]["fields"],
            size=int(config["source"]["elasticsearch"]["size"]),
            parallel_jobs=int(config["source"]["elasticsearch"]["parallel_jobs"]),
            chunk_size=int(config["source"]["elasticsearch"]["chunk_size"]),
            raw_file_row_limit=int(config["raw_storage"]["file_row_limit"]),
            timeout_seconds=int(
                config["source"]["elasticsearch"]["request_timeout_seconds"]
            ),
            retry_count=int(config["source"]["elasticsearch"]["retry_count"]),
            storage=storage,
        )
        return partition_date.isoformat()

    raw_partition_date = collect_raw(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}',
        "{{ run_id }}",
    )

    trigger_writer = TriggerDagRunOperator(
        task_id="trigger_parse_write",
        trigger_dag_id=CONFIG["dag"]["id"],
        conf={"partition_date": "{{ ti.xcom_pull(task_ids='collect_raw') }}"},
        wait_for_completion=False,
    )

    raw_partition_date >> trigger_writer


dag = search_query_sku_group_es_elasticsearch_collect_dag()
