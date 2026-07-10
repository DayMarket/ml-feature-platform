"""Parse raw Elasticsearch explain payloads into prepared parquet files."""

import importlib.util
import os
import sys
from datetime import timedelta

import pendulum
import yaml
from airflow.sdk import dag, task
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
except ImportError:
    from airflow.sensors.external_task import ExternalTaskSensor

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


def _writer_partition_date(logical_date, context) -> object:
    dag_run = context.get("dag_run")
    partition_date = (dag_run.conf or {}).get("partition_date") if dag_run else None
    if partition_date:
        return pendulum.parse(partition_date).date()

    if logical_date is None:
        logical_date = context.get("logical_date")
    if logical_date is None:
        raise ValueError(
            "Writer DAG requires dag_run.conf['partition_date'] or logical_date"
        )

    return pendulum.instance(logical_date).date() - timedelta(days=1)


def _elasticsearch_collect_logical_date(logical_date, **context):
    partition_date = _writer_partition_date(logical_date, context)

    schedule_parts = str(CONFIG["elasticsearch_collect_dag"]["schedule"]).split()
    if len(schedule_parts) < 2 or not schedule_parts[0].isdigit() or not schedule_parts[1].isdigit():
        raise ValueError(
            "Elasticsearch collect DAG schedule must start with numeric minute and hour fields "
            "to map partition_date to logical_date: "
            f"{CONFIG['elasticsearch_collect_dag']['schedule']!r}"
        )

    return pendulum.datetime(
        partition_date.year,
        partition_date.month,
        partition_date.day,
        int(schedule_parts[1]),
        int(schedule_parts[0]),
        tz="UTC",
    )


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
        "raw-parse",
    ],
    dagrun_timeout=timedelta(hours=int(CONFIG["dag"].get("dagrun_timeout_hours", 24))),
    is_paused_upon_creation=False,
    schedule=None,
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def search_query_sku_group_es_features_dag() -> None:
    wait_for_elasticsearch_collect = ExternalTaskSensor(
        task_id="wait_for_elasticsearch_collect",
        external_dag_id=CONFIG["elasticsearch_collect_dag"]["id"],
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=60,
        timeout=int(CONFIG["elasticsearch_collect_dag"].get("dagrun_timeout_hours", 72)) * 60 * 60,
        check_existence=True,
        execution_date_fn=_elasticsearch_collect_logical_date,
    )

    @task(executor_config=_executor_config())
    def prepare_parquet(partition_date_value: str) -> None:
        runtime = _load_job_module("runtime.py", "search_es_features_runtime")

        config = runtime.load_config(CONFIG_PATH)

        partition_date = runtime.parse_partition_date(partition_date_value)
        storage = runtime.raw_storage_config(config["raw_storage"])
        runtime.write_raw_features_to_prepared_parquet(
            partition_date=partition_date,
            storage=storage,
            fields=config["source"]["elasticsearch"]["fields"],
            write_chunk_size=int(
                config["source"]["elasticsearch"]["write_chunk_size"]
            ),
        )

    wait_for_elasticsearch_collect >> prepare_parquet(
        '{{ (dag_run.conf or {}).get("partition_date") or macros.ds_add(ds, -1) }}'
    )


dag = search_query_sku_group_es_features_dag()
