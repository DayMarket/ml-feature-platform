"""Append canonical query_id rows for search queries of the trailing log window."""

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
REPO_ROOT = os.path.abspath(os.path.join(ENTITY_DIR, "..", "..", "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")

from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

# Справочник query_text/version не партиционирован по дате (dq.scope: table в
# config.yaml). Значение всё равно нужно framework'у как "дата этого запуска" —
# берём ту же data_interval_start, что уже идёт в materialize() как partition_date_value.
DQ_PARTITION_DATE = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'


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
    # Sensor'а нет: источник — внешняя DE-таблица "dwh-iceberg".silver.search_logs, у
    # которой в репозитории нет DAG'а-владельца (тот же контракт, что у
    # layers/silver/query_sku_group_id/search_query_sku_group_es_features/v1). Окно
    # скользящее, поэтому недогруженный последний день не теряет запросы: они
    # проходят порог на одном из следующих 29 запусков.
    @task(executor_config=_executor_config())
    def materialize(partition_date_value: str, updated_at_value: str) -> None:
        runtime = _load_job_module("runtime.py", "search_query_id_runtime")
        query = _load_job_module("query.py", "search_query_id_query")

        output_config = runtime.load_config(CONFIG_PATH)
        output_ref = runtime.table_ref(output_config)

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
                search_logs_table=str(source_config["search_logs_table"]),
                query_id_table=runtime.trino_table_name(output_ref),
                version=version,
                lookback_days=int(source_config["lookback_days"]),
                short_query_max_length=int(source_config["short_query_max_length"]),
                short_query_min_installs=int(source_config["short_query_min_installs"]),
                long_query_min_installs=int(source_config["long_query_min_installs"]),
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

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: downstream ждёт
    # таску dq, поэтому падение профилей не блокирует потребителей.
    gold_task >> [dq_task, stats_task]


dag = search_query_id_dag()
