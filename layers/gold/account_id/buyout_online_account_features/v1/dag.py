"""Project the daily buyout account feature snapshot into the online serving table."""

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
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task
from feature_stats.task import build_feature_stats_task

CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
DQ_PARTITION_DATE = '{{ data_interval_start.in_timezone("UTC").strftime("%Y-%m-%d") }}'
JOB_DIR = os.path.join(ENTITY_DIR, "job")

SOURCE_CONFIG_PATH = os.path.join(
    REPO_ROOT,
    "layers",
    "gold",
    "account_id",
    "buyout_account_history_features",
    "v1",
    "config.yaml",
)
CITY_CONFIG_PATH = os.path.join(
    REPO_ROOT,
    "layers",
    "silver",
    "order_city_id",
    "order_completion_city_features",
    "v1",
    "config.yaml",
)
REGION_CONFIG_PATH = os.path.join(
    REPO_ROOT,
    "layers",
    "silver",
    "order_region_id",
    "order_completion_region_features",
    "v1",
    "config.yaml",
)

with open(CONFIG_PATH, encoding="utf-8") as config_stream:
    CONFIG = yaml.safe_load(config_stream)

# Проекция читает gold-витрину этого репозитория, а та считает свой DQ таской dq внутри
# собственного DAG'а, — ждём её, а не отдельный dbt-DQ-DAG. Отдельный dbt-DQ идёт в 01:00,
# то есть всегда раньше производителя (04:00), и партицию D он проверяет только в D+2 01:00 —
# это +19 ч к запуску проекции, за пределами её таймаутов.
SOURCE_DAG_ID = (
    "feature-platform.layers.gold.account_id.buyout_account_history_features"
)
SOURCE_DQ_TASK_ID = "dq"
# Разница расписаний: D 06:00 - 2ч = D 04:00 — логическая дата запуска витрины-источника,
# который пишет партицию D (её же читает materialize).
SOURCE_DQ_EXECUTION_DELTA = timedelta(hours=2)

# Гео-доли выкупа приходят из двух silver-витрин: они тоже считают свой DQ таской dq внутри
# собственного DAG'а. Обе идут в 03:00 UTC и помечают партицию датой analyze_date, как и
# витрина-источник, поэтому проекция читает партицию D из всех трёх.
CITY_DAG_ID = "feature-platform.layers.silver.order_city_id.order_completion_city_features"
REGION_DAG_ID = (
    "feature-platform.layers.silver.order_region_id.order_completion_region_features"
)
GEO_DQ_TASK_ID = "dq"
# D 06:00 - 3ч = D 03:00 — логическая дата запуска silver, который пишет партицию D.
GEO_DQ_EXECUTION_DELTA = timedelta(hours=3)


def _load_module(filename: str, module_name: str):
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
                            requests={"memory": "8Gi", "cpu": "2"},
                            limits={"memory": "8Gi"},
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
        "account",
        "buyout",
        "serving",
    ],
    dagrun_timeout=timedelta(hours=4),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=False,
)
def buyout_online_account_features_dag() -> None:
    wait_for_history_features = ExternalTaskSensor(
        task_id="wait_for_gold_buyout_account_history_features",
        external_dag_id=SOURCE_DAG_ID,
        external_task_id=SOURCE_DQ_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=60,
        timeout=4 * 60 * 60,
        check_existence=True,
        execution_delta=SOURCE_DQ_EXECUTION_DELTA,
    )

    wait_for_city_features = ExternalTaskSensor(
        task_id="wait_for_silver_order_completion_city_features",
        external_dag_id=CITY_DAG_ID,
        external_task_id=GEO_DQ_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=60,
        timeout=4 * 60 * 60,
        check_existence=True,
        execution_delta=GEO_DQ_EXECUTION_DELTA,
    )

    wait_for_region_features = ExternalTaskSensor(
        task_id="wait_for_silver_order_completion_region_features",
        external_dag_id=REGION_DAG_ID,
        external_task_id=GEO_DQ_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=60,
        timeout=4 * 60 * 60,
        check_existence=True,
        execution_delta=GEO_DQ_EXECUTION_DELTA,
    )

    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module("runtime.py", "buyout_online_account_features_runtime")
        query = _load_module("query.py", "buyout_online_account_features_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        source_ref = runtime.table_ref(runtime.load_config(SOURCE_CONFIG_PATH))
        city_ref = runtime.table_ref(runtime.load_config(CITY_CONFIG_PATH))
        region_ref = runtime.table_ref(runtime.load_config(REGION_CONFIG_PATH))
        for upstream_ref in (source_ref, city_ref, region_ref):
            if upstream_ref.catalog != ref.catalog:
                raise ValueError(
                    "Source and output configs must use one Iceberg catalog; "
                    f"output={ref.catalog!r}, source={upstream_ref.catalog!r}"
                )

        catalog = runtime.get_iceberg_catalog(ref)
        # Resolve every migrated table before reading the source partition.
        table = runtime.preflight_table(catalog, ref)
        for upstream_ref in (source_ref, city_ref, region_ref):
            runtime.preflight_table(catalog, upstream_ref)
        # Партиция витрины-источника за сутки D пишется DAG-ом в D+1 04:00 UTC.
        partition_date = runtime.previous_utc_date(interval_end_value)
        source_table = runtime.trino_table_name(source_ref)
        city_table = runtime.trino_table_name(city_ref)
        region_table = runtime.trino_table_name(region_ref)
        shards = runtime.shard_count(config)

        for shard in range(shards):
            sql = query.build_query(
                partition_date,
                source_table,
                city_table,
                region_table,
                shards,
                shard,
            )
            frame = runtime.query_trino(config["source"]["trino_conn_id"], sql)
            if shard == 0:
                runtime.require_non_empty(frame, partition_date)
            runtime.write_partition_shard(
                table,
                frame,
                partition_date,
                replace=shard == 0,
            )

    gold_task = materialize(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )
    [
        wait_for_history_features,
        wait_for_city_features,
        wait_for_region_features,
    ] >> gold_task

    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(DQ_PARTITION_DATE)

    # Статистика идёт параллельно DQ и ни на что не влияет: downstream ждёт
    # таску dq, поэтому падение профилей не блокирует потребителей.
    gold_task >> [dq_task, stats_task]


dag = buyout_online_account_features_dag()
