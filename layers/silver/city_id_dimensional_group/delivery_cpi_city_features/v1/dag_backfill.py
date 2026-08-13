"""One-off backfill DAG: fills historical partitions of the city delivery CPI table.

Отличия от регулярного `feature-platform.layers.silver.city_id_dimensional_group.
delivery_cpi_city_features`:

- `catchup=True` и явный `end_date`: каждый ран считает одну партицию, поэтому
  прогон перезапускаем и идемпотентен (перезапись партиции по `date`);
- три одновременных рана: источник — тяжёлое 90-дневное окно в Trino;
- без on-call callback: падение исторического прогона не должно будить дежурного.

Диапазон задан под первую загрузку модели невыкупов: раны за интервалы
2026-02-20 .. 2026-08-20 закрывают партиции 2026-02-21 .. 2026-08-19. Перед
снятием с паузы диапазон нужно сверить с фактически недостающими датами; после
заполнения — поставить на паузу и удалить DAG.
"""

import importlib.util
import os
import sys
from datetime import datetime, timedelta, timezone

import yaml
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from kubernetes.client import models as k8s

ENTITY_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ENTITY_DIR, "config.yaml")
JOB_DIR = os.path.join(ENTITY_DIR, "job")

BACKFILL_START = datetime(2026, 2, 20, tzinfo=timezone.utc)
BACKFILL_END = datetime(2026, 8, 20, tzinfo=timezone.utc)

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


@dag(
    default_args={
        "owner": CONFIG["dag"]["owner"],
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    dag_id=f"{CONFIG['dag']['id']}.backfill",
    max_active_runs=3,
    tags=[
        "feature-platform",
        CONFIG["dag"]["group_tag"],
        CONFIG["dag"]["team"],
        "silver",
        "buyout",
        "delivery",
        "cpi",
        "backfill",
    ],
    dagrun_timeout=timedelta(hours=6),
    is_paused_upon_creation=True,
    schedule=CronDataIntervalTimetable(
        cron=CONFIG["dag"]["schedule"],
        timezone="UTC",
    ),
    start_date=BACKFILL_START,
    end_date=BACKFILL_END,
    catchup=True,
)
def delivery_cpi_city_features_backfill_dag() -> None:
    @task(executor_config=_executor_config())
    def materialize(interval_end_value: str) -> None:
        runtime = _load_module(
            "runtime.py", "delivery_cpi_city_features_backfill_runtime"
        )
        query = _load_module("query.py", "delivery_cpi_city_features_backfill_query")
        config = runtime.load_config(CONFIG_PATH)
        ref = runtime.table_ref(config)
        catalog = runtime.get_iceberg_catalog(ref)

        table = runtime.preflight_table(catalog, ref)
        partition_date = runtime.parse_partition_date(interval_end_value)
        sql = query.build_query(partition_date, runtime.lookback_days(config))
        frame = runtime.query_trino(config["source"]["trino_conn_id"], sql)
        runtime.require_non_empty(frame, partition_date)
        runtime.write_daily_snapshot(table, frame, partition_date)

    materialize(
        '{{ data_interval_end.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S") }}'
    )


dag = delivery_cpi_city_features_backfill_dag()
