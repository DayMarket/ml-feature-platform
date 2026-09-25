"""Текущий seller-master каталог: полная замена из marts.sellers_info."""

from datetime import timedelta
from pathlib import Path
import sys

import pendulum
import yaml
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

ENTITY_DIR = Path(__file__).resolve().parent
CONFIG_PATH = str(ENTITY_DIR / "config.yaml")
REPO_ROOT = str(ENTITY_DIR.parents[4])
sys.path.insert(0, REPO_ROOT)

from dq.task import build_dq_task  # noqa: E402
from feature_stats.task import build_feature_stats_task  # noqa: E402

CONFIG = yaml.safe_load(Path(CONFIG_PATH).read_text(encoding="utf-8"))
RUNTIME = CONFIG["runtime"]
CAPTURE_TIMESTAMP = '{{ ti.xcom_pull(task_ids="write")["ingested_at"] }}'


def executor_config():
    resources = {"cpu": str(RUNTIME["cpu"]), "memory": str(RUNTIME["memory"])}
    return {"pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[
        k8s.V1Container(name="base", image=RUNTIME["image"],
                        resources=k8s.V1ResourceRequirements(requests=resources, limits=resources))
    ]))}


@dag(
    dag_id=CONFIG["dag"]["id"],
    schedule=CronDataIntervalTimetable(CONFIG["dag"]["schedule"], timezone="UTC"),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=CONFIG["dag"]["catchup"],
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": CONFIG["dag"]["owner"],
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
        "executor_config": executor_config(),
        "on_failure_callback": send_oncall_notification(
            team=CONFIG["alerts"]["team"],
            oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
            severity=CONFIG["alerts"]["severity"],
        ),
    },
    tags=["feature-platform", CONFIG["dag"]["group_tag"], CONFIG["dag"]["team"], "silver"],
)
def catalog_seller_dag():
    @task(task_id="write")
    def write() -> dict:
        from layers.silver.seller_id.demand_catalog_seller.v1.job.runtime import load

        return load(CONFIG, run_id=get_current_context()["run_id"])

    loaded = write()
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(CAPTURE_TIMESTAMP)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(CAPTURE_TIMESTAMP)
    loaded >> [dq_task, stats_task]


dag = catalog_seller_dag()
