"""Загрузить полный календарь штатным или ручным запуском одного DAG."""

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
CAPTURE_TIMESTAMP = '{{ ti.xcom_pull(task_ids="write_calendar")["ingested_at"] }}'


def executor_config():
    runtime = CONFIG["runtime"]
    resources = {"cpu": str(runtime["cpu"]), "memory": str(runtime["memory"])}
    return {"pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[
        k8s.V1Container(name="base", image=runtime["image"],
                        resources=k8s.V1ResourceRequirements(requests=resources, limits=resources))
    ]))}


def default_args():
    return {
        "owner": CONFIG["dag"]["owner"],
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=20),
        "executor_config": executor_config(),
        "on_failure_callback": send_oncall_notification(
            team=CONFIG["alerts"]["team"],
            oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
            severity=CONFIG["alerts"]["severity"],
        ),
    }


@dag(
    dag_id=CONFIG["dag"]["id"],
    schedule=CronDataIntervalTimetable(CONFIG["dag"]["schedule"], timezone="UTC"),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=CONFIG["dag"]["catchup"],
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=default_args(),
    tags=["feature-platform", CONFIG["dag"]["group_tag"], CONFIG["dag"]["team"], "silver"],
)
def calendar_dag():
    @task(task_id="write_calendar")
    def write_calendar():
        from layers.silver.date.demand_calendar.v1.job.runtime import execute_load
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        if set(conf) - {"mode", "openlineage"}:
            raise ValueError("Календарь загружается целиком; неизвестные параметры запуска")
        mode = conf.get("mode", "regular")
        if ((str(context["dag_run"].run_type) == "scheduled" and mode != "regular")
                or (str(context["dag_run"].run_type) == "manual" and mode != "manual")):
            raise ValueError("Scheduled требует regular, ручной запуск — mode=manual")
        return execute_load(CONFIG, REPO_ROOT, context["run_id"], mode)

    loaded = write_calendar()
    dq_task = build_dq_task(
        CONFIG_PATH, REPO_ROOT, receipt_task_id="write_calendar"
    )(CAPTURE_TIMESTAMP)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(CAPTURE_TIMESTAMP)
    loaded >> [dq_task, stats_task]


dag = calendar_dag()
