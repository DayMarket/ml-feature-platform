"""Загрузить полный справочник событий после DQ точного календарного запуска."""

from datetime import timedelta
from pathlib import Path
import sys

import pendulum
import yaml
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow_commons.helpers.oncall import send_oncall_notification
from kubernetes.client import models as k8s

ENTITY_DIR = Path(__file__).resolve().parent
CONFIG_PATH = str(ENTITY_DIR / "config.yaml")
REPO_ROOT = str(ENTITY_DIR.parents[4])
sys.path.insert(0, REPO_ROOT)

def load_config(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


CONFIG = load_config(CONFIG_PATH)
CALENDAR_CONFIG_PATH = str(Path(REPO_ROOT) / CONFIG["inputs"]["calendar_config"])
CALENDAR_CONFIG = load_config(CALENDAR_CONFIG_PATH)
CAPTURE_TIMESTAMP = '{{ ti.xcom_pull(task_ids="write_events")["ingested_at"] }}'


def default_args():
    runtime = CONFIG["runtime"]
    resources = {"cpu": str(runtime["cpu"]), "memory": str(runtime["memory"])}
    return {
        "owner": CONFIG["dag"]["owner"], "retries": 1,
        "retry_delay": timedelta(minutes=5), "execution_timeout": timedelta(minutes=20),
        "executor_config": {"pod_override": k8s.V1Pod(spec=k8s.V1PodSpec(containers=[
            k8s.V1Container(name="base", image=runtime["image"],
                            resources=k8s.V1ResourceRequirements(requests=resources, limits=resources))
        ]))},
        "on_failure_callback": send_oncall_notification(
            team=CONFIG["alerts"]["team"], oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
            severity=CONFIG["alerts"]["severity"]),
    }


def calendar_logical_date(_logical_date, **context):
    request = context["ti"].xcom_pull(task_ids="prepare_reference")
    return pendulum.parse(request["reference"]["logical_date"])


@dag(
    dag_id=CONFIG["dag"]["id"],
    schedule=CronDataIntervalTimetable(CONFIG["dag"]["schedule"], timezone="UTC"),
    start_date=pendulum.parse(CONFIG["dag"]["start_date"]).in_timezone("UTC"),
    catchup=CONFIG["dag"]["catchup"], max_active_runs=1, is_paused_upon_creation=True,
    default_args=default_args(),
    tags=["feature-platform", CONFIG["dag"]["group_tag"], CONFIG["dag"]["team"], "silver"],
)
def events_dag():
    from dq.task import build_dq_task
    from feature_stats.task import build_feature_stats_task

    @task(multiple_outputs=False)
    def prepare_reference():
        from layers.silver.event_code.demand_event_calendar.v1.job.runtime import (
            scheduled_calendar_reference, validate_reference,
        )
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        if set(conf) - {"mode", "calendar_reference", "openlineage"}:
            raise ValueError("События загружаются целиком; неизвестные параметры")
        mode = conf.get("mode", "regular")
        if mode == "regular":
            if str(context["dag_run"].run_type) != "scheduled" or "calendar_reference" in conf:
                raise ValueError("Regular разрешён только плановому запуску")
            reference = scheduled_calendar_reference(CONFIG, CALENDAR_CONFIG,
                                                       context["data_interval_start"], context["data_interval_end"])
        elif mode == "manual":
            if str(context["dag_run"].run_type) != "manual":
                raise ValueError("Mode manual разрешён только ручному запуску")
            reference = validate_reference(conf.get("calendar_reference"), CALENDAR_CONFIG)
        else:
            raise ValueError("Неизвестный режим событий")
        return {"mode": mode, "reference": reference}

    @task(task_id="write_events", multiple_outputs=False)
    def write_events(request):
        from layers.silver.event_code.demand_event_calendar.v1.job.runtime import execute_load
        context = get_current_context()
        reference = request["reference"]
        checked = context["ti"].xcom_pull(
            dag_id=reference["dag_id"], task_ids="dq", run_id=reference["run_id"], include_prior_dates=False)
        return execute_load(CONFIG, REPO_ROOT, context["run_id"], request["mode"], reference, checked)

    request = prepare_reference()
    ready = ExternalTaskSensor(
        task_id="wait_for_calendar_dq", external_dag_id=CALENDAR_CONFIG["dag"]["id"],
        external_task_id="dq", execution_date_fn=calendar_logical_date,
        allowed_states=["success"], failed_states=["failed", "upstream_failed", "skipped"],
        check_existence=True, mode="reschedule", poke_interval=30, timeout=3600,
        execution_timeout=timedelta(minutes=65),
    )
    loaded = write_events(request)
    request >> ready >> loaded
    dq_task = build_dq_task(
        CONFIG_PATH, REPO_ROOT, receipt_task_id="write_events"
    )(CAPTURE_TIMESTAMP)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(CAPTURE_TIMESTAMP)
    loaded >> [dq_task, stats_task]


dag = events_dag()
