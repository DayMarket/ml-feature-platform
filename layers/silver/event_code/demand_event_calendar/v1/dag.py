"""События demand forecast: праздники календаря и акции реестра, полная замена."""

from datetime import timedelta
from pathlib import Path
import sys

import pendulum
import yaml
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
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


def load_config(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


CONFIG = load_config(CONFIG_PATH)
CALENDAR_CONFIG_PATH = str(Path(REPO_ROOT) / CONFIG["inputs"]["calendar_config"])
CALENDAR_CONFIG = load_config(CALENDAR_CONFIG_PATH)
# События в 03:10 UTC, календарь в 03:00 UTC.
CALENDAR_DQ_DELTA = timedelta(minutes=10)
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
        "execution_timeout": timedelta(minutes=30),
        "executor_config": executor_config(),
        "on_failure_callback": send_oncall_notification(
            team=CONFIG["alerts"]["team"],
            oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
            severity=CONFIG["alerts"]["severity"],
        ),
    },
    tags=["feature-platform", CONFIG["dag"]["group_tag"], CONFIG["dag"]["team"], "silver"],
)
def events_dag():
    @task.branch(task_id="upstream_gate")
    def upstream_gate() -> list[str]:
        """Scheduled ждёт DQ календаря; ручной запуск читает текущий календарь."""
        run_type = get_current_context()["dag_run"].run_type
        if str(getattr(run_type, "value", run_type)) == "scheduled":
            return ["wait_for_calendar_dq", "write"]
        return ["write"]

    @task(task_id="write", trigger_rule="none_failed")
    def write() -> dict:
        from layers.silver.event_code.demand_event_calendar.v1.job.runtime import load

        return load(CONFIG, CALENDAR_CONFIG, run_id=get_current_context()["run_id"])

    gate = upstream_gate()
    calendar_ready = ExternalTaskSensor(
        task_id="wait_for_calendar_dq",
        external_dag_id=CALENDAR_CONFIG["dag"]["id"],
        external_task_id="dq",
        execution_delta=CALENDAR_DQ_DELTA,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"],
        check_existence=True,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )
    loaded = write()
    gate >> [calendar_ready, loaded]
    calendar_ready >> loaded
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(CAPTURE_TIMESTAMP)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(CAPTURE_TIMESTAMP)
    loaded >> [dq_task, stats_task]


dag = events_dag()
