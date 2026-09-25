"""Gold observed-панель: FULL JOIN sales и stock в Trino → Iceberg, окно пересчёта или ручной диапазон."""

from datetime import timedelta
from pathlib import Path
import sys

import pendulum
import yaml
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Param, dag, get_current_context, task
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
SALES_CONFIG_PATH = str(Path(REPO_ROOT) / CONFIG["inputs"]["sales_config"])
STOCK_CONFIG_PATH = str(Path(REPO_ROOT) / CONFIG["inputs"]["stock_config"])
SALES_CONFIG = load_config(SALES_CONFIG_PATH)
STOCK_CONFIG = load_config(STOCK_CONFIG_PATH)
# Gold в 05:00 UTC, sales и stock в 04:00 UTC.
UPSTREAM_DQ_DELTA = timedelta(hours=1)
RUNTIME = CONFIG["runtime"]
# Без params: последние refresh_days завершённых UTC-дней до run_after.
RUN_DATE = "dag_run.run_after.strftime('%Y-%m-%d')"
START = "{{ params.start or macros.ds_add(%s, -%d) }}" % (RUN_DATE, RUNTIME["refresh_days"])
END = "{{ params.end or macros.ds_add(%s, -1) }}" % RUN_DATE
PARTITION_DATES = '{{ ti.xcom_pull(task_ids="write") | join(",") }}'


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
    params={
        "start": Param(None, type=["null", "string"], format="date",
                       description="Первый день (включительно); пусто — окно пересчёта"),
        "end": Param(None, type=["null", "string"], format="date",
                     description="Последний день (включительно); пусто — вчера UTC"),
    },
    default_args={
        "owner": CONFIG["dag"]["owner"],
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "executor_config": executor_config(),
        "on_failure_callback": send_oncall_notification(
            team=CONFIG["alerts"]["team"],
            oncall_webhook_conn_id=CONFIG["alerts"]["oncall_webhook_conn_id"],
            severity=CONFIG["alerts"]["severity"],
        ),
    },
    tags=["feature-platform", CONFIG["dag"]["group_tag"], CONFIG["dag"]["team"], "gold"],
)
def observed_dag():
    @task.branch(task_id="upstream_gate")
    def upstream_gate() -> list[str]:
        """Scheduled ждёт DQ sales и stock; ручной запуск читает уже записанные партиции."""
        run_type = get_current_context()["dag_run"].run_type
        if str(getattr(run_type, "value", run_type)) == "scheduled":
            return ["wait_for_sales_dq", "wait_for_stock_dq", "write"]
        return ["write"]

    @task(task_id="write", trigger_rule="none_failed",
          execution_timeout=timedelta(hours=RUNTIME["write_timeout_hours"]))
    def write(start: str, end: str) -> list[str]:
        from layers.gold.sku_id.demand_observed_daily.v1.job.runtime import load_range

        return load_range(CONFIG, {"sales": SALES_CONFIG, "stock": STOCK_CONFIG}, REPO_ROOT,
                          start, end, run_id=get_current_context()["run_id"])

    gate = upstream_gate()
    sales_ready = ExternalTaskSensor(
        task_id="wait_for_sales_dq",
        external_dag_id=SALES_CONFIG["dag"]["id"],
        external_task_id="dq",
        execution_delta=UPSTREAM_DQ_DELTA,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"],
        check_existence=True,
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
    )
    stock_ready = ExternalTaskSensor(
        task_id="wait_for_stock_dq",
        external_dag_id=STOCK_CONFIG["dag"]["id"],
        external_task_id="dq",
        execution_delta=UPSTREAM_DQ_DELTA,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"],
        check_existence=True,
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
    )
    loaded = write(START, END)
    gate >> [sales_ready, stock_ready, loaded]
    [sales_ready, stock_ready] >> loaded
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(PARTITION_DATES)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(PARTITION_DATES)
    loaded >> [dq_task, stats_task]


dag = observed_dag()
