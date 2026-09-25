"""SKU-продажи: свёртка seller-silver в Trino → Iceberg, окно пересчёта или ручной диапазон."""

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
SELLER_CONFIG_PATH = str(Path(REPO_ROOT) / CONFIG["inputs"]["seller_config"])
SELLER_CONFIG = load_config(SELLER_CONFIG_PATH)
# Seller-sales запускается в то же время (04:00 UTC): logical date совпадает.
SELLER_DQ_DELTA = timedelta(minutes=0)
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
    tags=["feature-platform", CONFIG["dag"]["group_tag"], CONFIG["dag"]["team"], "silver"],
)
def sku_sales_dag():
    @task.branch(task_id="upstream_gate")
    def upstream_gate() -> list[str]:
        """Scheduled ждёт DQ seller-sales; ручной запуск читает уже записанные партиции."""
        run_type = get_current_context()["dag_run"].run_type
        if str(getattr(run_type, "value", run_type)) == "scheduled":
            return ["wait_for_seller_dq", "write"]
        return ["write"]

    @task(task_id="write", trigger_rule="none_failed",
          execution_timeout=timedelta(hours=RUNTIME["write_timeout_hours"]))
    def write(start: str, end: str) -> list[str]:
        from layers.silver.sku_id.demand_sales_daily.v1.job.runtime import load_range

        return load_range(CONFIG, SELLER_CONFIG, REPO_ROOT, start, end,
                          run_id=get_current_context()["run_id"])

    gate = upstream_gate()
    seller_ready = ExternalTaskSensor(
        task_id="wait_for_seller_dq",
        external_dag_id=SELLER_CONFIG["dag"]["id"],
        external_task_id="dq",
        execution_delta=SELLER_DQ_DELTA,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"],
        check_existence=True,
        mode="reschedule",
        poke_interval=60,
        timeout=6 * 60 * 60,
    )
    loaded = write(START, END)
    gate >> [seller_ready, loaded]
    seller_ready >> loaded
    dq_task = build_dq_task(CONFIG_PATH, REPO_ROOT)(PARTITION_DATES)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT)(PARTITION_DATES)
    loaded >> [dq_task, stats_task]


dag = sku_sales_dag()
