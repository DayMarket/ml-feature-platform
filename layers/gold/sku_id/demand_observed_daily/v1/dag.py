"""Соединить проверенные дневные sales/stock snapshots одним owner DAG."""

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

from dq.task import build_dq_task  # noqa: E402
from feature_stats.task import build_feature_stats_task  # noqa: E402
from layers.gold.sku_id.demand_observed_daily.v1.job.budget import (  # noqa: E402
    configured_limits,
    run_guard,
)

CONFIG = yaml.safe_load(Path(CONFIG_PATH).read_text(encoding="utf-8"))
MAX_RUN_SECONDS = configured_limits(CONFIG)["manual"]
PARTITION_DATE = '{{ ti.xcom_pull(task_ids="write_observed")["dates"][-1] }}'
SALES_CONFIG_PATH = str(Path(REPO_ROOT) / CONFIG["inputs"]["sales_config"])
STOCK_CONFIG_PATH = str(Path(REPO_ROOT) / CONFIG["inputs"]["stock_config"])


def load_config(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


SALES_CONFIG = load_config(SALES_CONFIG_PATH)
STOCK_CONFIG = load_config(STOCK_CONFIG_PATH)
SOURCES = {"sales": SALES_CONFIG, "stock": STOCK_CONFIG}


def owner_guard(context):
    return run_guard(CONFIG, context)


def sales_logical_date(_logical_date, **context):
    references = context["ti"].xcom_pull(task_ids="prepare_references", include_prior_dates=False)
    return pendulum.parse(references["sales"]["logical_date"])


def stock_logical_date(_logical_date, **context):
    references = context["ti"].xcom_pull(task_ids="prepare_references", include_prior_dates=False)
    return pendulum.parse(references["stock"]["logical_date"])


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
        "execution_timeout": timedelta(seconds=MAX_RUN_SECONDS),
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
    dagrun_timeout=timedelta(seconds=MAX_RUN_SECONDS),
    is_paused_upon_creation=True,
    default_args=default_args(),
    tags=["feature-platform", CONFIG["dag"]["group_tag"], CONFIG["dag"]["team"], "gold"],
)
def observed_dag():
    @task(task_id="prepare_references", multiple_outputs=False)
    def prepare_references():
        from layers.gold.sku_id.demand_observed_daily.v1.job.requests import resolve_references
        context = get_current_context()
        with owner_guard(context):
            return resolve_references(
                CONFIG, SOURCES, context["dag_run"].conf, run_type=context["dag_run"].run_type,
                run_id=context["run_id"], logical_date=context["logical_date"],
                interval_start=context["data_interval_start"], interval_end=context["data_interval_end"],
            )

    @task(task_id="prepare_request", multiple_outputs=False)
    def prepare_request(references):
        from layers.gold.sku_id.demand_observed_daily.v1.job.requests import (
            prepare_owner_request,
        )
        context = get_current_context()
        with owner_guard(context):
            return prepare_owner_request(
                CONFIG, REPO_ROOT, dict(context["dag_run"].conf or {}, references=references), run_id=context["run_id"],
                run_type=context["dag_run"].run_type,
                interval_start=context["data_interval_start"],
                interval_end=context["data_interval_end"], run_after=context["dag_run"].run_after,
                logical_date=context.get("logical_date"),
            )

    @task(task_id="write_observed", multiple_outputs=False)
    def write_observed(request):
        from layers.gold.sku_id.demand_observed_daily.v1.job.orchestration import (
            execute_request,
        )
        context = get_current_context()
        with owner_guard(context):
            return execute_request(CONFIG, REPO_ROOT, request, task_instance=context["ti"])

    references = prepare_references()
    sales_ready = ExternalTaskSensor(
        task_id="wait_for_sales_dq", external_dag_id=SALES_CONFIG["dag"]["id"], external_task_id="dq",
        execution_date_fn=sales_logical_date, allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"], check_existence=True,
        mode="reschedule", deferrable=False, poke_interval=30, timeout=MAX_RUN_SECONDS,
    )
    stock_ready = ExternalTaskSensor(
        task_id="wait_for_stock_dq", external_dag_id=STOCK_CONFIG["dag"]["id"], external_task_id="dq",
        execution_date_fn=stock_logical_date, allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"], check_existence=True,
        mode="reschedule", deferrable=False, poke_interval=30, timeout=MAX_RUN_SECONDS,
    )
    references >> [sales_ready, stock_ready]
    request = prepare_request(references)
    [sales_ready, stock_ready] >> request
    loaded = write_observed(request)
    dq_task = build_dq_task(
        CONFIG_PATH, REPO_ROOT, range_receipt_task_id="write_observed", range_guard=owner_guard,
    )(PARTITION_DATE)
    stats_task = build_feature_stats_task(
        CONFIG_PATH, REPO_ROOT, range_receipt_task_id="write_observed",
        range_timeout_seconds=MAX_RUN_SECONDS, range_guard=owner_guard,
    )(PARTITION_DATE)
    loaded >> [dq_task, stats_task]


dag = observed_dag()
