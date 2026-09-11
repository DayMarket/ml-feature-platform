"""Материализовать полный sku-каталог; ручной capture использует того же owner."""

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
from layers.silver.sku_id.demand_catalog_sku.v1.job.budget import (  # noqa: E402
    configured_limits,
    run_guard,
)

CONFIG = yaml.safe_load(Path(CONFIG_PATH).read_text(encoding="utf-8"))
MAX_RUN_SECONDS = configured_limits(CONFIG)["manual"]
CAPTURE_TIMESTAMP = '{{ ti.xcom_pull(task_ids="write_catalog")["ingested_at"] }}'
SOURCE_CONFIG_PATH = str(Path(REPO_ROOT) / CONFIG["inputs"]["seller_config"])


def load_config(path):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


SOURCE = load_config(SOURCE_CONFIG_PATH)


def owner_guard(context):
    return run_guard(CONFIG, context)


def source_logical_date(_logical_date, **context):
    reference = context["ti"].xcom_pull(task_ids="prepare_request", include_prior_dates=False)
    return pendulum.parse(reference["reference"]["logical_date"])


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
    tags=["feature-platform", CONFIG["dag"]["group_tag"], CONFIG["dag"]["team"], "silver"],
)
def catalog_dag():
    @task(task_id="prepare_request", multiple_outputs=False)
    def prepare_request():
        from layers.silver.sku_id.demand_catalog_sku.v1.job.requests import capture_request
        context = get_current_context()
        with owner_guard(context):
            return capture_request(
                CONFIG, SOURCE, context["dag_run"].conf, run_id=context["run_id"],
                run_type=context["dag_run"].run_type, logical_date=context.get("logical_date"),
                interval_start=context.get("data_interval_start"), interval_end=context.get("data_interval_end"),
            )

    @task(task_id="write_catalog", multiple_outputs=False)
    def write_catalog(request):
        from layers.silver.sku_id.demand_catalog_sku.v1.job.orchestration import execute_capture
        context = get_current_context()
        with owner_guard(context):
            return execute_capture(CONFIG, REPO_ROOT,
                source_manifest_id=request["source_manifest_id"],
                reference={key: request["reference"][key] for key in ("dag_id", "run_id")},
                task_instance=context["ti"],
            )

    request = prepare_request()
    loaded = write_catalog(request)
    ready = ExternalTaskSensor(
        task_id="wait_for_source_dq", external_dag_id=SOURCE["dag"]["id"], external_task_id="dq",
        execution_date_fn=source_logical_date, allowed_states=["success"],
        failed_states=["failed", "upstream_failed", "skipped"], check_existence=True,
        mode="reschedule", deferrable=False, poke_interval=30, timeout=MAX_RUN_SECONDS,
    )
    request >> ready >> loaded
    dq_task = build_dq_task(
        CONFIG_PATH,
        REPO_ROOT,
        receipt_task_id="write_catalog",
        task_guard=owner_guard,
    )(CAPTURE_TIMESTAMP)
    stats_task = build_feature_stats_task(CONFIG_PATH, REPO_ROOT, task_guard=owner_guard)(CAPTURE_TIMESTAMP)
    loaded >> [dq_task, stats_task]


dag = catalog_dag()
