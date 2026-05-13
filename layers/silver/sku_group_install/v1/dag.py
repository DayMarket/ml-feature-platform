import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.dagrun import DagRun
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

from airflow_commons.helpers.oncall import send_oncall_notification

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

from config.factory import get_deployment

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")

default_args = {
    "owner": "team:search",
    "depends_on_past": False,
    "trigger_rule": "all_success",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": send_oncall_notification(
        severity="P3",
        team="search",
        oncall_webhook_conn_id="oncall_webhook_search",
    ),
}


@provide_session
def _is_external_dag_succeeded(
    external_dag_id: str,
    logical_date: str,
    session=None,
) -> bool:
    target_dt = datetime.fromisoformat(logical_date)
    day_start = target_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    dag_run = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == external_dag_id,
            DagRun.logical_date >= day_start,
            DagRun.logical_date < day_end,
        )
        .order_by(DagRun.logical_date.desc())
        .first()
    )

    if dag_run is None:
        logger.info(
            "No DagRun found yet for external_dag_id=%s on %s",
            external_dag_id,
            day_start.date().isoformat(),
        )
        return False

    logger.info(
        "External DagRun check: dag_id=%s logical_date=%s state=%s",
        external_dag_id,
        dag_run.logical_date,
        dag_run.state,
    )
    return dag_run.state == DagRunState.SUCCESS


@dag(
    default_args=default_args,
    max_active_runs=1,
    tags=["spark", "feature-platform", "team::search", "silver"],
    is_paused_upon_creation=True,
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 1, 1, 0, 0, 0),
    dag_id="feature_platform_sku_group_install_silver_stats_dag",
)
def collect_silver_sku_group_query_install_stats():
    wait_for_sessions_dq = PythonSensor(
        task_id="wait_for_sessions_dq",
        python_callable=_is_external_dag_succeeded,
        op_kwargs={
            "external_dag_id": "dbt.tests.dbt_clickhouse_dwh.sessions.dq",
            "logical_date": "{{ logical_date.isoformat() }}",
        },
        mode="reschedule",
        poke_interval=30,
        timeout=6 * 60 * 60,
    )

    wait_for_events_dq = PythonSensor(
        task_id="wait_for_events_dq",
        python_callable=_is_external_dag_succeeded,
        op_kwargs={
            "external_dag_id": "dbt.tests.dbt_clickhouse_dwh.events.dq",
            "logical_date": "{{ logical_date.isoformat() }}",
        },
        mode="reschedule",
        poke_interval=30,
        timeout=6 * 60 * 60,
    )

    collect_stats = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=3),
        task_id="getting_sku_group_query_install_stats",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_silver_sku_group_statistics.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    [wait_for_sessions_dq, wait_for_events_dq] >> collect_stats


dag = collect_silver_sku_group_query_install_stats()
