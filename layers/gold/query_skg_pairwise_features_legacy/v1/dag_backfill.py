"""One-off backfill DAG: fills the gold legacy pairwise partitions that were
never produced by the regular Spark pipeline, by copying the already-validated
upstream feature_store snapshot date by date.

This DAG is intentionally separate from the scheduled
`feature_platform_query_skg_pairwise_features_legacy_gold_dag`:

- It runs the backfill entrypoint (copy from
  `iceberg.um_prod_feature_store_iceberg.query_query_skg_pairwise_features`),
  NOT the silver-computed job.
- It uses catchup to iterate one partition (`{{ ds }}`) per run over the
  missing range, so each run is small, restartable, and idempotent
  (`overwritePartitions` replaces a single date).
- It has no on-call failure callback: a backfill run failing should not page.

After the range is filled, pause and delete this DAG. Verify gold min/max and
re-run any missing date by clearing its task instance.

Adjust `start_date` / `end_date` to the actual missing range before unpausing.
As of creation the gold table held only 2026-06-01 and the source spanned
2026-04-01 .. 2026-06-10, so the missing range is the whole 2026-04-01 ..
2026-06-10 span. The catchup is run as one contiguous range; re-copying the
already-present 2026-06-01 partition is a no-op (identical data, idempotent
`overwritePartitions`).
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

DAG_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, DAG_DIR)

from config.factory import get_dag_settings, get_deployment

dag_settings = get_dag_settings()

logger = logging.getLogger("airflow.task")
logger.setLevel("INFO")

default_args = {
    "owner": dag_settings["owner"],
    "depends_on_past": False,
    "trigger_rule": "all_success",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    default_args=default_args,
    max_active_runs=2,
    catchup=True,
    tags=[
        "spark",
        "feature-platform",
        dag_settings["team_tag"],
        "gold",
        "legacy",
        "query-skg",
        "backfill",
    ],
    is_paused_upon_creation=True,
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc),
    end_date=datetime(2026, 6, 11, 0, 0, 0, tzinfo=timezone.utc),
    dag_id="feature_platform_query_skg_pairwise_features_legacy_gold_backfill_dag",
)
def backfill_gold_query_skg_pairwise_features_legacy():
    backfill_partition = SparkKubernetesOperator(
        execution_timeout=timedelta(hours=6),
        task_id="backfill_query_skg_pairwise_features_legacy_from_feature_store",
        namespace="svc-data-spark-jobs",
        application_file=get_deployment(
            ".",
            "fetch_gold_query_skg_pairwise_features_legacy_backfill.yaml",
        ),
        kubernetes_conn_id="spark_k8s",
    )

    backfill_partition


dag = backfill_gold_query_skg_pairwise_features_legacy()
