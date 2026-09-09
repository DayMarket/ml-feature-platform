"""Идемпотентная запись истории DQ-прогонов в Iceberg.

Модуль называется results_writer, а не results, потому что каталог `dq/results/`
хранит config.yaml и миграцию самой таблицы и как namespace-пакет перекрыл бы `dq.results`.
"""

from __future__ import annotations

import json
import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, TypeVar

import yaml

from dq.config import DqSettings, RenderContext
from dq.runner import DqRunOutcome

RESULTS_CONFIG_PATH = Path("dq") / "results" / "config.yaml"

# Значения совпадают с job/runtime.py Trino/ClickHouse-энтити: pyiceberg не читает
# конфигурацию Spark и без явных свойств падает на отсутствующем URI метастора.
HIVE_METASTORE_URIS = "thrift://hive-metastore.svc-data-hive-metastore.svc.cluster.local:9083"
ICEBERG_WAREHOUSE = "s3a://um-prod-data-platform-landing-layer/"
S3_ENDPOINT = "http://storage.yandexcloud.net"
S3_REGION = "ru-central1"
S3_CONNECTION_ID = "spark_ycs_connection"
ICEBERG_LOCK_CHECK_MIN_WAIT_SECONDS = 2
ICEBERG_LOCK_CHECK_MAX_WAIT_SECONDS = 60
ICEBERG_LOCK_CHECK_RETRIES = 10
ICEBERG_COMMIT_RETRY_ATTEMPTS = 8
ICEBERG_COMMIT_RETRY_INITIAL_SECONDS = 1.0
ICEBERG_COMMIT_RETRY_MAX_SECONDS = 30.0

logger = logging.getLogger("airflow.task")
T = TypeVar("T")


@dataclass(frozen=True)
class RunMeta:
    dag_id: str
    task_id: str
    run_id: str
    try_number: int
    run_ts: datetime


def results_table_ref(repo_root: Path) -> tuple[str, str]:
    """PyIceberg-идентификатор таблицы результатов: ровно (schema, name)."""
    config = yaml.safe_load((Path(repo_root) / RESULTS_CONFIG_PATH).read_text(encoding="utf-8"))
    table = config["table"]
    schema = str(table["schema"]).strip()
    name = str(table["name"]).strip()
    if not schema or not name:
        raise ValueError(f"{RESULTS_CONFIG_PATH}: table.schema и table.name обязательны")
    return schema, name


def results_catalog_name(repo_root: Path) -> str:
    """Имя каталога таблицы результатов из её же config.yaml."""
    config = yaml.safe_load((Path(repo_root) / RESULTS_CONFIG_PATH).read_text(encoding="utf-8"))
    catalog = str(config["table"]["catalog"]).strip()
    if not catalog:
        raise ValueError(f"{RESULTS_CONFIG_PATH}: table.catalog обязателен")
    return catalog


def catalog_properties(access_key_id: str, secret_access_key: str) -> dict[str, str]:
    """Свойства pyiceberg-каталога. Без них load_catalog не знает ни метастор, ни S3."""
    return {
        "type": "hive",
        "uri": HIVE_METASTORE_URIS,
        "warehouse": ICEBERG_WAREHOUSE,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": access_key_id,
        "s3.secret-access-key": secret_access_key,
        "s3.region": S3_REGION,
        "s3.path-style-access": "true",
        "lock-check-min-wait-time": str(ICEBERG_LOCK_CHECK_MIN_WAIT_SECONDS),
        "lock-check-max-wait-time": str(ICEBERG_LOCK_CHECK_MAX_WAIT_SECONDS),
        "lock-check-retries": str(ICEBERG_LOCK_CHECK_RETRIES),
    }


def load_results_catalog(catalog_name: str):
    """Каталог для записи результатов; креды S3 берутся из Airflow-соединения."""
    from airflow.sdk import BaseHook
    from pyiceberg.catalog import load_catalog

    extra = BaseHook.get_connection(S3_CONNECTION_ID).extra_dejson
    return load_catalog(
        catalog_name,
        **catalog_properties(extra["aws_access_key_id"], extra["aws_secret_access_key"]),
    )


def _commit_failed_exception_type() -> type[Exception]:
    from pyiceberg.exceptions import CommitFailedException

    return CommitFailedException


def run_iceberg_commit_with_retry(
    operation: Callable[[], T],
    operation_name: str,
    *,
    attempts: int = ICEBERG_COMMIT_RETRY_ATTEMPTS,
    initial_sleep_seconds: float = ICEBERG_COMMIT_RETRY_INITIAL_SECONDS,
    max_sleep_seconds: float = ICEBERG_COMMIT_RETRY_MAX_SECONDS,
    sleep_fn: Callable[[float], None] = time.sleep,
    jitter_fn: Callable[[float, float], float] = random.uniform,
) -> T:
    """Повторяет optimistic commit после конкурентного изменения Iceberg branch."""
    if attempts <= 0:
        raise ValueError("Iceberg commit retry attempts must be positive")

    commit_failed = _commit_failed_exception_type()
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except commit_failed:
            if attempt == attempts:
                raise

            ceiling = min(
                max_sleep_seconds,
                initial_sleep_seconds * (2 ** (attempt - 1)),
            )
            delay = jitter_fn(ceiling / 2, ceiling)
            logger.warning(
                "Concurrent Iceberg commit during %s, attempt %d/%d; "
                "refreshing table metadata and retrying in %.2fs",
                operation_name,
                attempt,
                attempts,
                delay,
            )
            sleep_fn(delay)

    raise RuntimeError(f"Unexpected retry loop exit during {operation_name}")


def build_rows(
    outcome: DqRunOutcome,
    ctx: RenderContext,
    settings: DqSettings,
    meta: RunMeta,
) -> list[dict[str, Any]]:
    _ = settings  # сигнатура сохранена ради симметрии с write_results
    rows: list[dict[str, Any]] = []
    for result in outcome.results:
        rows.append(
            {
                "date": ctx.partition_date,
                "run_ts": meta.run_ts,
                "dag_id": meta.dag_id,
                "task_id": meta.task_id,
                "run_id": meta.run_id,
                "try_number": int(meta.try_number),
                "catalog": ctx.catalog_alias,
                "schema_name": ctx.schema,
                "table_name": ctx.table,
                "team": ctx.team,
                "test_name": result.name,
                "test_key": result.test_key,
                "test_family": result.family,
                "status": result.status,
                "severity": result.severity,
                "failed_rows": int(result.failed_rows),
                "observed": result.observed,
                "threshold": result.threshold,
                "params": json.dumps(result.params, ensure_ascii=False, default=str),
                "sql_text": result.sql,
                "sample": result.sample,
                "duration_ms": int(result.duration_ms),
                "skip_reason": result.skip_reason,
                "warmup_active": bool(outcome.warmup_active),
            }
        )
    return rows


def write_results(
    repo_root: Path,
    outcome: DqRunOutcome,
    ctx: RenderContext,
    settings: DqSettings,
    meta: RunMeta,
) -> None:
    rows = build_rows(outcome, ctx, settings, meta)
    if not rows:
        return

    import pyarrow as pa
    from pyiceberg.expressions import And, EqualTo

    schema, name = results_table_ref(repo_root)
    catalog = load_results_catalog(results_catalog_name(repo_root))
    identifier = (schema, name)

    def overwrite_current_results() -> None:
        table = catalog.load_table(identifier)
        arrow_table = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
        table.overwrite(
            arrow_table,
            overwrite_filter=And(
                EqualTo("date", ctx.partition_date),
                EqualTo("dag_id", meta.dag_id),
            ),
        )

    run_iceberg_commit_with_retry(
        overwrite_current_results,
        f"write DQ results for dag_id={meta.dag_id} date={ctx.partition_date}",
    )
