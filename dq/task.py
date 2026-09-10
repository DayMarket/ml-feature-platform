"""Фабрика Airflow-таски dq."""

from __future__ import annotations

from datetime import date, datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import yaml

from dq.config import (
    DEFAULT_TEAM,
    DqConfigError,
    RenderContext,
    load_dq_settings,
    trino_catalog_alias,
)
from dq.report import format_alert, format_log
from dq.results_writer import RunMeta, write_results
from dq.runner import run_dq

TASK_ID = "dq"


class DqTestsFailed(Exception):
    """Хотя бы один DQ-тест с severity error не прошёл."""


def guarded_task(guard):
    """Применить opt-in бюджет владельца ко всему телу задачи."""
    def decorate(function):
        if guard is None:
            return function

        @wraps(function)
        def guarded(*args, **kwargs):
            from airflow.sdk import get_current_context

            with guard(get_current_context()):
                return function(*args, **kwargs)

        return guarded

    return decorate


def parse_partition_value(value: Any, granularity: str) -> tuple[date, datetime | None]:
    """Разбирает отрендеренный Airflow'ом шаблон партиции в дату и, для снапшота, момент.

    Дневная энтити отдаёт `YYYY-MM-DD`, снапшотная — `YYYY-MM-DD HH:MM:SS` в UTC.
    """
    raw = str(value).strip()
    if granularity != "timestamp":
        return date.fromisoformat(raw[:10]), None

    normalized = raw.replace("T", " ")[:19]
    try:
        moment = datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S")
    except ValueError as error:
        raise DqConfigError(
            f"dq.partition_date_template при partition_granularity: timestamp обязан отдавать "
            f"'YYYY-MM-DD HH:MM:SS' в UTC, получено {raw!r}"
        ) from error
    return moment.date(), moment


def build_render_context(
    config: dict[str, Any], repo_root: Path, partition_value: Any
) -> RenderContext:
    table = config["table"]
    settings = load_dq_settings(config)
    meta = table.get("meta") or {}
    partition_date, partition_timestamp = parse_partition_value(
        partition_value, settings.partition_granularity
    )
    return RenderContext(
        catalog_alias=trino_catalog_alias(repo_root, str(table["catalog"])),
        schema=str(table["schema"]),
        table=str(table["name"]),
        primary_key=tuple(
            column.strip() for column in str(table["primary_key"]).split(",") if column.strip()
        ),
        partition_column=settings.partition_column,
        partition_date=partition_date,
        scope=settings.scope,
        sample_rows=settings.sample_rows,
        team=str(meta.get("team") or DEFAULT_TEAM),
        partition_granularity=settings.partition_granularity,
        partition_timestamp=partition_timestamp,
        snapshot_interval_hours=settings.snapshot_interval_hours,
    )


def build_dq_task(
    config_path: str,
    repo_root: str,
    *,
    range_receipt_task_id: str | None = None,
    receipt_task_id: str | None = None,
    range_guard: Callable | None = None,
    task_guard: Callable | None = None,
) -> Callable:
    """Возвращает штатную dq-таску; opt-in range проверяет каждый записанный день."""
    from airflow.providers.trino.hooks.trino import TrinoHook
    from airflow.sdk import get_current_context, task
    from airflow_commons.helpers.oncall import send_oncall_notification

    config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    settings = load_dq_settings(config)
    if receipt_task_id is not None:
        if range_receipt_task_id is not None:
            raise DqConfigError("receipt_task_id не совмещается с range receipt")
        if not isinstance(receipt_task_id, str) or not receipt_task_id.strip():
            raise DqConfigError("Нужен task_id записи")
    if task_guard is not None and (not callable(task_guard) or range_guard is not None):
        raise DqConfigError("task_guard требует callable и не совмещается с range_guard")
    if range_guard is not None and (
        range_receipt_task_id is None or not callable(range_guard)
    ):
        raise DqConfigError("range_guard требует callable и range_receipt_task_id")
    if range_receipt_task_id is not None:
        from dq.day_range import validate_settings

        if not isinstance(range_receipt_task_id, str) or not range_receipt_task_id.strip():
            raise DqConfigError("Нужен task_id записи диапазона")
        validate_settings(settings)
    alerts = config["alerts"]

    @task(
        task_id=TASK_ID,
        retries=1,
        on_failure_callback=send_oncall_notification(
            team=alerts["team"],
            oncall_webhook_conn_id=alerts["oncall_webhook_conn_id"],
            severity=alerts["severity"],
        ),
    )
    @guarded_task(task_guard if task_guard is not None else range_guard)
    def dq(partition_date_value: str) -> dict | None:
        import logging

        logger = logging.getLogger("airflow.task")
        ctx = build_render_context(config, Path(repo_root), partition_date_value)

        hook = TrinoHook(trino_conn_id=settings.trino_conn_id)

        def query(sql: str) -> list:
            logger.info("DQ query:\n%s", sql)
            return hook.get_records(sql)

        airflow_context = get_current_context()
        task_instance = airflow_context["task_instance"]
        if range_receipt_task_id is not None:
            from dq.day_range import run_range_dq, validate_written

            written = task_instance.xcom_pull(
                task_ids=range_receipt_task_id,
                include_prior_dates=False,
            )
            days = validate_written(written)
            if ctx.partition_date != days[-1]:
                raise DqConfigError("Шаблон DQ не совпал с последним днём диапазона")
            checks = run_range_dq(
                config,
                Path(repo_root),
                written,
                query,
                RunMeta(
                    dag_id=task_instance.dag_id,
                    task_id=TASK_ID,
                    run_id=task_instance.run_id,
                    try_number=int(task_instance.try_number),
                    run_ts=datetime.now(timezone.utc),
                ),
            )
            return {
                "dq_status": "passed",
                "dag_id": task_instance.dag_id,
                "run_id": task_instance.run_id,
                "receipt": written,
                "day_checks": checks,
            }

        outcome = run_dq(settings, ctx, query)
        logger.info("\n%s", format_log(outcome, ctx))

        write_results(
            Path(repo_root),
            outcome,
            ctx,
            settings,
            RunMeta(
                dag_id=task_instance.dag_id,
                task_id=TASK_ID,
                run_id=task_instance.run_id,
                try_number=int(task_instance.try_number),
                run_ts=airflow_context["logical_date"],
            ),
        )

        if outcome.has_errors:
            log_url = getattr(task_instance, "log_url", "")
            raise DqTestsFailed(format_alert(outcome, ctx, log_url))
        if receipt_task_id is not None:
            receipt = task_instance.xcom_pull(
                task_ids=receipt_task_id,
                include_prior_dates=False,
            )
            if not isinstance(receipt, dict) or receipt.get("status") != "written":
                raise DqConfigError("DQ не получил writer receipt")
            return {
                "dq_status": "passed",
                "dag_id": task_instance.dag_id,
                "run_id": task_instance.run_id,
                "receipt": receipt,
            }
        return None

    return dq
