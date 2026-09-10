"""Фабрика Airflow-таски dq."""

from __future__ import annotations

from datetime import date, datetime
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
from dq.results_writer import RunMeta, write_results_batch
from dq.runner import run_dq

TASK_ID = "dq"


class DqTestsFailed(Exception):
    """Хотя бы один DQ-тест с severity error не прошёл."""


def partition_values(value: Any) -> list[str]:
    """Отрендеренный шаблон партиции: одно значение или несколько через запятую.

    Список нужен витринам, которые одним запуском перезаписывают несколько дней
    (штатное окно пересчёта или ручной диапазон): таска проверяет каждую партицию
    и сохраняет результаты одним commit'ом.
    """
    values = [part.strip() for part in str(value).split(",") if part.strip()]
    if not values:
        raise DqConfigError(f"dq.partition_date_template отрендерился в пустое значение: {value!r}")
    return values


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
    failure_callback_enabled: bool = True,
) -> Callable:
    """Возвращает штатную dq-таску энтити из блока `dq:` её config.yaml."""
    from airflow.providers.trino.hooks.trino import TrinoHook
    from airflow.sdk import get_current_context, task
    from airflow_commons.helpers.oncall import send_oncall_notification

    config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    settings = load_dq_settings(config)
    if not isinstance(failure_callback_enabled, bool):
        raise DqConfigError("failure_callback_enabled должен быть bool")
    alerts = config["alerts"]
    failure_callback = None
    if failure_callback_enabled:
        failure_callback = send_oncall_notification(
            team=alerts["team"],
            oncall_webhook_conn_id=alerts["oncall_webhook_conn_id"],
            severity=alerts["severity"],
        )

    @task(
        task_id=TASK_ID,
        retries=1,
        on_failure_callback=failure_callback,
    )
    def dq(partition_date_value: str) -> None:
        import logging

        logger = logging.getLogger("airflow.task")
        hook = TrinoHook(trino_conn_id=settings.trino_conn_id)

        def query(sql: str) -> list:
            logger.info("DQ query:\n%s", sql)
            return hook.get_records(sql)

        checked = []
        for value in partition_values(partition_date_value):
            ctx = build_render_context(config, Path(repo_root), value)
            outcome = run_dq(settings, ctx, query)
            logger.info("\n%s", format_log(outcome, ctx))
            checked.append((outcome, ctx))

        airflow_context = get_current_context()
        task_instance = airflow_context["task_instance"]
        write_results_batch(
            Path(repo_root),
            checked,
            settings,
            RunMeta(
                dag_id=task_instance.dag_id,
                task_id=TASK_ID,
                run_id=task_instance.run_id,
                try_number=int(task_instance.try_number),
                # У ручного запуска Airflow 3 logical_date может быть пустым.
                run_ts=airflow_context.get("logical_date") or airflow_context["dag_run"].run_after,
            ),
        )

        failed = [(outcome, ctx) for outcome, ctx in checked if outcome.has_errors]
        if failed:
            log_url = getattr(task_instance, "log_url", "")
            raise DqTestsFailed(
                "\n\n".join(format_alert(outcome, ctx, log_url) for outcome, ctx in failed)[:3500]
            )

    return dq
