"""Фабрика Airflow-таски feature_stats."""

from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import yaml

from dq.config import DqConfigError, RenderContext, trino_catalog_alias
from dq.task import parse_partition_value

from feature_stats.config import (
    DEFAULT_TEAM,
    FeatureStatsConfigError,
    StatsContext,
    load_feature_stats_settings,
)
from feature_stats.results_writer import RunMeta, write_results
from feature_stats.runner import run_feature_stats

TASK_ID = "feature_stats"


def partition_instant(partition_date: date, partition_timestamp: datetime | None) -> datetime:
    """Канонический момент партиции в UTC.

    У снапшотной энтити это записанный снапшот, у дневной — полночь её партиции.
    Заполнен всегда: он входит в ключ таблицы результатов и в фильтр перезаписи,
    а нулевое значение потребовало бы ветвления на IsNull в обоих местах.
    """
    if partition_timestamp is not None:
        return partition_timestamp.replace(tzinfo=timezone.utc)
    return datetime.combine(partition_date, time.min, tzinfo=timezone.utc)


def fetch_rows(hook: Any, sql: str) -> list:
    """Выполнить один SQL и вернуть все строки, минуя разбор запроса на клиенте.

    Намеренно не `hook.get_records`: он идёт через `DbApiHook.run`, а тот гоняет
    SQL через `sqlparse.split_sql_string`, чтобы разбить его на statement'ы.
    Начиная с sqlparse 0.6.0 группировка отказывается разбирать больше 10000
    токенов (`MAX_GROUPING_TOKENS`, защита от DoS), а запрос профиля стоит около
    112 токенов на признак: на 89 признаках
    (`sku_group_search_conversion_features_v2`) это 10005 токенов и
    `SQLParseError`, поднятый на клиенте — запрос до Trino не доезжает вовсе.
    Делить наш SQL и не требуется: `render_stats_query` всегда отдаёт ровно один
    statement. Дробление на `columns_per_query` здесь не лечение: каждая партия —
    это ещё один полный скан партиции, а следующая широкая таблица упрётся в тот
    же лимит на другом числе колонок.
    """
    with closing(hook.get_conn()) as connection:
        with closing(connection.cursor()) as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


def build_stats_context(config: dict[str, Any], repo_root: Path, partition_value: Any) -> StatsContext:
    table = config["table"]
    settings = load_feature_stats_settings(config)
    meta = table.get("meta") or {}
    try:
        partition_date, partition_timestamp = parse_partition_value(
            partition_value, settings.partition_granularity
        )
    except DqConfigError as error:
        # parse_partition_value переиспользован из dq.task и говорит об ошибке
        # словами dq.partition_date_template — на debug'е feature_stats это
        # уводит по ложному следу, поэтому переупаковываем в свою ошибку.
        raise FeatureStatsConfigError(
            f"feature_stats.partition_date_template при partition_granularity: "
            f"timestamp обязан отдавать 'YYYY-MM-DD HH:MM:SS' в UTC, получено {partition_value!r}"
        ) from error
    render = RenderContext(
        catalog_alias=trino_catalog_alias(repo_root, str(table["catalog"])),
        schema=str(table["schema"]),
        table=str(table["name"]),
        primary_key=tuple(
            column.strip() for column in str(table["primary_key"]).split(",") if column.strip()
        ),
        partition_column=settings.partition_column,
        partition_date=partition_date,
        # Профиль всегда считается по одной партиции: table-wide скан 30M строк
        # на каждый DAG-ран не окупается ничем.
        scope="partition",
        sample_rows=0,
        team=str(meta.get("team") or DEFAULT_TEAM),
        partition_granularity=settings.partition_granularity,
        partition_timestamp=partition_timestamp,
        snapshot_interval_hours=settings.snapshot_interval_hours,
    )
    return StatsContext(
        render=render, partition_ts=partition_instant(partition_date, partition_timestamp)
    )


def build_feature_stats_task(config_path: str, repo_root: str) -> Callable:
    """Возвращает готовую Airflow-таску feature_stats для DAG'а энтити."""
    from airflow.providers.trino.hooks.trino import TrinoHook
    from airflow.sdk import get_current_context, task
    from airflow_commons.helpers.oncall import send_oncall_notification

    config = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    settings = load_feature_stats_settings(config)
    alerts = config["alerts"]

    @task(
        task_id=TASK_ID,
        retries=1,
        # query_timeout_seconds обязан реально ограничивать таску, а не только
        # значиться в конфиге: иначе зависший запрос держит воркер-слот бессрочно,
        # а понижение таймаута и редеплой ничего не меняют.
        execution_timeout=timedelta(seconds=settings.query_timeout_seconds),
        on_failure_callback=send_oncall_notification(
            team=alerts["team"],
            oncall_webhook_conn_id=alerts["oncall_webhook_conn_id"],
            severity=alerts["severity"],
        ),
    )
    def feature_stats(partition_date_value: str) -> None:
        import logging

        logger = logging.getLogger("airflow.task")
        ctx = build_stats_context(config, Path(repo_root), partition_date_value)

        hook = TrinoHook(trino_conn_id=settings.trino_conn_id)

        def query(sql: str) -> list:
            logger.info("Feature stats query:\n%s", sql)
            return fetch_rows(hook, sql)

        stats = run_feature_stats(settings, ctx, query)
        logger.info(
            "Профилей посчитано: %s для %s.%s.%s за %s",
            len(stats),
            ctx.render.catalog_alias,
            ctx.render.schema,
            ctx.render.table,
            ctx.partition_ts.isoformat(),
        )
        if not stats:
            return

        airflow_context = get_current_context()
        task_instance = airflow_context["task_instance"]
        write_results(
            Path(repo_root),
            stats,
            ctx,
            RunMeta(
                dag_id=task_instance.dag_id,
                task_id=TASK_ID,
                run_id=task_instance.run_id,
                try_number=int(task_instance.try_number),
                run_ts=airflow_context["logical_date"],
            ),
        )

    return feature_stats
