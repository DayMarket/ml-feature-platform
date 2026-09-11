"""Feature statistics всех дней при неизменном current Iceberg snapshot."""

import math
from pathlib import Path

from dq.config import load_dq_settings
from dq.day_range import validate_written, verify_current
from dq.results_writer import load_results_catalog
from feature_stats.config import FeatureStatsConfigError, load_feature_stats_settings
from feature_stats.results_writer import RunMeta, write_results
from feature_stats.runner import FeatureStatsError, run_feature_stats
from feature_stats.task import build_stats_context


def validate_range_settings(config):
    """DQ и profiles должны описывать одну дневную партицию."""
    settings = load_feature_stats_settings(config)
    dq = load_dq_settings(config)
    if not settings.enabled or dq.scope != "partition" or settings.partition_granularity != "date":
        raise FeatureStatsConfigError("Диапазон требует включённых дневных профилей")
    for key in (
        "partition_column",
        "partition_granularity",
        "partition_date_template",
        "snapshot_interval_hours",
    ):
        if getattr(settings, key) != getattr(dq, key):
            raise FeatureStatsConfigError(f"feature_stats.{key} расходится с DQ")
    return settings


def strict_query(query):
    """Проверить форму metadata и агрегатных ответов только в range-режиме."""
    def checked(sql):
        rows = query(sql)
        if "information_schema.columns" in sql:
            if (
                not isinstance(rows, (list, tuple))
                or not rows
                or any(
                    not isinstance(row, (list, tuple))
                    or len(row) != 2
                    or any(not isinstance(value, str) or not value.strip() for value in row)
                    for row in rows
                )
                or len({row[0] for row in rows}) != len(rows)
            ):
                raise FeatureStatsError("Неполная или повторяющаяся схема профиля")
        elif (
            not isinstance(rows, (list, tuple))
            or len(rows) != 1
            or not isinstance(rows[0], (list, tuple))
        ):
            raise FeatureStatsError("Нужна одна полная строка профилей")
        return rows

    return checked


def run_range_feature_stats(config, repo_root, written, query, meta: RunMeta, *, catalog=None):
    """Сохранить профили всех дней одной нетерминальной задачей."""
    days = validate_written(written)
    settings = validate_range_settings(config)
    if (
        meta.task_id != "feature_stats"
        or not meta.dag_id
        or not meta.run_id
        or type(meta.try_number) is not int
        or meta.try_number <= 0
    ):
        raise FeatureStatsConfigError("Нужны точные DAG/run/task=feature_stats")
    root = Path(repo_root)
    catalog = catalog or load_results_catalog(config["table"]["catalog"])
    checked_query = strict_query(query)
    for day, receipt in zip(days, written["day_receipts"], strict=True):
        verify_current(config, written, catalog)
        ctx = build_stats_context(config, root, day.isoformat())
        stats = run_feature_stats(settings, ctx, checked_query)
        if any(
            stat.rows_total != receipt["rows_written"]
            or not 0 <= stat.non_null_count <= stat.rows_total
            or any(
                value is not None and not math.isfinite(value)
                for value in (stat.null_share, stat.mean, stat.min_value, stat.max_value, *stat.percentiles)
            )
            for stat in stats
        ):
            raise FeatureStatsError("Профиль не совпал с writer receipt или содержит неверные значения")
        verify_current(config, written, catalog)
        write_results(root, stats, ctx, meta)
        verify_current(config, written, catalog)
