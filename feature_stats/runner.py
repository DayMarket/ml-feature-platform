"""Определение набора признаков и расчёт их профилей распределения."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Sequence

from feature_stats.config import (
    NUMERIC_TYPES,
    PERCENTILE_COLUMNS,
    FeatureStatsSettings,
    StatsContext,
)
from feature_stats.query import VALUES_PER_COLUMN, render_columns_query, render_stats_query

Query = Callable[[str], list]
TypedColumn = tuple[str, str]


class FeatureStatsError(RuntimeError):
    """Расчёт невозможен: таблица недоступна, конфиг врёт или Trino вернул не то."""


@dataclass(frozen=True)
class FeatureStat:
    feature_name: str
    data_type: str
    rows_total: int
    non_null_count: int
    null_share: float | None
    mean: float | None
    min_value: float | None
    max_value: float | None
    percentiles: tuple[float | None, ...]
    duration_ms: int
    sql: str


def is_numeric(data_type: str) -> bool:
    normalized = str(data_type).strip().lower()
    return normalized in NUMERIC_TYPES or normalized.startswith("decimal(")


def fetch_typed_columns(query: Query, ctx: StatsContext) -> list[TypedColumn]:
    rows = query(render_columns_query(ctx))
    if not rows:
        raise FeatureStatsError(
            f"Таблица {ctx.render.catalog_alias}.{ctx.render.schema}.{ctx.render.table} "
            "не найдена в каталоге Trino. Таска feature_stats запускается только после того, "
            "как миграции применены; проверьте, что миграция энтити проехала на master."
        )
    return [(str(row[0]), str(row[1])) for row in rows]


def select_feature_columns(
    typed_columns: Sequence[TypedColumn], settings: FeatureStatsSettings, ctx: StatsContext
) -> list[TypedColumn]:
    """Числовые колонки-признаки в порядке объявления в таблице."""
    available = {name for name, _ in typed_columns}
    missing = [column for column in settings.exclude_columns if column not in available]
    if missing:
        raise FeatureStatsError(
            f"feature_stats.exclude_columns ссылается на колонки {missing}, которых нет в "
            f"{ctx.render.catalog_alias}.{ctx.render.schema}.{ctx.render.table}. "
            "Опечатка молча вернула бы признак под наблюдение, поэтому это ошибка конфигурации."
        )

    skip = set(ctx.render.primary_key) | {settings.partition_column} | set(settings.exclude_columns)
    return [
        (name, data_type)
        for name, data_type in typed_columns
        if name not in skip and is_numeric(data_type)
    ]


def batches(
    columns: Sequence[TypedColumn], columns_per_query: int | None
) -> list[list[TypedColumn]]:
    """Разбиение на партии запросов.

    Дефолт — одна партия: каждый дополнительный запрос это ещё один полный скан
    партиции, что дороже широкого списка агрегатов в одном плане.
    """
    if not columns_per_query:
        return [list(columns)]
    return [
        list(columns[start : start + columns_per_query])
        for start in range(0, len(columns), columns_per_query)
    ]


def parse_stats_row(
    row: Sequence[Any], batch: Sequence[TypedColumn], duration_ms: int, sql: str
) -> list[FeatureStat]:
    expected_width = 1 + len(batch) * VALUES_PER_COLUMN
    if len(row) != expected_width:
        raise FeatureStatsError(
            f"Trino вернул {len(row)} значений вместо {expected_width} "
            f"для {len(batch)} признаков — раскладка строки результата разъехалась с рендером"
        )

    rows_total = int(row[0]) if row[0] is not None else 0
    stats: list[FeatureStat] = []
    for index, (name, data_type) in enumerate(batch):
        base = 1 + index * VALUES_PER_COLUMN
        non_null_count = int(row[base]) if row[base] is not None else 0
        stats.append(
            FeatureStat(
                feature_name=name,
                data_type=data_type,
                rows_total=rows_total,
                non_null_count=non_null_count,
                null_share=(1.0 - non_null_count / rows_total) if rows_total else None,
                mean=_as_float(row[base + 1]),
                min_value=_as_float(row[base + 2]),
                max_value=_as_float(row[base + 3]),
                percentiles=_as_percentiles(row[base + 4], name),
                duration_ms=duration_ms,
                sql=sql,
            )
        )
    return stats


def run_feature_stats(
    settings: FeatureStatsSettings, ctx: StatsContext, query: Query
) -> list[FeatureStat]:
    if not settings.enabled:
        return []

    features = select_feature_columns(fetch_typed_columns(query, ctx), settings, ctx)
    if not features:
        # Таблица из одних ключей — не ошибка, просто нечего профилировать.
        return []

    stats: list[FeatureStat] = []
    for batch in batches(features, settings.columns_per_query):
        sql = render_stats_query(ctx, [name for name, _ in batch])
        started = time.monotonic()
        rows = query(sql)
        duration_ms = int((time.monotonic() - started) * 1000)
        if not rows:
            raise FeatureStatsError(
                "Агрегатный запрос без GROUP BY обязан вернуть строку, получено пусто. "
                f"SQL:\n{sql}"
            )
        stats.extend(parse_stats_row(rows[0], batch, duration_ms, sql))
    return stats


def _as_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _as_percentiles(value: Any, feature_name: str) -> tuple[float | None, ...]:
    # При нуле непустых значений approx_percentile отдаёт NULL вместо массива.
    if value is None:
        return (None,) * len(PERCENTILE_COLUMNS)
    values = list(value)
    if len(values) != len(PERCENTILE_COLUMNS):
        raise FeatureStatsError(
            f"{feature_name}: Trino вернул {len(values)} перцентилей вместо "
            f"{len(PERCENTILE_COLUMNS)}"
        )
    return tuple(_as_float(item) for item in values)
