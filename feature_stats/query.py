"""Рендер Trino SQL для профилей распределения признаков.

Запрос статистик возвращает ровно одну строку следующей раскладки:
  [rows_total, cnt_0, mean_0, min_0, max_0, pct_0, cnt_1, mean_1, ...]
то есть одно общее значение плюс VALUES_PER_COLUMN значений на каждый признак
в том же порядке, в каком признаки переданы в render_stats_query.
"""

from __future__ import annotations

from typing import Sequence

from dq.tests import (
    partition_expression,
    partition_literal,
    quote_identifier,
    quote_literal,
    table_ref,
)

from feature_stats.config import PERCENTILES, StatsContext

# cnt, mean, min, max, pct
VALUES_PER_COLUMN = 5


def percentile_array_literal() -> str:
    return "ARRAY[" + ", ".join(repr(value) for value in PERCENTILES) + "]"


def render_columns_query(ctx: StatsContext) -> str:
    """SQL, отдающий имена и типы колонок целевой таблицы в порядке объявления."""
    # information_schema резолвится в дефолтном каталоге соединения, а он у trino_*
    # смотрит в hive. Квалифицируем каталогом таблицы, иначе CATALOG_NOT_FOUND.
    return (
        f"SELECT column_name, data_type\n"
        f"FROM {quote_identifier(ctx.render.catalog_alias)}.information_schema.columns\n"
        f"WHERE table_schema = {quote_literal(ctx.render.schema)}\n"
        f"  AND table_name = {quote_literal(ctx.render.table)}\n"
        f"ORDER BY ordinal_position"
    )


def render_stats_query(ctx: StatsContext, columns: Sequence[str]) -> str:
    """SQL профиля распределения для одной партии признаков."""
    if not columns:
        raise ValueError("render_stats_query вызван без колонок")

    percentiles = percentile_array_literal()
    projections = ["  count(*) AS rows_total"]
    for index, column in enumerate(columns):
        quoted = quote_identifier(column)
        # Алиасы позиционные: имя признака может превысить лимит идентификатора
        # или совпасть с соседним после нормализации.
        value = f"CAST({quoted} AS DOUBLE)"
        projections.extend(
            [
                f"  count({quoted}) AS cnt_{index}",
                f"  avg({value}) AS mean_{index}",
                f"  min({value}) AS min_{index}",
                f"  max({value}) AS max_{index}",
                f"  approx_percentile({value}, {percentiles}) AS pct_{index}",
            ]
        )

    return (
        "SELECT\n"
        + ",\n".join(projections)
        + f"\nFROM {table_ref(ctx.render)}\n"
        + f"WHERE {partition_expression(ctx.render)} = {partition_literal(ctx.render)}"
    )
