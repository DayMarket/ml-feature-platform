"""Рендер DQ-тестов в Trino SQL.

Каждый запрос возвращает ровно одну строку (failed_rows BIGINT, observed DOUBLE):
  failed_rows > 0  — тест упал, значение = число нарушающих строк или 1 для агрегатных тестов
  failed_rows = 0  — тест прошёл
  failed_rows < 0  — тест не мог быть выполнен, runner превращает это в статус skipped
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from dq.config import RenderContext, TestSpec


@dataclass(frozen=True)
class RenderedTest:
    spec: TestSpec
    test_key: str
    sql: str
    sample_sql: str | None
    threshold: str
    needs_baseline: bool = False


def quote_identifier(name: str) -> str:
    escaped = str(name).replace('"', '""')
    return f'"{escaped}"'


def quote_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def table_ref(ctx: RenderContext) -> str:
    return ".".join(
        (quote_identifier(ctx.catalog_alias), quote_identifier(ctx.schema), quote_identifier(ctx.table))
    )


def scope_predicate(ctx: RenderContext) -> str:
    if ctx.scope == "table":
        return "TRUE"
    return f"{quote_identifier(ctx.partition_column)} = DATE {quote_literal(ctx.partition_date.isoformat())}"


def _where(ctx: RenderContext, spec: TestSpec, violation: str) -> str:
    parts = [scope_predicate(ctx), f"({violation})"]
    if spec.where:
        parts.insert(1, f"({spec.where})")
    return " AND ".join(parts)


def _count_query(ctx: RenderContext, spec: TestSpec, violation: str) -> str:
    return (
        "SELECT count(*) AS failed_rows, CAST(count(*) AS DOUBLE) AS observed\n"
        f"FROM {table_ref(ctx)} AS target\n"
        f"WHERE {_where(ctx, spec, violation)}"
    )


def _sample_query(
    ctx: RenderContext, spec: TestSpec, violation: str, extra_columns: tuple[str, ...] = ()
) -> str:
    columns = list(dict.fromkeys(list(ctx.primary_key) + list(extra_columns)))
    projection = ", ".join(quote_identifier(column) for column in columns)
    return (
        f"SELECT {projection}\n"
        f"FROM {table_ref(ctx)} AS target\n"
        f"WHERE {_where(ctx, spec, violation)}\n"
        f"LIMIT {int(ctx.sample_rows)}"
    )


def _render_primary_key_not_null(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    violation = " OR ".join(f"{quote_identifier(column)} IS NULL" for column in ctx.primary_key)
    return RenderedTest(
        spec=spec,
        test_key="primary_key_not_null",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation),
        threshold="0 rows with NULL primary key columns",
    )


def _render_primary_key_unique(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    return _render_unique(spec, ctx, ctx.primary_key, "primary_key_unique")


def _render_unique(
    spec: TestSpec, ctx: RenderContext, columns: tuple[str, ...], test_key: str
) -> RenderedTest:
    projection = ", ".join(quote_identifier(column) for column in columns)
    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")
    sql = (
        "SELECT count(*) AS failed_rows, CAST(count(*) AS DOUBLE) AS observed\n"
        "FROM (\n"
        f"  SELECT {projection}\n"
        f"  FROM {table_ref(ctx)}\n"
        f"  WHERE {' AND '.join(filters)}\n"
        f"  GROUP BY {projection}\n"
        "  HAVING count(*) > 1\n"
        ") AS duplicates"
    )
    sample_sql = (
        f"SELECT {projection}, count(*) AS duplicate_rows\n"
        f"FROM {table_ref(ctx)}\n"
        f"WHERE {' AND '.join(filters)}\n"
        f"GROUP BY {projection}\n"
        "HAVING count(*) > 1\n"
        f"LIMIT {int(ctx.sample_rows)}"
    )
    return RenderedTest(
        spec=spec,
        test_key=test_key,
        sql=sql,
        sample_sql=sample_sql,
        threshold="0 duplicate key groups",
    )


def _render_row_count_min(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    min_rows = int(spec.params["min_rows"])
    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")
    sql = (
        f"SELECT CASE WHEN row_count <= {min_rows} THEN 1 ELSE 0 END AS failed_rows,\n"
        "       CAST(row_count AS DOUBLE) AS observed\n"
        "FROM (\n"
        f"  SELECT count(*) AS row_count FROM {table_ref(ctx)} WHERE {' AND '.join(filters)}\n"
        ") AS counted"
    )
    return RenderedTest(
        spec=spec,
        test_key="row_count_min",
        sql=sql,
        sample_sql=None,
        threshold=f"row_count > {min_rows}",
    )


def _render_row_count_growth(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    ratio = float(spec.params["max_growth_ratio"])
    direction = str(spec.params["direction"])
    baseline = ctx.partition_date - timedelta(days=1)
    column = quote_identifier(ctx.partition_column)
    current_literal = f"DATE {quote_literal(ctx.partition_date.isoformat())}"
    baseline_literal = f"DATE {quote_literal(baseline.isoformat())}"

    violations = []
    if direction in ("both", "up"):
        violations.append(f"current_row_count > previous_row_count * {1 + ratio}")
    if direction in ("both", "down"):
        violations.append(f"current_row_count < previous_row_count * {1 - ratio}")
    violation = " OR ".join(violations)

    sql = (
        "SELECT CASE WHEN previous_row_count = 0 THEN -1\n"
        f"            WHEN {violation} THEN 1\n"
        "            ELSE 0 END AS failed_rows,\n"
        "       CASE WHEN previous_row_count = 0 THEN NULL\n"
        "            ELSE CAST(current_row_count AS DOUBLE) / previous_row_count - 1 END AS observed\n"
        "FROM (\n"
        f"  SELECT count_if(CAST({column} AS DATE) = {current_literal}) AS current_row_count,\n"
        f"         count_if(CAST({column} AS DATE) = {baseline_literal}) AS previous_row_count\n"
        f"  FROM {table_ref(ctx)}\n"
        f"  WHERE CAST({column} AS DATE) IN ({current_literal}, {baseline_literal})\n"
        ") AS counts"
    )
    return RenderedTest(
        spec=spec,
        test_key="row_count_growth",
        sql=sql,
        sample_sql=None,
        threshold=f"|growth| <= {ratio} (direction={direction}, baseline={baseline.isoformat()})",
        needs_baseline=True,
    )


def _render_freshness(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    max_lag_days = int(spec.params["max_lag_days"])
    column = quote_identifier(ctx.partition_column)
    current_literal = f"DATE {quote_literal(ctx.partition_date.isoformat())}"
    sql = (
        "SELECT CASE WHEN max_partition IS NULL THEN 1\n"
        f"            WHEN date_diff('day', max_partition, {current_literal}) > {max_lag_days} THEN 1\n"
        "            ELSE 0 END AS failed_rows,\n"
        f"       CAST(date_diff('day', max_partition, {current_literal}) AS DOUBLE) AS observed\n"
        "FROM (\n"
        f"  SELECT max(CAST({column} AS DATE)) AS max_partition FROM {table_ref(ctx)}\n"
        ") AS bounds"
    )
    return RenderedTest(
        spec=spec,
        test_key="freshness",
        sql=sql,
        sample_sql=None,
        threshold=f"lag <= {max_lag_days} days",
    )


RENDERERS = {
    "primary_key_not_null": _render_primary_key_not_null,
    "primary_key_unique": _render_primary_key_unique,
    "row_count_min": _render_row_count_min,
    "row_count_growth": _render_row_count_growth,
    "freshness": _render_freshness,
}


def render(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    try:
        renderer = RENDERERS[spec.name]
    except KeyError as error:
        raise KeyError(f"Нет рендерера для DQ-теста {spec.name!r}") from error
    return renderer(spec, ctx)
