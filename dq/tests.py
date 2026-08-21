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


def _columns_key(name: str, columns: Any) -> str:
    return f"{name}[{','.join(str(column) for column in columns)}]"


def _null_violation(columns: tuple[str, ...]) -> str:
    return " OR ".join(f"{quote_identifier(column)} IS NULL" for column in columns)


def _render_not_null(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(spec.params["columns"])
    max_null_share = spec.params.get("max_null_share")
    violation = _null_violation(columns)
    test_key = _columns_key("not_null", columns)

    if max_null_share is None:
        return RenderedTest(
            spec=spec,
            test_key=test_key,
            sql=_count_query(ctx, spec, violation),
            sample_sql=_sample_query(ctx, spec, violation, columns),
            threshold="0 rows with NULL values",
        )

    share = float(max_null_share)
    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")
    sql = (
        f"SELECT CASE WHEN null_share > {share} THEN null_rows ELSE 0 END AS failed_rows,\n"
        "       null_share AS observed\n"
        "FROM (\n"
        f"  SELECT count_if({violation}) AS null_rows,\n"
        f"         CAST(count_if({violation}) AS DOUBLE) / NULLIF(count(*), 0) AS null_share\n"
        f"  FROM {table_ref(ctx)}\n"
        f"  WHERE {' AND '.join(filters)}\n"
        ") AS shares"
    )
    return RenderedTest(
        spec=spec,
        test_key=test_key,
        sql=sql,
        sample_sql=_sample_query(ctx, spec, violation, columns),
        threshold=f"null share <= {share}",
    )


def _render_null_share_below(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = str(spec.params["column"])
    share = float(spec.params["max_share"])
    inner = TestSpec(
        name="not_null",
        family=spec.family,
        params={"columns": [column], "max_null_share": share},
        severity=spec.severity,
        where=spec.where,
    )
    rendered = _render_not_null(inner, ctx)
    return RenderedTest(
        spec=spec,
        test_key=f"null_share_below[{column}]",
        sql=rendered.sql,
        sample_sql=rendered.sample_sql,
        threshold=f"null share <= {share}",
    )


def _render_unique_combination(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(str(column) for column in spec.params["columns"])
    return _render_unique(spec, ctx, columns, _columns_key("unique_combination", columns))


def _values_list(values: Any) -> str:
    return ", ".join(quote_literal(value) for value in values)


def _render_accepted_values(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = quote_identifier(str(spec.params["column"]))
    violation = f"{column} NOT IN ({_values_list(spec.params['values'])})"
    if not spec.params.get("ignore_nulls", True):
        violation = f"{violation} OR {column} IS NULL"
    else:
        violation = f"{column} IS NOT NULL AND ({violation})"
    return RenderedTest(
        spec=spec,
        test_key=f"accepted_values[{spec.params['column']}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, (str(spec.params["column"]),)),
        threshold=f"values in ({_values_list(spec.params['values'])})",
    )


def _render_not_accepted_values(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = quote_identifier(str(spec.params["column"]))
    violation = f"{column} IN ({_values_list(spec.params['values'])})"
    return RenderedTest(
        spec=spec,
        test_key=f"not_accepted_values[{spec.params['column']}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, (str(spec.params["column"]),)),
        threshold=f"values not in ({_values_list(spec.params['values'])})",
    )


def _render_accepted_range(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = quote_identifier(str(spec.params["column"]))
    clauses = []
    if spec.params.get("min") is not None:
        operator = "<" if spec.params.get("min_inclusive", True) else "<="
        clauses.append(f"{column} {operator} {quote_literal(spec.params['min'])}")
    if spec.params.get("max") is not None:
        operator = ">" if spec.params.get("max_inclusive", True) else ">="
        clauses.append(f"{column} {operator} {quote_literal(spec.params['max'])}")
    violation = " OR ".join(clauses)
    if not spec.params.get("ignore_nulls", True):
        violation = f"({violation}) OR {column} IS NULL"
    return RenderedTest(
        spec=spec,
        test_key=f"accepted_range[{spec.params['column']}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, (str(spec.params["column"]),)),
        threshold=f"{spec.params.get('min')} <= value <= {spec.params.get('max')}",
    )


def _render_non_negative(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(str(column) for column in spec.params["columns"])
    clauses = [f"{quote_identifier(column)} < 0" for column in columns]
    violation = " OR ".join(clauses)
    if not spec.params.get("ignore_nulls", True):
        violation = f"({violation}) OR ({_null_violation(columns)})"
    return RenderedTest(
        spec=spec,
        test_key=_columns_key("non_negative", columns),
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, columns),
        threshold="value >= 0",
    )


def _render_string_not_blank(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(str(column) for column in spec.params["columns"])
    clauses = []
    for column in columns:
        quoted = quote_identifier(column)
        clauses.append(f"{quoted} IS NULL OR trim({quoted}) = ''")
    violation = " OR ".join(clauses)
    return RenderedTest(
        spec=spec,
        test_key=_columns_key("string_not_blank", columns),
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, columns),
        threshold="non-empty trimmed string",
    )


def _render_distinct_count_between(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    columns = tuple(str(column) for column in spec.params["columns"])
    projection = ", ".join(quote_identifier(column) for column in columns)
    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")
    clauses = []
    if spec.params.get("min") is not None:
        clauses.append(f"distinct_count < {int(spec.params['min'])}")
    if spec.params.get("max") is not None:
        clauses.append(f"distinct_count > {int(spec.params['max'])}")
    violation = " OR ".join(clauses)
    sql = (
        f"SELECT CASE WHEN {violation} THEN 1 ELSE 0 END AS failed_rows,\n"
        "       CAST(distinct_count AS DOUBLE) AS observed\n"
        "FROM (\n"
        "  SELECT count(*) AS distinct_count FROM (\n"
        f"    SELECT DISTINCT {projection} FROM {table_ref(ctx)} WHERE {' AND '.join(filters)}\n"
        "  ) AS distinct_rows\n"
        ") AS counted"
    )
    return RenderedTest(
        spec=spec,
        test_key=_columns_key("distinct_count_between", columns),
        sql=sql,
        sample_sql=None,
        threshold=f"{spec.params.get('min')} <= distinct_count <= {spec.params.get('max')}",
    )


def _render_columns_sum_equals(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    parts = tuple(str(part) for part in spec.params["parts"])
    total = str(spec.params["total"])
    tolerance = float(spec.params.get("tolerance", 1e-6))
    summed = " + ".join(quote_identifier(part) for part in parts)
    violation = f"abs(({summed}) - {quote_identifier(total)}) > {tolerance}"
    return RenderedTest(
        spec=spec,
        test_key=f"columns_sum_equals[{total}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, parts + (total,)),
        threshold=f"|sum(parts) - {total}| <= {tolerance}",
    )


def _render_expression_is_true(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    expression = str(spec.params["expression"])
    # IS NOT TRUE ловит и FALSE, и NULL: выражение, не вычислившееся в TRUE, считается нарушением.
    violation = f"({expression}) IS NOT TRUE"
    return RenderedTest(
        spec=spec,
        test_key=f"expression_is_true[{expression[:40]}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation),
        threshold=f"({expression}) IS TRUE",
    )


def _render_relationships(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    column = quote_identifier(str(spec.params["column"]))
    to_table = str(spec.params["to_table"])
    to_column = quote_identifier(str(spec.params["to_column"]))
    reference_where = spec.params.get("where")
    reference_filter = f" AND ({reference_where})" if reference_where else ""
    violation = (
        f"{column} IS NOT NULL AND NOT EXISTS ("
        f"SELECT 1 FROM {to_table} AS reference "
        f"WHERE reference.{to_column} = target.{column}{reference_filter})"
    )
    return RenderedTest(
        spec=spec,
        test_key=f"relationships[{spec.params['column']}]",
        sql=_count_query(ctx, spec, violation),
        sample_sql=_sample_query(ctx, spec, violation, (str(spec.params["column"]),)),
        threshold=f"every value exists in {to_table}",
    )


def _render_row_count_matches_reference(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    reference_table = str(spec.params["reference_table"])
    reference_date_column = quote_identifier(str(spec.params["reference_date_column"]))
    reference_where = spec.params.get("reference_where")
    tolerance = float(spec.params.get("tolerance_ratio", 0.0))
    current_literal = f"DATE {quote_literal(ctx.partition_date.isoformat())}"
    reference_filter = f" AND ({reference_where})" if reference_where else ""

    filters = [scope_predicate(ctx)]
    if spec.where:
        filters.append(f"({spec.where})")

    sql = (
        "SELECT CASE WHEN reference_row_count = 0 THEN -1\n"
        f"            WHEN abs(CAST(target_row_count AS DOUBLE) / reference_row_count - 1) > {tolerance} THEN 1\n"
        "            ELSE 0 END AS failed_rows,\n"
        "       CASE WHEN reference_row_count = 0 THEN NULL\n"
        "            ELSE CAST(target_row_count AS DOUBLE) / reference_row_count - 1 END AS observed\n"
        "FROM (\n"
        f"  SELECT (SELECT count(*) FROM {table_ref(ctx)} WHERE {' AND '.join(filters)}) AS target_row_count,\n"
        f"         (SELECT count(*) FROM {reference_table} "
        f"WHERE CAST({reference_date_column} AS DATE) = {current_literal}{reference_filter}) AS reference_row_count\n"
        ") AS counts"
    )
    return RenderedTest(
        spec=spec,
        test_key=f"row_count_matches_reference[{reference_table}]",
        sql=sql,
        sample_sql=None,
        threshold=f"|target/reference - 1| <= {tolerance}",
        needs_baseline=True,
    )


RENDERERS = {
    "primary_key_not_null": _render_primary_key_not_null,
    "primary_key_unique": _render_primary_key_unique,
    "row_count_min": _render_row_count_min,
    "row_count_growth": _render_row_count_growth,
    "freshness": _render_freshness,
    "not_null": _render_not_null,
    "null_share_below": _render_null_share_below,
    "unique_combination": _render_unique_combination,
    "accepted_values": _render_accepted_values,
    "not_accepted_values": _render_not_accepted_values,
    "accepted_range": _render_accepted_range,
    "non_negative": _render_non_negative,
    "string_not_blank": _render_string_not_blank,
    "distinct_count_between": _render_distinct_count_between,
    "columns_sum_equals": _render_columns_sum_equals,
    "row_count_matches_reference": _render_row_count_matches_reference,
    "expression_is_true": _render_expression_is_true,
    "relationships": _render_relationships,
}


def render(spec: TestSpec, ctx: RenderContext) -> RenderedTest:
    try:
        renderer = RENDERERS[spec.name]
    except KeyError as error:
        raise KeyError(f"Нет рендерера для DQ-теста {spec.name!r}") from error
    return renderer(spec, ctx)
