"""Исполнение DQ-тестов и сборка результатов."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from dq.config import DqSettings, RenderContext
from dq.tests import (
    baseline_description,
    partition_expression,
    partition_literal,
    quote_identifier,
    quote_literal,
    render,
    table_ref,
)

Query = Callable[[str], list]


class DqPreflightError(RuntimeError):
    """Таблица недоступна до начала проверок."""


@dataclass(frozen=True)
class TestResult:
    name: str
    test_key: str
    family: str
    status: str
    severity: str
    failed_rows: int
    observed: float | None
    threshold: str
    duration_ms: int
    sql: str
    sample: str = ""
    skip_reason: str = ""
    params: dict = field(default_factory=dict)


@dataclass
class DqRunOutcome:
    results: list[TestResult] = field(default_factory=list)
    warmup_active: bool = False
    skipped_by_active_from: bool = False

    @property
    def has_errors(self) -> bool:
        return any(result.status == "failed" for result in self.results)


def preflight(query: Query, ctx: RenderContext) -> None:
    # information_schema резолвится в дефолтном каталоге соединения, а он у trino_*
    # смотрит в hive. Квалифицируем каталогом таблицы, иначе preflight падает
    # с CATALOG_NOT_FOUND ещё до первого теста.
    sql = (
        f"SELECT count(*) FROM {quote_identifier(ctx.catalog_alias)}.information_schema.tables\n"
        f"WHERE table_schema = {quote_literal(ctx.schema)} AND table_name = {quote_literal(ctx.table)}"
    )
    rows = query(sql)
    if not rows or int(rows[0][0]) == 0:
        raise DqPreflightError(
            f"Таблица {ctx.catalog_alias}.{ctx.schema}.{ctx.table} не найдена в каталоге Trino. "
            "DQ запускается только после того, как миграции применены; проверьте, что миграция "
            "этой энтити проехала на master."
        )


def _partition_history_count(query: Query, ctx: RenderContext) -> int:
    # Для снапшотной энтити «прогретость» считается в снапшотах, а не в днях:
    # у неё за сутки набегает несколько партиций, и warmup_days: 1 означал бы
    # «прогреться за один снапшот».
    partition_expr = partition_expression(ctx)
    sql = (
        f"SELECT COUNT(DISTINCT {partition_expr})\n"
        f"FROM {table_ref(ctx)}\n"
        f"WHERE {partition_expr} < {partition_literal(ctx)}"
    )
    rows = query(sql)
    return int(rows[0][0]) if rows and rows[0][0] is not None else 0


def _format_sample(rows: list) -> str:
    if not rows:
        return ""
    return " | ".join("(" + ", ".join(str(value) for value in row) + ")" for row in rows)


def run_dq(settings: DqSettings, ctx: RenderContext, query: Query) -> DqRunOutcome:
    outcome = DqRunOutcome()
    if not settings.enabled:
        return outcome

    preflight(query, ctx)

    if settings.active_from is not None and ctx.partition_date < settings.active_from:
        outcome.skipped_by_active_from = True
        return outcome

    if settings.warmup_days > 0:
        outcome.warmup_active = _partition_history_count(query, ctx) < settings.warmup_days

    for spec in settings.tests:
        rendered = render(spec, ctx)
        started = time.monotonic()
        rows = query(rendered.sql)
        duration_ms = int((time.monotonic() - started) * 1000)

        failed_rows = int(rows[0][0]) if rows and rows[0][0] is not None else 0
        observed = float(rows[0][1]) if rows and len(rows[0]) > 1 and rows[0][1] is not None else None

        if failed_rows < 0:
            baseline = baseline_description(ctx)
            outcome.results.append(
                _result(
                    spec,
                    rendered,
                    "skipped",
                    spec.severity,
                    0,
                    observed,
                    duration_ms,
                    skip_reason=f"no baseline data for {baseline}",
                )
            )
            continue

        if failed_rows == 0:
            outcome.results.append(_result(spec, rendered, "passed", spec.severity, 0, observed, duration_ms))
            continue

        sample = ""
        if rendered.sample_sql:
            sample = _format_sample(query(rendered.sample_sql))

        effective_severity = "warn" if outcome.warmup_active else spec.severity
        status = "failed" if effective_severity == "error" else "warned"
        outcome.results.append(
            _result(spec, rendered, status, effective_severity, failed_rows, observed, duration_ms, sample=sample)
        )

    return outcome


def _result(
    spec: Any,
    rendered: Any,
    status: str,
    severity: str,
    failed_rows: int,
    observed: float | None,
    duration_ms: int,
    sample: str = "",
    skip_reason: str = "",
) -> TestResult:
    return TestResult(
        name=spec.name,
        test_key=rendered.test_key,
        family=spec.family,
        status=status,
        severity=severity,
        failed_rows=failed_rows,
        observed=observed,
        threshold=rendered.threshold,
        duration_ms=duration_ms,
        sql=rendered.sql,
        sample=sample,
        skip_reason=skip_reason,
        params=dict(spec.params),
    )
