"""Форматирование результатов DQ для лога таски и для oncall-сообщения."""

from __future__ import annotations

from dq.config import RenderContext
from dq.runner import DqRunOutcome, TestResult

STATUS_LABEL = {
    "passed": "PASS",
    "failed": "FAIL",
    "warned": "WARN",
    "skipped": "SKIP",
    "errored": "ERR ",
}


def _table_fqn(ctx: RenderContext) -> str:
    return f"{ctx.catalog_alias}.{ctx.schema}.{ctx.table}"


def _summary_line(result: TestResult) -> str:
    label = STATUS_LABEL.get(result.status, result.status.upper())
    detail = result.skip_reason if result.status == "skipped" else f"{result.failed_rows} rows"
    return (
        f"{label}  {result.test_key:<34} {detail:<34} "
        f"{result.duration_ms / 1000:.1f}s  severity={result.severity}"
    )


def format_log(outcome: DqRunOutcome, ctx: RenderContext) -> str:
    warmup = "ACTIVE (severity error понижен до warn)" if outcome.warmup_active else "off"
    lines = [
        f"DQ  {_table_fqn(ctx)}  date={ctx.partition_date.isoformat()}  "
        f"scope={ctx.scope}  warmup: {warmup}"
    ]
    if outcome.skipped_by_active_from:
        lines.append("SKIP всего прогона: партиция раньше dq.active_from")
        return "\n".join(lines)

    lines.extend(_summary_line(result) for result in outcome.results)

    for result in outcome.results:
        if result.status not in ("failed", "warned"):
            continue
        lines.extend(
            [
                "",
                f"--- {STATUS_LABEL[result.status]} {result.test_key} ---",
                f"family    : {result.family}",
                f"threshold : {result.threshold}",
                f"observed  : {result.observed}",
                f"rows      : {result.failed_rows}",
                f"sql       : {result.sql}",
                f"samples   : {result.sample or '(нет — агрегатный тест)'}",
            ]
        )
    return "\n".join(lines)


def format_alert(outcome: DqRunOutcome, ctx: RenderContext, log_url: str, limit: int = 3500) -> str:
    problems = [result for result in outcome.results if result.status in ("failed", "warned")]
    failed = sum(1 for result in problems if result.status == "failed")
    warned = len(problems) - failed

    header = [
        f"DQ FAILED: {_table_fqn(ctx)}",
        f"партиция: {ctx.partition_date.isoformat()}  errors: {failed}  warnings: {warned}",
    ]
    if outcome.warmup_active:
        header.append("warmup активен — severity error понижен до warn")

    body = []
    for result in problems:
        body.append(
            f"{STATUS_LABEL[result.status]} {result.test_key}: {result.failed_rows} rows, "
            f"observed={result.observed}, threshold={result.threshold}"
        )
        if result.sample:
            body.append(f"    примеры: {result.sample}")

    footer = [f"лог: {log_url}"]

    text = "\n".join(header + body + footer)
    if len(text) <= limit:
        return text

    budget = limit - len("\n".join(header + footer)) - len("\n… отчёт обрезан\n")
    trimmed: list[str] = []
    used = 0
    for line in body:
        if used + len(line) + 1 > budget:
            break
        trimmed.append(line)
        used += len(line) + 1
    return "\n".join(header + trimmed + ["… отчёт обрезан"] + footer)[:limit]
