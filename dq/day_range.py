"""DQ всех дней диапазона при неизменном current Iceberg snapshot владельца."""

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import math
import time

from dq.config import DqConfigError, load_dq_settings
from dq.results_writer import RunMeta, load_results_catalog, write_results
from dq.runner import DqPreflightError, TestResult, run_dq
from dq.task import DqTestsFailed, build_render_context
from dq.tests import quote_identifier, quote_literal, scope_predicate, table_ref

REQUIRED_TESTS = {"primary_key_not_null", "primary_key_unique", "row_count_min"}


def capture_time(receipt):
    """Преобразовать время дневной записи в aware UTC timestamp."""
    try:
        moment = datetime.fromisoformat(receipt["ingested_at"].replace("Z", "+00:00"))
        if moment.utcoffset() is None:
            raise ValueError("Нет зоны")
        return moment.astimezone(timezone.utc)
    except (KeyError, AttributeError, TypeError, ValueError) as error:
        raise DqConfigError("ingested_at требует timestamp с timezone") from error


def validate_written(written):
    """Проверить минимальный контракт writer receipt диапазона."""
    if not isinstance(written, dict) or written.get("status") != "written":
        raise DqConfigError("Диапазон требует receipt written")
    for key in ("request_id", "table_uuid"):
        if not isinstance(written.get(key), str) or not written[key].strip():
            raise DqConfigError(f"Нет {key} диапазона")
    if type(written.get("snapshot_id")) is not int or written["snapshot_id"] <= 0:
        raise DqConfigError("Нет точного snapshot диапазона")
    values = written.get("dates")
    receipts = written.get("day_receipts")
    if (
        not isinstance(values, list)
        or not values
        or not isinstance(receipts, list)
        or len(values) != len(receipts)
    ):
        raise DqConfigError("Нужны непустые даты и receipt каждого дня")
    try:
        days = [date.fromisoformat(value) for value in values]
    except (TypeError, ValueError) as error:
        raise DqConfigError("Даты диапазона должны быть ISO DATE") from error
    if days != sorted(set(days)) or [day.isoformat() for day in days] != values:
        raise DqConfigError("Даты должны возрастать без дублей")
    for day, receipt in zip(values, receipts, strict=True):
        if (
            not isinstance(receipt, dict)
            or receipt.get("status") != "written"
            or receipt.get("date") != day
            or receipt.get("table_uuid") != written["table_uuid"]
            or type(receipt.get("rows_written")) is not int
            or receipt["rows_written"] <= 0
        ):
            raise DqConfigError("Дневной receipt не соответствует диапазону")
        for key in ("source_manifest_id", "source_contract_version"):
            if not isinstance(receipt.get(key), str) or not receipt[key].strip():
                raise DqConfigError(f"Нет {key} дневной записи")
        capture_time(receipt)
    return days


def validate_settings(settings):
    """Range-режим не применяет freshness/growth календарного времени к истории."""
    if (
        not settings.enabled
        or settings.scope != "partition"
        or settings.partition_granularity != "date"
        or settings.warmup_days != 0
        or settings.active_from is not None
    ):
        raise DqConfigError(
            "Range DQ требует daily partition, enabled, warmup_days=0 и отсутствие active_from"
        )
    by_name = {spec.name: spec for spec in settings.tests}
    if not REQUIRED_TESTS.issubset(by_name):
        raise DqConfigError("Range DQ требует ключ, уникальность и минимальный объём")
    if any(spec.severity != "error" for spec in settings.tests):
        raise DqConfigError("Range DQ не допускает warn-проверки")
    if any(spec.where for spec in settings.tests if spec.name in REQUIRED_TESTS):
        raise DqConfigError("Базовые проверки диапазона не исключают строки через where")


def settings_for_day(settings, day, receipt):
    """Freshness/growth нужны для вчерашнего дня на момент записи, включая retry DQ."""
    if day == capture_time(receipt).date() - timedelta(days=1):
        return settings
    return replace(settings, tests=tuple(
        spec for spec in settings.tests if spec.name not in {"freshness", "row_count_growth"}
    ))


def verify_current(config, written, catalog):
    """Не принимать DQ, если current snapshot сменился после записи диапазона."""
    table_config = config["table"]
    identifier = (table_config["schema"], table_config["name"])
    if catalog.name != table_config["catalog"] or not catalog.table_exists(identifier):
        raise DqConfigError("Нет целевой Iceberg таблицы диапазона")
    table = catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    if (
        str(table.metadata.table_uuid) != written["table_uuid"]
        or snapshot is None
        or snapshot.snapshot_id != written["snapshot_id"]
    ):
        raise DqConfigError("Current snapshot сменился после записи; повторите owner DAG")


def write_matches_sql(ctx, receipt):
    """Сверить число строк и provenance дня с writer receipt."""
    moment = capture_time(receipt).strftime("%Y-%m-%d %H:%M:%S.%f")
    conditions = [
        f"{quote_identifier(key)} IS DISTINCT FROM {quote_literal(receipt[key])}"
        for key in ("source_manifest_id", "source_contract_version")
    ]
    conditions.append(
        "with_timezone(CAST(\"ingested_at\" AS TIMESTAMP(6)), 'UTC') "
        f"IS DISTINCT FROM TIMESTAMP '{moment} UTC'"
    )
    return (
        f"SELECT CASE WHEN count(*) <> {receipt['rows_written']} OR "
        f"count_if({' OR '.join(conditions)}) > 0 THEN 1 ELSE 0 END AS failed_rows,\n"
        "       CAST(count(*) AS DOUBLE) AS observed\n"
        f"FROM {table_ref(ctx)} WHERE {scope_predicate(ctx)}"
    )


def strict_query(query):
    """Не позволить range-DQ принять пустой или повреждённый ответ как успех."""
    def checked(sql):
        rows = query(sql)
        if "AS failed_rows" not in sql:
            return rows
        if (
            not isinstance(rows, (list, tuple))
            or len(rows) != 1
            or not isinstance(rows[0], (list, tuple))
            or len(rows[0]) != 2
        ):
            raise DqPreflightError("Нужна одна строка DQ (failed_rows, observed)")
        failed, observed = rows[0]
        minimum = -1 if "previous_row_count" in sql else 0
        if type(failed) is not int or failed < minimum:
            raise DqPreflightError("Некорректный failed_rows DQ")
        if observed is not None and (
            type(observed) not in (int, float) or not math.isfinite(observed)
        ):
            raise DqPreflightError("Некорректный observed DQ")
        return rows

    return checked


def run_range_dq(config, repo_root, written, query, meta: RunMeta, *, catalog=None):
    """Проверить и сохранить DQ каждого дня одной терминальной задачей."""
    days = validate_written(written)
    settings = load_dq_settings(config)
    validate_settings(settings)
    if (
        meta.task_id != "dq"
        or not meta.dag_id
        or not meta.run_id
        or type(meta.try_number) is not int
        or meta.try_number <= 0
    ):
        raise DqConfigError("Нужны точные DAG/run/task=dq")
    root = Path(repo_root)
    catalog = catalog or load_results_catalog(config["table"]["catalog"])
    checked_query = strict_query(query)
    checks = []
    for day, receipt in zip(days, written["day_receipts"], strict=True):
        verify_current(config, written, catalog)
        ctx = build_render_context(config, root, day.isoformat())
        day_settings = settings_for_day(settings, day, receipt)
        outcome = run_dq(day_settings, ctx, checked_query)
        sql = write_matches_sql(ctx, receipt)
        started = time.monotonic()
        rows = checked_query(sql)
        failed, observed = rows[0]
        outcome.results.append(
            TestResult(
                name="range_write_matches",
                test_key="range_write_matches",
                family="consistency",
                status="failed" if failed else "passed",
                severity="error",
                failed_rows=failed,
                observed=observed,
                threshold="exact writer count/manifest/version/capture",
                duration_ms=int((time.monotonic() - started) * 1000),
                sql=sql,
            )
        )
        verify_current(config, written, catalog)
        write_results(root, outcome, ctx, day_settings, meta)
        verify_current(config, written, catalog)
        if outcome.has_errors or any(result.status != "passed" for result in outcome.results):
            raise DqTestsFailed(f"Range DQ не пройден за {day}")
        check = {
            "date": day.isoformat(),
            "request_id": written["request_id"],
            "snapshot_id": written["snapshot_id"],
            "table_uuid": written["table_uuid"],
            "source_manifest_id": receipt["source_manifest_id"],
            "rows_checked": receipt["rows_written"],
            "dq_status": "passed",
            "dag_id": meta.dag_id,
            "run_id": meta.run_id,
        }
        if "source_day" in receipt:
            check["source_day"] = receipt["source_day"]
        checks.append(check)
    return checks
