import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from dq.report import format_alert, format_log
from dq.runner import DqRunOutcome, TestResult

CTX = RenderContext(
    catalog_alias="dwh-iceberg",
    schema="silver",
    table="feature_platform_sku_group_id_prices",
    primary_key=("date", "sku_group_id"),
    partition_column="date",
    partition_date=date(2026, 8, 19),
    scope="partition",
    sample_rows=5,
)


def make_outcome() -> DqRunOutcome:
    return DqRunOutcome(
        results=[
            TestResult(
                "primary_key_not_null", "primary_key_not_null", "null_checks", "passed", "error",
                0, 0.0, "0 rows", 1200, "SELECT 1",
            ),
            TestResult(
                "primary_key_unique", "primary_key_unique", "uniqueness", "failed", "error",
                1843, 1843.0, "0 duplicate key groups", 8700, "SELECT dup",
                sample="(2026-08-19, 118823, 2)",
            ),
            TestResult(
                "accepted_range", "accepted_range[price]", "domain_values", "warned", "warn",
                12, 12.0, "0 <= value <= 1e9", 900, "SELECT rng",
                sample="(2026-08-19, 940112)",
            ),
            TestResult(
                "row_count_growth", "row_count_growth", "consistency", "skipped", "error",
                0, None, "|growth| <= 0.2", 400, "SELECT growth",
                skip_reason="no baseline data for 2026-08-18",
            ),
        ]
    )


def test_log_contains_summary_and_failure_details() -> None:
    text = format_log(make_outcome(), CTX)
    assert "dwh-iceberg.silver.feature_platform_sku_group_id_prices" in text
    assert "date=2026-08-19" in text
    assert "warmup: off" in text
    assert "PASS  primary_key_not_null" in text
    assert "FAIL  primary_key_unique" in text
    assert "WARN  accepted_range[price]" in text
    assert "SKIP  row_count_growth" in text
    assert "no baseline data for 2026-08-18" in text
    assert "--- FAIL primary_key_unique ---" in text
    assert "samples   : (2026-08-19, 118823, 2)" in text
    assert "SELECT dup" in text


def test_log_marks_active_warmup() -> None:
    outcome = make_outcome()
    outcome.warmup_active = True
    assert "warmup: ACTIVE" in format_log(outcome, CTX)


def test_alert_lists_only_failed_and_warned_and_respects_limit() -> None:
    text = format_alert(make_outcome(), CTX, "https://airflow/log/1", limit=4000)
    assert "feature_platform_sku_group_id_prices" in text
    assert "primary_key_unique" in text
    assert "https://airflow/log/1" in text
    assert "primary_key_not_null" not in text

    trimmed = format_alert(make_outcome(), CTX, "https://airflow/log/1", limit=200)
    assert len(trimmed) <= 200
    assert "… отчёт обрезан" in trimmed
    assert "https://airflow/log/1" in trimmed


def main() -> int:
    test_log_contains_summary_and_failure_details()
    test_log_marks_active_warmup()
    test_alert_lists_only_failed_and_warned_and_respects_limit()
    print("DQ report tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
