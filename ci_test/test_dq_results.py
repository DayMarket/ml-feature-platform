import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext, load_dq_settings
from dq.results_writer import (
    RunMeta,
    build_rows,
    catalog_properties,
    results_catalog_name,
    results_table_ref,
    run_iceberg_commit_with_retry,
)
import dq.results_writer as results_writer
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

META = RunMeta(
    dag_id="feature-platform.layers.silver.sku_group_id.sku_group_id_prices",
    task_id="dq",
    run_id="scheduled__2026-08-19T01:00:00+00:00",
    try_number=1,
    run_ts=datetime(2026, 8, 20, 1, 5, tzinfo=timezone.utc),
)


def test_results_table_ref_comes_from_config() -> None:
    assert results_table_ref(Path(".")) == ("silver", "feature_platform_dq_results")


def test_build_rows_maps_every_field() -> None:
    settings = load_dq_settings(
        {
            "table": {
                "catalog": "iceberg",
                "schema": "silver",
                "name": "feature_platform_sku_group_id_prices",
                "primary_key": "date,sku_group_id",
            }
        }
    )
    outcome = DqRunOutcome(
        results=[
            TestResult(
                "row_count_min", "row_count_min", "consistency", "failed", "error", 1, 0.0,
                "row_count > 0", 1500, "SELECT 1", sample="", params={"min_rows": 0},
            ),
        ],
        warmup_active=True,
    )
    rows = build_rows(outcome, CTX, settings, META)
    assert len(rows) == 1
    row = rows[0]
    assert row["date"] == date(2026, 8, 19)
    assert row["dag_id"] == META.dag_id
    assert row["task_id"] == "dq"
    assert row["try_number"] == 1
    assert row["catalog"] == "dwh-iceberg"
    assert row["schema_name"] == "silver"
    assert row["table_name"] == "feature_platform_sku_group_id_prices"
    assert row["team"] == "team:search"
    assert row["test_family"] == "consistency"
    assert row["status"] == "failed"
    assert row["failed_rows"] == 1
    assert row["warmup_active"] is True
    assert json.loads(row["params"])["min_rows"] == 0


def test_build_rows_keeps_params_of_duplicate_test_names_apart() -> None:
    settings = load_dq_settings(
        {"table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"}}
    )
    outcome = DqRunOutcome(
        results=[
            TestResult(
                "expression_is_true", "expression_is_true[a <= b]", "row_expr", "failed", "error", 1, 1.0,
                "(a <= b) IS TRUE", 100, "SELECT a", params={"expression": "a <= b"},
            ),
            TestResult(
                "expression_is_true", "expression_is_true[c <= d]", "row_expr", "failed", "error", 2, 2.0,
                "(c <= d) IS TRUE", 100, "SELECT c", params={"expression": "c <= d"},
            ),
        ]
    )
    rows = build_rows(outcome, CTX, settings, META)
    assert json.loads(rows[0]["params"])["expression"] == "a <= b"
    assert json.loads(rows[1]["params"])["expression"] == "c <= d"


def test_build_rows_is_empty_when_run_skipped_by_active_from() -> None:
    settings = load_dq_settings(
        {
            "table": {"catalog": "iceberg", "schema": "silver", "name": "t", "primary_key": "date,sku_group_id"},
            "dq": {"active_from": "2026-09-01"},
        }
    )
    outcome = DqRunOutcome(skipped_by_active_from=True)
    assert build_rows(outcome, CTX, settings, META) == []


def test_results_catalog_name_comes_from_config() -> None:
    assert results_catalog_name(Path(".")) == "iceberg"


def test_catalog_properties_are_complete() -> None:
    """pyiceberg не наследует конфигурацию Spark: без явных свойств load_catalog
    падает с 'URI missing ... PYICEBERG_CATALOG__ICEBERG__URI'."""
    properties = catalog_properties("key-id", "secret")
    assert properties["type"] == "hive"
    assert properties["uri"].startswith("thrift://")
    assert properties["warehouse"].startswith("s3a://")
    assert properties["s3.access-key-id"] == "key-id"
    assert properties["s3.secret-access-key"] == "secret"
    assert properties["s3.path-style-access"] == "true"
    assert all(str(value) for value in properties.values())


def test_build_rows_carries_owning_team() -> None:
    """Команда-владелец таблицы едет в результаты, чтобы Superset мог фильтровать по ней."""
    settings = load_dq_settings({"table": {"catalog": "iceberg", "schema": "silver",
                                           "name": "t", "primary_key": "date,id"}})
    ctx = RenderContext(
        catalog_alias="dwh-iceberg", schema="silver", table="t", primary_key=("date", "id"),
        partition_column="date", partition_date=date(2026, 8, 19), scope="partition",
        sample_rows=5, team="team:recsys",
    )
    outcome = DqRunOutcome(results=[
        TestResult("row_count_min", "row_count_min", "consistency", "passed", "error", 0, 1.0,
                   "row_count > 0", 10, "SELECT 1"),
    ])
    assert build_rows(outcome, ctx, settings, META)[0]["team"] == "team:recsys"


def test_iceberg_commit_retry_refreshes_after_concurrent_commit(monkeypatch) -> None:
    class CommitFailedException(Exception):
        pass

    calls = []
    sleeps = []

    def operation():
        calls.append(len(calls) + 1)
        if len(calls) < 3:
            raise CommitFailedException("branch main has changed")
        return "committed"

    monkeypatch.setattr(
        results_writer,
        "_commit_failed_exception_type",
        lambda: CommitFailedException,
    )
    result = run_iceberg_commit_with_retry(
        operation,
        "test commit",
        attempts=3,
        sleep_fn=sleeps.append,
        jitter_fn=lambda lower, upper: upper,
    )

    assert result == "committed"
    assert calls == [1, 2, 3]
    assert sleeps == [1.0, 2.0]


def main() -> int:
    test_results_table_ref_comes_from_config()
    test_results_catalog_name_comes_from_config()
    test_catalog_properties_are_complete()
    test_build_rows_maps_every_field()
    test_build_rows_carries_owning_team()
    # Retry проверяется pytest-тестом с monkeypatch и поэтому не вызывается здесь.
    test_build_rows_keeps_params_of_duplicate_test_names_apart()
    test_build_rows_is_empty_when_run_skipped_by_active_from()
    print("DQ results tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
