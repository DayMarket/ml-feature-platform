import sys
from dataclasses import replace
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext, load_dq_settings
from dq.runner import DqPreflightError, run_dq

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

BASE_CONFIG = {
    "table": {
        "catalog": "iceberg",
        "schema": "silver",
        "name": "feature_platform_sku_group_id_prices",
        "primary_key": "date,sku_group_id",
    },
}


class FakeQuery:
    """Отдаёт заранее заданные ответы, сопоставляя их по подстроке в SQL."""

    def __init__(self, answers: list[tuple[str, list]], default: list | None = None) -> None:
        self.answers = answers
        self.default = default if default is not None else [(0, 0.0)]
        self.executed: list[str] = []

    def __call__(self, sql: str) -> list:
        self.executed.append(sql)
        for needle, rows in self.answers:
            if needle in sql:
                return rows
        return self.default


def test_all_passing_run() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery([("information_schema.tables", [(1,)]), ("COUNT(DISTINCT", [(30,)])])
    outcome = run_dq(settings, CTX, query)
    assert outcome.warmup_active is False
    assert [result.status for result in outcome.results] == ["passed"] * 5


def test_failed_test_carries_sample_and_severity() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery(
        [
            ("information_schema.tables", [(1,)]),
            ("COUNT(DISTINCT", [(30,)]),
            ("HAVING count(*) > 1\n) AS duplicates", [(3, 3.0)]),
            ("HAVING count(*) > 1\nLIMIT", [("2026-08-19", 118823, 2)]),
        ]
    )
    outcome = run_dq(settings, CTX, query)
    failed = [result for result in outcome.results if result.status == "failed"]
    assert len(failed) == 1
    assert failed[0].test_key == "primary_key_unique"
    assert failed[0].failed_rows == 3
    assert failed[0].severity == "error"
    assert "118823" in failed[0].sample


def test_negative_failed_rows_becomes_skipped() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery(
        [
            ("information_schema.tables", [(1,)]),
            ("COUNT(DISTINCT", [(30,)]),
            ("previous_row_count", [(-1, None)]),
        ]
    )
    outcome = run_dq(settings, CTX, query)
    growth = [result for result in outcome.results if result.test_key == "row_count_growth"][0]
    assert growth.status == "skipped"
    assert "2026-08-18" in growth.skip_reason


def test_warm_up_downgrades_error_to_warn() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery(
        [
            ("information_schema.tables", [(1,)]),
            ("COUNT(DISTINCT", [(0,)]),
            ("HAVING count(*) > 1\n) AS duplicates", [(3, 3.0)]),
            ("HAVING count(*) > 1\nLIMIT", []),
        ]
    )
    outcome = run_dq(settings, CTX, query)
    assert outcome.warmup_active is True
    unique = [result for result in outcome.results if result.test_key == "primary_key_unique"][0]
    assert unique.status == "warned"


def test_warn_severity_never_fails() -> None:
    config = {
        **BASE_CONFIG,
        "dq": {"tests": [{"name": "non_negative", "columns": ["orders_cnt"], "severity": "warn"}]},
    }
    settings = load_dq_settings(config)
    query = FakeQuery(
        [("information_schema.tables", [(1,)]), ("COUNT(DISTINCT", [(30,)]), ('"orders_cnt" < 0', [(7, 7.0)])]
    )
    outcome = run_dq(settings, CTX, query)
    non_negative = [result for result in outcome.results if result.name == "non_negative"][0]
    assert non_negative.status == "warned"


def test_active_from_skips_whole_run() -> None:
    config = {**BASE_CONFIG, "dq": {"active_from": "2026-09-01"}}
    settings = load_dq_settings(config)
    query = FakeQuery([("information_schema.tables", [(1,)])])
    outcome = run_dq(settings, CTX, query)
    assert outcome.skipped_by_active_from is True
    assert outcome.results == []


def test_preflight_query_is_catalog_qualified() -> None:
    """Соединение Trino может смотреть в чужой дефолтный каталог (hive),
    поэтому information_schema обязана быть квалифицирована каталогом таблицы."""
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery([("information_schema.tables", [(1,)]), ("COUNT(DISTINCT", [(30,)])])
    run_dq(settings, CTX, query)
    preflight_sql = query.executed[0]
    assert '"dwh-iceberg".information_schema.tables' in preflight_sql, preflight_sql


def test_missing_table_raises_diagnostic_error() -> None:
    settings = load_dq_settings(BASE_CONFIG)
    query = FakeQuery([("information_schema.tables", [(0,)])])
    try:
        run_dq(settings, CTX, query)
    except DqPreflightError as error:
        message = str(error)
        assert "dwh-iceberg" in message
        assert "feature_platform_sku_group_id_prices" in message
        assert "миграц" in message
    else:
        raise AssertionError("preflight must fail when the table is missing")


def test_table_scope_never_counts_partitions() -> None:
    # Защита в глубину к запрету из load_dq_settings: RenderContext может быть
    # собран в обход конфига, а COUNT(DISTINCT ...) по колонке партиции на
    # беспартиционной таблице роняет весь ран на COLUMN_NOT_FOUND.
    settings = load_dq_settings(
        {
            "table": {
                "catalog": "iceberg",
                "schema": "gold",
                "name": "feature_platform_search_query_id",
                "primary_key": "query_text,version",
            },
            "dq": {
                "scope": "table",
                "warmup_days": 0,
                "tests": [
                    {"name": "freshness", "enabled": False},
                    {"name": "row_count_growth", "enabled": False},
                ],
            },
        }
    )
    settings = replace(settings, warmup_days=7)
    ctx = RenderContext(**{**CTX.__dict__, "scope": "table"})
    query = FakeQuery([("information_schema.tables", [(1,)])])
    outcome = run_dq(settings, ctx, query)
    assert outcome.warmup_active is False
    assert not any("COUNT(DISTINCT" in sql for sql in query.executed)


def main() -> int:
    test_all_passing_run()
    test_failed_test_carries_sample_and_severity()
    test_negative_failed_rows_becomes_skipped()
    test_warm_up_downgrades_error_to_warn()
    test_warn_severity_never_fails()
    test_active_from_skips_whole_run()
    test_preflight_query_is_catalog_qualified()
    test_missing_table_raises_diagnostic_error()
    test_table_scope_never_counts_partitions()
    print("DQ runner tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
