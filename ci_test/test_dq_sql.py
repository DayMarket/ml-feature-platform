import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext, TestSpec
from dq.tests import quote_literal, render

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

TABLE = '"dwh-iceberg"."silver"."feature_platform_sku_group_id_prices"'


def spec(name: str, **params) -> TestSpec:
    from dq.config import TEST_FAMILIES

    severity = params.pop("severity", "error")
    where = params.pop("where", None)
    return TestSpec(name=name, family=TEST_FAMILIES[name], params=params, severity=severity, where=where)


def test_primary_key_not_null() -> None:
    rendered = render(spec("primary_key_not_null"), CTX)
    assert rendered.test_key == "primary_key_not_null"
    assert TABLE in rendered.sql
    assert '"date" IS NULL OR "sku_group_id" IS NULL' in rendered.sql
    assert '"date" = DATE \'2026-08-19\'' in rendered.sql
    assert rendered.sample_sql is not None
    assert "LIMIT 5" in rendered.sample_sql
    assert rendered.needs_baseline is False


def test_primary_key_unique() -> None:
    rendered = render(spec("primary_key_unique"), CTX)
    assert 'GROUP BY "date", "sku_group_id"' in rendered.sql
    assert "HAVING count(*) > 1" in rendered.sql


def test_row_count_min_uses_non_strict_comparison() -> None:
    rendered = render(spec("row_count_min", min_rows=0), CTX)
    # Дословная семантика dbt-макроса row_count_greater_than_for_date: падение при row_count <= min_rows.
    assert "WHEN row_count <= 0 THEN 1" in rendered.sql
    assert rendered.sample_sql is None
    assert rendered.threshold == "row_count > 0"


def test_row_count_growth_two_sided_and_skips_without_baseline() -> None:
    rendered = render(spec("row_count_growth", max_growth_ratio=0.2, direction="both"), CTX)
    assert rendered.needs_baseline is True
    assert "WHEN previous_row_count = 0 THEN -1" in rendered.sql
    assert "current_row_count > previous_row_count * 1.2" in rendered.sql
    assert "current_row_count < previous_row_count * 0.8" in rendered.sql
    assert "DATE '2026-08-18'" in rendered.sql


def test_row_count_growth_one_sided_up() -> None:
    rendered = render(spec("row_count_growth", max_growth_ratio=0.2, direction="up"), CTX)
    assert "current_row_count > previous_row_count * 1.2" in rendered.sql
    assert "current_row_count < previous_row_count * 0.8" not in rendered.sql


def test_freshness_is_table_wide() -> None:
    rendered = render(spec("freshness", max_lag_days=2), CTX)
    assert 'max(CAST("date" AS DATE))' in rendered.sql
    assert "date_diff('day', max_partition, DATE '2026-08-19') > 2" in rendered.sql
    assert '"date" = DATE \'2026-08-19\'' not in rendered.sql


def test_scope_table_drops_partition_filter() -> None:
    ctx = RenderContext(**{**CTX.__dict__, "scope": "table"})
    rendered = render(spec("primary_key_unique"), ctx)
    assert '"date" = DATE' not in rendered.sql


def test_quote_literal_escapes_quotes_and_unicode() -> None:
    assert quote_literal("o'reilly") == "'o''reilly'"
    assert quote_literal("телефон") == "'телефон'"
    assert quote_literal(5) == "5"
    assert quote_literal(True) == "TRUE"


def main() -> int:
    test_primary_key_not_null()
    test_primary_key_unique()
    test_row_count_min_uses_non_strict_comparison()
    test_row_count_growth_two_sided_and_skips_without_baseline()
    test_row_count_growth_one_sided_up()
    test_freshness_is_table_wide()
    test_scope_table_drops_partition_filter()
    test_quote_literal_escapes_quotes_and_unicode()
    print("DQ SQL tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
