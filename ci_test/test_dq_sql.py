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


def test_not_null_without_tolerance() -> None:
    rendered = render(spec("not_null", columns=["orders_cnt", "price_avg"], max_null_share=None), CTX)
    assert rendered.test_key == "not_null[orders_cnt,price_avg]"
    assert '"orders_cnt" IS NULL OR "price_avg" IS NULL' in rendered.sql


def test_not_null_with_tolerance_zeroes_failed_rows_below_share() -> None:
    rendered = render(spec("not_null", columns=["orders_cnt"], max_null_share=0.01), CTX)
    assert "> 0.01" in rendered.sql
    assert "ELSE 0 END AS failed_rows" in rendered.sql


def test_accepted_values_quotes_strings() -> None:
    rendered = render(spec("accepted_values", column="platform", values=["ios", "o'reilly"], ignore_nulls=True), CTX)
    assert "'o''reilly'" in rendered.sql
    assert '"platform" NOT IN' in rendered.sql
    assert '"platform" IS NOT NULL' in rendered.sql


def test_not_accepted_values() -> None:
    rendered = render(spec("not_accepted_values", column="platform", values=["unknown"], ignore_nulls=True), CTX)
    assert '"platform" IN (\'unknown\')' in rendered.sql


def test_accepted_range_inclusive_flags() -> None:
    rendered = render(
        spec("accepted_range", column="conversion_rate", min=0, max=1, min_inclusive=True, max_inclusive=False, ignore_nulls=True),
        CTX,
    )
    assert '"conversion_rate" < 0' in rendered.sql
    assert '"conversion_rate" >= 1' in rendered.sql
    assert rendered.test_key == "accepted_range[conversion_rate]"


def test_non_negative() -> None:
    rendered = render(spec("non_negative", columns=["orders_cnt"], ignore_nulls=True), CTX)
    assert '"orders_cnt" < 0' in rendered.sql


def test_null_share_below() -> None:
    rendered = render(spec("null_share_below", column="price_avg", max_share=0.05), CTX)
    assert "> 0.05" in rendered.sql


def test_string_not_blank() -> None:
    rendered = render(spec("string_not_blank", columns=["query"]), CTX)
    assert "trim(\"query\") = ''" in rendered.sql


def test_unique_combination() -> None:
    rendered = render(spec("unique_combination", columns=["date", "query"]), CTX)
    assert rendered.test_key == "unique_combination[date,query]"
    assert 'GROUP BY "date", "query"' in rendered.sql


def test_distinct_count_between() -> None:
    rendered = render(spec("distinct_count_between", columns=["sku_group_id"], min=1000, max=None), CTX)
    assert "distinct_count < 1000" in rendered.sql
    assert rendered.sample_sql is None


def test_columns_sum_equals() -> None:
    rendered = render(spec("columns_sum_equals", parts=["a", "b"], total="total_cnt", tolerance=1e-6), CTX)
    assert 'abs(("a" + "b") - "total_cnt") > 1e-06' in rendered.sql


def test_expression_is_true_treats_null_as_failure() -> None:
    rendered = render(spec("expression_is_true", expression="min_price <= max_price"), CTX)
    assert "(min_price <= max_price) IS NOT TRUE" in rendered.sql


def test_relationships_uses_not_exists() -> None:
    rendered = render(
        spec(
            "relationships",
            column="sku_group_id",
            to_table='"dwh-iceberg"."silver"."sku_dim"',
            to_column="sku_group_id",
            where=None,
        ),
        CTX,
    )
    assert "NOT EXISTS" in rendered.sql
    assert '"dwh-iceberg"."silver"."sku_dim"' in rendered.sql
    assert "target.\"sku_group_id\"" in rendered.sql


def test_row_count_matches_reference_skips_without_reference_rows() -> None:
    rendered = render(
        spec(
            "row_count_matches_reference",
            reference_table='"dwh-iceberg"."silver"."upstream"',
            reference_date_column="event_date",
            reference_where=None,
            tolerance_ratio=0.0,
        ),
        CTX,
    )
    assert rendered.needs_baseline is True
    assert "WHEN reference_row_count = 0 THEN -1" in rendered.sql


def test_every_configured_test_has_a_renderer() -> None:
    from dq.config import TEST_PARAMS
    from dq.tests import RENDERERS

    assert set(RENDERERS) == set(TEST_PARAMS)


def main() -> int:
    test_primary_key_not_null()
    test_primary_key_unique()
    test_row_count_min_uses_non_strict_comparison()
    test_row_count_growth_two_sided_and_skips_without_baseline()
    test_row_count_growth_one_sided_up()
    test_freshness_is_table_wide()
    test_scope_table_drops_partition_filter()
    test_quote_literal_escapes_quotes_and_unicode()
    test_not_null_without_tolerance()
    test_not_null_with_tolerance_zeroes_failed_rows_below_share()
    test_accepted_values_quotes_strings()
    test_not_accepted_values()
    test_accepted_range_inclusive_flags()
    test_non_negative()
    test_null_share_below()
    test_string_not_blank()
    test_unique_combination()
    test_distinct_count_between()
    test_columns_sum_equals()
    test_expression_is_true_treats_null_as_failure()
    test_relationships_uses_not_exists()
    test_row_count_matches_reference_skips_without_reference_rows()
    test_every_configured_test_has_a_renderer()
    print("DQ SQL tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
