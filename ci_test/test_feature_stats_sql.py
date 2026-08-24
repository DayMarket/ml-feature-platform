import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from feature_stats.config import PERCENTILES, FeatureStatsConfigError, StatsContext
from feature_stats.query import (
    VALUES_PER_COLUMN,
    percentile_array_literal,
    render_columns_query,
    render_stats_query,
)

DAILY = StatsContext(
    render=RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_sku_group_price_features",
        primary_key=("date", "sku_group_id"),
        partition_column="date",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
    ),
    partition_ts=datetime(2026, 8, 22, tzinfo=timezone.utc),
)

SNAPSHOT = StatsContext(
    render=RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_dynamic_pricing_sku_group_price_features",
        primary_key=("calculated_at", "sku_group_id", "promotion_id"),
        partition_column="calculated_at",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
        partition_granularity="timestamp",
        partition_timestamp=datetime(2026, 8, 22, 6, 0, 0),
        snapshot_interval_hours=3,
    ),
    partition_ts=datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc),
)


def test_values_per_column_matches_the_select_list() -> None:
    # cnt, mean, min, max, pct — раннер режет строку результата по этому шагу.
    assert VALUES_PER_COLUMN == 5


def test_percentile_array_literal_matches_the_configured_set() -> None:
    assert percentile_array_literal() == "ARRAY[0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]"
    assert len(PERCENTILES) == 7


def test_columns_query_qualifies_information_schema_with_the_catalog() -> None:
    sql = render_columns_query(DAILY)
    # Дефолтный каталог соединений trino_* — hive; без квалификации это CATALOG_NOT_FOUND.
    assert '"dwh-iceberg".information_schema.columns' in sql
    assert "table_schema = 'gold'" in sql
    assert "table_name = 'feature_platform_sku_group_price_features'" in sql
    assert "ORDER BY ordinal_position" in sql


def test_stats_query_daily_partition() -> None:
    sql = render_stats_query(DAILY, ["sell_price_eod", "abs_discount"])
    assert '"dwh-iceberg"."gold"."feature_platform_sku_group_price_features"' in sql
    assert "count(*) AS rows_total" in sql
    assert 'count("sell_price_eod") AS cnt_0' in sql
    assert 'avg(CAST("sell_price_eod" AS DOUBLE)) AS mean_0' in sql
    assert 'min(CAST("sell_price_eod" AS DOUBLE)) AS min_0' in sql
    assert 'max(CAST("sell_price_eod" AS DOUBLE)) AS max_0' in sql
    assert (
        'approx_percentile(CAST("sell_price_eod" AS DOUBLE), '
        "ARRAY[0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]) AS pct_0"
    ) in sql
    assert 'count("abs_discount") AS cnt_1' in sql
    assert "CAST(\"date\" AS DATE) = DATE '2026-08-22'" in sql


def test_stats_query_aliases_are_positional_not_derived_from_names() -> None:
    # Имя признака может превысить лимит идентификатора или совпасть после нормализации.
    sql = render_stats_query(DAILY, ["a", "b", "c"])
    for index in range(3):
        assert f"AS cnt_{index}" in sql
    assert "AS cnt_a" not in sql


def test_stats_query_snapshot_pins_the_utc_instant() -> None:
    sql = render_stats_query(SNAPSHOT, ["avg_sell_price"])
    # Сессия Trino в Europe/Moscow: голый TIMESTAMP молча указал бы на соседний снапшот.
    assert "\"calculated_at\" = TIMESTAMP '2026-08-22 06:00:00 UTC'" in sql
    assert "CAST(" not in sql.split("WHERE")[1]


def test_stats_query_rejects_an_empty_column_list() -> None:
    try:
        render_stats_query(DAILY, [])
    except FeatureStatsConfigError as error:
        assert "колонок" in str(error)
    else:
        raise AssertionError("пустой список колонок обязан падать, а не рендерить голый count(*)")


def main() -> int:
    test_values_per_column_matches_the_select_list()
    test_percentile_array_literal_matches_the_configured_set()
    test_columns_query_qualifies_information_schema_with_the_catalog()
    test_stats_query_daily_partition()
    test_stats_query_aliases_are_positional_not_derived_from_names()
    test_stats_query_snapshot_pins_the_utc_instant()
    test_stats_query_rejects_an_empty_column_list()
    print("Feature stats SQL tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
