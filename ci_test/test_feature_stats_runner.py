import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from feature_stats.config import StatsContext, load_feature_stats_settings
from feature_stats.runner import (
    FeatureStatsError,
    batches,
    is_numeric,
    parse_stats_row,
    run_feature_stats,
    select_feature_columns,
)

TABLE = {
    "catalog": "iceberg",
    "schema": "gold",
    "name": "feature_platform_sku_group_search_conversion_features_v2",
    "primary_key": "date,sku_group_id",
}

CTX = StatsContext(
    render=RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_sku_group_search_conversion_features_v2",
        primary_key=("date", "sku_group_id"),
        partition_column="date",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
    ),
    partition_ts=datetime(2026, 8, 22, tzinfo=timezone.utc),
)

TYPED_COLUMNS = [
    ("date", "date"),
    ("sku_group_id", "bigint"),
    ("category_id", "bigint"),
    ("conv_imp2order_7", "double"),
    ("skg_days_since_last_atc", "integer"),
    ("price_bucket", "decimal(18,2)"),
    ("query", "varchar"),
    ("window_length", "interval day to second"),
]


def settings(**block):
    return load_feature_stats_settings({"table": TABLE, "feature_stats": block})


def test_is_numeric_accepts_trino_numeric_types() -> None:
    for data_type in ("tinyint", "smallint", "integer", "bigint", "real", "double", "decimal(18,2)"):
        assert is_numeric(data_type), data_type


def test_is_numeric_rejects_interval_despite_the_int_prefix() -> None:
    # Префиксная проверка утащила бы "interval day to second" в признаки.
    assert not is_numeric("interval day to second")
    assert not is_numeric("varchar")
    assert not is_numeric("date")
    assert not is_numeric("timestamp(6) with time zone")


def test_select_drops_keys_partition_and_excluded() -> None:
    selected = select_feature_columns(TYPED_COLUMNS, settings(exclude_columns=["category_id"]), CTX)
    assert [name for name, _ in selected] == [
        "conv_imp2order_7",
        "skg_days_since_last_atc",
        "price_bucket",
    ]


def test_select_preserves_declaration_order() -> None:
    selected = select_feature_columns(TYPED_COLUMNS, settings(), CTX)
    assert [name for name, _ in selected][:2] == ["category_id", "conv_imp2order_7"]


def test_select_rejects_an_exclude_column_that_does_not_exist() -> None:
    # Опечатка в exclude_columns иначе молча вернула бы признак под наблюдение.
    try:
        select_feature_columns(TYPED_COLUMNS, settings(exclude_columns=["categoryy_id"]), CTX)
    except FeatureStatsError as error:
        assert "categoryy_id" in str(error)
    else:
        raise AssertionError("несуществующая колонка в exclude_columns обязана падать")


def test_batches_default_to_a_single_query() -> None:
    columns = [(f"c{i}", "double") for i in range(5)]
    assert batches(columns, None) == [columns]


def test_batches_split_by_size() -> None:
    columns = [(f"c{i}", "double") for i in range(5)]
    split = batches(columns, 2)
    assert [len(chunk) for chunk in split] == [2, 2, 1]


def test_parse_stats_row_maps_every_metric() -> None:
    batch = [("conv_imp2order_7", "double"), ("skg_return_rate_7", "double")]
    row = [
        1000,
        800, 0.25, 0.0, 1.0, [0.01, 0.02, 0.1, 0.2, 0.4, 0.7, 0.9],
        1000, 0.5, 0.1, 0.9, [0.11, 0.12, 0.2, 0.5, 0.7, 0.8, 0.85],
    ]
    stats = parse_stats_row(row, batch, 1500, "SELECT 1")
    assert [stat.feature_name for stat in stats] == ["conv_imp2order_7", "skg_return_rate_7"]
    first = stats[0]
    assert first.rows_total == 1000
    assert first.non_null_count == 800
    assert abs(first.null_share - 0.2) < 1e-9
    assert first.mean == 0.25
    assert first.min_value == 0.0
    assert first.max_value == 1.0
    assert first.percentiles == (0.01, 0.02, 0.1, 0.2, 0.4, 0.7, 0.9)
    assert first.duration_ms == 1500
    assert first.sql == "SELECT 1"
    assert stats[1].null_share == 0.0


def test_parse_stats_row_handles_a_fully_null_feature() -> None:
    # approx_percentile отдаёт NULL вместо массива, но строка всё равно нужна:
    # "признак целиком пустой на этой партиции" — сам по себе сигнал.
    batch = [("ratio_crnt_min_to_avg_min_full_price_30", "double")]
    stats = parse_stats_row([1000, 0, None, None, None, None], batch, 10, "SELECT 1")
    assert len(stats) == 1
    assert stats[0].non_null_count == 0
    assert stats[0].null_share == 1.0
    assert stats[0].mean is None
    assert stats[0].percentiles == (None,) * 7


def test_parse_stats_row_handles_an_empty_partition() -> None:
    batch = [("conv_imp2order_7", "double")]
    stats = parse_stats_row([0, 0, None, None, None, None], batch, 10, "SELECT 1")
    assert stats[0].rows_total == 0
    assert stats[0].null_share is None


def test_parse_stats_row_rejects_a_percentile_array_of_wrong_length() -> None:
    batch = [("conv_imp2order_7", "double")]
    try:
        parse_stats_row([10, 10, 0.5, 0.0, 1.0, [0.1, 0.2]], batch, 10, "SELECT 1")
    except FeatureStatsError as error:
        assert "перцентил" in str(error)
    else:
        raise AssertionError("несовпадение длины массива перцентилей обязано падать")


def test_run_feature_stats_end_to_end_on_a_fake_query() -> None:
    seen = []

    def query(sql: str):
        seen.append(sql)
        if "information_schema" in sql:
            return TYPED_COLUMNS
        # 1 + 3 признака * 5 значений = 16 позиций
        return [
            [1000, 900, 0.3, 0.0, 1.0, [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6]]
            + [900, 2.0, 1.0, 3.0, [1.0, 1.1, 1.2, 2.0, 2.5, 2.8, 2.9]]
            + [1000, 5.0, 0.0, 9.0, [0.5, 1.0, 2.0, 5.0, 7.0, 8.0, 8.5]]
        ]

    stats = run_feature_stats(settings(exclude_columns=["category_id"]), CTX, query)
    assert len(seen) == 2
    assert "information_schema" in seen[0]
    assert [stat.feature_name for stat in stats] == [
        "conv_imp2order_7",
        "skg_days_since_last_atc",
        "price_bucket",
    ]


def test_run_feature_stats_returns_nothing_when_disabled() -> None:
    def query(sql: str):
        raise AssertionError("выключенная таска не должна ходить в Trino")

    assert run_feature_stats(settings(enabled=False), CTX, query) == []


def test_run_feature_stats_fails_when_the_table_is_missing() -> None:
    def query(sql: str):
        return []

    try:
        run_feature_stats(settings(), CTX, query)
    except FeatureStatsError as error:
        assert "feature_platform_sku_group_search_conversion_features_v2" in str(error)
    else:
        raise AssertionError("отсутствующая таблица обязана падать диагностикой")


def test_run_feature_stats_is_a_noop_on_a_key_only_table() -> None:
    def query(sql: str):
        if "information_schema" in sql:
            return [("date", "date"), ("sku_group_id", "bigint")]
        raise AssertionError("без признаков запрос статистик не нужен")

    assert run_feature_stats(settings(), CTX, query) == []


def main() -> int:
    test_is_numeric_accepts_trino_numeric_types()
    test_is_numeric_rejects_interval_despite_the_int_prefix()
    test_select_drops_keys_partition_and_excluded()
    test_select_preserves_declaration_order()
    test_select_rejects_an_exclude_column_that_does_not_exist()
    test_batches_default_to_a_single_query()
    test_batches_split_by_size()
    test_parse_stats_row_maps_every_metric()
    test_parse_stats_row_handles_a_fully_null_feature()
    test_parse_stats_row_handles_an_empty_partition()
    test_parse_stats_row_rejects_a_percentile_array_of_wrong_length()
    test_run_feature_stats_end_to_end_on_a_fake_query()
    test_run_feature_stats_returns_nothing_when_disabled()
    test_run_feature_stats_fails_when_the_table_is_missing()
    test_run_feature_stats_is_a_noop_on_a_key_only_table()
    print("Feature stats runner tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
