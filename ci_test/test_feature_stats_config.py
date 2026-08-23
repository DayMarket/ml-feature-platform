import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from feature_stats.config import (
    PERCENTILE_COLUMNS,
    PERCENTILES,
    FeatureStatsConfigError,
    StatsContext,
    load_feature_stats_settings,
)

TABLE = {
    "catalog": "iceberg",
    "schema": "gold",
    "name": "feature_platform_sku_group_price_features",
    "primary_key": "date,sku_group_id",
}


def test_defaults_when_block_is_absent() -> None:
    settings = load_feature_stats_settings({"table": TABLE})
    assert settings.enabled is True
    assert settings.trino_conn_id == "trino_search"
    assert settings.partition_column == "date"
    assert settings.partition_granularity == "date"
    assert settings.snapshot_interval_hours == 24
    assert settings.exclude_columns == ()
    assert settings.columns_per_query is None
    assert settings.query_timeout_seconds == 600


def test_percentile_sets_stay_aligned() -> None:
    # Колонки p05..p95 в таблице результатов позиционно соответствуют PERCENTILES.
    assert PERCENTILES == (0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95)
    assert PERCENTILE_COLUMNS == ("p05", "p10", "p25", "p50", "p75", "p90", "p95")
    assert len(PERCENTILES) == len(PERCENTILE_COLUMNS)


def test_exclude_columns_are_normalized_to_a_tuple() -> None:
    settings = load_feature_stats_settings(
        {"table": TABLE, "feature_stats": {"exclude_columns": ["category_id", " brand_id "]}}
    )
    assert settings.exclude_columns == ("category_id", "brand_id")


def test_primary_key_is_required() -> None:
    try:
        load_feature_stats_settings({"table": {**TABLE, "primary_key": ""}})
    except FeatureStatsConfigError as error:
        assert "primary_key" in str(error)
    else:
        raise AssertionError("пустой primary_key обязан падать")


def test_unknown_key_is_rejected() -> None:
    try:
        load_feature_stats_settings({"table": TABLE, "feature_stats": {"percentiles": [0.5]}})
    except FeatureStatsConfigError as error:
        assert "percentiles" in str(error)
    else:
        raise AssertionError("неизвестный ключ обязан падать: набор перцентилей не конфигурируется")


def test_snapshot_requires_interval_and_template() -> None:
    base = {"partition_granularity": "timestamp", "partition_column": "calculated_at"}
    try:
        load_feature_stats_settings({"table": TABLE, "feature_stats": dict(base)})
    except FeatureStatsConfigError as error:
        assert "snapshot_interval_hours" in str(error)
    else:
        raise AssertionError("timestamp без snapshot_interval_hours обязан падать")

    try:
        load_feature_stats_settings(
            {"table": TABLE, "feature_stats": {**base, "snapshot_interval_hours": 3}}
        )
    except FeatureStatsConfigError as error:
        assert "partition_date_template" in str(error)
    else:
        raise AssertionError("timestamp без partition_date_template обязан падать")


def test_snapshot_block_is_accepted() -> None:
    settings = load_feature_stats_settings(
        {
            "table": TABLE,
            "feature_stats": {
                "partition_granularity": "timestamp",
                "partition_column": "calculated_at",
                "snapshot_interval_hours": 3,
                "partition_date_template": "{{ x }}",
            },
        }
    )
    assert settings.partition_granularity == "timestamp"
    assert settings.snapshot_interval_hours == 3
    assert settings.partition_column == "calculated_at"


def test_unknown_granularity_is_rejected() -> None:
    try:
        load_feature_stats_settings(
            {"table": TABLE, "feature_stats": {"partition_granularity": "hour"}}
        )
    except FeatureStatsConfigError as error:
        assert "partition_granularity" in str(error)
    else:
        raise AssertionError("неизвестная гранулярность обязана падать")


def test_columns_per_query_must_be_positive() -> None:
    try:
        load_feature_stats_settings({"table": TABLE, "feature_stats": {"columns_per_query": 0}})
    except FeatureStatsConfigError as error:
        assert "columns_per_query" in str(error)
    else:
        raise AssertionError("нулевой батч обязан падать: пустой список колонок не запрос")


def test_stats_context_holds_render_context_and_partition_ts() -> None:
    render = RenderContext(
        catalog_alias="dwh-iceberg",
        schema="gold",
        table="feature_platform_sku_group_price_features",
        primary_key=("date", "sku_group_id"),
        partition_column="date",
        partition_date=date(2026, 8, 22),
        scope="partition",
        sample_rows=0,
    )
    ctx = StatsContext(render=render, partition_ts=datetime(2026, 8, 22, tzinfo=timezone.utc))
    assert ctx.render.table == "feature_platform_sku_group_price_features"
    assert ctx.partition_ts.tzinfo is timezone.utc


def main() -> int:
    test_defaults_when_block_is_absent()
    test_percentile_sets_stay_aligned()
    test_exclude_columns_are_normalized_to_a_tuple()
    test_primary_key_is_required()
    test_unknown_key_is_rejected()
    test_snapshot_requires_interval_and_template()
    test_snapshot_block_is_accepted()
    test_unknown_granularity_is_rejected()
    test_columns_per_query_must_be_positive()
    test_stats_context_holds_render_context_and_partition_ts()
    print("Feature stats config tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
