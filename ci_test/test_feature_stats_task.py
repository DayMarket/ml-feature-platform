import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from feature_stats.task import TASK_ID, build_stats_context, partition_instant

DAILY_CONFIG = {
    "table": {
        "catalog": "iceberg",
        "schema": "gold",
        "name": "feature_platform_sku_group_price_features",
        "primary_key": "date,sku_group_id",
        "meta": {"team": "team:search"},
    },
    "feature_stats": {"exclude_columns": []},
}

SNAPSHOT_CONFIG = {
    "table": {
        "catalog": "iceberg",
        "schema": "gold",
        "name": "feature_platform_dynamic_pricing_sku_group_price_features",
        "primary_key": "calculated_at,sku_group_id,promotion_id",
        "meta": {"team": "team:search"},
    },
    "feature_stats": {
        "partition_granularity": "timestamp",
        "partition_column": "calculated_at",
        "snapshot_interval_hours": 3,
        "partition_date_template": "{{ x }}",
    },
}


def test_task_id_is_stable() -> None:
    # На это имя опирается wiring-тест; downstream-сенсоры на него вешать нельзя.
    assert TASK_ID == "feature_stats"


def test_partition_instant_for_a_daily_entity_is_midnight_utc() -> None:
    assert partition_instant(date(2026, 8, 22), None) == datetime(2026, 8, 22, tzinfo=timezone.utc)


def test_partition_instant_for_a_snapshot_entity_is_the_snapshot() -> None:
    naive = datetime(2026, 8, 22, 6, 0, 0)
    assert partition_instant(date(2026, 8, 22), naive) == datetime(
        2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc
    )


def test_build_stats_context_daily() -> None:
    ctx = build_stats_context(DAILY_CONFIG, Path("."), "2026-08-22")
    assert ctx.render.catalog_alias == "dwh-iceberg"
    assert ctx.render.schema == "gold"
    assert ctx.render.table == "feature_platform_sku_group_price_features"
    assert ctx.render.primary_key == ("date", "sku_group_id")
    assert ctx.render.partition_column == "date"
    assert ctx.render.partition_date == date(2026, 8, 22)
    assert ctx.render.partition_granularity == "date"
    assert ctx.render.team == "team:search"
    assert ctx.partition_ts == datetime(2026, 8, 22, tzinfo=timezone.utc)


def test_build_stats_context_snapshot() -> None:
    ctx = build_stats_context(SNAPSHOT_CONFIG, Path("."), "2026-08-22 06:00:00")
    assert ctx.render.partition_column == "calculated_at"
    assert ctx.render.partition_granularity == "timestamp"
    assert ctx.render.partition_timestamp == datetime(2026, 8, 22, 6, 0, 0)
    assert ctx.render.snapshot_interval_hours == 3
    assert ctx.partition_ts == datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc)


def test_build_stats_context_defaults_the_team() -> None:
    config = {"table": {**DAILY_CONFIG["table"]}}
    config["table"].pop("meta")
    assert build_stats_context(config, Path("."), "2026-08-22").render.team == "team:search"


def main() -> int:
    test_task_id_is_stable()
    test_partition_instant_for_a_daily_entity_is_midnight_utc()
    test_partition_instant_for_a_snapshot_entity_is_the_snapshot()
    test_build_stats_context_daily()
    test_build_stats_context_snapshot()
    test_build_stats_context_defaults_the_team()
    print("Feature stats task tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
