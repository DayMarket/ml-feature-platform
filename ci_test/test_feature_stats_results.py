import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dq.config import RenderContext
from feature_stats.config import PERCENTILE_COLUMNS, StatsContext
from feature_stats.results_writer import (
    RunMeta,
    build_rows,
    overwrite_filter_values,
    results_catalog_name,
    results_table_ref,
)
from feature_stats.runner import FeatureStat

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
        team="team:search",
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
        team="team:recsys",
        partition_granularity="timestamp",
        partition_timestamp=datetime(2026, 8, 22, 6, 0, 0),
        snapshot_interval_hours=3,
    ),
    partition_ts=datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc),
)

META = RunMeta(
    dag_id="feature-platform.layers.gold.sku_group_id.sku_group_price_features",
    task_id="feature_stats",
    run_id="scheduled__2026-08-22T02:00:00+00:00",
    try_number=1,
    run_ts=datetime(2026, 8, 23, 2, 5, tzinfo=timezone.utc),
)

STAT = FeatureStat(
    feature_name="sell_price_eod",
    data_type="double",
    rows_total=5_400_000,
    non_null_count=5_400_000,
    null_share=0.0,
    mean=125.5,
    min_value=1.0,
    max_value=99000.0,
    percentiles=(5.0, 9.0, 30.0, 90.0, 190.0, 450.0, 800.0),
    duration_ms=8400,
    sql="SELECT 1",
)


def test_results_table_ref_comes_from_config() -> None:
    assert results_table_ref(Path(".")) == ("silver", "feature_platform_feature_stats")


def test_results_catalog_name_comes_from_config() -> None:
    assert results_catalog_name(Path(".")) == "iceberg"


def test_build_rows_maps_every_field() -> None:
    row = build_rows([STAT], DAILY, META)[0]
    assert row["date"] == date(2026, 8, 22)
    assert row["partition_ts"] == datetime(2026, 8, 22, tzinfo=timezone.utc)
    assert row["run_ts"] == META.run_ts
    assert row["dag_id"] == META.dag_id
    assert row["task_id"] == "feature_stats"
    assert row["run_id"] == META.run_id
    assert row["try_number"] == 1
    assert row["catalog"] == "dwh-iceberg"
    assert row["schema_name"] == "gold"
    assert row["table_name"] == "feature_platform_sku_group_price_features"
    assert row["team"] == "team:search"
    assert row["feature_name"] == "sell_price_eod"
    assert row["data_type"] == "double"
    assert row["rows_total"] == 5_400_000
    assert row["non_null_count"] == 5_400_000
    assert row["null_share"] == 0.0
    assert row["mean"] == 125.5
    assert row["min_value"] == 1.0
    assert row["max_value"] == 99000.0
    assert row["duration_ms"] == 8400
    assert row["sql_text"] == "SELECT 1"
    for index, column in enumerate(PERCENTILE_COLUMNS):
        assert row[column] == STAT.percentiles[index]


def test_build_rows_carries_the_owning_team() -> None:
    assert build_rows([STAT], SNAPSHOT, META)[0]["team"] == "team:recsys"


def test_build_rows_writes_the_snapshot_instant() -> None:
    row = build_rows([STAT], SNAPSHOT, META)[0]
    assert row["date"] == date(2026, 8, 22)
    assert row["partition_ts"] == datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc)


def test_build_rows_keeps_null_metrics_as_none() -> None:
    empty = FeatureStat(
        feature_name="ratio_crnt_min_to_avg_min_full_price_30",
        data_type="double",
        rows_total=1000,
        non_null_count=0,
        null_share=1.0,
        mean=None,
        min_value=None,
        max_value=None,
        percentiles=(None,) * 7,
        duration_ms=10,
        sql="SELECT 1",
    )
    row = build_rows([empty], DAILY, META)[0]
    assert row["mean"] is None
    assert row["p50"] is None
    assert row["null_share"] == 1.0


def test_build_rows_is_empty_without_stats() -> None:
    assert build_rows([], DAILY, META) == []


def test_overwrite_filter_pins_the_snapshot_not_just_the_day() -> None:
    # Снапшотная энтити пишет 8 партиций в сутки: фильтр без partition_ts
    # затирал бы профили предыдущих снапшотов того же дня.
    daily = overwrite_filter_values(DAILY, META)
    snapshot = overwrite_filter_values(SNAPSHOT, META)
    assert daily == {
        "date": date(2026, 8, 22),
        "dag_id": META.dag_id,
        "table_name": "feature_platform_sku_group_price_features",
        "partition_ts": datetime(2026, 8, 22, tzinfo=timezone.utc),
    }
    assert snapshot["partition_ts"] == datetime(2026, 8, 22, 6, 0, 0, tzinfo=timezone.utc)
    assert snapshot["table_name"] == "feature_platform_dynamic_pricing_sku_group_price_features"


def test_overwrite_filter_keys_are_exactly_the_four_that_govern_the_filter() -> None:
    # write_results строит фильтр из этого словаря, поэтому его форма — контракт,
    # а не деталь: лишний ключ сузит перезапись, потерянный partition_ts или
    # table_name вернёт дефект dq/results_writer.py или откроет его в feature_stats,
    # если однажды в одном DAG'е появится вторая build_feature_stats_task(...).
    assert list(overwrite_filter_values(SNAPSHOT, META)) == [
        "date",
        "dag_id",
        "table_name",
        "partition_ts",
    ]


def test_build_rows_keys_match_the_results_table_ddl() -> None:
    # Единственная автоматическая защита write_results: pyiceberg недоступен в этом
    # окружении (write_results юнит-тестом не покрыть), поэтому здесь проверяется
    # только то, что build_rows и миграция таблицы результатов не разошлись — иначе
    # pa.Table.from_pylist(..., schema=...) молча роняет лишний ключ build_rows и
    # молча заполняет NULL пропущенную в build_rows колонку схемы, без исключения.
    ddl_path = (
        Path(__file__).resolve().parents[1]
        / "feature_stats"
        / "results"
        / "migrations"
        / "create_table.sql"
    )
    ddl_text = ddl_path.read_text(encoding="utf-8")
    column_line = re.compile(
        r"^\s+([A-Za-z_][A-Za-z0-9_]*)\s+[A-Z]+(?:\([^)]*\))?\s+COMMENT\s+'", re.MULTILINE
    )
    ddl_columns = set(column_line.findall(ddl_text))
    assert ddl_columns, "регулярка не нашла ни одной колонки — сама регулярка сломана"

    row_keys = set(build_rows([STAT], DAILY, META)[0])
    assert ddl_columns == row_keys


def main() -> int:
    test_results_table_ref_comes_from_config()
    test_results_catalog_name_comes_from_config()
    test_build_rows_maps_every_field()
    test_build_rows_carries_the_owning_team()
    test_build_rows_writes_the_snapshot_instant()
    test_build_rows_keeps_null_metrics_as_none()
    test_build_rows_is_empty_without_stats()
    test_overwrite_filter_pins_the_snapshot_not_just_the_day()
    test_overwrite_filter_keys_are_exactly_the_four_that_govern_the_filter()
    test_build_rows_keys_match_the_results_table_ddl()
    print("Feature stats results tests completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
