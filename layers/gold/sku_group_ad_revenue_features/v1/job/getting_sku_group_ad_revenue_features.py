from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


SOURCE_TABLE = "iceberg.silver.feature_platform_sku_group_ad_revenue_daily"

WINDOWS = (7, 14, 30)

# Аддитивное сглаживание среднего заработка с рекламы на показ к глобальному
# среднему. PRIOR_MEAN_ADREV_PER_IMP — глобальное среднее adrev/impression,
# PRIOR_IMP — сила приора в псевдо-показах. Константы оценены по
# silver.adv_funnel_daily за 30 дней и подлежат настройке владельцем модели.
PRIOR_MEAN_ADREV_PER_IMP = 19.9
PRIOR_IMP = 50.0
PRIOR_ADREV = PRIOR_MEAN_ADREV_PER_IMP * PRIOR_IMP  # 995.0

SELECTED_COLUMNS = (
    "date",
    "sku_group_id",
    "smooth_adrev_per_imp_7",
    "smooth_adrev_per_imp_14",
    "smooth_adrev_per_imp_30",
    "adrev_per_imp_7",
    "adrev_per_imp_14",
    "adrev_per_imp_30",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _safe_div(num: Column, den: Column) -> Column:
    return num / den


def _window_bounds(run_date: str) -> tuple[str, str, dict[int, str]]:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    finish_dt = run_dt - timedelta(days=1)
    return (
        (run_dt - timedelta(days=max(WINDOWS))).isoformat(),
        finish_dt.isoformat(),
        {window: (run_dt - timedelta(days=window)).isoformat() for window in WINDOWS},
    )


def _sum_since(column_name: str, start_date: str) -> Column:
    return F.sum(
        F.when(
            F.col("date") >= F.lit(start_date).cast("date"),
            F.col(column_name),
        ).otherwise(0.0)
    )


def _build_daily_stats(
    spark: SparkSession,
    start_date: str,
    finish_date: str,
) -> DataFrame:
    return (
        spark.table(SOURCE_TABLE)
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") <= F.lit(finish_date).cast("date"))
        )
        .filter(F.col("sku_group_id").isNotNull())
        .select(
            F.col("date"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("ad_impressions").cast("double").alias("ad_impressions"),
            F.col("ad_revenue").cast("double").alias("ad_revenue"),
        )
    )


def _build_window_sums(
    daily_stats: DataFrame,
    window_dates: dict[int, str],
) -> DataFrame:
    aggregations = []
    for window in WINDOWS:
        aggregations.extend(
            (
                _sum_since("ad_impressions", window_dates[window]).alias(
                    f"skg_ad_impressions_{window}"
                ),
                _sum_since("ad_revenue", window_dates[window]).alias(
                    f"skg_ad_revenue_{window}"
                ),
            )
        )

    return daily_stats.groupBy("sku_group_id").agg(*aggregations)


def build_sku_group_ad_revenue_features(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    start_date, finish_date, window_dates = _window_bounds(run_date)

    daily_stats = _build_daily_stats(spark, start_date, finish_date)

    features = _build_window_sums(daily_stats, window_dates)

    for window in WINDOWS:
        features = features.withColumn(
            f"adrev_per_imp_{window}",
            _safe_div(
                F.col(f"skg_ad_revenue_{window}"),
                F.col(f"skg_ad_impressions_{window}"),
            ),
        ).withColumn(
            f"smooth_adrev_per_imp_{window}",
            (
                F.lit(PRIOR_ADREV) + F.col(f"skg_ad_revenue_{window}")
            ) / (
                F.lit(PRIOR_IMP) + F.col(f"skg_ad_impressions_{window}")
            ),
        )

    return (
        features.withColumn("date", F.lit(run_date).cast("date"))
        .select(*SELECTED_COLUMNS)
    )


def save_sku_group_ad_revenue_features(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_ad_revenue_features(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_ad_revenue_features(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
