from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30)
SMOOTH_ALPHA = 0.003384
SMOOTH_BETA = 1.402240
SELECTED_COLUMNS = (
    "date",
    "sku_group_id",
    "smooth_conv_imp2order_3",
    "smooth_conv_imp2order_7",
    "smooth_conv_imp2order_14",
    "conv_imp2order_3",
    "conv_imp2order_7",
    "conv_imp2order_14",
    "imp2order_3_to_1",
    "imp2order_21_to_14",
    "imp2order_30_to_21",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _safe_div(num: Column, den: Column) -> Column:
    return num / den


def _zero_div(num: Column, den: Column) -> Column:
    return F.when(den == F.lit(0.0), F.lit(0.0)).otherwise(num / den)


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


def _build_daily_impressions(
    spark: SparkSession,
    start_date: str,
    finish_date: str,
) -> DataFrame:
    return (
        spark.table("iceberg.silver.feature_platform_search_sku_group_id_install_query")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") <= F.lit(finish_date).cast("date"))
        )
        .filter(F.col("space") == F.lit("SEARCH_RESULTS"))
        .filter(F.col("sku_group_id").isNotNull())
        .select(
            F.col("date"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("sum_impressions").cast("double").alias("sum_impressions"),
        )
        .groupBy("date", "sku_group_id")
        .agg(F.sum(F.col("sum_impressions").cast("double")).alias("uniq_impressions"))
    )


def _build_daily_orders(
    spark: SparkSession,
    start_date: str,
    finish_date: str,
) -> DataFrame:
    return (
        spark.table("iceberg.silver.feature_platform_sku_group_query_search_orders")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") <= F.lit(finish_date).cast("date"))
        )
        .filter(F.col("sku_group_id").isNotNull())
        .select(
            F.col("date"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("orders_generated").cast("double").alias("orders_generated"),
        )
        .groupBy("date", "sku_group_id")
        .agg(F.sum(F.col("orders_generated").cast("double")).alias("uniq_orders"))
    )


def _build_window_sums(
    daily_stats: DataFrame,
    window_dates: dict[int, str],
) -> DataFrame:
    aggregations = []
    for window in WINDOWS:
        aggregations.extend(
            (
                _sum_since("uniq_impressions", window_dates[window]).alias(
                    f"skg_uniq_impressions_{window}"
                ),
                _sum_since("uniq_orders", window_dates[window]).alias(
                    f"skg_uniq_orders_{window}"
                ),
            )
        )

    return daily_stats.groupBy("sku_group_id").agg(*aggregations)


def build_sku_group_search_conversion_features(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    start_date, finish_date, window_dates = _window_bounds(run_date)

    daily_impressions = _build_daily_impressions(spark, start_date, finish_date)
    daily_orders = _build_daily_orders(spark, start_date, finish_date)

    daily_stats = daily_impressions.join(
        daily_orders,
        on=["date", "sku_group_id"],
        how="full",
    )

    for column_name in ("uniq_impressions", "uniq_orders"):
        daily_stats = daily_stats.withColumn(
            column_name,
            F.coalesce(F.col(column_name), F.lit(0.0)),
        )

    features = _build_window_sums(daily_stats, window_dates)

    for window in WINDOWS:
        features = features.withColumn(
            f"skg_conv_imp2order_{window}",
            _safe_div(
                F.col(f"skg_uniq_orders_{window}"),
                F.col(f"skg_uniq_impressions_{window}"),
            ),
        )

    for window in (3, 7, 14):
        features = (
            features.withColumn(
                f"smooth_conv_imp2order_{window}",
                (
                    F.lit(SMOOTH_ALPHA) + F.col(f"skg_uniq_orders_{window}")
                ) / (
                    F.lit(SMOOTH_ALPHA + SMOOTH_BETA)
                    + F.col(f"skg_uniq_impressions_{window}")
                ),
            )
            .withColumn(
                f"conv_imp2order_{window}",
                _zero_div(
                    F.col(f"skg_uniq_orders_{window}"),
                    F.col(f"skg_uniq_impressions_{window}"),
                ),
            )
        )

    return (
        features.withColumn(
            "imp2order_3_to_1",
            _safe_div(F.col("skg_conv_imp2order_3"), F.col("skg_conv_imp2order_1")),
        )
        .withColumn(
            "imp2order_21_to_14",
            _safe_div(F.col("skg_conv_imp2order_21"), F.col("skg_conv_imp2order_14")),
        )
        .withColumn(
            "imp2order_30_to_21",
            _safe_div(F.col("skg_conv_imp2order_30"), F.col("skg_conv_imp2order_21")),
        )
        .withColumn("date", F.lit(run_date).cast("date"))
        .select(*SELECTED_COLUMNS)
    )


def save_sku_group_search_conversion_features(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_search_conversion_features(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_search_conversion_features(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
