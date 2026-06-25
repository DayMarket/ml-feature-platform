from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)
RECENCY_LOOKBACK_DAYS = 90
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
    "skg_days_since_last_impression",
    "skg_days_since_last_atc",
    "skg_conv_atc2order_1",
    "skg_conv_atc2order_3",
    "skg_conv_atc2order_7",
    "skg_conv_atc2order_14",
    "skg_conv_atc2order_21",
    "skg_conv_atc2order_30",
    "skg_conv_atc2order_60",
    "skg_conv_atc2order_90",
    "skg_return_rate_1",
    "skg_return_rate_3",
    "skg_return_rate_7",
    "skg_return_rate_14",
    "skg_return_rate_21",
    "skg_return_rate_30",
    "skg_return_rate_60",
    "skg_return_rate_90",
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


def _build_daily_search_events(
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
            F.col("sum_atc").cast("double").alias("sum_atc"),
        )
        .groupBy("date", "sku_group_id")
        .agg(
            F.sum(F.col("sum_impressions").cast("double")).alias("uniq_impressions"),
            F.sum(F.col("sum_atc").cast("double")).alias("uniq_atcs"),
        )
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
            F.col("returned_orders").cast("double").alias("returned_orders"),
        )
        .groupBy("date", "sku_group_id")
        .agg(
            F.sum(F.col("orders_generated").cast("double")).alias("uniq_orders"),
            F.sum(F.col("returned_orders").cast("double")).alias("returned_orders"),
        )
    )


def _build_event_recency(
    spark: SparkSession,
    start_date: str,
    run_date: str,
) -> DataFrame:
    last_events = (
        spark.table("iceberg.silver.feature_platform_search_sku_group_id_install_query")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(run_date).cast("date"))
        )
        .filter(F.col("space") == F.lit("SEARCH_RESULTS"))
        .filter(F.col("sku_group_id").isNotNull())
        .select(
            F.col("date"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("sum_impressions").cast("double").alias("sum_impressions"),
            F.col("sum_atc").cast("double").alias("sum_atc"),
        )
        .groupBy("sku_group_id")
        .agg(
            F.max(
                F.when(
                    F.col("sum_impressions") > F.lit(0.0),
                    F.col("date"),
                )
            ).alias("last_impression_date"),
            F.max(
                F.when(
                    F.col("sum_atc") > F.lit(0.0),
                    F.col("date"),
                )
            ).alias("last_atc_date"),
        )
    )

    return (
        last_events.withColumn(
            "skg_days_since_last_impression",
            F.datediff(F.lit(run_date).cast("date"), F.col("last_impression_date")).cast("int"),
        )
        .withColumn(
            "skg_days_since_last_atc",
            F.datediff(F.lit(run_date).cast("date"), F.col("last_atc_date")).cast("int"),
        )
        .select(
            "sku_group_id",
            "skg_days_since_last_impression",
            "skg_days_since_last_atc",
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
                _sum_since("uniq_impressions", window_dates[window]).alias(
                    f"skg_uniq_impressions_{window}"
                ),
                _sum_since("uniq_orders", window_dates[window]).alias(
                    f"skg_uniq_orders_{window}"
                ),
                _sum_since("uniq_atcs", window_dates[window]).alias(
                    f"skg_uniq_atcs_{window}"
                ),
                _sum_since("returned_orders", window_dates[window]).alias(
                    f"skg_returned_orders_{window}"
                ),
            )
        )

    return daily_stats.groupBy("sku_group_id").agg(*aggregations)


def build_sku_group_search_conversion_features(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    start_date, finish_date, window_dates = _window_bounds(run_date)
    recency_start_date = (
        datetime.strptime(run_date, "%Y-%m-%d").date()
        - timedelta(days=RECENCY_LOOKBACK_DAYS)
    ).isoformat()

    daily_search_events = _build_daily_search_events(spark, start_date, finish_date)
    daily_orders = _build_daily_orders(spark, start_date, finish_date)
    event_recency = _build_event_recency(spark, recency_start_date, run_date)

    daily_stats = daily_search_events.join(
        daily_orders,
        on=["date", "sku_group_id"],
        how="full",
    )

    for column_name in ("uniq_impressions", "uniq_atcs", "uniq_orders", "returned_orders"):
        daily_stats = daily_stats.withColumn(
            column_name,
            F.coalesce(F.col(column_name), F.lit(0.0)),
        )

    features = _build_window_sums(daily_stats, window_dates).join(
        event_recency,
        on="sku_group_id",
        how="left",
    )

    for window in WINDOWS:
        features = features.withColumn(
            f"skg_conv_imp2order_{window}",
            _safe_div(
                F.col(f"skg_uniq_orders_{window}"),
                F.col(f"skg_uniq_impressions_{window}"),
            ),
        )
        features = features.withColumn(
            f"skg_conv_atc2order_{window}",
            _safe_div(
                F.col(f"skg_uniq_orders_{window}"),
                F.col(f"skg_uniq_atcs_{window}"),
            ),
        )
        features = features.withColumn(
            f"skg_return_rate_{window}",
            _safe_div(
                F.col(f"skg_returned_orders_{window}"),
                F.col(f"skg_uniq_orders_{window}"),
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
