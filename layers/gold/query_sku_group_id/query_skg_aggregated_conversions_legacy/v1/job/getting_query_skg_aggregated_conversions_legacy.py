from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)
RATIO_WINDOWS = ((1, 3), (3, 7), (7, 14), (14, 30), (30, 60), (60, 90))
PAIRWISE_COLUMNS = (
    "query_skg_uniq_impressions_1",
    "query_skg_uniq_clicks_1",
    "query_skg_uniq_atcs_1",
    "query_skg_uniq_orders_1",
    "query_skg_conv_imp2click_1",
    "query_skg_conv_imp2atc_1",
    "query_skg_conv_imp2order_1",
    "query_skg_uniq_impressions_3",
    "query_skg_uniq_clicks_3",
    "query_skg_uniq_atcs_3",
    "query_skg_uniq_orders_3",
    "query_skg_conv_imp2click_3",
    "query_skg_conv_imp2atc_3",
    "query_skg_conv_imp2order_3",
    "query_skg_uniq_impressions_7",
    "query_skg_uniq_clicks_7",
    "query_skg_uniq_atcs_7",
    "query_skg_uniq_orders_7",
    "query_skg_conv_imp2click_7",
    "query_skg_conv_imp2atc_7",
    "query_skg_conv_imp2order_7",
    "query_skg_uniq_impressions_14",
    "query_skg_uniq_clicks_14",
    "query_skg_uniq_atcs_14",
    "query_skg_uniq_orders_14",
    "query_skg_conv_imp2click_14",
    "query_skg_conv_imp2atc_14",
    "query_skg_conv_imp2order_14",
    "query_skg_uniq_impressions_21",
    "query_skg_uniq_clicks_21",
    "query_skg_uniq_atcs_21",
    "query_skg_uniq_orders_21",
    "query_skg_conv_imp2click_21",
    "query_skg_conv_imp2atc_21",
    "query_skg_conv_imp2order_21",
    "query_skg_uniq_impressions_30",
    "query_skg_uniq_clicks_30",
    "query_skg_uniq_atcs_30",
    "query_skg_uniq_orders_30",
    "query_skg_conv_imp2click_30",
    "query_skg_conv_imp2atc_30",
    "query_skg_conv_imp2order_30",
    "query_skg_imp2click_3_to_1",
    "query_skg_imp2atc_3_to_1",
    "query_skg_imp2order_3_to_1",
    "query_skg_imp2click_7_to_3",
    "query_skg_imp2atc_7_to_3",
    "query_skg_imp2order_7_to_3",
    "query_skg_imp2click_14_to_7",
    "query_skg_imp2atc_14_to_7",
    "query_skg_imp2order_14_to_7",
    "query_skg_imp2click_30_to_14",
    "query_skg_imp2atc_30_to_14",
    "query_skg_imp2order_30_to_14",
    "query_skg_uniq_impressions_60",
    "query_skg_uniq_clicks_60",
    "query_skg_uniq_atcs_60",
    "query_skg_uniq_orders_60",
    "query_skg_conv_imp2click_60",
    "query_skg_conv_imp2atc_60",
    "query_skg_conv_imp2order_60",
    "query_skg_uniq_impressions_90",
    "query_skg_uniq_clicks_90",
    "query_skg_uniq_atcs_90",
    "query_skg_uniq_orders_90",
    "query_skg_conv_imp2click_90",
    "query_skg_conv_imp2atc_90",
    "query_skg_conv_imp2order_90",
    "query_skg_imp2click_60_to_30",
    "query_skg_imp2atc_60_to_30",
    "query_skg_imp2order_60_to_30",
    "query_skg_imp2click_90_to_60",
    "query_skg_imp2atc_90_to_60",
    "query_skg_imp2order_90_to_60",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _safe_div(num: Column, den: Column) -> Column:
    return num / den


def _window_start_dates(run_date: str) -> dict[int, str]:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return {
        window: (run_dt - timedelta(days=window)).isoformat()
        for window in WINDOWS
    }


def _sum_since(column_name: str, start_date: str) -> Column:
    return F.sum(
        F.when(
            F.col("date") >= F.lit(start_date).cast("date"),
            F.col(column_name).cast("double"),
        ).otherwise(0.0)
    )


def build_query_skg_aggregated_conversions_legacy(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    window_dates = _window_start_dates(run_date)
    start_date = window_dates[max(WINDOWS)]

    daily_stats = (
        spark.table("iceberg.silver.feature_platform_query_skg_daily_conversions_legacy")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") <= F.lit(run_date).cast("date"))
        )
        .select(
            F.col("date").cast("date").alias("date"),
            F.col("query"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("uniq_impressions").cast("double").alias("uniq_impressions"),
            F.col("uniq_clicks").cast("double").alias("uniq_clicks"),
            F.col("uniq_atcs").cast("double").alias("uniq_atcs"),
            F.col("uniq_orders").cast("double").alias("uniq_orders"),
        )
        .groupBy("date", "query", "sku_group_id")
        .agg(
            F.sum("uniq_impressions").alias("uniq_impressions"),
            F.sum("uniq_clicks").alias("uniq_clicks"),
            F.sum("uniq_atcs").alias("uniq_atcs"),
            F.sum("uniq_orders").alias("uniq_orders"),
        )
    )

    aggregation_exprs = []
    for window in WINDOWS:
        aggregation_exprs.extend(
            (
                _sum_since("uniq_impressions", window_dates[window]).alias(
                    f"query_skg_uniq_impressions_{window}"
                ),
                _sum_since("uniq_clicks", window_dates[window]).alias(
                    f"query_skg_uniq_clicks_{window}"
                ),
                _sum_since("uniq_atcs", window_dates[window]).alias(
                    f"query_skg_uniq_atcs_{window}"
                ),
                _sum_since("uniq_orders", window_dates[window]).alias(
                    f"query_skg_uniq_orders_{window}"
                ),
            )
        )

    features = daily_stats.groupBy("query", "sku_group_id").agg(*aggregation_exprs)

    for window in WINDOWS:
        features = (
            features.withColumn(
                f"query_skg_conv_imp2click_{window}",
                _safe_div(
                    F.col(f"query_skg_uniq_clicks_{window}"),
                    F.col(f"query_skg_uniq_impressions_{window}"),
                ),
            )
            .withColumn(
                f"query_skg_conv_imp2atc_{window}",
                _safe_div(
                    F.col(f"query_skg_uniq_atcs_{window}"),
                    F.col(f"query_skg_uniq_impressions_{window}"),
                ),
            )
            .withColumn(
                f"query_skg_conv_imp2order_{window}",
                _safe_div(
                    F.col(f"query_skg_uniq_orders_{window}"),
                    F.col(f"query_skg_uniq_impressions_{window}"),
                ),
            )
        )

    features = features.filter(F.col("query_skg_uniq_impressions_14") >= F.lit(2.0))

    for left_window, right_window in RATIO_WINDOWS:
        features = (
            features.withColumn(
                f"query_skg_imp2click_{right_window}_to_{left_window}",
                _safe_div(
                    F.col(f"query_skg_conv_imp2click_{right_window}"),
                    F.col(f"query_skg_conv_imp2click_{left_window}"),
                ),
            )
            .withColumn(
                f"query_skg_imp2atc_{right_window}_to_{left_window}",
                _safe_div(
                    F.col(f"query_skg_conv_imp2atc_{right_window}"),
                    F.col(f"query_skg_conv_imp2atc_{left_window}"),
                ),
            )
            .withColumn(
                f"query_skg_imp2order_{right_window}_to_{left_window}",
                _safe_div(
                    F.col(f"query_skg_conv_imp2order_{right_window}"),
                    F.col(f"query_skg_conv_imp2order_{left_window}"),
                ),
            )
        )

    return features.withColumn("date", F.lit(run_date).cast("date")).select(
        "date",
        "query",
        "sku_group_id",
        *PAIRWISE_COLUMNS,
    )


def save_query_skg_aggregated_conversions_legacy(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_query_skg_aggregated_conversions_legacy(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_query_skg_aggregated_conversions_legacy(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )

