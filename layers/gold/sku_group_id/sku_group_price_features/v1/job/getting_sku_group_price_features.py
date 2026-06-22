from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _safe_nullable_div(num: Column, den: Column) -> Column:
    return F.when(
        (den.isNull()) | (den == 0),
        F.lit(None).cast("double"),
    ).otherwise(num / den)


def _get_yesterday(run_date: str) -> str:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return (run_dt - timedelta(days=1)).isoformat()


def _build_base_price_features(spark: SparkSession, run_date: str) -> DataFrame:
    prices = (
        spark.table("iceberg.silver.feature_platform_sku_group_id_prices")
        .filter(F.col("date") == F.lit(run_date).cast("date"))
        .select(
            F.col("date"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("avg_sell_price_eod").cast("double").alias("avg_sell_price_eod"),
            F.col("median_sell_price_eod").cast("double").alias("median_sell_price_eod"),
            F.col("median_full_price_eod").cast("double").alias("median_full_price_eod"),
        )
    )

    sku_categories = (
        spark.table("iceberg.silver.sku")
        .select(
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("category_id").cast("long").alias("category_id"),
        )
        .filter(F.col("sku_group_id").isNotNull())
        .filter(F.col("category_id").isNotNull())
        .distinct()
    )

    category_window = Window.partitionBy("category_id")

    return (
        prices.join(sku_categories, on="sku_group_id", how="inner")
        .withColumn(
            "category_mean_sell_price",
            F.avg("avg_sell_price_eod").over(category_window),
        )
        .select(
            F.col("date"),
            F.col("sku_group_id"),
            F.col("category_mean_sell_price"),
            F.log1p(F.col("avg_sell_price_eod")).alias("sell_price_eod"),
            (F.col("median_full_price_eod") - F.col("median_sell_price_eod")).alias(
                "abs_discount"
            ),
            _safe_nullable_div(
                F.col("median_sell_price_eod"),
                F.col("median_full_price_eod"),
            ).alias("fraq_discount"),
        )
    )


def _build_historical_price_ratios(spark: SparkSession, run_date: str) -> DataFrame:
    yesterday = _get_yesterday(run_date)
    hist_start = (
        datetime.strptime(yesterday, "%Y-%m-%d").date() - timedelta(days=30)
    ).isoformat()
    hist_end = (
        datetime.strptime(yesterday, "%Y-%m-%d").date() - timedelta(days=1)
    ).isoformat()
    hist_14_start = (
        datetime.strptime(yesterday, "%Y-%m-%d").date() - timedelta(days=14)
    ).isoformat()

    source = spark.table("iceberg.silver.feature_platform_sku_group_id_prices")

    yesterday_prices = (
        source.filter(F.col("date") == F.lit(yesterday).cast("date"))
        .select(
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("min_full_price_eod").cast("double").alias("crnt_min_full_price_eod"),
        )
    )

    hist_prices = (
        source.filter(
            (F.col("date") >= F.lit(hist_start).cast("date"))
            & (F.col("date") <= F.lit(hist_end).cast("date"))
        )
        .select(
            F.col("date"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("min_full_price_eod").cast("double").alias("min_full_price_eod"),
        )
        .groupBy("sku_group_id")
        .agg(
            F.avg(
                F.when(
                    F.col("date") >= F.lit(hist_14_start).cast("date"),
                    F.col("min_full_price_eod"),
                )
            ).alias("avg_full_price_14"),
            F.avg("min_full_price_eod").alias("avg_full_price_30"),
        )
    )

    return (
        yesterday_prices.join(hist_prices, on="sku_group_id", how="left")
        .select(
            F.col("sku_group_id"),
            _safe_nullable_div(
                F.col("crnt_min_full_price_eod"),
                F.col("avg_full_price_14"),
            ).alias("ratio_crnt_min_to_avg_min_full_price_14"),
            _safe_nullable_div(
                F.col("crnt_min_full_price_eod"),
                F.col("avg_full_price_30"),
            ).alias("ratio_crnt_min_to_avg_min_full_price_30"),
        )
    )


def build_sku_group_price_features(spark: SparkSession, run_date: str) -> DataFrame:
    base_features = _build_base_price_features(spark, run_date)
    historical_ratios = _build_historical_price_ratios(spark, run_date)

    return (
        base_features.join(historical_ratios, on="sku_group_id", how="left")
        .select(
            F.col("date"),
            F.col("sku_group_id"),
            F.col("category_mean_sell_price"),
            F.col("sell_price_eod"),
            F.col("abs_discount"),
            F.col("fraq_discount"),
            F.col("ratio_crnt_min_to_avg_min_full_price_14"),
            F.col("ratio_crnt_min_to_avg_min_full_price_30"),
        )
    )


def save_sku_group_price_features(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_price_features(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_price_features(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
