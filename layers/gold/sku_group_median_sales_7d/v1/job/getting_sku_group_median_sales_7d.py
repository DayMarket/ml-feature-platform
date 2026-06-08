from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from job.entities import Arguments


ORDER_ITEMS_TABLE = "iceberg.silver.order_items"
SKU_TABLE = "iceberg.silver.sku"


def build_sku_group_median_sales_7d(
    spark: SparkSession,
    partition_end: str,
):
    cutoff_ts = F.to_timestamp(F.lit(partition_end))
    start_ts = cutoff_ts - F.expr("INTERVAL 7 DAYS")

    order_items = (
        spark.table(ORDER_ITEMS_TABLE)
        .filter(F.col("order_item_status") == F.lit("COMPLETED"))
        .filter(F.col("issued_at") >= start_ts)
        .filter(F.col("issued_at") < cutoff_ts)
        .filter((F.col("returned_at").isNull()) | (F.col("returned_at") >= cutoff_ts))
        .select(
            F.col("sku_id").cast("bigint").alias("sku_id"),
            F.col("issued_at"),
            F.col("item_quantity").cast("double").alias("item_quantity"),
        )
    )

    sku = (
        spark.table(SKU_TABLE)
        .select(
            F.col("id").cast("bigint").alias("sku_id"),
            F.col("sku_group_id").cast("bigint").alias("sku_group_id"),
        )
        .filter(F.col("sku_group_id").isNotNull())
    )

    sales = (
        order_items.join(sku, on="sku_id", how="inner")
        .withColumn(
            "bucket_id",
            F.floor(
                (
                    F.unix_timestamp(F.col("issued_at"))
                    - F.unix_timestamp(start_ts)
                )
                / F.lit(86400)
            ).cast("int"),
        )
        .filter((F.col("bucket_id") >= 0) & (F.col("bucket_id") < 7))
        .groupBy("sku_group_id", "bucket_id")
        .agg(F.sum("item_quantity").alias("sales_count"))
    )

    active_sku_groups = sales.select("sku_group_id").distinct()
    buckets = spark.range(0, 7).select(F.col("id").cast("int").alias("bucket_id"))

    daily_sales = (
        active_sku_groups.crossJoin(buckets)
        .join(sales, on=["sku_group_id", "bucket_id"], how="left")
        .fillna({"sales_count": 0.0})
    )

    return (
        daily_sales.groupBy("sku_group_id")
        .agg(
            F.percentile_approx(F.col("sales_count"), F.lit(0.5)).cast("double").alias(
                "median_sales_count_7d"
            )
        )
        .select(
            F.to_date(F.lit(partition_end)).alias("date"),
            F.col("sku_group_id"),
            F.col("median_sales_count_7d"),
        )
    )


def save_sku_group_median_sales_7d(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    features = build_sku_group_median_sales_7d(spark, partition_end)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_median_sales_7d(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
