from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from job.entities import Arguments


SOURCE_TABLE = "iceberg.silver.feature_platform_sku_group_query_search_orders"


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _window_bounds(run_date: str) -> tuple[str, str]:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return (
        (run_dt - timedelta(days=7)).isoformat(),
        (run_dt - timedelta(days=1)).isoformat(),
    )


def build_sku_group_search_sales_7d(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    start_date, finish_date = _window_bounds(run_date)

    return (
        spark.table(SOURCE_TABLE)
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") <= F.lit(finish_date).cast("date"))
        )
        .filter(F.col("sku_group_id").isNotNull())
        .select(
            F.col("sku_group_id").cast("bigint").alias("sku_group_id"),
            F.col("items_completed").cast("double").alias("items_completed"),
        )
        .groupBy("sku_group_id")
        .agg(
            F.sum(F.col("items_completed").cast("double")).alias(
                "search_sales_count_7d"
            )
        )
        .select(
            F.lit(run_date).cast("date").alias("date"),
            F.col("sku_group_id"),
            F.col("search_sales_count_7d").cast("double"),
        )
    )


def save_sku_group_search_sales_7d(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_search_sales_7d(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_search_sales_7d(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
