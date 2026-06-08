from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


ORDER_ITEMS_ATTRIBUTION_TABLE = "iceberg.silver.order_items_attribution"
ORDER_ITEMS_TABLE = "iceberg.silver.order_items"
SKU_TABLE = "iceberg.silver.sku"

HOME_RECOMMENDATION_SPACES = (
    "HOME_RECOMMENDATIONS",
    "HOME_PAGE_RECOMMENDATIONS",
    "MAIN_RECOMMENDATIONS",
    "MAIN_PAGE_RECOMMENDATIONS",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _window_bounds(run_date: str) -> tuple[str, str]:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return (
        (run_dt - timedelta(days=7)).isoformat(),
        run_dt.isoformat(),
    )


def _home_recommendation_filter() -> Column:
    space_name = F.upper(F.coalesce(F.col("widget_space_name"), F.lit("")))
    section_name = F.upper(F.coalesce(F.col("widget_section_name"), F.lit("")))
    placement_name = F.concat_ws(" ", space_name, section_name)

    known_space = space_name.isin(*HOME_RECOMMENDATION_SPACES)
    inferred_home_reco = (
        (placement_name.contains("RECOMMEND") | placement_name.contains("RECO"))
        & (placement_name.contains("HOME") | placement_name.contains("MAIN"))
    )
    return known_space | inferred_home_reco


def build_sku_group_home_reco_avg_sales_7d(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    start_date, finish_date = _window_bounds(run_date)
    start_ts = F.lit(f"{start_date} 00:00:00").cast("timestamp")
    finish_ts = F.lit(f"{finish_date} 00:00:00").cast("timestamp")
    attribution_start_ts = start_ts - F.expr("INTERVAL 20 DAYS")

    attributed_items = (
        spark.table(ORDER_ITEMS_ATTRIBUTION_TABLE)
        .filter(F.col("generated_at") >= attribution_start_ts)
        .filter(F.col("generated_at") < finish_ts)
        .filter(_home_recommendation_filter())
        .select(F.col("order_item_id").cast("bigint").alias("order_item_id"))
        .distinct()
    )

    sku = (
        spark.table(SKU_TABLE)
        .select(
            F.col("id").cast("bigint").alias("sku_id"),
            F.col("sku_group_id").cast("bigint").alias("sku_group_id"),
        )
        .filter(F.col("sku_group_id").isNotNull())
    )

    completed_sales = (
        spark.table(ORDER_ITEMS_TABLE)
        .filter(F.col("order_item_status") == F.lit("COMPLETED"))
        .filter(F.col("generated_at") >= attribution_start_ts)
        .filter(F.col("generated_at") < finish_ts)
        .filter(F.col("issued_at") >= start_ts)
        .filter(F.col("issued_at") < finish_ts)
        .filter((F.col("returned_at").isNull()) | (F.col("returned_at") >= finish_ts))
        .select(
            F.col("order_item_id").cast("bigint").alias("order_item_id"),
            F.col("sku_id").cast("bigint").alias("sku_id"),
            F.to_date(F.col("issued_at")).alias("sale_date"),
            F.col("item_quantity").cast("double").alias("item_quantity"),
        )
    )

    daily_sales = (
        attributed_items.join(completed_sales, on="order_item_id", how="inner")
        .join(sku, on="sku_id", how="inner")
        .groupBy("sku_group_id", "sale_date")
        .agg(F.sum("item_quantity").alias("sales_count"))
    )

    active_sku_groups = daily_sales.select("sku_group_id").distinct()
    days = (
        spark.range(0, 7)
        .select(
            F.date_add(F.lit(start_date).cast("date"), F.col("id").cast("int")).alias(
                "sale_date"
            )
        )
    )

    return (
        active_sku_groups.crossJoin(days)
        .join(daily_sales, on=["sku_group_id", "sale_date"], how="left")
        .fillna({"sales_count": 0.0})
        .groupBy("sku_group_id")
        .agg(
            F.avg(F.col("sales_count")).cast("double").alias(
                "home_reco_avg_sales_count_7d"
            )
        )
        .select(
            F.lit(run_date).cast("date").alias("date"),
            F.col("sku_group_id"),
            F.col("home_reco_avg_sales_count_7d"),
        )
    )


def save_sku_group_home_reco_avg_sales_7d(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_home_reco_avg_sales_7d(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_home_reco_avg_sales_7d(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
