from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _events_for_day(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
) -> DataFrame:
    return (
        spark.table("iceberg.silver_b2c_clickstream.events")
        .filter(
            (F.col("received_at") >= F.lit(partition_start))
            & (F.col("received_at") < F.lit(partition_end))
            & F.col("event_type").isin(
                "PRODUCT_IMPRESSION",
                "PRODUCT_VIEW",
                "ADD_TO_CART",
                "",
            )
            & F.col("install_id").isNotNull()
            & F.col("session_id").isNotNull()
        )
        .select(
            "received_at",
            "platform",
            "sku_group_id",
            "sku_id",
            "event_type",
            "session_id",
            "install_id",
            "widget_section_name",
            "query",
            "logged_at",
        )
    )


def _sku_mapping(spark: SparkSession) -> DataFrame:
    return (
        spark.table("iceberg.silver.sku")
        .select(
            F.col("id").alias("sku_id_map"),
            F.col("sku_group_id").cast("long").alias("sku_group_id_map"),
        )
        .distinct()
    )


def _legacy_query_expr() -> Column:
    return (
        F.when(F.col("query").isNull(), F.col("prev_query"))
        .otherwise(F.lower(F.col("query")))
        .alias("query")
    )


def _build_daily_event_conversions(
    events: DataFrame,
    skus: DataFrame,
    conversion_event_type: str,
) -> DataFrame:
    window = Window.partitionBy("session_id", "skg_id").orderBy("logged_at")
    prepared = (
        events.join(skus, events["sku_id"] == skus["sku_id_map"], "left")
        .withColumn("date", F.to_date(F.col("received_at")))
        .withColumn(
            "skg_id",
            F.when(
                F.col("sku_group_id").isNull(),
                F.col("sku_group_id_map"),
            ).otherwise(F.col("sku_group_id").cast("long")),
        )
        .withColumn(
            "prev_query",
            F.lag(F.lower(F.col("query")), 1, "0").over(window),
        )
        .withColumn(
            "prev_product_list",
            F.lag(F.col("widget_section_name"), 1, "0").over(window),
        )
    )

    return (
        prepared.groupBy(
            "date",
            "platform",
            F.col("skg_id").alias("sku_group_id"),
            _legacy_query_expr(),
        )
        .agg(
            F.countDistinct(
                F.when(
                    (F.col("event_type") == F.lit("PRODUCT_IMPRESSION"))
                    & (F.col("widget_section_name") == F.lit("SEARCH_RESULTS")),
                    F.col("session_id"),
                )
            ).alias("uniq_impressions"),
            F.countDistinct(
                F.when(
                    (F.col("event_type") == F.lit(conversion_event_type))
                    & (F.col("prev_product_list") == F.lit("SEARCH_RESULTS")),
                    F.col("session_id"),
                )
            ).alias("uniq_conversions"),
        )
    )


def _build_daily_orders(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
) -> DataFrame:
    search_attribution = (
        spark.table("iceberg.silver.order_items_attribution")
        .filter(
            (F.col("generated_at") >= F.lit(partition_start))
            & (F.col("generated_at") < F.lit(partition_end))
            & (F.col("widget_section_name") == F.lit("SEARCH_RESULTS"))
        )
        .select(
            F.col("order_item_id"),
            F.col("query"),
            F.col("last_atc_platform").alias("platform"),
        )
        .distinct()
    )

    order_items = (
        spark.table("iceberg.silver.order_items")
        .filter(
            (F.col("generated_at") >= F.lit(partition_start))
            & (F.col("generated_at") < F.lit(partition_end))
            & ~F.col("order_item_status").isin("CREATED", "NOT_CREATED")
        )
        .select(
            F.col("order_item_id"),
            F.col("sku_id"),
        )
    )

    skus = _sku_mapping(spark)

    return (
        search_attribution.join(order_items, on="order_item_id", how="inner")
        .join(skus, order_items["sku_id"] == skus["sku_id_map"], how="left")
        .groupBy(
            "query",
            "platform",
            F.col("sku_group_id_map").alias("sku_group_id"),
        )
        .agg(F.countDistinct("order_item_id").alias("uniq_orders"))
    )


def build_query_skg_daily_conversions_legacy(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
) -> DataFrame:
    events = _events_for_day(spark, partition_start, partition_end)
    skus = _sku_mapping(spark)

    daily_clicks = _build_daily_event_conversions(
        events,
        skus,
        "PRODUCT_VIEW",
    ).withColumnRenamed("uniq_conversions", "uniq_clicks")

    daily_atcs = (
        _build_daily_event_conversions(
            events.filter(F.col("event_type").isin("PRODUCT_IMPRESSION", "ADD_TO_CART")),
            skus,
            "ADD_TO_CART",
        )
        .withColumnRenamed("uniq_conversions", "uniq_atcs")
        .drop("uniq_impressions")
    )

    daily_orders = _build_daily_orders(spark, partition_start, partition_end)

    daily_conversions = (
        daily_clicks.join(
            daily_atcs,
            on=["date", "query", "platform", "sku_group_id"],
            how="inner",
        )
        .join(
            daily_orders,
            on=["query", "platform", "sku_group_id"],
            how="left",
        )
        .withColumn("query", F.trim(F.col("query")))
    )

    for metric in ("uniq_impressions", "uniq_clicks", "uniq_atcs", "uniq_orders"):
        daily_conversions = daily_conversions.withColumn(
            metric,
            F.coalesce(F.col(metric), F.lit(0)).cast("bigint"),
        )

    return (
        daily_conversions.filter(F.col("sku_group_id").isNotNull())
        .filter(F.col("platform").isNotNull())
        .filter((F.col("query") != F.lit("0")) & (F.col("query") != F.lit("")))
        .filter(F.col("uniq_impressions") != F.lit(0))
        .select(
            F.col("date").cast("date").alias("date"),
            F.col("query"),
            F.col("platform"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            "uniq_impressions",
            "uniq_clicks",
            "uniq_atcs",
            "uniq_orders",
        )
    )


def save_query_skg_daily_conversions_legacy(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_query_skg_daily_conversions_legacy(
        spark,
        partition_start,
        partition_end,
    )
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_query_skg_daily_conversions_legacy(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
    )
