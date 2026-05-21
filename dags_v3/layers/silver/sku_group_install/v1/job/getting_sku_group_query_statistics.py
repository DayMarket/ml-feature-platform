from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    expr,
    get_json_object,
    lag,
    lit,
    regexp_replace,
    sum as spark_sum,
    to_date,
    trim,
    when,
)

from job.entities import Arguments


def _category_id_expr(
    category_col: str = "category_id",
    section_col: str = "widget_section_id",
):
    return (
        when(
            col(category_col).isNotNull() & ~col(category_col).isin(-1, 0),
            col(category_col),
        )
        .when(col(section_col).isNotNull(), col(section_col).cast("long"))
        .otherwise(lit(0))
    )


def _query_expr(query_col: str = "query"):
    return coalesce(trim(_normalize_query_quotes(col(query_col))), lit(""))


def _non_empty_query_expr(query_col: str):
    trimmed_query = trim(_normalize_query_quotes(col(query_col)))
    return when(trimmed_query != "", trimmed_query)


def _normalize_query_quotes(column):
    return regexp_replace(column, r"[`'ʻ']", "'")


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_search_results_stats(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
) -> DataFrame:
    received_at_start = lit(partition_start).cast("timestamp") - expr("INTERVAL 2 DAY")
    received_at_end = lit(partition_end).cast("timestamp") + expr("INTERVAL 2 DAY")
    logged_at_start = lit(partition_start).cast("timestamp")
    logged_at_end = lit(partition_end).cast("timestamp")

    df_sessions = (
        spark.table("iceberg.silver_b2c_clickstream.events")
        .filter(
            (col("received_at") >= received_at_start)
            & (col("received_at") < received_at_end)
            & (col("logged_at") >= logged_at_start)
            & (col("logged_at") < logged_at_end)
            & col("session_id").isNotNull()
            & col("install_id").isNotNull()
        )
        .select("session_id")
        .distinct()
    )

    df_events_filtered = spark.table("iceberg.silver_b2c_clickstream.events").filter(
        (col("received_at") >= received_at_start)
        & (col("received_at") < received_at_end)
        & (col("logged_at") >= logged_at_start)
        & (col("logged_at") < logged_at_end)
        & col("event_type").isin(
            "SEARCH_RESULTS",
            "PRODUCT_IMPRESSION",
            "PRODUCT_VIEW",
            "ADD_TO_CART",
        )
        & col("session_id").isNotNull()
        & col("install_id").isNotNull()
    )

    df_skus = (
        spark.table("iceberg.silver.sku")
        .select(
            col("id").alias("skus_id"),
            col("sku_group_id").alias("skus_sku_group_id"),
        )
        .distinct()
    )

    df_events = (
        df_events_filtered.join(
            df_sessions.select("session_id"),
            on="session_id",
            how="inner",
        )
        .join(df_skus, col("sku_id") == col("skus_id"), "left")
        .select(
            col("session_id"),
            col("install_id").cast("string").alias("install_id"),
            col("received_at"),
            col("event_type"),
            col("widget_space_name"),
            col("widget_section_name"),
            col("widget_group_position"),
            col("category_id"),
            col("widget_section_id"),
            col("query"),
            get_json_object(
                col("event_properties"),
                "$.event_parameters.initial_query",
            ).alias("initial_query"),
            get_json_object(
                col("event_properties"),
                "$.event_parameters.is_category_full_matched",
            ).alias("is_full_catpred"),
            col("product_id"),
            col("sku_id"),
            when(
                col("sku_group_id").isNull() | (col("sku_group_id") == 0),
                coalesce(col("skus_sku_group_id"), lit(0)),
            )
            .otherwise(col("sku_group_id"))
            .alias("sku_group_id"),
            col("rid"),
            col("logged_at"),
        )
    )

    df_impressions = (
        df_events.filter(
            (col("event_type") == "PRODUCT_IMPRESSION")
            & coalesce(col("widget_section_name"), col("widget_space_name")).isin(
                "SEARCH_RESULTS",
                "CATEGORY",
            )
        )
        .select(
            to_date(col("received_at")).alias("date"),
            "install_id",
            "session_id",
            "sku_group_id",
            when(
                (col("widget_space_name") == "SEARCH_RESULTS")
                & (coalesce(col("is_full_catpred"), lit("false")) != "true"),
                lit("SEARCH_RESULTS"),
            )
            .otherwise(lit("CATEGORY"))
            .alias("space"),
            when(
                (col("widget_space_name") == "SEARCH_RESULTS")
                & (coalesce(col("is_full_catpred"), lit("false")) != "true"),
                coalesce(
                    _non_empty_query_expr("query"),
                    _non_empty_query_expr("initial_query"),
                    lit(""),
                ),
            )
            .otherwise(_category_id_expr().cast("string"))
            .alias("uniqs"),
        )
        .groupBy("date", "install_id", "session_id", "sku_group_id", "space", "uniqs")
        .count()
        .withColumnRenamed("count", "sum_impressions")
    )

    window = Window.partitionBy("session_id", "product_id").orderBy("logged_at")

    df_clicks_window = (
        df_events.filter(
            col("event_type").isin("PRODUCT_IMPRESSION", "PRODUCT_VIEW")
            & col("product_id").isNotNull()
        )
        .withColumn("date", to_date(col("received_at")))
        .withColumn("prev_event_type", lag("event_type").over(window))
        .withColumn("prev_sku_group_id", lag("sku_group_id").over(window))
        .withColumn("prev_is_full_catpred", lag("is_full_catpred").over(window))
        .withColumn("prev_category_id", lag("category_id").over(window))
        .withColumn("prev_widget_section_id", lag("widget_section_id").over(window))
        .withColumn("prev_query", lag("query").over(window))
        .withColumn("prev_initial_query", lag("initial_query").over(window))
        .withColumn(
            "prev_section_name",
            lag(coalesce(col("widget_section_name"), col("widget_space_name"))).over(window),
        )
    )

    df_clicks = (
        df_clicks_window.filter(
            (col("event_type") == "PRODUCT_VIEW")
            & (col("prev_event_type") == "PRODUCT_IMPRESSION")
            & col("prev_section_name").isin("SEARCH_RESULTS", "CATEGORY")
        )
        .select(
            "date",
            "install_id",
            "session_id",
            coalesce(col("prev_sku_group_id"), lit(0)).alias("sku_group_id"),
            when(
                (col("prev_section_name") == "SEARCH_RESULTS")
                & (coalesce(col("prev_is_full_catpred"), lit("false")) != "true"),
                lit("SEARCH_RESULTS"),
            )
            .otherwise(lit("CATEGORY"))
            .alias("space"),
            when(
                (col("prev_section_name") == "SEARCH_RESULTS")
                & (coalesce(col("prev_is_full_catpred"), lit("false")) != "true"),
                coalesce(
                    _non_empty_query_expr("prev_query"),
                    _non_empty_query_expr("prev_initial_query"),
                    lit(""),
                ),
            )
            .otherwise(
                _category_id_expr("prev_category_id", "prev_widget_section_id").cast("string")
            )
            .alias("uniqs"),
        )
        .groupBy("date", "install_id", "session_id", "sku_group_id", "space", "uniqs")
        .count()
        .withColumnRenamed("count", "sum_clicks")
    )

    df_atc_window = (
        df_events.filter(
            col("event_type").isin("PRODUCT_IMPRESSION", "ADD_TO_CART")
            & col("product_id").isNotNull()
        )
        .withColumn("date", to_date(col("received_at")))
        .withColumn("prev_event_type", lag("event_type").over(window))
        .withColumn("prev_sku_group_id", lag("sku_group_id").over(window))
        .withColumn("prev_is_full_catpred", lag("is_full_catpred").over(window))
        .withColumn("prev_category_id", lag("category_id").over(window))
        .withColumn("prev_widget_section_id", lag("widget_section_id").over(window))
        .withColumn("prev_query", lag("query").over(window))
        .withColumn("prev_initial_query", lag("initial_query").over(window))
        .withColumn(
            "prev_section_name",
            lag(coalesce(col("widget_section_name"), col("widget_space_name"))).over(window),
        )
    )

    df_atc = (
        df_atc_window.filter(
            (col("event_type") == "ADD_TO_CART")
            & (col("prev_event_type") == "PRODUCT_IMPRESSION")
            & col("prev_section_name").isin("SEARCH_RESULTS", "CATEGORY")
        )
        .select(
            "date",
            "install_id",
            "session_id",
            coalesce(col("prev_sku_group_id"), lit(0)).alias("sku_group_id"),
            when(
                (col("prev_section_name") == "SEARCH_RESULTS")
                & (coalesce(col("prev_is_full_catpred"), lit("false")) != "true"),
                lit("SEARCH_RESULTS"),
            )
            .otherwise(lit("CATEGORY"))
            .alias("space"),
            when(
                (col("prev_section_name") == "SEARCH_RESULTS")
                & (coalesce(col("prev_is_full_catpred"), lit("false")) != "true"),
                coalesce(
                    _non_empty_query_expr("prev_query"),
                    _non_empty_query_expr("prev_initial_query"),
                    lit(""),
                ),
            )
            .otherwise(
                _category_id_expr("prev_category_id", "prev_widget_section_id").cast("string")
            )
            .alias("uniqs"),
        )
        .groupBy("date", "install_id", "session_id", "sku_group_id", "space", "uniqs")
        .count()
        .withColumnRenamed("count", "sum_atc")
    )

    zero = lit(0).cast("long")
    return (
        df_impressions.select(
            "date",
            "install_id",
            "sku_group_id",
            "space",
            "uniqs",
            zero.alias("sum_atc"),
            zero.alias("sum_clicks"),
            col("sum_impressions").cast("long"),
        )
        .unionByName(
            df_clicks.select(
                "date",
                "install_id",
                "sku_group_id",
                "space",
                "uniqs",
                zero.alias("sum_atc"),
                col("sum_clicks").cast("long"),
                zero.alias("sum_impressions"),
            )
        )
        .unionByName(
            df_atc.select(
                "date",
                "install_id",
                "sku_group_id",
                "space",
                "uniqs",
                col("sum_atc").cast("long"),
                zero.alias("sum_clicks"),
                zero.alias("sum_impressions"),
            )
        )
        .groupBy("date", "install_id", "sku_group_id", "space", "uniqs")
        .agg(
            spark_sum("sum_atc").alias("sum_atc"),
            spark_sum("sum_clicks").alias("sum_clicks"),
            spark_sum("sum_impressions").alias("sum_impressions"),
        )
    )


def save_search_atc_stats_to_stage(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
) -> None:
    stats = build_search_results_stats(spark, partition_start, partition_end).select(
        col("install_id").cast("string").alias("install_id"),
        col("sku_group_id").cast("long").alias("sku_group_id"),
        col("space").cast("string").alias("space"),
        col("uniqs").cast("string").alias("uniqs"),
        col("sum_atc").cast("long").alias("sum_atc"),
        col("sum_clicks").cast("long").alias("sum_clicks"),
        col("sum_impressions").cast("long").alias("sum_impressions"),
        col("date"),
    )

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    stats.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_search_atc_stats_to_stage(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
    )
