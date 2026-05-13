from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
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


def _partition_date_expr(partition_date: str):
    return to_date(lit(partition_date))


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_search_results_stats(spark: SparkSession, partition_date: str) -> DataFrame:
    target_date = _partition_date_expr(partition_date)

    df_sessions = (
        spark.table("iceberg.silver_b2c_clickstream.events")
        .filter(
            (to_date(col("received_at")) == target_date)
            & col("session_id").isNotNull()
            & col("install_id").isNotNull()
        )
        .select("session_id")
        .distinct()
    )

    df_events_filtered = spark.table("iceberg.silver_b2c_clickstream.events").filter(
        (to_date(col("received_at")) == target_date)
        & col("event_type").isin(
            "SEARCH_RESULTS",
            "PRODUCT_IMPRESSION",
            "PRODUCT_VIEW",
            "ADD_TO_CART",
        )
        & col("session_id").isNotNull()
        & col("install_id").isNotNull()
    )

    df_events = df_events_filtered.join(
        df_sessions.select("session_id"),
        on="session_id",
        how="inner",
    ).select(
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
        coalesce(col("sku_group_id"), lit(0)).alias("sku_group_id"),
        col("rid"),
        col("logged_at"),
    )

    df_sr = (
        df_events.filter(
            (col("event_type") == "SEARCH_RESULTS")
            & (
                (col("widget_space_name") == "SEARCH_RESULTS")
                | (coalesce(col("widget_section_name"), col("widget_space_name")) == "CATEGORY")
            )
        )
        .select(
            "install_id",
            "session_id",
            when(col("widget_space_name") == "SEARCH_RESULTS", lit("SEARCH_RESULTS"))
            .when(
                coalesce(col("widget_section_name"), col("widget_space_name")) == "CATEGORY",
                lit("CATEGORY"),
            )
            .alias("join_section_name"),
            when(
                (col("widget_space_name") == "SEARCH_RESULTS")
                & (coalesce(col("is_full_catpred"), lit("false")) != "true"),
                lit("SEARCH_RESULTS"),
            )
            .otherwise(lit("CATEGORY"))
            .alias("section"),
            _query_expr("query").alias("event_query"),
            _category_id_expr().alias("category_id"),
            when(
                (col("widget_space_name") == "SEARCH_RESULTS")
                & (coalesce(col("is_full_catpred"), lit("false")) != "true"),
                coalesce(
                    _non_empty_query_expr("initial_query"),
                    _non_empty_query_expr("query"),
                    lit(""),
                ),
            )
            .otherwise(_category_id_expr().cast("string"))
            .alias("uniqs"),
        )
        .distinct()
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
            coalesce(col("widget_section_name"), col("widget_space_name")).alias(
                "join_section_name"
            ),
            _query_expr("query").alias("event_query"),
            _category_id_expr().alias("category_id"),
        )
        .join(
            df_sr,
            ["install_id", "session_id", "join_section_name", "event_query", "category_id"],
            "inner",
        )
        .groupBy("date", "install_id", "session_id", "sku_group_id", "section", "uniqs")
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
        .withColumn(
            "prev_section_name",
            lag(coalesce(col("widget_section_name"), col("widget_space_name"))).over(window),
        )
        .withColumn("prev_event_query", lag(_query_expr("query")).over(window))
        .withColumn("prev_category_id", lag(_category_id_expr()).over(window))
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
            col("prev_section_name").alias("join_section_name"),
            col("prev_event_query").alias("event_query"),
            col("prev_category_id").alias("category_id"),
        )
        .join(
            df_sr,
            ["install_id", "session_id", "join_section_name", "event_query", "category_id"],
            "inner",
        )
        .groupBy("date", "install_id", "session_id", "sku_group_id", "section", "uniqs")
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
        .withColumn(
            "prev_section_name",
            lag(coalesce(col("widget_section_name"), col("widget_space_name"))).over(window),
        )
        .withColumn("prev_event_query", lag(_query_expr("query")).over(window))
        .withColumn("prev_category_id", lag(_category_id_expr()).over(window))
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
            col("prev_section_name").alias("join_section_name"),
            col("prev_event_query").alias("event_query"),
            col("prev_category_id").alias("category_id"),
        )
        .join(
            df_sr,
            ["install_id", "session_id", "join_section_name", "event_query", "category_id"],
            "inner",
        )
        .groupBy("date", "install_id", "session_id", "sku_group_id", "section", "uniqs")
        .count()
        .withColumnRenamed("count", "sum_atc")
    )

    zero = lit(0).cast("long")
    return (
        df_impressions.select(
            "date",
            "install_id",
            "sku_group_id",
            "section",
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
                "section",
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
                "section",
                "uniqs",
                col("sum_atc").cast("long"),
                zero.alias("sum_clicks"),
                zero.alias("sum_impressions"),
            )
        )
        .groupBy("date", "install_id", "sku_group_id", "section", "uniqs")
        .agg(
            spark_sum("sum_atc").alias("sum_atc"),
            spark_sum("sum_clicks").alias("sum_clicks"),
            spark_sum("sum_impressions").alias("sum_impressions"),
        )
    )


def save_search_atc_stats_to_stage(
    spark: SparkSession,
    partition_date: str,
    target_table: str,
) -> None:
    stats = build_search_results_stats(spark, partition_date).select(
        col("install_id").cast("string").alias("install_id"),
        col("sku_group_id").cast("long").alias("sku_group_id"),
        col("section").cast("string").alias("section"),
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
    run_date_str = datetime.strptime(arguments.trigger_date, "%Y-%m-%d").date().isoformat()
    save_search_atc_stats_to_stage(spark, run_date_str, arguments.table_name)
