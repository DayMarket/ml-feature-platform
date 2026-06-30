from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from job.entities import Arguments
from job.partition import parse_airflow_timestamp


LOOKBACK_DAYS = 14
MODEL_NAME = "search_unified_model_v6"
TOP_QUERIES_LIMIT = 200


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _format_spark_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _ranking_events(
    spark: SparkSession,
    window_start: datetime,
    window_end: datetime,
) -> DataFrame:
    return (
        spark.table("iceberg.silver.ranking_analytics_events")
        .filter(
            (F.col("fired_at") >= F.lit(_format_spark_timestamp(window_start)).cast("timestamp"))
            & (F.col("fired_at") < F.lit(_format_spark_timestamp(window_end)).cast("timestamp"))
            & (F.col("model_name") == F.lit(MODEL_NAME))
            & F.col("ranking_candidates").isNotNull()
            & (F.size(F.col("ranking_candidates")) > F.lit(0))
        )
        .select(
            F.col("search_query"),
            F.col("ranking_candidates"),
            F.col("install_id"),
        )
    )


def _sku_product_mapping(spark: SparkSession) -> DataFrame:
    return (
        spark.table("iceberg.silver.sku")
        .select(
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("product_id").cast("long").alias("product_id"),
        )
        .filter(F.col("sku_group_id").isNotNull() & F.col("product_id").isNotNull())
        .distinct()
    )


def build_product_search_queries(
    spark: SparkSession,
    partition_end: str,
) -> DataFrame:
    interval_end = parse_airflow_timestamp(partition_end)
    interval_start = interval_end - timedelta(days=LOOKBACK_DAYS)
    partition_date = interval_end.date().isoformat()

    events = _ranking_events(spark, interval_start, interval_end)
    installs_by_query = events.groupBy("search_query").agg(
        F.countDistinct("install_id").cast("long").alias("uniq_installs")
    )

    candidates = (
        events.select(
            F.col("search_query"),
            F.explode(F.col("ranking_candidates")).alias("sku_group_id_raw"),
        )
        .select(
            F.col("search_query"),
            F.col("sku_group_id_raw").cast("long").alias("sku_group_id"),
        )
    )

    product_candidates = candidates.join(_sku_product_mapping(spark), on="sku_group_id", how="inner")

    product_queries = (
        product_candidates.alias("candidate")
        .join(
            installs_by_query.alias("install"),
            F.col("candidate.search_query").eqNullSafe(F.col("install.search_query")),
            how="inner",
        )
        .select(
            F.col("candidate.product_id"),
            F.col("candidate.search_query"),
            F.col("install.uniq_installs"),
        )
        .distinct()
    )

    sortable_queries = product_queries.select(
        F.col("product_id"),
        F.struct(
            (-F.col("uniq_installs")).alias("sort_uniq_installs"),
            F.col("search_query").alias("search_query"),
            F.col("uniq_installs").alias("uniq_installs"),
        ).alias("query_item"),
    )

    return (
        sortable_queries.groupBy("product_id")
        .agg(F.array_sort(F.collect_list("query_item")).alias("sorted_queries"))
        .select(
            F.lit(partition_date).cast("date").alias("date"),
            F.col("product_id").cast("long").alias("product_id"),
            F.transform(
                F.slice(F.col("sorted_queries"), 1, TOP_QUERIES_LIMIT),
                lambda query: query["search_query"],
            ).alias("search_queries"),
        )
    )


def save_product_search_queries(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_product_search_queries(spark, partition_end)
    _align_to_target_schema(spark, features, target_table).writeTo(
        target_table
    ).overwritePartitions()


def _align_to_target_schema(
    spark: SparkSession,
    frame: DataFrame,
    target_table: str,
) -> DataFrame:
    target_schema = spark.table(target_table).schema
    aligned_frame = frame

    for field in target_schema.fields:
        if field.name not in aligned_frame.columns:
            aligned_frame = aligned_frame.withColumn(
                field.name,
                F.lit(None).cast(field.dataType),
            )

    return aligned_frame.select(*(F.col(field.name) for field in target_schema.fields))


def run(spark: SparkSession, arguments: Arguments):
    save_product_search_queries(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
