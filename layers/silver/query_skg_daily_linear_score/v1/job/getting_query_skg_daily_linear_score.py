from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments

MODEL_NAME_PREFIX = "search_unified_model_v"


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _normalized_query_expr() -> Column:
    query = F.lower(F.col("search_query"))
    query = F.regexp_replace(query, "ё", "е")
    query = F.regexp_replace(query, r"\s+", " ")
    return F.trim(query).alias("query")


def _json_score_array(json_path: str) -> Column:
    return F.from_json(
        F.get_json_object(F.col("external_features"), json_path),
        "array<double>",
    )


def build_query_skg_daily_linear_score(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
) -> DataFrame:
    events = (
        spark.table("iceberg.silver.ranking_analytics_events")
        .filter(
            (F.col("fired_at") >= F.lit(partition_start))
            & (F.col("fired_at") < F.lit(partition_end))
            & F.col("model_name").startswith(MODEL_NAME_PREFIX)
            & F.col("search_query").isNotNull()
            & F.col("ranking_candidates").isNotNull()
        )
        .select(
            F.to_date(F.col("fired_at")).alias("date"),
            _normalized_query_expr(),
            F.col("ranking_candidates").alias("sku_group_ids"),
            _json_score_array("$.linear_score").alias("linear_scores"),
            _json_score_array("$.normalized_linear_score").alias("normalized_linear_scores"),
        )
    )

    exploded = (
        events.withColumn(
            "candidate",
            F.explode(
                F.arrays_zip("sku_group_ids", "linear_scores", "normalized_linear_scores")
            ),
        )
        .select(
            "date",
            "query",
            F.col("candidate.sku_group_ids").cast("long").alias("sku_group_id"),
            F.col("candidate.linear_scores").alias("linear_score"),
            F.col("candidate.normalized_linear_scores").alias("normalized_linear_score"),
        )
        .filter((F.col("query") != F.lit("")) & F.col("sku_group_id").isNotNull())
    )

    return (
        exploded.groupBy("date", "query", "sku_group_id")
        .agg(
            F.avg("linear_score").alias("avg_linear_score"),
            F.avg("normalized_linear_score").alias("avg_normalized_linear_score"),
            F.count("linear_score").alias("observations"),
        )
        .filter(F.col("avg_linear_score").isNotNull())
        .select(
            F.col("date").cast("date").alias("date"),
            F.col("query"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("avg_linear_score").cast("double").alias("avg_linear_score"),
            F.col("avg_normalized_linear_score").cast("double").alias("avg_normalized_linear_score"),
            F.col("observations").cast("bigint").alias("observations"),
        )
    )


def save_query_skg_daily_linear_score(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_query_skg_daily_linear_score(
        spark,
        partition_start,
        partition_end,
    )
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_query_skg_daily_linear_score(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
    )
