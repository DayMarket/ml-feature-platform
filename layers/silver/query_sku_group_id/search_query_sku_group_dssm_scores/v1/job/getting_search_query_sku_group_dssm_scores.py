from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from job.entities import Arguments
from job.partition import parse_airflow_timestamp, utc_day_bounds_from_interval_start


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _unquote_scalar(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _read_simple_config(path: Path) -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    stack = [(-1, config)]
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()
        key, separator, value = line.partition(":")
        if not separator or not key:
            continue

        while stack and indent <= stack[-1][0]:
            stack.pop()

        parent = stack[-1][1]
        if value.strip():
            parent[key.strip()] = _unquote_scalar(value.strip())
        else:
            nested: Dict[str, Any] = {}
            parent[key.strip()] = nested
            stack.append((indent, nested))
    return config


def _load_config() -> Dict[str, Any]:
    return _read_simple_config(Path(__file__).resolve().parent.parent / "config.yaml")


def _format_spark_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _source_events(
    spark: SparkSession,
    config: Dict[str, Any],
    window_start: datetime,
    window_end: datetime,
) -> DataFrame:
    source = config["source"]
    return (
        spark.table(source["ranking_events_table"])
        .filter(
            (F.col("fired_at") >= F.lit(_format_spark_timestamp(window_start)).cast("timestamp"))
            & (F.col("fired_at") < F.lit(_format_spark_timestamp(window_end)).cast("timestamp"))
            & F.col("model_name").like(source["model_name_like"])
            & F.col("ranking_candidates").isNotNull()
            & (F.size(F.col("ranking_candidates")) > F.lit(0))
            & F.col("search_query").isNotNull()
            & (F.trim(F.col("search_query")) != F.lit(""))
            & F.col("external_features").isNotNull()
        )
        .select(
            F.col("fired_at").cast("timestamp").alias("source_fired_at"),
            F.col("search_query").alias("query"),
            F.col("ranking_candidates").alias("ranking_candidates"),
            F.get_json_object(
                F.col("external_features"),
                source["dssm_json_path"],
            ).alias("dssm_score_json"),
        )
    )


def _explode_dssm_scores(events: DataFrame) -> DataFrame:
    prepared = (
        events.withColumn(
            "dssm_score_array",
            F.from_json(F.col("dssm_score_json"), ArrayType(StringType())),
        )
        .withColumn(
            "dssm_score_scalar",
            F.regexp_replace(F.col("dssm_score_json"), '^"|"$', "").cast("double"),
        )
    )

    array_scores = (
        prepared.filter(
            F.col("dssm_score_array").isNotNull()
            & (F.size(F.col("dssm_score_array")) == F.size(F.col("ranking_candidates")))
        )
        .withColumn(
            "candidate_score",
            F.explode(F.arrays_zip(F.col("ranking_candidates"), F.col("dssm_score_array"))),
        )
        .select(
            F.col("source_fired_at"),
            F.col("query"),
            F.col("candidate_score.ranking_candidates").cast("long").alias("sku_group_id"),
            F.col("candidate_score.dssm_score_array").cast("double").alias("dssm_score"),
        )
    )

    scalar_scores = (
        prepared.filter(
            F.col("dssm_score_array").isNull()
            & F.col("dssm_score_scalar").isNotNull()
        )
        .withColumn("sku_group_id_raw", F.explode(F.col("ranking_candidates")))
        .select(
            F.col("source_fired_at"),
            F.col("query"),
            F.col("sku_group_id_raw").cast("long").alias("sku_group_id"),
            F.col("dssm_score_scalar").alias("dssm_score"),
        )
    )

    return (
        array_scores.unionByName(scalar_scores)
        .filter(F.col("sku_group_id").isNotNull() & F.col("dssm_score").isNotNull())
    )


def _latest_daily_source(scores: DataFrame, partition_start: datetime) -> DataFrame:
    partition_date = partition_start.date().isoformat()
    window = Window.partitionBy("date", "query", "sku_group_id").orderBy(
        F.col("source_fired_at").desc(),
        F.col("dssm_score").desc(),
    )
    return (
        scores.withColumn("date", F.lit(partition_date).cast("date"))
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == F.lit(1))
        .select(
            F.col("date"),
            F.col("query"),
            F.col("sku_group_id"),
            F.col("dssm_score"),
        )
    )


def _latest_existing(
    spark: SparkSession,
    target_table: str,
    partition_start: datetime,
) -> DataFrame:
    partition_date = partition_start.date().isoformat()
    window = Window.partitionBy("date", "query", "sku_group_id").orderBy(
        F.col("collected_at").desc()
    )
    return (
        spark.table(target_table)
        .filter(F.col("date") == F.lit(partition_date).cast("date"))
        .select(
            F.col("date"),
            F.col("query"),
            F.col("sku_group_id"),
            F.col("dssm_score").alias("old_dssm_score"),
            F.col("collected_at"),
        )
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == F.lit(1))
        .select("date", "query", "sku_group_id", "old_dssm_score")
    )


def _changed_rows(
    source_scores: DataFrame,
    existing_scores: DataFrame,
    change_threshold: float,
) -> DataFrame:
    joined = source_scores.join(
        existing_scores,
        on=["date", "query", "sku_group_id"],
        how="left",
    )
    old_score = F.col("old_dssm_score")
    new_score = F.col("dssm_score")
    changed = (
        old_score.isNull()
        | ((old_score == F.lit(0.0)) & (F.abs(new_score) > F.lit(0.0)))
        | (
            (old_score != F.lit(0.0))
            & ((F.abs(new_score - old_score) / F.abs(old_score)) >= F.lit(change_threshold))
        )
    )
    return (
        joined.filter(changed)
        .select("date", "query", "sku_group_id", "dssm_score")
        .withColumn("collected_at", F.current_timestamp())
        .select("date", "query", "sku_group_id", "collected_at", "dssm_score")
    )


def build_search_query_sku_group_dssm_scores(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
    config: Dict[str, Any],
) -> DataFrame:
    window_start = parse_airflow_timestamp(partition_start)
    window_end = parse_airflow_timestamp(partition_end)
    if window_end <= window_start:
        raise ValueError(
            f"partition_end must be greater than partition_start: {partition_start!r}, {partition_end!r}"
        )

    day_start, day_end = utc_day_bounds_from_interval_start(partition_start)
    raw_events = _source_events(spark, config, day_start, day_end)
    source_scores = _latest_daily_source(_explode_dssm_scores(raw_events), day_start)
    existing_scores = _latest_existing(spark, target_table, day_start)
    return _changed_rows(
        source_scores,
        existing_scores,
        float(config["source"]["change_threshold"]),
    )


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


def save_search_query_sku_group_dssm_scores(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_search_query_sku_group_dssm_scores(
        spark,
        partition_start,
        partition_end,
        target_table,
        _load_config(),
    )
    _align_to_target_schema(spark, features, target_table).writeTo(target_table).append()


def run(spark: SparkSession, arguments: Arguments):
    save_search_query_sku_group_dssm_scores(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
    )
