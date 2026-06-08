from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from job.entities import Arguments


INPUT_TABLE = "iceberg.gold.feature_platform_query_skg_aggregated_conversions_legacy"
WINDOW_SIZE_DAYS = 30
KEY_COLUMNS = ("date", "query", "sku_group_id")


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _window_start(run_date: str) -> str:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return (run_dt - timedelta(days=WINDOW_SIZE_DAYS)).isoformat()


def build_query_skg_pairwise_features_legacy(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    start_date = _window_start(run_date)
    aggregated = (
        spark.table(INPUT_TABLE)
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") <= F.lit(run_date).cast("date"))
        )
    )

    feature_columns = [column for column in aggregated.columns if column not in KEY_COLUMNS]
    selected = aggregated.select("date", "query", "sku_group_id", *feature_columns)
    window = Window.partitionBy("sku_group_id", "query").orderBy(F.col("date").desc())

    return (
        selected.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == F.lit(1))
        .drop("_rn", "date")
        .withColumn("date", F.lit(run_date).cast("date"))
        .select("date", "query", "sku_group_id", *feature_columns)
    )


def save_query_skg_pairwise_features_legacy(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_query_skg_pairwise_features_legacy(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_query_skg_pairwise_features_legacy(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )

