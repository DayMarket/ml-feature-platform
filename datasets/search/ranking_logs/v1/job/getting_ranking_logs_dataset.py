from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments, DatasetSettings
from job.partition import collection_date as run_collection_date
from job.partition import event_date_bounds
from job.query import build_dataset_query
from job.settings import load_dataset_settings


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_ranking_logs_dataset(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    settings: DatasetSettings,
):
    event_date_start, event_date_end = event_date_bounds(partition_start, partition_end)
    return spark.sql(
        build_dataset_query(
            collection_date=run_collection_date(partition_end),
            event_date_start=event_date_start,
            event_date_end=event_date_end,
            settings=settings,
        )
    )


def save_ranking_logs_dataset(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
    settings: DatasetSettings,
) -> None:
    dataset = build_ranking_logs_dataset(spark, partition_start, partition_end, settings)

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    dataset.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_ranking_logs_dataset(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
        load_dataset_settings(),
    )
