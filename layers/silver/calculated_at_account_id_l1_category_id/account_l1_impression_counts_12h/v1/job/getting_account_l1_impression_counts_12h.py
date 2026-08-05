from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments
from job.partition import parse_airflow_timestamp
from job.query import (
    build_account_l1_impression_counts_query,
    build_category_depth_validation_query,
)
from job.runtime_config import SourceSettings, load_source_settings


def _require_tables(spark: SparkSession, table_names: tuple[str, ...]) -> None:
    missing_tables = [
        table_name
        for table_name in table_names
        if not spark.catalog.tableExists(table_name)
    ]
    if missing_tables:
        raise RuntimeError(
            "Required Iceberg tables are missing: "
            f"{', '.join(missing_tables)}"
        )


def _validate_category_depth(
    spark: SparkSession,
    settings: SourceSettings,
) -> None:
    categories_deeper_than_supported = spark.sql(
        build_category_depth_validation_query(settings)
    ).take(1)
    if categories_deeper_than_supported:
        category_id = categories_deeper_than_supported[0]["id"]
        raise RuntimeError(
            "Category hierarchy exceeds the supported depth of "
            f"{settings.max_category_depth} levels; "
            f"example category_id={category_id}"
        )


def build_account_l1_impression_counts(
    spark: SparkSession,
    partition_end: str,
    settings: SourceSettings,
) -> DataFrame:
    calculated_at = parse_airflow_timestamp(partition_end)
    return spark.sql(
        build_account_l1_impression_counts_query(settings, calculated_at)
    )


def save_account_l1_impression_counts(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    settings = load_source_settings()
    _require_tables(spark, settings.table_names)
    _require_tables(spark, (target_table,))
    _validate_category_depth(spark, settings)

    features = build_account_l1_impression_counts(
        spark,
        partition_end,
        settings,
    )
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_account_l1_impression_counts(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
