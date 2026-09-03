from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments
from job.partition import parse_airflow_timestamp
from job.query import (
    build_account_l1_imp_counts_merge_query,
    build_account_l1_imp_counts_query,
    build_category_path_validation_query,
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


def _validate_category_path(
    spark: SparkSession,
    settings: SourceSettings,
) -> None:
    categories_deeper_than_supported = spark.sql(
        build_category_path_validation_query(settings)
    ).take(1)
    if categories_deeper_than_supported:
        category_id = categories_deeper_than_supported[0]["id"]
        raise RuntimeError(
            "Category hierarchy exceeds the supported traversal depth of "
            "8 levels; "
            f"example category_id={category_id}"
        )


def build_account_l1_imp_counts(
    spark: SparkSession,
    partition_end: str,
    settings: SourceSettings,
) -> DataFrame:
    calculated_at = parse_airflow_timestamp(partition_end)
    return spark.sql(
        build_account_l1_imp_counts_query(settings, calculated_at)
    )


def save_account_l1_imp_counts(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    settings = load_source_settings()
    _require_tables(spark, settings.table_names)
    _require_tables(spark, (target_table,))
    _validate_category_path(spark, settings)

    calculated_at = parse_airflow_timestamp(partition_end)
    features = spark.sql(
        build_account_l1_imp_counts_query(settings, calculated_at)
    )
    features.createOrReplaceTempView(
        "account_l1_imp_counts_for_calculated_at"
    )
    spark.sql(
        build_account_l1_imp_counts_merge_query(
            target_table,
            calculated_at,
        )
    )


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_account_l1_imp_counts(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
