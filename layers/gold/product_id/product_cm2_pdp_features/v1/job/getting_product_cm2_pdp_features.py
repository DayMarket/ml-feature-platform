"""Build the PDP CM2 product snapshot and write it to Iceberg."""

from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments
from job.partition import parse_airflow_timestamp
from job.query import (
    build_product_cm2_pdp_features_merge_query,
    build_product_cm2_pdp_features_query,
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
            f"Required Iceberg tables are missing: {', '.join(missing_tables)}"
        )


def build_product_cm2_pdp_features(
    spark: SparkSession,
    partition_end: str,
    settings: SourceSettings,
) -> DataFrame:
    calculated_at = parse_airflow_timestamp(partition_end)
    return spark.sql(build_product_cm2_pdp_features_query(settings, calculated_at))


def save_product_cm2_pdp_features(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    settings = load_source_settings()
    _require_tables(spark, settings.table_names)
    _require_tables(spark, (target_table,))
    calculated_at = parse_airflow_timestamp(partition_end)
    features = spark.sql(build_product_cm2_pdp_features_query(settings, calculated_at))
    features.cache()
    try:
        if not features.take(1):
            raise RuntimeError(
                "PDP CM2 produced no rows; check the selected S6 snapshot, commissions, and USD rate"
            )
        features.createOrReplaceTempView("product_cm2_pdp_features_for_calculated_at")
        spark.sql(
            build_product_cm2_pdp_features_merge_query(
                target_table,
                calculated_at,
                settings.business_timezone,
            )
        )
    finally:
        features.unpersist()


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_product_cm2_pdp_features(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
