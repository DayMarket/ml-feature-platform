from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments
from job.partition import parse_airflow_timestamp
from job.query import build_account_product_session_action_counts_query
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


def build_account_product_session_action_counts(
    spark: SparkSession,
    partition_end: str,
    settings: SourceSettings,
) -> DataFrame:
    calculated_at = parse_airflow_timestamp(partition_end)
    return spark.sql(
        build_account_product_session_action_counts_query(
            settings,
            calculated_at,
        )
    )


def save_account_product_session_action_counts(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    settings = load_source_settings()
    _require_tables(spark, settings.table_names)
    _require_tables(spark, (target_table,))

    features = build_account_product_session_action_counts(
        spark,
        partition_end,
        settings,
    )
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_account_product_session_action_counts(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
