from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from job.entities import Arguments
from job.partition import parse_airflow_timestamp
from job.query import build_product_feedback_counts_query
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


def _validate_feedback_counts(features: DataFrame) -> None:
    invalid_rows = features.filter(
        (F.col("feedback_count") < 0)
        | (F.col("rating_sum") < 0)
        | (F.col("feedback_gte_4") < 0)
        | (F.col("feedback_lte_3") < 0)
        | (
            F.col("feedback_gte_4") + F.col("feedback_lte_3")
            != F.col("feedback_count")
        )
        | (F.col("rating_sum") < F.col("feedback_count"))
        | (F.col("rating_sum") > F.lit(5) * F.col("feedback_count"))
    ).take(1)
    if invalid_rows:
        row = invalid_rows[0]
        raise RuntimeError(
            "Product feedback counts violate the output contract; "
            f"example product_id={row['product_id']}"
        )


def build_product_feedback_counts(
    spark: SparkSession,
    partition_end: str,
    settings: SourceSettings,
) -> DataFrame:
    calculated_at = parse_airflow_timestamp(partition_end)
    return spark.sql(build_product_feedback_counts_query(settings, calculated_at))


def save_product_feedback_counts(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    spark.conf.set("spark.sql.session.timeZone", "Asia/Tashkent")
    settings = load_source_settings()
    _require_tables(spark, settings.table_names)
    _require_tables(spark, (target_table,))

    features = build_product_feedback_counts(
        spark,
        partition_end,
        settings,
    )
    _validate_feedback_counts(features)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_product_feedback_counts(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
