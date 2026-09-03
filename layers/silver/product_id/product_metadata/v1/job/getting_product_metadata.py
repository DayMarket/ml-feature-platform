from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments
from job.partition import dt_from_partition_end
from job.query import (
    build_category_depth_validation_query,
    build_product_metadata_merge_query,
    build_product_metadata_query,
    build_required_l6_validation_query,
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


def _validate_required_l6(
    spark: SparkSession,
    settings: SourceSettings,
) -> None:
    rows_without_l6 = spark.sql(
        build_required_l6_validation_query(settings)
    ).take(1)
    if rows_without_l6:
        product_id = rows_without_l6[0]["product_id"]
        raise RuntimeError(
            "l6_category_id must contain the final non-technical category; "
            f"example product_id={product_id}"
        )


def build_product_metadata(
    spark: SparkSession,
    dt: datetime,
    settings: SourceSettings,
) -> DataFrame:
    return spark.sql(build_product_metadata_query(settings, dt))


def save_product_metadata(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    spark.conf.set("spark.sql.session.timeZone", "Asia/Tashkent")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    settings = load_source_settings()
    _require_tables(spark, settings.table_names)
    _require_tables(spark, (target_table,))
    _validate_category_depth(spark, settings)
    _validate_required_l6(spark, settings)

    dt = dt_from_partition_end(partition_end)
    metadata = build_product_metadata(
        spark,
        dt,
        settings,
    )
    metadata.createOrReplaceTempView("product_metadata_for_dt")
    spark.sql(build_product_metadata_merge_query(target_table, dt))


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_product_metadata(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
