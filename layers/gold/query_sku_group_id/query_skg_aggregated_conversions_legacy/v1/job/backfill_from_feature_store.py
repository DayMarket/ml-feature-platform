"""One-off backfill: copy aggregated conversions from the upstream feature_store
mart into the gold legacy table for a single partition date.

This is NOT the regular gold pipeline (which is computed from
`iceberg.silver.feature_platform_query_skg_daily_conversions_legacy`). It exists
only to fill the gold partitions that were never backfilled by the Spark job,
by copying the already-validated upstream snapshot:

    iceberg.um_prod_feature_store_iceberg.query_query_skg_aggregated_conversions

The two sources were validated as equivalent on overlapping dates (feature
values correlate ~0.98-0.999 on shared keys; the upstream is a subset of the
computed gold by ~6% of rows). Lineage for the backfilled partitions therefore
differs from the silver-computed partitions on purpose.

The copy is a pure projection (no shuffle): it selects the source columns by
name in the gold schema order and casts each to the gold column type
(varchar date -> date, integer sku_group_id -> bigint, bigint uniq_* counters
-> double). `stored_at` and any other extra source column is dropped.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from job.entities import Arguments


SOURCE_TABLE = (
    "iceberg.um_prod_feature_store_iceberg.query_query_skg_aggregated_conversions"
)


def build_backfill_partition(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> DataFrame:
    target_schema = spark.table(target_table).schema

    source = spark.table(SOURCE_TABLE).filter(F.col("date") == F.lit(run_date))

    select_exprs = [
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in target_schema.fields
    ]
    return source.select(*select_exprs)


def backfill_query_skg_aggregated_conversions_legacy(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        raise RuntimeError(
            f"Target table {target_table} does not exist; create it via the "
            "layer migration before backfilling."
        )

    partition = build_backfill_partition(spark, run_date, target_table)
    partition.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments) -> None:
    backfill_query_skg_aggregated_conversions_legacy(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
