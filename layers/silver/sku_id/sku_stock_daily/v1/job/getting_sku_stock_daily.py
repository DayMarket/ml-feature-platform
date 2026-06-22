from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments


SOURCE_TABLE = "iceberg.silver.sku_eod"


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _parse_partition_date(partition_start: str) -> str:
    value = partition_start.strip()

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        pass

    for date_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, date_format).date().isoformat()
        except ValueError:
            continue

    raise ValueError(
        f"Unsupported partition_start format: {partition_start!r}. "
        "Expected ISO datetime or YYYY-MM-DD HH:MM:SS."
    )


def build_sku_stock_daily(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    return spark.sql(
        f"""
SELECT
    DATE '{run_date}' AS date,
    CAST(sku_id AS BIGINT) AS sku_id,
    CAST(SUM(quantity_active_eod) AS BIGINT) AS total_stock
FROM {SOURCE_TABLE}
WHERE dt = DATE '{run_date}'
    AND sku_id IS NOT NULL
GROUP BY
    DATE '{run_date}',
    CAST(sku_id AS BIGINT)
"""
    )


def save_sku_stock_daily(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    features = build_sku_stock_daily(spark, run_date)

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_stock_daily(
        spark,
        _parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
