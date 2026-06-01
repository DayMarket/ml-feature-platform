from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_sku_group_id_prices(
    spark: SparkSession,
    partition_date: str,
):
    return spark.sql(
        f"""
SELECT
    DATE '{partition_date}' AS date,
    CAST(s.sku_group_id AS BIGINT) AS sku_group_id,
    AVG(CAST(se.sell_price_eod AS DOUBLE)) AS avg_sell_price_eod,
    percentile_approx(CAST(se.sell_price_eod AS DOUBLE), 0.5) AS median_sell_price_eod,
    AVG(CAST(se.full_price_eod AS DOUBLE)) AS avg_full_price_eod,
    percentile_approx(CAST(se.full_price_eod AS DOUBLE), 0.5) AS median_full_price_eod
FROM iceberg.silver.sku_eod se
INNER JOIN (
    SELECT
        id AS sku_id,
        product_id,
        sku_group_id
    FROM iceberg.silver.sku
) s ON s.sku_id = se.sku_id
WHERE
    se.dt = DATE '{partition_date}'
    AND s.sku_group_id IS NOT NULL
GROUP BY
    s.sku_group_id
"""
    )


def save_sku_group_id_prices(
    spark: SparkSession,
    partition_date: str,
    target_table: str,
) -> None:
    features = build_sku_group_id_prices(spark, partition_date)

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_id_prices(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
