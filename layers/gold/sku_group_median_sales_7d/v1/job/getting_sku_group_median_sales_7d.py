from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_sku_group_median_sales_7d(
    spark: SparkSession,
    partition_end: str,
):
    return spark.sql(
        f"""
WITH params AS (
    SELECT
        TIMESTAMP '{partition_end}' AS end_at,
        TIMESTAMP '{partition_end}' - INTERVAL 7 DAY AS start_at
),
skus_mapping AS (
    SELECT
        id AS sku_id,
        sku_group_id
    FROM iceberg.silver.sku
),
sales_events AS (
    SELECT
        skus.sku_group_id,
        CAST(
            FLOOR(
                (UNIX_TIMESTAMP(oi.issued_at) - UNIX_TIMESTAMP(p.start_at)) / 86400
            ) AS INT
        ) AS day_bucket,
        oi.item_quantity
    FROM iceberg.silver.order_items oi
    CROSS JOIN params p
    INNER JOIN skus_mapping skus
        ON skus.sku_id = oi.sku_id
    WHERE
        oi.order_item_status = 'COMPLETED'
        AND oi.issued_at >= p.start_at
        AND oi.issued_at < p.end_at
        AND (oi.returned_at IS NULL OR oi.returned_at >= p.end_at)
        AND skus.sku_group_id IS NOT NULL
),
sku_groups AS (
    SELECT DISTINCT
        sku_group_id
    FROM sales_events
),
buckets AS (
    SELECT
        EXPLODE(SEQUENCE(0, 6)) AS day_bucket
),
sku_bucket_grid AS (
    SELECT
        sku_groups.sku_group_id,
        buckets.day_bucket
    FROM sku_groups
    CROSS JOIN buckets
),
bucket_sales AS (
    SELECT
        sku_group_id,
        day_bucket,
        SUM(item_quantity) AS sales_count
    FROM sales_events
    GROUP BY
        sku_group_id,
        day_bucket
),
daily_sales AS (
    SELECT
        sku_bucket_grid.sku_group_id,
        sku_bucket_grid.day_bucket,
        COALESCE(bucket_sales.sales_count, 0) AS sales_count
    FROM sku_bucket_grid
    LEFT JOIN bucket_sales
        ON sku_bucket_grid.sku_group_id = bucket_sales.sku_group_id
        AND sku_bucket_grid.day_bucket = bucket_sales.day_bucket
)
SELECT
    CAST(TIMESTAMP '{partition_end}' AS DATE) AS date,
    CAST(sku_group_id AS BIGINT) AS sku_group_id,
    percentile_approx(CAST(sales_count AS DOUBLE), 0.5) AS median_sales_count_7d
FROM daily_sales
GROUP BY
    sku_group_id
"""
    )


def save_sku_group_median_sales_7d(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_median_sales_7d(spark, partition_end)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_median_sales_7d(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
