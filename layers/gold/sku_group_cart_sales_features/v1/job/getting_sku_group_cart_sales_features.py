from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_sku_group_cart_sales_features(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
):
    return spark.sql(
        f"""
WITH params AS (
    SELECT
        TIMESTAMP '{partition_start}' AS target_date,
        TIMESTAMP '{partition_end}' AS end_date,
        TIMESTAMP '{partition_start}' - INTERVAL 6 DAY AS target_date_minus_6d,
        TIMESTAMP '{partition_start}' - INTERVAL 13 DAY AS target_date_minus_13d,
        TIMESTAMP '{partition_start}' - INTERVAL 27 DAY AS target_date_minus_27d,
        TIMESTAMP '{partition_start}' - INTERVAL 47 DAY AS attribution_start_date
),
cart_order_items AS (
    SELECT DISTINCT
        order_item_id
    FROM iceberg.silver.order_items_attribution
    CROSS JOIN params p
    WHERE
        widget_space_name = 'CART'
        AND generated_at >= p.attribution_start_date
        AND generated_at < p.end_date
),
skus_mapping AS (
    SELECT
        id AS sku_id,
        sku_group_id
    FROM iceberg.silver.sku
),
cart_sales AS (
    SELECT
        p.target_date AS date,
        sk.sku_group_id,
        oi.order_id,
        oi.issued_at
    FROM iceberg.silver.order_items oi
    CROSS JOIN params p
    INNER JOIN cart_order_items coi
        ON coi.order_item_id = oi.order_item_id
    INNER JOIN skus_mapping sk
        ON sk.sku_id = oi.sku_id
    WHERE
        oi.order_item_status = 'COMPLETED'
        AND oi.issued_at >= p.target_date_minus_27d
        AND oi.issued_at < p.end_date
        AND oi.generated_at >= p.attribution_start_date
        AND oi.generated_at < p.end_date
        AND (oi.returned_at IS NULL OR oi.returned_at >= p.end_date)
        AND sk.sku_group_id IS NOT NULL
)
SELECT
    CAST(p.target_date AS DATE) AS date,
    CAST(cs.sku_group_id AS BIGINT) AS sku_group_id,
    CAST(COUNT(DISTINCT CASE
        WHEN cs.issued_at >= p.target_date_minus_6d THEN cs.order_id
    END) AS BIGINT) AS cart_sales_count_7d,
    CAST(COUNT(DISTINCT CASE
        WHEN cs.issued_at >= p.target_date_minus_13d THEN cs.order_id
    END) AS BIGINT) AS cart_sales_count_14d,
    CAST(COUNT(DISTINCT cs.order_id) AS BIGINT) AS cart_sales_count_28d
FROM cart_sales cs
CROSS JOIN params p
GROUP BY
    p.target_date,
    cs.sku_group_id
"""
    )


def save_sku_group_cart_sales_features(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_cart_sales_features(
        spark,
        partition_start,
        partition_end,
    )
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_cart_sales_features(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
    )
