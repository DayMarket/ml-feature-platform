from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_sku_group_orders(
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
        TIMESTAMP '{partition_start}' - INTERVAL 20 DAY AS target_date_minus_20d
),
skus_mapping AS (
    SELECT
        id AS sku_id,
        sku_group_id
    FROM iceberg.silver.sku
),
sku_groups_stats AS (
    SELECT
        p.target_date AS date,
        skus.sku_group_id,
        SUM(oi.gmv_net) AS gmv_net,
        SUM(oi.gmv_completed) AS gmv_completed,
        SUM(oi.gmv_generated) AS gmv_generated,
        SUM(oi.returned_amount) AS return_amount,
        SUM(
            CASE
                WHEN COALESCE(oi.returned_at, TIMESTAMP '1970-01-01 06:00:00') >= p.target_date
                    AND COALESCE(oi.returned_at, TIMESTAMP '1970-01-01 06:00:00') < p.end_date
                    THEN oi.returned_quantity
                ELSE 0
            END
        ) AS real_returned_items,
        SUM(
            CASE
                WHEN COALESCE(oi.returned_at, TIMESTAMP '1970-01-01 06:00:00') >= p.target_date
                    AND COALESCE(oi.returned_at, TIMESTAMP '1970-01-01 06:00:00') < p.end_date
                    THEN oi.gmv_returned
                ELSE 0
            END
        ) AS real_returned_gmv,
        COUNT(DISTINCT CASE
            WHEN COALESCE(oi.returned_at, TIMESTAMP '1970-01-01 06:00:00') >= p.target_date
                AND COALESCE(oi.returned_at, TIMESTAMP '1970-01-01 06:00:00') < p.end_date
                THEN oi.order_id
        END) AS returned_orders,
        SUM(
            CASE
                WHEN oi.order_item_status = 'COMPLETED'
                    AND COALESCE(oi.issued_at, TIMESTAMP '1970-01-01 06:00:00') >= p.target_date
                    AND COALESCE(oi.issued_at, TIMESTAMP '1970-01-01 06:00:00') < p.end_date
                    AND (oi.returned_at IS NULL OR oi.returned_at >= p.end_date)
                    THEN oi.item_quantity
                ELSE 0
            END
        ) AS completed_items,
        COUNT(DISTINCT CASE
            WHEN oi.order_item_status = 'COMPLETED'
                AND COALESCE(oi.issued_at, TIMESTAMP '1970-01-01 06:00:00') >= p.target_date
                AND COALESCE(oi.issued_at, TIMESTAMP '1970-01-01 06:00:00') < p.end_date
                AND (oi.returned_at IS NULL OR oi.returned_at >= p.end_date)
                THEN oi.order_id
        END) AS completed_orders,
        SUM(
            CASE
                WHEN oi.order_item_status = 'COMPLETED'
                    AND COALESCE(oi.issued_at, TIMESTAMP '1970-01-01 06:00:00') >= p.target_date
                    AND COALESCE(oi.issued_at, TIMESTAMP '1970-01-01 06:00:00') < p.end_date
                    AND (oi.returned_at IS NULL OR oi.returned_at >= p.end_date)
                    THEN oi.gmv_completed
                ELSE 0
            END
        ) AS completed_gmv
    FROM iceberg.silver.order_items oi
    CROSS JOIN params p
    INNER JOIN skus_mapping skus ON skus.sku_id = oi.sku_id
    WHERE
        oi.order_item_status NOT IN ('CREATED', 'NOT_CREATED')
        AND GREATEST(
            COALESCE(CAST(oi.issued_at AS DATE), DATE '1970-01-01'),
            COALESCE(CAST(oi.returned_at AS DATE), DATE '1970-01-01')
        ) >= CAST(p.target_date AS DATE)
        AND oi.generated_at < p.end_date
        AND oi.generated_at >= p.target_date_minus_20d
    GROUP BY
        skus.sku_group_id,
        p.target_date
),
generated_stats AS (
    SELECT
        p.target_date AS date,
        skus.sku_group_id,
        COUNT(DISTINCT oi.order_id) AS orders_generated,
        SUM(oi.item_quantity) AS items_generated,
        SUM(oi.gmv_generated) AS gmv_generated
    FROM iceberg.silver.order_items oi
    CROSS JOIN params p
    INNER JOIN skus_mapping skus ON skus.sku_id = oi.sku_id
    WHERE
        oi.order_item_status NOT IN ('CREATED', 'NOT_CREATED')
        AND oi.generated_at >= p.target_date
        AND oi.generated_at < p.end_date
    GROUP BY
        skus.sku_group_id,
        p.target_date
)
SELECT
    CAST(COALESCE(gen.date, stats.date) AS DATE) AS date,
    CAST(COALESCE(gen.sku_group_id, stats.sku_group_id) AS BIGINT) AS sku_group_id,
    CAST(COALESCE(gen.orders_generated, 0) AS BIGINT) AS orders_generated,
    CAST(COALESCE(gen.items_generated, 0) AS BIGINT) AS items_generated,
    COALESCE(gen.gmv_generated, 0) AS gmv_generated,
    CAST(COALESCE(stats.completed_items, 0) AS BIGINT) AS items_completed,
    COALESCE(stats.completed_gmv, 0) AS gmv_completed,
    CAST(COALESCE(stats.completed_orders, 0) AS BIGINT) AS completed_orders,
    CAST(COALESCE(stats.real_returned_items, 0) AS BIGINT) AS returned_items,
    COALESCE(stats.real_returned_gmv, 0) AS returned_gmv,
    CAST(COALESCE(stats.returned_orders, 0) AS BIGINT) AS returned_orders
FROM sku_groups_stats stats
FULL JOIN generated_stats gen
    ON stats.sku_group_id = gen.sku_group_id
    AND stats.date = gen.date
WHERE
    COALESCE(stats.real_returned_items, 0)
    + COALESCE(stats.completed_items, 0)
    + COALESCE(gen.items_generated, 0) > 0
"""
    )


def save_sku_group_orders(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
) -> None:
    features = build_sku_group_orders(
        spark,
        partition_start,
        partition_end,
    )

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_orders(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
    )
