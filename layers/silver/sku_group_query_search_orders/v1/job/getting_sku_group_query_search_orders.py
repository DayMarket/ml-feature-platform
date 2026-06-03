from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_sku_group_query_search_orders(
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
search_orders AS (
    SELECT DISTINCT
        order_item_id,
        query
    FROM iceberg.silver.order_items_attribution
    CROSS JOIN params p
    WHERE
        generated_at > p.target_date_minus_20d
        AND generated_at < p.end_date
        AND widget_space_name IN (
            'SHOP_SEARCH_RESULTS',
            'COLLECTION_SEARCH_RESULTS',
            'SEARCH',
            'SEARCH_RESULTS'
        )
        AND query != ''
),
sku_groups_stats AS (
    SELECT
        p.target_date AS date,
        oi.order_item_id,
        sk.sku_group_id,
        SUM(oi.gmv_net) AS gmv_net,
        SUM(oi.gmv_completed) AS gmv_completed,
        SUM(oi.gmv_generated) AS gmv_generated,
        SUM(oi.returned_amount) AS return_amount,
        SUM(
            CASE
                WHEN oi.returned_at >= p.target_date AND oi.returned_at < p.end_date
                    THEN oi.returned_quantity
                ELSE 0
            END
        ) AS real_returned_items,
        SUM(
            CASE
                WHEN oi.returned_at >= p.target_date AND oi.returned_at < p.end_date
                    THEN oi.gmv_returned
                ELSE 0
            END
        ) AS real_returned_gmv,
        COUNT(DISTINCT CASE
            WHEN oi.returned_at >= p.target_date AND oi.returned_at < p.end_date
                THEN oi.order_id
        END) AS returned_orders,
        SUM(
            CASE
                WHEN oi.order_item_status = 'COMPLETED'
                    AND oi.issued_at >= p.target_date
                    AND oi.issued_at < p.end_date
                    AND (oi.returned_at IS NULL OR oi.returned_at >= p.end_date)
                    THEN oi.item_quantity
                ELSE 0
            END
        ) AS completed_items,
        COUNT(DISTINCT CASE
            WHEN oi.order_item_status = 'COMPLETED'
                AND oi.issued_at >= p.target_date
                AND oi.issued_at < p.end_date
                AND (oi.returned_at IS NULL OR oi.returned_at >= p.end_date)
                THEN oi.order_id
        END) AS completed_orders,
        SUM(
            CASE
                WHEN oi.order_item_status = 'COMPLETED'
                    AND oi.issued_at >= p.target_date
                    AND oi.issued_at < p.end_date
                    AND (oi.returned_at IS NULL OR oi.returned_at >= p.end_date)
                    THEN oi.gmv_completed
                ELSE 0
            END
        ) AS completed_gmv
    FROM iceberg.silver.order_items oi
    CROSS JOIN params p
    INNER JOIN skus_mapping sk ON sk.sku_id = oi.sku_id
    WHERE
        oi.order_item_status NOT IN ('CREATED', 'NOT_CREATED')
        AND oi.generated_at <= p.end_date
        AND oi.generated_at >= p.target_date_minus_20d
        AND (
            (oi.issued_at >= p.target_date AND oi.issued_at < p.end_date)
            OR oi.returned_at >= p.target_date
        )
    GROUP BY
        sk.sku_group_id,
        oi.order_item_id,
        p.target_date
),
generated_stats AS (
    SELECT
        p.target_date AS date,
        sk.sku_group_id,
        oi.order_item_id,
        COUNT(DISTINCT oi.order_item_id) AS orders_generated,
        SUM(oi.item_quantity) AS items_generated,
        SUM(oi.gmv_generated) AS gmv_generated
    FROM iceberg.silver.order_items oi
    CROSS JOIN params p
    INNER JOIN skus_mapping sk ON sk.sku_id = oi.sku_id
    WHERE
        oi.order_item_status NOT IN ('CREATED', 'NOT_CREATED')
        AND oi.generated_at >= p.target_date
        AND oi.generated_at < p.end_date
    GROUP BY
        sk.sku_group_id,
        p.target_date,
        oi.order_item_id
),
search_generated_orders AS (
    SELECT
        date,
        query,
        sku_group_id,
        SUM(orders_generated) AS orders_generated,
        SUM(items_generated) AS items_generated,
        SUM(gmv_generated) AS gmv_generated
    FROM search_orders so
    INNER JOIN generated_stats sgs
        ON sgs.order_item_id = so.order_item_id
    GROUP BY
        date,
        query,
        sku_group_id
),
search_completed_returned AS (
    SELECT
        date,
        sku_group_id,
        query,
        SUM(gmv_net) AS gmv_net,
        SUM(gmv_completed) AS gmv_completed,
        SUM(gmv_generated) AS gmv_generated,
        SUM(return_amount) AS return_amount,
        SUM(real_returned_items) AS real_returned_items,
        SUM(real_returned_gmv) AS real_returned_gmv,
        SUM(returned_orders) AS returned_orders,
        SUM(completed_items) AS completed_items,
        SUM(completed_orders) AS completed_orders,
        SUM(completed_gmv) AS completed_gmv
    FROM search_orders so
    INNER JOIN sku_groups_stats sgs
        ON sgs.order_item_id = so.order_item_id
    GROUP BY
        date,
        sku_group_id,
        query
)
SELECT
    CAST(COALESCE(gen.date, stats.date) AS DATE) AS date,
    COALESCE(gen.query, stats.query) AS query,
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
FROM search_completed_returned stats
FULL JOIN search_generated_orders gen
    ON gen.sku_group_id = stats.sku_group_id
    AND gen.query = stats.query
    AND gen.date = stats.date
WHERE
    COALESCE(stats.real_returned_items, 0)
    + COALESCE(stats.completed_items, 0)
    + COALESCE(gen.items_generated, 0) > 0
"""
    )


def save_sku_group_query_search_orders(
    spark: SparkSession,
    partition_start: str,
    partition_end: str,
    target_table: str,
) -> None:
    features = build_sku_group_query_search_orders(
        spark,
        partition_start,
        partition_end,
    )

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_query_search_orders(
        spark,
        arguments.partition_start,
        arguments.partition_end,
        arguments.table_name,
    )
