from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments
from job.partition import parse_partition_date


CATEGORY_LEVEL = 1


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _partition_date(partition_start: str) -> str:
    return parse_partition_date(partition_start).isoformat()


def build_account_l1_event_w_imps_counts(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    return spark.sql(
        f"""
WITH category_levels AS (
    SELECT
        id AS category_id,
        l1_category AS l1_cat,
        CASE
            WHEN l2_category > 0 THEN l2_category
            ELSE l1_category
        END AS l2_cat,
        CASE
            WHEN l3_category > 0 THEN l3_category
            WHEN l2_category > 0 THEN l2_category
            ELSE l1_category
        END AS l3_cat
    FROM iceberg.silver.category
),
product_categories AS (
    SELECT
        id AS product_id,
        category_id
    FROM iceberg.silver.product
    WHERE id IS NOT NULL
),
prepared_events AS (
    SELECT
        e.account_id,
        e.session_id,
        e.product_id,
        e.event_type,
        cl.l{CATEGORY_LEVEL}_cat AS category_id
    FROM iceberg.silver_b2c_clickstream.events e
    LEFT JOIN product_categories pc
        ON e.product_id = pc.product_id
    INNER JOIN category_levels cl
        ON CASE
            WHEN e.category_id > 0 THEN e.category_id
            ELSE pc.category_id
        END = cl.category_id
    WHERE TO_DATE(FROM_UTC_TIMESTAMP(e.received_at, 'Asia/Tashkent')) = DATE('{run_date}')
        AND e.account_id > 0
        AND e.event_type IN (
            'PRODUCT_IMPRESSION',
            'PRODUCT_VIEW',
            'ADD_TO_CART',
            'ADD_TO_FAVORITES'
        )
),
session_events AS (
    SELECT
        account_id,
        session_id,
        category_id,
        COUNT(DISTINCT CASE WHEN event_type = 'PRODUCT_IMPRESSION' THEN product_id END) AS _n_imps,
        COUNT(DISTINCT CASE WHEN event_type = 'PRODUCT_VIEW' THEN product_id END) AS _n_clicks,
        COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_CART' THEN product_id END) AS _n_atcs,
        COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_FAVORITES' THEN product_id END) AS _n_atfs
    FROM prepared_events
    GROUP BY account_id, session_id, category_id
)
SELECT
    DATE('{run_date}') AS date,
    account_id,
    category_id,
    SUM(_n_imps) AS n_imps,
    SUM(_n_clicks) AS n_clicks,
    SUM(_n_atcs) AS n_atcs,
    SUM(_n_atfs) AS n_atfs
FROM session_events
GROUP BY account_id, category_id
"""
    )


def save_account_l1_event_w_imps_counts(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    features = build_account_l1_event_w_imps_counts(
        spark,
        run_date,
    )

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_account_l1_event_w_imps_counts(
        spark,
        _partition_date(arguments.partition_start),
        arguments.table_name,
    )
