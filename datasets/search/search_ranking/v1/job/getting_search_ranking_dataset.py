from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _parse_airflow_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    for candidate in (normalized, normalized.replace(" ", "T", 1)):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue

    raise ValueError(
        "Unsupported partition timestamp format. "
        f"Expected Airflow ISO timestamp or YYYY-MM-DD HH:MM:SS, got {value!r}"
    )


def _collection_date(partition_start: str) -> date:
    return _parse_airflow_datetime(partition_start).date()


def build_search_ranking_dataset(
    spark: SparkSession,
    partition_start: str,
):
    collection_date = _collection_date(partition_start)
    event_date = collection_date - timedelta(days=20)

    return spark.sql(
        f"""
WITH params AS (
    SELECT
        DATE '{collection_date.isoformat()}' AS collection_date,
        DATE '{event_date.isoformat()}' AS event_date
),
attributed_orders AS (
    SELECT DISTINCT
        last_search_session_id,
        install_id,
        order_id,
        order_item_id,
        trim(lower(query)) AS query
    FROM iceberg.silver.order_items_attribution
    CROSS JOIN params p
    WHERE
        query != ''
        AND widget_space_name IN (
            'SHOP_SEARCH_RESULTS',
            'COLLECTION_SEARCH_RESULTS',
            'SEARCH',
            'SEARCH_RESULTS'
        )
        AND event_received_at >= p.event_date
        AND event_received_at < DATE_ADD(p.event_date, 1)
        AND COALESCE(is_full_catpred, 'false') = 'false'
),
order_items_enhanced AS (
    SELECT
        oi.order_item_id,
        s.sku_group_id
    FROM iceberg.silver.order_items oi
    INNER JOIN iceberg.silver.sku s ON s.id = oi.sku_id
    CROSS JOIN params p
    WHERE
        oi.order_item_status NOT IN ('CREATED', 'NOT_CREATED')
        AND oi.generated_at >= DATE_SUB(p.event_date, 15)
        AND oi.generated_at < DATE_ADD(p.event_date, 1)
),
orders AS (
    SELECT
        ao.last_search_session_id,
        ao.install_id,
        ao.query,
        oie.sku_group_id,
        1 AS is_generated_order
    FROM attributed_orders ao
    INNER JOIN order_items_enhanced oie
        ON oie.order_item_id = ao.order_item_id
    GROUP BY
        ao.last_search_session_id,
        ao.install_id,
        ao.query,
        oie.sku_group_id
),
sessions_raw AS (
    SELECT
        p.collection_date,
        p.event_date,
        logged_at,
        received_at,
        install_id,
        session_id,
        CAST(sku_group_id AS BIGINT) AS sku_group_id,
        trim(lower(query)) AS query,
        CAST(`position` AS INT) AS `position`,
        widget_section_name,
        widget_space_name
    FROM iceberg.silver_b2c_clickstream.events
    CROSS JOIN params p
    WHERE
        logged_at >= DATE_SUB(p.event_date, 3)
        AND logged_at < DATE_ADD(p.event_date, 4)
        AND received_at >= p.event_date
        AND received_at < DATE_ADD(p.event_date, 1)
        AND event_type = 'PRODUCT_IMPRESSION'
        AND widget_space_name = 'SEARCH_RESULTS'
        AND widget_section_name = 'SEARCH_RESULTS'
        AND query IS NOT NULL
        AND trim(query) != ''
        AND COALESCE(is_full_catpred, false) = false
),
sessions_with_duplicate_stats AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY event_date, install_id, session_id, query, `position`
        ) AS position_duplicate_count,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, install_id, session_id, query, `position`
            ORDER BY received_at ASC, logged_at ASC, sku_group_id ASC
        ) AS position_duplicate_rank
    FROM sessions_raw
),
sessions_deduplicated AS (
    SELECT
        collection_date,
        event_date,
        logged_at,
        received_at,
        install_id,
        session_id,
        sku_group_id,
        query,
        `position`,
        widget_section_name,
        widget_space_name,
        position_duplicate_count
    FROM sessions_with_duplicate_stats
    WHERE position_duplicate_rank = 1
),
sessions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_date, install_id, session_id, query
            ORDER BY `position` ASC, received_at ASC, logged_at ASC, sku_group_id ASC
        ) AS deduplicate_rank
    FROM sessions_deduplicated
)
SELECT
    s.collection_date,
    s.event_date,
    s.logged_at,
    s.received_at,
    s.install_id,
    s.session_id,
    s.sku_group_id,
    s.query,
    s.`position`,
    CAST(s.deduplicate_rank AS BIGINT) AS deduplicate_rank,
    CAST(s.position_duplicate_count AS BIGINT) AS position_duplicate_count,
    s.widget_section_name,
    s.widget_space_name,
    COALESCE(o.is_generated_order, 0) AS is_generated_order
FROM sessions s
LEFT JOIN orders o
    ON o.install_id = s.install_id
    AND o.last_search_session_id = s.session_id
    AND o.sku_group_id = s.sku_group_id
    AND o.query = s.query
"""
    )


def save_search_ranking_dataset(
    spark: SparkSession,
    partition_start: str,
    target_table: str,
) -> None:
    dataset = build_search_ranking_dataset(
        spark,
        partition_start,
    )

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    dataset.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_search_ranking_dataset(
        spark,
        arguments.partition_start,
        arguments.table_name,
    )
