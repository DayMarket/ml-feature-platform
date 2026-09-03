from datetime import datetime, timedelta, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

TASHKENT_TIME_ZONE = ZoneInfo("Asia/Tashkent")


class SourceSettings(Protocol):
    events_table: str
    product_table: str
    category_table: str
    impression_event_type: str
    technical_category_root_id: int
    window_hours: int


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        normalized = value.replace(tzinfo=timezone.utc)
    else:
        normalized = value.astimezone(timezone.utc)
    return normalized.strftime("%Y-%m-%d %H:%M:%S")


def _tashkent_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(TASHKENT_TIME_ZONE).strftime("%Y-%m-%d %H:%M:%S")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_account_l1_imp_counts_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    window_start = calculated_at - timedelta(hours=settings.window_hours)
    calculated_at_utc_sql = _utc_timestamp_literal(calculated_at)
    calculated_at_tashkent_sql = _tashkent_timestamp_literal(calculated_at)
    window_start_utc_sql = _utc_timestamp_literal(window_start)
    event_type_sql = _sql_string(settings.impression_event_type)

    return f"""
WITH category_paths AS (
    SELECT
        category.id AS category_id,
        FILTER(
            TRANSFORM(
                SPLIT(category.path, '[.]'),
                value -> CAST(value AS INT)
            ),
            value -> value > 0
                AND value != {settings.technical_category_root_id}
        ) AS hierarchy
    FROM {settings.category_table} category
    WHERE category.id > 0
      AND category.id != {settings.technical_category_root_id}
      AND category.path IS NOT NULL
),
category_levels AS (
    SELECT
        category_id,
        hierarchy[0] AS l1_category_id
    FROM category_paths
    WHERE SIZE(hierarchy) > 0
),
product_categories AS (
    SELECT
        CAST(product.id AS INT) AS product_id,
        category.l1_category_id
    FROM {settings.product_table} product
    LEFT JOIN category_levels category
        ON product.category_id = category.category_id
    WHERE product.id IS NOT NULL
),
resolved_events AS (
    SELECT
        CAST(event.account_id AS INT) AS account_id,
        event.session_id,
        CAST(event.product_id AS INT) AS product_id,
        product_category.l1_category_id
    FROM {settings.events_table} event
    LEFT JOIN product_categories product_category
        ON event.product_id = product_category.product_id
    WHERE event.event_type = {event_type_sql}
        AND event.account_id > 0
        AND event.product_id > 0
        AND event.session_id IS NOT NULL
        AND event.received_at >= TIMESTAMP '{window_start_utc_sql}'
        AND event.received_at < TIMESTAMP '{calculated_at_utc_sql}'
),
session_counts AS (
    SELECT
        account_id,
        session_id,
        l1_category_id,
        COUNT(DISTINCT product_id) AS n_impressions
    FROM resolved_events
    WHERE l1_category_id IS NOT NULL
    GROUP BY
        account_id,
        session_id,
        l1_category_id
)
SELECT
    TIMESTAMP '{calculated_at_tashkent_sql}' AS calculated_at,
    account_id,
    l1_category_id,
    SUM(n_impressions) AS n_impressions
FROM session_counts
GROUP BY
    account_id,
    l1_category_id
"""


def build_account_l1_imp_counts_merge_query(
    target_table: str,
    calculated_at: datetime,
) -> str:
    calculated_at_sql = _tashkent_timestamp_literal(calculated_at)

    return f"""
MERGE INTO {target_table} AS target
USING account_l1_imp_counts_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.l1_category_id = source.l1_category_id
WHEN MATCHED THEN UPDATE SET
    target.n_impressions = source.n_impressions
WHEN NOT MATCHED THEN INSERT (
    calculated_at,
    account_id,
    l1_category_id,
    n_impressions
) VALUES (
    source.calculated_at,
    source.account_id,
    source.l1_category_id,
    source.n_impressions
)
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_sql}'
THEN DELETE
"""
