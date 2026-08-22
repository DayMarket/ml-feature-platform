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
    max_category_depth: int
    window_hours: int


def _category_joins(settings: SourceSettings) -> str:
    return f"""
LEFT JOIN {settings.category_table} c1
    ON c0.parent_id = c1.id
LEFT JOIN {settings.category_table} c2
    ON c1.parent_id = c2.id
LEFT JOIN {settings.category_table} c3
    ON c2.parent_id = c3.id
LEFT JOIN {settings.category_table} c4
    ON c3.parent_id = c4.id
LEFT JOIN {settings.category_table} c5
    ON c4.parent_id = c5.id
"""


def build_category_depth_validation_query(settings: SourceSettings) -> str:
    return f"""
SELECT c0.id
FROM {settings.category_table} c0
{_category_joins(settings)}
WHERE c5.parent_id IS NOT NULL
    AND c5.parent_id > 0
    AND c5.parent_id != {settings.technical_category_root_id}
LIMIT 1
"""


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


def build_account_l2_impression_counts_query(
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
        CAST(c0.id AS INT) AS category_id,
        REVERSE(
            FILTER(
                ARRAY(c0.id, c1.id, c2.id, c3.id, c4.id, c5.id),
                value -> value IS NOT NULL
                    AND value != {settings.technical_category_root_id}
            )
        ) AS hierarchy
    FROM {settings.category_table} c0
    {_category_joins(settings)}
),
category_levels AS (
    SELECT
        category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS INT
        ) AS l2_category_id
    FROM category_paths
),
product_categories AS (
    SELECT
        CAST(product.id AS INT) AS product_id,
        category.l2_category_id
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
        product_category.l2_category_id
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
        l2_category_id,
        CAST(COUNT(DISTINCT product_id) AS INT) AS n_impressions
    FROM resolved_events
    WHERE l2_category_id IS NOT NULL
    GROUP BY
        account_id,
        session_id,
        l2_category_id
)
SELECT
    TIMESTAMP '{calculated_at_tashkent_sql}' AS calculated_at,
    account_id,
    l2_category_id,
    CAST(SUM(n_impressions) AS INT) AS n_impressions
FROM session_counts
GROUP BY
    account_id,
    l2_category_id
"""


def build_account_l2_impression_counts_merge_query(
    target_table: str,
    calculated_at: datetime,
) -> str:
    calculated_at_sql = _tashkent_timestamp_literal(calculated_at)

    return f"""
MERGE INTO {target_table} AS target
USING account_l2_impression_counts_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.l2_category_id = source.l2_category_id
WHEN MATCHED THEN UPDATE SET
    target.n_impressions = source.n_impressions
WHEN NOT MATCHED THEN INSERT (
    calculated_at,
    account_id,
    l2_category_id,
    n_impressions
) VALUES (
    source.calculated_at,
    source.account_id,
    source.l2_category_id,
    source.n_impressions
)
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_sql}'
THEN DELETE
"""
