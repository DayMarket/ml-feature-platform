from datetime import datetime, timedelta, timezone
from typing import Protocol


class SourceSettings(Protocol):
    events_table: str
    product_table: str
    category_table: str
    impression_event_type: str
    technical_category_root_id: int
    max_category_depth: int
    window_hours: int


def _category_joins(settings: SourceSettings) -> str:
    return "\n".join(
        (
            f"LEFT JOIN {settings.category_table} c{level} "
            f"ON c{level - 1}.parent_id = c{level}.id"
        )
        for level in range(1, settings.max_category_depth)
    )


def build_category_depth_validation_query(settings: SourceSettings) -> str:
    last_alias = f"c{settings.max_category_depth - 1}"
    return f"""
SELECT c0.id
FROM {settings.category_table} c0
{_category_joins(settings)}
WHERE {last_alias}.parent_id IS NOT NULL
    AND {last_alias}.parent_id > 0
    AND {last_alias}.parent_id <> {settings.technical_category_root_id}
LIMIT 1
"""


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        normalized = value.replace(tzinfo=timezone.utc)
    else:
        normalized = value.astimezone(timezone.utc)
    return normalized.strftime("%Y-%m-%d %H:%M:%S")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_account_l2_impression_counts_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    window_start = calculated_at - timedelta(hours=settings.window_hours)
    calculated_at_sql = _utc_timestamp_literal(calculated_at)
    window_start_sql = _utc_timestamp_literal(window_start)
    event_type_sql = _sql_string(settings.impression_event_type)
    category_ids = ",\n                    ".join(
        f"c{level}.id"
        for level in range(settings.max_category_depth)
    )

    return f"""
WITH category_paths AS (
    SELECT
        CAST(c0.id AS BIGINT) AS category_id,
        REVERSE(
            FILTER(
                ARRAY(
                    {category_ids}
                ),
                value -> value IS NOT NULL
                    AND value <> {settings.technical_category_root_id}
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
            ) AS BIGINT
        ) AS l2_category_id
    FROM category_paths
),
product_categories AS (
    SELECT
        CAST(product.id AS BIGINT) AS product_id,
        category.l2_category_id
    FROM {settings.product_table} product
    LEFT JOIN category_levels category
        ON CAST(product.category_id AS BIGINT) = category.category_id
    WHERE product.id IS NOT NULL
),
resolved_events AS (
    SELECT
        CAST(event.account_id AS BIGINT) AS account_id,
        event.session_id,
        CAST(event.product_id AS BIGINT) AS product_id,
        COALESCE(
            event_category.l2_category_id,
            product_category.l2_category_id
        ) AS l2_category_id
    FROM {settings.events_table} event
    LEFT JOIN category_levels event_category
        ON CAST(event.category_id AS BIGINT) = event_category.category_id
    LEFT JOIN product_categories product_category
        ON CAST(event.product_id AS BIGINT) = product_category.product_id
    WHERE event.event_type = {event_type_sql}
        AND CAST(event.account_id AS BIGINT) > 0
        AND CAST(event.product_id AS BIGINT) > 0
        AND event.session_id IS NOT NULL
        AND event.received_at >= TIMESTAMP '{window_start_sql}'
        AND event.received_at < TIMESTAMP '{calculated_at_sql}'
),
session_counts AS (
    SELECT
        account_id,
        session_id,
        l2_category_id,
        CAST(COUNT(DISTINCT product_id) AS BIGINT) AS n_impressions
    FROM resolved_events
    WHERE l2_category_id IS NOT NULL
    GROUP BY
        account_id,
        session_id,
        l2_category_id
)
SELECT
    TIMESTAMP '{calculated_at_sql}' AS calculated_at,
    account_id,
    l2_category_id,
    CAST(SUM(n_impressions) AS BIGINT) AS n_impressions
FROM session_counts
GROUP BY
    account_id,
    l2_category_id
"""
