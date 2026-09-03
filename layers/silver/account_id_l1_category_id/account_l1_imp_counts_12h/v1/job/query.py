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


def _parent_category_joins(settings: SourceSettings) -> str:
    return f"""
LEFT JOIN {settings.category_table} parent_1
    ON leaf_category.parent_id = parent_1.id
LEFT JOIN {settings.category_table} parent_2
    ON parent_1.parent_id = parent_2.id
LEFT JOIN {settings.category_table} parent_3
    ON parent_2.parent_id = parent_3.id
LEFT JOIN {settings.category_table} parent_4
    ON parent_3.parent_id = parent_4.id
LEFT JOIN {settings.category_table} parent_5
    ON parent_4.parent_id = parent_5.id
LEFT JOIN {settings.category_table} parent_6
    ON parent_5.parent_id = parent_6.id
LEFT JOIN {settings.category_table} parent_7
    ON parent_6.parent_id = parent_7.id
"""


def build_category_path_validation_query(settings: SourceSettings) -> str:
    return f"""
SELECT leaf_category.id
FROM {settings.category_table} leaf_category
{_parent_category_joins(settings)}
WHERE parent_7.parent_id IS NOT NULL
    AND parent_7.parent_id > 0
    AND parent_7.parent_id != {settings.technical_category_root_id}
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
WITH category_ancestors AS (
    SELECT
        leaf_category.id AS category_id,
        parent_1.id AS parent_1_id,
        parent_2.id AS parent_2_id,
        parent_3.id AS parent_3_id,
        parent_4.id AS parent_4_id,
        parent_5.id AS parent_5_id,
        parent_6.id AS parent_6_id,
        parent_7.id AS parent_7_id,
        CASE
            WHEN parent_7.id > 0
             AND parent_7.id != {settings.technical_category_root_id} THEN 8
            WHEN parent_6.id > 0
             AND parent_6.id != {settings.technical_category_root_id} THEN 7
            WHEN parent_5.id > 0
             AND parent_5.id != {settings.technical_category_root_id} THEN 6
            WHEN parent_4.id > 0
             AND parent_4.id != {settings.technical_category_root_id} THEN 5
            WHEN parent_3.id > 0
             AND parent_3.id != {settings.technical_category_root_id} THEN 4
            WHEN parent_2.id > 0
             AND parent_2.id != {settings.technical_category_root_id} THEN 3
            WHEN parent_1.id > 0
             AND parent_1.id != {settings.technical_category_root_id} THEN 2
            ELSE 1
        END AS path_depth
    FROM {settings.category_table} leaf_category
    {_parent_category_joins(settings)}
    WHERE leaf_category.id > 0
      AND leaf_category.id != {settings.technical_category_root_id}
),
category_levels AS (
    SELECT
        category_id,
        CASE path_depth
            WHEN 8 THEN parent_7_id
            WHEN 7 THEN parent_6_id
            WHEN 6 THEN parent_5_id
            WHEN 5 THEN parent_4_id
            WHEN 4 THEN parent_3_id
            WHEN 3 THEN parent_2_id
            WHEN 2 THEN parent_1_id
            ELSE category_id
        END AS l1_category_id
    FROM category_ancestors
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
