"""Trino query for city-level order completion and no-show shares."""

from __future__ import annotations

from datetime import date

SOURCE_TABLE = '"dwh-iceberg".silver.history_order_items'

# Возвраты по вине маркетплейса/контента: по ним статус заказа не отражает
# поведение покупателя, поэтому такие позиции не участвуют ни в числителе,
# ни в знаменателе.
EXCLUDED_RETURN_CAUSES = (
    "MISSING",
    "DEFECTED",
    "BAD_QUALITY",
    "WRONG_ITEM",
    "PHOTO_MISMATCH",
    "CONTENT",
)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(analyze_date: date, lookback_days: int) -> str:
    analyze_date_sql = f"DATE {_sql_string(analyze_date.isoformat())}"
    excluded_causes_sql = ", ".join(
        _sql_string(cause) for cause in EXCLUDED_RETURN_CAUSES
    )

    return f"""
WITH city_stats AS (
    SELECT
        CAST(order_city_id AS BIGINT) AS order_city_id,
        COUNT(DISTINCT order_id) FILTER (
            WHERE real_order_item_status != 'ACTIVE'
        ) AS total_orders,
        COUNT(DISTINCT order_id) FILTER (
            WHERE real_order_item_status = 'COMPLETED'
        ) AS completed_orders,
        COUNT(DISTINCT order_id) FILTER (
            WHERE real_order_item_status = 'RETURNED NO SHOW'
        ) AS returned_no_show_orders
    FROM {SOURCE_TABLE}
    WHERE analyze_date = {analyze_date_sql}
      AND generated_at > {analyze_date_sql} - INTERVAL '{lookback_days}' DAY
      AND return_cause NOT IN ({excluded_causes_sql})
      AND order_city_id IS NOT NULL
    GROUP BY order_city_id
)
SELECT
    {analyze_date_sql} AS date,
    order_city_id,
    CAST(completed_orders AS DOUBLE) / total_orders AS part_completed_orders,
    CAST(returned_no_show_orders AS DOUBLE) / total_orders AS part_no_show_from_total
FROM city_stats
WHERE total_orders > 0
"""
