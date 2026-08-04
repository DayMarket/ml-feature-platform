"""Trino query for daily dynamic-pricing discounts."""

from __future__ import annotations

from datetime import date, timedelta

PROMOTION_ID_PREFIX = "dyno_pricing_"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(partition_date: date) -> str:
    next_date = partition_date + timedelta(days=1)
    date_sql = _sql_string(partition_date.isoformat())
    start_sql = _sql_string(f"{partition_date.isoformat()} 00:00:00")
    end_sql = _sql_string(f"{next_date.isoformat()} 00:00:00")
    promotion_prefix_sql = _sql_string(PROMOTION_ID_PREFIX)

    return f"""
WITH daily_prices AS (
    SELECT
        CAST(sku_id AS BIGINT) AS sku_id,
        promotion_id,
        CAST(discount_amount AS DOUBLE) AS discount_amount,
        CAST(calculated_for_price AS DOUBLE) AS calculated_for_price,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY sku_id, promotion_id
            ORDER BY created_at DESC
        ) AS rn
    FROM promotions.public.dynamic_discount
    WHERE created_at >= CAST({start_sql} AS TIMESTAMP(6))
      AND created_at < CAST({end_sql} AS TIMESTAMP(6))
      AND starts_with(promotion_id, {promotion_prefix_sql})
)
SELECT
    CAST({date_sql} AS DATE) AS date,
    sku_id,
    promotion_id,
    discount_amount,
    calculated_for_price,
    created_at
FROM daily_prices
WHERE rn = 1
"""
