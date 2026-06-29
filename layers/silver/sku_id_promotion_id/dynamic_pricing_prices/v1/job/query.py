"""Trino query for daily dynamic-pricing discounts."""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Sequence

PROMOTION_ID_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _promotion_values(promotion_ids: Sequence[str]) -> str:
    if not promotion_ids:
        raise ValueError("source.promotion_ids must contain at least one promotion_id")

    rows = []
    for promotion_id in promotion_ids:
        if not PROMOTION_ID_PATTERN.fullmatch(promotion_id):
            raise ValueError(f"Unsupported promotion_id value: {promotion_id!r}")
        rows.append(f"        ({_sql_string(promotion_id)})")
    return ",\n".join(rows)


def build_query(partition_date: date, promotion_ids: Sequence[str]) -> str:
    next_date = partition_date + timedelta(days=1)
    date_sql = _sql_string(partition_date.isoformat())
    start_sql = _sql_string(f"{partition_date.isoformat()} 00:00:00")
    end_sql = _sql_string(f"{next_date.isoformat()} 00:00:00")
    promotion_values = _promotion_values(promotion_ids)

    return f"""
WITH
promotion_filter(promotion_id) AS (
    VALUES
{promotion_values}
),
daily_prices AS (
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
      AND promotion_id IN (SELECT promotion_id FROM promotion_filter)
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
