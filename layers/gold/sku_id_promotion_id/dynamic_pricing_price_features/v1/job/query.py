"""Trino queries for dynamic-pricing gold price features."""

from __future__ import annotations

import re
from datetime import datetime
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


def build_today_discounts_query(
    calculated_at: datetime,
    promotion_ids: Sequence[str],
) -> str:
    day_start = calculated_at.replace(hour=0, minute=0, second=0, microsecond=0)
    start_sql = _sql_string(day_start.strftime("%Y-%m-%d %H:%M:%S"))
    end_sql = _sql_string(calculated_at.strftime("%Y-%m-%d %H:%M:%S"))
    promotion_values = _promotion_values(promotion_ids)

    return f"""
WITH
promotion_filter(promotion_id) AS (
    VALUES
{promotion_values}
)
SELECT
    CAST(created_at AS DATE) AS date,
    CAST(sku_id AS BIGINT) AS sku_id,
    promotion_id,
    CAST(discount_amount AS DOUBLE) AS discount_amount,
    CAST(calculated_for_price AS DOUBLE) AS calculated_for_price,
    created_at
FROM promotions.public.dynamic_discount
WHERE created_at >= CAST({start_sql} AS TIMESTAMP(6))
  AND created_at < CAST({end_sql} AS TIMESTAMP(6))
  AND promotion_id IN (SELECT promotion_id FROM promotion_filter)
"""


def build_actual_sku_query() -> str:
    return """
SELECT
    CAST(id AS BIGINT) AS sku_id,
    CAST(sku_group_id AS BIGINT) AS sku_group_id,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(sell_price AS DOUBLE) AS sell_price
FROM kazanexpress.public.sku
"""
