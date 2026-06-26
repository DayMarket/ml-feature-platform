"""Trino query for dynamic-pricing SKU price snapshots."""

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


def build_query(calculated_at: datetime, promotion_ids: Sequence[str]) -> str:
    calculated_at_sql = _sql_string(calculated_at.strftime("%Y-%m-%d %H:%M:%S"))
    promotion_values = _promotion_values(promotion_ids)

    return f"""
WITH
promotion_filter(promotion_id) AS (
    VALUES
{promotion_values}
),
dyno_prices AS (
    SELECT
        sku_id,
        promotion_id,
        CAST(discount_amount AS DOUBLE) AS discount_amount,
        CAST(calculated_for_price AS DOUBLE) AS calculated_for_price,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY sku_id, promotion_id
            ORDER BY created_at DESC
        ) AS rn
    FROM promotions.public.dynamic_discount
    WHERE created_at >= current_timestamp - INTERVAL '14' DAY
      AND promotion_id IN (SELECT promotion_id FROM promotion_filter)
),
actual_prices AS (
    SELECT
        id,
        sku_group_id,
        product_id,
        CAST(sell_price AS DOUBLE) AS sell_price
    FROM kazanexpress.public.sku
),
latest_dyno_prices AS (
    SELECT
        sku_id,
        promotion_id,
        discount_amount,
        calculated_for_price,
        created_at
    FROM dyno_prices
    WHERE rn = 1
),
final_prices_aggregation AS (
    SELECT
        CAST({calculated_at_sql} AS TIMESTAMP(6)) AS calculated_at,
        ap.id AS sku_id,
        pf.promotion_id,
        ap.sku_group_id,
        ap.product_id,
        CASE
            WHEN ap.sell_price = dp.calculated_for_price
                THEN ap.sell_price - COALESCE(dp.discount_amount, 0)
            ELSE ap.sell_price
        END AS final_price,
        CASE
            WHEN ap.sell_price = dp.calculated_for_price
                THEN COALESCE(dp.discount_amount, 0)
            ELSE 0
        END AS real_discount,
        dp.created_at AS dynamic_discount_created_at
    FROM actual_prices ap
    CROSS JOIN promotion_filter pf
    LEFT JOIN latest_dyno_prices dp
        ON dp.sku_id = ap.id
       AND dp.promotion_id = pf.promotion_id
)
SELECT
    calculated_at,
    CAST(sku_id AS BIGINT) AS sku_id,
    promotion_id,
    CAST(sku_group_id AS BIGINT) AS sku_group_id,
    CAST(product_id AS BIGINT) AS product_id,
    real_discount AS discount,
    final_price AS sell_price,
    CAST(real_discount AS DOUBLE) / NULLIF(CAST(final_price AS DOUBLE), 0.0) AS discount_fraction,
    dynamic_discount_created_at
FROM final_prices_aggregation
"""
