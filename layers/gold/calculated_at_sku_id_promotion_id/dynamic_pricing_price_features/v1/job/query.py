"""Trino query for dynamic-pricing gold price features."""

from __future__ import annotations

import re
from datetime import datetime

PROMOTION_ID_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
PROMOTION_ID_PREFIX = "dyno_pricing_"
DEFAULT_PROMOTION_ID = "0"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validate_promotion_id(promotion_id: str) -> str:
    if not PROMOTION_ID_PATTERN.fullmatch(promotion_id):
        raise ValueError(f"Unsupported promotion_id value: {promotion_id!r}")
    if promotion_id != DEFAULT_PROMOTION_ID and not promotion_id.startswith(
        PROMOTION_ID_PREFIX
    ):
        raise ValueError(
            f"promotion_id must start with {PROMOTION_ID_PREFIX!r}: {promotion_id!r}"
        )
    return promotion_id


def build_promotion_ids_query(
    silver_table: str,
    history_days: int,
) -> str:
    if history_days < 2:
        raise ValueError("history_days must be at least 2")

    prefix_sql = _sql_string(PROMOTION_ID_PREFIX)
    return f"""
SELECT DISTINCT promotion_id
FROM (
    SELECT promotion_id
    FROM promotions.public.dynamic_discount
    WHERE created_at >= current_timestamp - INTERVAL '1' DAY
      AND starts_with(promotion_id, {prefix_sql})

    UNION ALL

    SELECT promotion_id
    FROM {silver_table}
    WHERE date > current_date - INTERVAL '{history_days - 1}' DAY
      AND starts_with(promotion_id, {prefix_sql})
)
ORDER BY promotion_id
"""


def build_today_discounts_query(
    calculated_at: datetime,
    promotion_id: str,
) -> str:
    day_start = calculated_at.replace(hour=0, minute=0, second=0, microsecond=0)
    start_sql = _sql_string(day_start.strftime("%Y-%m-%d %H:%M:%S"))
    promotion_id_sql = _sql_string(_validate_promotion_id(promotion_id))
    prefix_sql = _sql_string(PROMOTION_ID_PREFIX)

    return f"""
SELECT
    CAST(created_at AS DATE) AS date,
    CAST(sku_id AS BIGINT) AS sku_id,
    promotion_id,
    CAST(discount_amount AS DOUBLE) AS discount_amount,
    CAST(calculated_for_price AS DOUBLE) AS calculated_for_price,
    created_at
FROM promotions.public.dynamic_discount
WHERE created_at >= CAST({start_sql} AS TIMESTAMP(6))
  AND promotion_id = {promotion_id_sql}
  AND starts_with(promotion_id, {prefix_sql})
"""


def build_gold_query(
    calculated_at: datetime,
    promotion_id: str,
    silver_table: str,
    history_days: int,
) -> str:
    if history_days < 2:
        raise ValueError("history_days must be at least 2")

    calculated_at_sql = _sql_string(calculated_at.strftime("%Y-%m-%d %H:%M:%S"))
    promotion_id = _validate_promotion_id(promotion_id)
    promotion_id_sql = _sql_string(promotion_id)
    prefix_sql = _sql_string(PROMOTION_ID_PREFIX)
    source_predicate = (
        f"FALSE\n      AND starts_with(promotion_id, {prefix_sql})"
        if promotion_id == DEFAULT_PROMOTION_ID
        else (
            f"promotion_id = {promotion_id_sql}\n"
            f"      AND starts_with(promotion_id, {prefix_sql})"
        )
    )

    return f"""
WITH dyno_prices_actual AS (
    SELECT
        CAST(sku_id AS BIGINT) AS sku_id,
        promotion_id,
        CAST(discount_amount AS DOUBLE) AS discount_amount,
        CAST(calculated_for_price AS DOUBLE) AS calculated_for_price,
        created_at
    FROM promotions.public.dynamic_discount
    WHERE created_at >= current_timestamp - INTERVAL '1' DAY
      AND {source_predicate}

    UNION ALL

    SELECT
        CAST(sku_id AS BIGINT) AS sku_id,
        promotion_id,
        CAST(discount_amount AS DOUBLE) AS discount_amount,
        CAST(calculated_for_price AS DOUBLE) AS calculated_for_price,
        created_at
    FROM {silver_table}
    WHERE date > current_date - INTERVAL '{history_days - 1}' DAY
      AND {source_predicate}
),
rn_actual_prices AS (
    SELECT
        sku_id,
        promotion_id,
        discount_amount,
        calculated_for_price,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY sku_id, promotion_id
            ORDER BY created_at DESC
        ) AS rn
    FROM dyno_prices_actual
),
actual_prices AS (
    SELECT
        CAST(id AS BIGINT) AS sku_id,
        CAST(sku_group_id AS BIGINT) AS sku_group_id,
        CAST(product_id AS BIGINT) AS product_id,
        CAST(sell_price AS DOUBLE) AS sell_price
    FROM kazanexpress.public.sku
),
latest_prices AS (
    SELECT
        sku_id,
        promotion_id,
        discount_amount,
        calculated_for_price,
        created_at
    FROM rn_actual_prices
    WHERE rn = 1
),
final_prices_aggregation AS (
    SELECT
        CAST({calculated_at_sql} AS TIMESTAMP(6)) AS calculated_at,
        ap.sku_id,
        {promotion_id_sql} AS promotion_id,
        ap.sku_group_id,
        ap.product_id,
        dp.calculated_for_price,
        dp.created_at AS dynamic_discount_created_at,
        CASE
            WHEN ap.sell_price = dp.calculated_for_price
                THEN ap.sell_price - COALESCE(dp.discount_amount, 0)
            ELSE ap.sell_price
        END AS final_price,
        CASE
            WHEN ap.sell_price = dp.calculated_for_price
                THEN COALESCE(dp.discount_amount, 0)
            ELSE 0
        END AS real_discount
    FROM actual_prices ap
    LEFT JOIN latest_prices dp
        ON dp.sku_id = ap.sku_id
       AND dp.promotion_id = {promotion_id_sql}
)
SELECT
    calculated_at,
    sku_id,
    promotion_id,
    sku_group_id,
    product_id,
    calculated_for_price,
    real_discount AS discount,
    final_price AS sell_price,
    CAST(real_discount AS DOUBLE) / NULLIF(final_price, 0.0) AS discount_fraction,
    dynamic_discount_created_at
FROM final_prices_aggregation
"""
