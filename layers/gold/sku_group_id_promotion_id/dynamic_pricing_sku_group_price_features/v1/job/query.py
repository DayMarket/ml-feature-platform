"""Trino query for SKU-group dynamic-pricing price aggregates."""

from __future__ import annotations

from datetime import datetime


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_aggregate_query(calculated_at: datetime, source_table: str) -> str:
    calculated_at_sql = _sql_string(calculated_at.strftime("%Y-%m-%d %H:%M:%S"))
    return f"""
SELECT
    CAST({calculated_at_sql} AS TIMESTAMP(6)) AS calculated_at,
    CAST(sku_group_id AS BIGINT) AS sku_group_id,
    promotion_id,
    MIN(sell_price) AS min_sell_price,
    MAX(sell_price) AS max_sell_price,
    AVG(sell_price) AS avg_sell_price,
    MIN(discount) AS min_discount,
    MAX(discount) AS max_discount,
    AVG(discount) AS avg_discount,
    MIN(discount_fraction) AS min_discount_fraction,
    MAX(discount_fraction) AS max_discount_fraction,
    AVG(discount_fraction) AS avg_discount_fraction
FROM {source_table}
WHERE calculated_at = CAST({calculated_at_sql} AS TIMESTAMP(6))
  AND sku_group_id IS NOT NULL
  AND promotion_id IS NOT NULL
GROUP BY
    CAST(sku_group_id AS BIGINT),
    promotion_id
"""
