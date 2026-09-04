"""Trino queries for daily product price facts."""

from __future__ import annotations

from datetime import date, datetime

MAX_VALID_PRICE = 1_000_000_000


def _date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _timestamp_literal(value: datetime) -> str:
    return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='seconds')}'"


def build_query(dt: datetime, source_price_date: date) -> str:
    dt_sql = _timestamp_literal(dt)
    date_sql = _date_literal(source_price_date)

    return f"""
WITH sku_prices AS (
    SELECT
        {dt_sql} AS dt,
        CAST(s.product_id AS INTEGER) AS product_id,
        CAST(s.sku_group_id AS INTEGER) AS sku_group_id,
        CASE
            WHEN p.full_price_eod BETWEEN 0 AND {MAX_VALID_PRICE}
            THEN CAST(p.full_price_eod AS DOUBLE)
        END AS full_price_eod,
        CASE
            WHEN p.sell_price_eod BETWEEN 0 AND {MAX_VALID_PRICE}
            THEN CAST(p.sell_price_eod AS DOUBLE)
        END AS sell_price_eod,
        (
            s.status = 'ACTIVE'
            AND (
                COALESCE(s.quantity_active, 0) > 0
                OR COALESCE(s.quantity_fbs, 0) > 0
            )
        ) AS is_available
    FROM "dwh-clickhouse".marts.daily_sku_quantity_eod p
    INNER JOIN "dwh-clickhouse".dict.sku s
        ON p.sku_id = s.id
    WHERE p.dt = {date_sql}
      AND s.product_id IS NOT NULL
      AND s.sku_group_id IS NOT NULL
),
sku_group_prices AS (
    SELECT
        dt,
        product_id,
        sku_group_id,
        MIN(sell_price_eod) AS min_sell_price_eod,
        AVG(sell_price_eod) AS avg_sell_price_eod,
        MAX(sell_price_eod) AS max_sell_price_eod,
        MIN(full_price_eod) AS min_full_price_eod,
        MAX(full_price_eod) AS max_full_price_eod,
        AVG(
            CASE WHEN is_available THEN sell_price_eod END
        ) AS avg_active_sku_sell_price_eod,
        MIN(
            CASE WHEN is_available THEN sell_price_eod END
        ) AS min_active_sku_sell_price_eod,
        MAX(
            CASE WHEN is_available THEN sell_price_eod END
        ) AS max_active_sku_sell_price_eod
    FROM sku_prices
    GROUP BY
        dt,
        product_id,
        sku_group_id
)
SELECT
    dt,
    product_id,
    MIN(min_sell_price_eod) AS min_sell_price_eod,
    AVG(avg_sell_price_eod) AS avg_sell_price_eod,
    MAX(max_sell_price_eod) AS max_sell_price_eod,
    MIN(min_full_price_eod) AS min_full_price_eod,
    MAX(max_full_price_eod) AS max_full_price_eod,
    AVG(
        avg_active_sku_sell_price_eod
    ) AS avg_active_sku_sell_price_eod,
    MIN(
        min_active_sku_sell_price_eod
    ) AS min_active_sku_sell_price_eod,
    MAX(
        max_active_sku_sell_price_eod
    ) AS max_active_sku_sell_price_eod
FROM sku_group_prices
GROUP BY
    dt,
    product_id
"""


def build_source_metrics_query(dt: date) -> str:
    date_sql = _date_literal(dt)

    return f"""
WITH price_skus AS (
    SELECT
        CAST(sku_id AS BIGINT) AS sku_id,
        CAST(product_id AS INTEGER) AS source_product_id
    FROM "dwh-clickhouse".marts.daily_sku_quantity_eod
    WHERE dt = {date_sql}
),
sku_mapping AS (
    SELECT
        CAST(id AS BIGINT) AS sku_id,
        CAST(product_id AS INTEGER) AS product_id,
        CAST(sku_group_id AS INTEGER) AS sku_group_id
    FROM "dwh-clickhouse".dict.sku
)
SELECT
    COUNT(*) AS source_rows,
    COUNT(DISTINCT p.sku_id) AS source_skus,
    COUNT(DISTINCT p.source_product_id) AS source_products,
    COUNT(DISTINCT CASE
        WHEN s.product_id IS NOT NULL AND s.sku_group_id IS NOT NULL
        THEN p.sku_id
    END) AS mapped_skus,
    COUNT(DISTINCT CASE
        WHEN s.product_id IS NOT NULL AND s.sku_group_id IS NOT NULL
        THEN p.source_product_id
    END) AS mapped_source_products,
    COUNT_IF(
        s.product_id IS NOT NULL
        AND s.product_id != p.source_product_id
    ) AS mapping_mismatch_rows
FROM price_skus p
LEFT JOIN sku_mapping s
    ON p.sku_id = s.sku_id
"""
