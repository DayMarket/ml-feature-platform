"""Build the daily SKU CM2 input query."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

TASHKENT_TIME_ZONE = ZoneInfo("Asia/Tashkent")
ORDERS_LOOKBACK_DAYS = 28


def _date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _tashkent_timestamp_literal(value: datetime) -> str:
    normalized = (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    ).astimezone(TASHKENT_TIME_ZONE)
    return normalized.strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S Asia/Tashkent'")


def _utc_timestamp_literal(value: datetime) -> str:
    normalized = (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )
    return normalized.strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")


def build_query(
    *,
    dt: date,
    interval_end: datetime,
    sku_table: str,
    prices_table: str,
    commission_table: str,
    orders_table: str,
) -> str:
    dt_sql = _date_literal(dt)
    price_dt_sql = _date_literal(dt - timedelta(days=1))
    window_start = interval_end - timedelta(days=ORDERS_LOOKBACK_DAYS)
    window_end_sql = _tashkent_timestamp_literal(interval_end)
    window_start_sql = _tashkent_timestamp_literal(window_start)
    window_end_utc_sql = _utc_timestamp_literal(interval_end)
    window_start_utc_sql = _utc_timestamp_literal(window_start)

    return f"""
WITH sku_base AS (
    SELECT
        CAST(id AS INTEGER) AS sku_id,
        CAST(product_id AS INTEGER) AS product_id,
        COALESCE(CAST(dimensional_group AS VARCHAR), 'SMALL') AS dimensional_group
    FROM {sku_table}
    WHERE id IS NOT NULL
      AND product_id IS NOT NULL
),
daily_prices AS (
    SELECT
        CAST(sku_id AS INTEGER) AS sku_id,
        CAST(sell_price_eod AS DOUBLE) AS sell_price_uzs
    FROM {prices_table}
    WHERE dt = {price_dt_sql}
),
commissions AS (
    SELECT
        CAST(sku_id AS INTEGER) AS sku_id,
        CAST(comission AS DOUBLE) AS commission_pct
    FROM {commission_table}
),
order_counts AS (
    SELECT
        CAST(sku_id AS INTEGER) AS sku_id,
        COUNT(*) AS n_orders_28d
    FROM {orders_table}
    WHERE order_created_at >= {window_start_utc_sql}
      AND order_created_at < {window_end_utc_sql}
      AND AT_TIMEZONE(
            WITH_TIMEZONE(CAST(order_created_at AS TIMESTAMP), 'UTC'),
            'Asia/Tashkent'
          ) >= {window_start_sql}
      AND AT_TIMEZONE(
            WITH_TIMEZONE(CAST(order_created_at AS TIMESTAMP), 'UTC'),
            'Asia/Tashkent'
          ) < {window_end_sql}
      AND sku_id IS NOT NULL
    GROUP BY CAST(sku_id AS INTEGER)
)
SELECT
    {dt_sql} AS dt,
    sku.sku_id,
    sku.product_id,
    sku.dimensional_group,
    price.sell_price_uzs,
    commission.commission_pct,
    COALESCE(orders.n_orders_28d, 0) AS n_orders_28d
FROM sku_base sku
LEFT JOIN daily_prices price
    ON sku.sku_id = price.sku_id
LEFT JOIN commissions commission
    ON sku.sku_id = commission.sku_id
LEFT JOIN order_counts orders
    ON sku.sku_id = orders.sku_id
"""
