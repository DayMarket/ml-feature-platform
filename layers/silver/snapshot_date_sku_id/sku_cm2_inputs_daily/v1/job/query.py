"""Trino queries for the daily SKU CM2 input snapshot."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


def _date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        normalized = value.replace(tzinfo=timezone.utc)
    else:
        normalized = value.astimezone(timezone.utc)
    return normalized.strftime("%Y-%m-%d %H:%M:%S")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validate_identifier(value: str) -> str:
    if not value.isidentifier():
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def _query_values(
    snapshot_date: date,
    calculated_at: datetime,
    orders_lookback_days: int,
    default_dimensional_group: str,
) -> dict[str, str]:
    lookback_days = int(orders_lookback_days)
    if lookback_days <= 0:
        raise ValueError("orders_lookback_days must be positive")

    calculated_at_utc = (
        calculated_at.replace(tzinfo=timezone.utc)
        if calculated_at.tzinfo is None
        else calculated_at.astimezone(timezone.utc)
    )
    window_start = calculated_at_utc - timedelta(days=lookback_days)
    return {
        "snapshot_date": _date_literal(snapshot_date),
        "calculated_at": _utc_timestamp_literal(calculated_at_utc),
        "window_start": _utc_timestamp_literal(window_start),
        "default_group": _sql_string(default_dimensional_group),
    }


def build_query(
    *,
    snapshot_date: date,
    calculated_at: datetime,
    sku_table: str,
    prices_table: str,
    commission_table: str,
    commission_column: str,
    orders_table: str,
    orders_lookback_days: int,
    default_dimensional_group: str,
) -> str:
    values = _query_values(
        snapshot_date,
        calculated_at,
        orders_lookback_days,
        default_dimensional_group,
    )
    commission = _validate_identifier(commission_column)

    return f"""
WITH sku_base AS (
    SELECT
        CAST(id AS BIGINT) AS sku_id,
        CAST(product_id AS BIGINT) AS product_id,
        COALESCE(
            CAST(dimensional_group AS VARCHAR),
            {values["default_group"]}
        ) AS dimensional_group
    FROM {sku_table}
    WHERE id IS NOT NULL
      AND product_id IS NOT NULL
),
daily_prices AS (
    SELECT
        CAST(sku_id AS BIGINT) AS sku_id,
        CAST(sell_price_eod AS DOUBLE) AS sell_price_uzs
    FROM {prices_table}
    WHERE dt = {values["snapshot_date"]}
),
commissions AS (
    SELECT
        CAST(sku_id AS BIGINT) AS sku_id,
        CAST({commission} AS DOUBLE) AS commission_pct
    FROM {commission_table}
),
order_counts AS (
    SELECT
        CAST(sku_id AS BIGINT) AS sku_id,
        CAST(COUNT(*) AS BIGINT) AS n_orders_28d
    FROM {orders_table}
    WHERE order_created_at >= TIMESTAMP '{values["window_start"]}'
      AND order_created_at < TIMESTAMP '{values["calculated_at"]}'
      AND sku_id IS NOT NULL
    GROUP BY CAST(sku_id AS BIGINT)
)
SELECT
    {values["snapshot_date"]} AS snapshot_date,
    sku.sku_id,
    sku.product_id,
    sku.dimensional_group,
    price.sell_price_uzs,
    commission.commission_pct,
    CAST(COALESCE(orders.n_orders_28d, 0) AS BIGINT) AS n_orders_28d
FROM sku_base sku
LEFT JOIN daily_prices price
    ON sku.sku_id = price.sku_id
LEFT JOIN commissions commission
    ON sku.sku_id = commission.sku_id
LEFT JOIN order_counts orders
    ON sku.sku_id = orders.sku_id
"""
