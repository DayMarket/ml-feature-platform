from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

FEATURE_NAMESPACE = "PRODUCT_CM2_MAIN"
FEATURE_COLUMNS = (f"{FEATURE_NAMESPACE}__cm2_main_uzs",)

DEFAULT_COMMISSION_PCT = 20.0
VAT_DIVISOR = 1.12
FORWARD_COST_MULTIPLIER = 0.7
SELLER_COMPENSATION_RATE = 0.0028
MIN_WEIGHTED_ORDERS = 5


class SourceSettings(Protocol):
    sku_cm2_inputs_table: str
    currency_rates_table: str
    business_timezone: str


def _local_timestamp_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M:%S")


def build_product_cm2_main_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    return f"""
WITH latest_s6_dt AS (
    SELECT MAX(dt) AS dt
    FROM {settings.sku_cm2_inputs_table}
    WHERE dt <= TIMESTAMP '{calculated_at_local}'
),
selected_s6 AS (
    SELECT
        CAST(source.sku_id AS BIGINT) AS sku_id,
        CAST(source.product_id AS INT) AS product_id,
        source.dimensional_group,
        CAST(source.sell_price_uzs AS DOUBLE) AS sell_price_uzs,
        CAST(source.commission_pct AS DOUBLE) AS commission_pct,
        CAST(source.n_orders_28d AS BIGINT) AS n_orders_28d
    FROM {settings.sku_cm2_inputs_table} source
    INNER JOIN latest_s6_dt latest
        ON source.dt = latest.dt
    WHERE source.sell_price_uzs IS NOT NULL
),
price_cap AS (
    SELECT percentile(sell_price_uzs, 0.999D) AS sell_price_p999_uzs
    FROM selected_s6
),
capped_skus AS (
    SELECT
        source.sku_id,
        source.product_id,
        source.dimensional_group,
        LEAST(source.sell_price_uzs, cap.sell_price_p999_uzs) AS sell_price_uzs,
        source.commission_pct,
        source.n_orders_28d
    FROM selected_s6 source
    CROSS JOIN price_cap cap
),
latest_currency_rate AS (
    SELECT usd_rate
    FROM (
        SELECT
            CAST(rate AS DOUBLE) AS usd_rate,
            ROW_NUMBER() OVER (
                ORDER BY requested_dt DESC, rate DESC
            ) AS row_number
        FROM {settings.currency_rates_table}
        WHERE currency_name = 'USD'
          AND CAST(requested_dt AS TIMESTAMP) <= TIMESTAMP '{calculated_at_local}'
    ) ranked
    WHERE row_number = 1
      AND usd_rate > 0.0D
),
sku_inputs AS (
    SELECT
        sku.product_id,
        sku.n_orders_28d,
        sku.sell_price_uzs,
        COALESCE(sku.commission_pct, {DEFAULT_COMMISSION_PCT}D) / 100.0D
            AS commission_rate,
        CASE sku.dimensional_group
            WHEN 'MEDIUM' THEN 8000.0D
            WHEN 'LARGE' THEN 20000.0D
            ELSE 5000.0D
        END AS logistics_uzs,
        CASE sku.dimensional_group
            WHEN 'MEDIUM' THEN 1.9041D
            WHEN 'LARGE' THEN 6.3470D
            ELSE 0.3183D
        END AS forward_cost_usd,
        rate.usd_rate
    FROM capped_skus sku
    CROSS JOIN latest_currency_rate rate
),
sku_cashflows AS (
    SELECT
        product_id,
        n_orders_28d,
        usd_rate,
        (
            sell_price_uzs * commission_rate + logistics_uzs
        ) / {VAT_DIVISOR}D / usd_rate AS net_inflow_usd,
        sell_price_uzs * {SELLER_COMPENSATION_RATE}D / usd_rate
            AS seller_compensation_usd,
        forward_cost_usd
    FROM sku_inputs
),
sku_scores AS (
    SELECT
        product_id,
        n_orders_28d,
        usd_rate,
        net_inflow_usd
            - {FORWARD_COST_MULTIPLIER}D * forward_cost_usd
            - seller_compensation_usd AS cm2_sku_usd
    FROM sku_cashflows
),
product_aggregates AS (
    SELECT
        product_id,
        SUM(n_orders_28d) AS total_orders,
        SUM(cm2_sku_usd * CAST(n_orders_28d AS DOUBLE)) AS weighted_sum,
        AVG(cm2_sku_usd) AS mean_score,
        MAX(usd_rate) AS usd_rate
    FROM sku_scores
    GROUP BY product_id
)
SELECT
    TIMESTAMP '{calculated_at_local}' AS calculated_at,
    product_id,
    (
        CASE
            WHEN total_orders >= {MIN_WEIGHTED_ORDERS}
                THEN weighted_sum / NULLIF(CAST(total_orders AS DOUBLE), 0.0D)
            ELSE mean_score
        END
    ) * usd_rate AS {FEATURE_NAMESPACE}__cm2_main_uzs
FROM product_aggregates
"""


def build_product_cm2_main_features_merge_query(
    target_table: str,
    calculated_at: datetime,
    business_timezone: str,
) -> str:
    calculated_at_local = _local_timestamp_literal(calculated_at, business_timezone)
    feature = FEATURE_COLUMNS[0]
    return f"""
MERGE INTO {target_table} AS target
USING product_cm2_main_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    target.{feature} = source.{feature}
WHEN NOT MATCHED THEN INSERT (
    calculated_at,
    product_id,
    {feature}
) VALUES (
    source.calculated_at,
    source.product_id,
    source.{feature}
)
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
