from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

FEATURE_NAMESPACE = "PRODUCT"
BASE_FEATURE_COLUMNS = ("score", "net_inflow", "weighted_price", "today_rate")
FEATURE_COLUMNS = tuple(
    f"{FEATURE_NAMESPACE}__{column}" for column in BASE_FEATURE_COLUMNS
)

VAT_DIVISOR = 1.12
MIN_WEIGHTED_ORDERS = 5


class SourceSettings(Protocol):
    sku_cm2_inputs_table: str
    currency_rates_table: str
    business_timezone: str


def _local_timestamp_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M:%S")


def _weighted_value_expression(weighted_sum: str, mean_value: str) -> str:
    return (
        "CASE "
        f"WHEN total_orders >= {MIN_WEIGHTED_ORDERS} "
        f"THEN {weighted_sum} / NULLIF(CAST(total_orders AS DOUBLE), 0.0D) "
        f"ELSE {mean_value} END"
    )


def build_product_cm2_pdp_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    weighted_net_inflow = _weighted_value_expression(
        "weighted_net_inflow_sum",
        "mean_net_inflow",
    )
    weighted_price = _weighted_value_expression(
        "weighted_price_sum",
        "mean_price",
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
        LEAST(source.sell_price_uzs, cap.sell_price_p999_uzs) AS sell_price_uzs,
        source.commission_pct,
        source.n_orders_28d
    FROM selected_s6 source
    CROSS JOIN price_cap cap
),
commissioned_skus AS (
    SELECT *
    FROM capped_skus
    WHERE commission_pct IS NOT NULL
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
sku_values AS (
    SELECT
        sku.product_id,
        sku.n_orders_28d,
        sku.sell_price_uzs,
        sku.sell_price_uzs
            * (sku.commission_pct / 100.0D)
            / {VAT_DIVISOR}D
            / rate.usd_rate AS net_inflow_sku,
        rate.usd_rate
    FROM commissioned_skus sku
    CROSS JOIN latest_currency_rate rate
),
product_aggregates AS (
    SELECT
        product_id,
        SUM(n_orders_28d) AS total_orders,
        SUM(net_inflow_sku * CAST(n_orders_28d AS DOUBLE))
            AS weighted_net_inflow_sum,
        AVG(net_inflow_sku) AS mean_net_inflow,
        SUM(sell_price_uzs * CAST(n_orders_28d AS DOUBLE))
            AS weighted_price_sum,
        AVG(sell_price_uzs) AS mean_price,
        MAX(usd_rate) AS usd_rate
    FROM sku_values
    GROUP BY product_id
),
product_features AS (
    SELECT
        product_id,
        {weighted_net_inflow} AS net_inflow,
        {weighted_price} AS weighted_price,
        usd_rate
    FROM product_aggregates
)
SELECT
    TIMESTAMP '{calculated_at_local}' AS calculated_at,
    product_id,
    net_inflow AS {FEATURE_NAMESPACE}__score,
    net_inflow AS {FEATURE_NAMESPACE}__net_inflow,
    weighted_price AS {FEATURE_NAMESPACE}__weighted_price,
    usd_rate AS {FEATURE_NAMESPACE}__today_rate
FROM product_features
"""


def build_product_cm2_pdp_features_merge_query(
    target_table: str,
    calculated_at: datetime,
    business_timezone: str,
) -> str:
    calculated_at_local = _local_timestamp_literal(calculated_at, business_timezone)
    update_columns = ",\n    ".join(
        f"target.{column} = source.{column}" for column in FEATURE_COLUMNS
    )
    insert_columns = ",\n    ".join(("calculated_at", "product_id", *FEATURE_COLUMNS))
    source_columns = ",\n    ".join(
        f"source.{column}"
        for column in ("calculated_at", "product_id", *FEATURE_COLUMNS)
    )
    return f"""
MERGE INTO {target_table} AS target
USING product_cm2_pdp_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    {update_columns}
WHEN NOT MATCHED THEN INSERT (
    {insert_columns}
) VALUES (
    {source_columns}
)
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
