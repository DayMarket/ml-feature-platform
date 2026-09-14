from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

FEATURE_COLUMNS = (
    "pid_n_clicks_3d",
    "pid_n_clicks_7d",
    "pid_n_clicks_14d",
    "pid_n_clicks_28d",
    "pid_n_clicks_3d_ratio",
    "pid_n_clicks_7d_ratio",
    "pid_n_clicks_14d_ratio",
    "pid_n_clicks_28d_ratio",
    "pid_n_atcs_3d",
    "pid_n_atcs_7d",
    "pid_n_atcs_14d",
    "pid_n_atcs_28d",
    "pid_n_atcs_3d_ratio",
    "pid_n_atcs_7d_ratio",
    "pid_n_atcs_14d_ratio",
    "pid_n_atcs_28d_ratio",
    "pid_n_atfs_3d",
    "pid_n_atfs_7d",
    "pid_n_atfs_14d",
    "pid_n_atfs_28d",
    "pid_n_atfs_3d_ratio",
    "pid_n_atfs_7d_ratio",
    "pid_n_atfs_14d_ratio",
    "pid_n_atfs_28d_ratio",
    "pid_neg_n_hours_since_last_click",
    "pid_neg_n_hours_since_last_click_rel",
    "pid_n_orders_3d",
    "pid_n_orders_7d",
    "pid_n_orders_14d",
    "pid_n_orders_28d",
    "pid_n_orders_60d",
    "pid_n_orders_90d",
    "pid_n_orders_3d_ratio",
    "pid_n_orders_7d_ratio",
    "pid_n_orders_14d_ratio",
    "pid_n_orders_28d_ratio",
    "pid_n_orders_60d_ratio",
    "pid_n_orders_90d_ratio",
    "pid_n_orders_28d_over_90d",
    "pid_gmv_3d",
    "pid_gmv_7d",
    "pid_gmv_14d",
    "pid_gmv_28d",
    "pid_gmv_60d",
    "pid_gmv_90d",
    "pid_gmv_3d_ratio",
    "pid_gmv_7d_ratio",
    "pid_gmv_14d_ratio",
    "pid_gmv_28d_ratio",
    "pid_gmv_60d_ratio",
    "pid_gmv_90d_ratio",
    "pid_neg_n_days_since_last_purchase",
    "last_click_before_last_purchase",
)


class SourceSettings(Protocol):
    action_counts_table: str
    order_items_table: str
    sku_table: str
    business_timezone: str


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _local_timestamp_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M:%S")


def build_account_product_features_query(
    settings: SourceSettings, calculated_at: datetime
) -> str:
    calculated_at_utc = _utc_timestamp_literal(calculated_at)
    calculated_at_local = _local_timestamp_literal(
        calculated_at, settings.business_timezone
    )
    return f"""
WITH deduplicated_actions AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        CAST(product_id AS INT) AS product_id,
        event_type,
        session_id,
        MAX(last_received_at) AS last_received_at
    FROM {settings.action_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND last_received_at < TIMESTAMP '{calculated_at_local}'
    GROUP BY account_id, product_id, event_type, session_id
),
action_features AS (
    SELECT
        account_id,
        product_id,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN session_id END) AS INT) AS pid_n_clicks_3d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN session_id END) AS INT) AS pid_n_clicks_7d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN session_id END) AS INT) AS pid_n_clicks_14d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN session_id END) AS INT) AS pid_n_clicks_28d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN session_id END) AS INT) AS pid_n_atcs_3d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN session_id END) AS INT) AS pid_n_atcs_7d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN session_id END) AS INT) AS pid_n_atcs_14d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN session_id END) AS INT) AS pid_n_atcs_28d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN session_id END) AS INT) AS pid_n_atfs_3d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN session_id END) AS INT) AS pid_n_atfs_7d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN session_id END) AS INT) AS pid_n_atfs_14d,
        CAST(COUNT(DISTINCT CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN session_id END) AS INT) AS pid_n_atfs_28d,
        MAX(CASE WHEN event_type = 'PRODUCT_VIEW' THEN last_received_at END) AS last_click_at
    FROM deduplicated_actions
    GROUP BY account_id, product_id
),
sku_mapping AS (
    SELECT CAST(id AS INT) AS sku_id, CAST(MIN(product_id) AS INT) AS product_id
    FROM {settings.sku_table}
    GROUP BY id
),
filtered_orders AS (
    SELECT
        CAST(order_item.account_id AS INT) AS account_id,
        sku.product_id,
        CAST(order_item.order_id AS INT) AS order_id,
        CAST(order_item.generated_at AS TIMESTAMP) AS generated_at,
        CAST(order_item.payment_price AS DOUBLE) * CAST(order_item.item_quantity AS DOUBLE) AS line_gmv
    FROM {settings.order_items_table} order_item
    INNER JOIN sku_mapping sku ON CAST(order_item.sku_id AS INT) = sku.sku_id
    WHERE order_item.generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS
        AND order_item.generated_at < TIMESTAMP '{calculated_at_utc}'
        AND order_item.order_item_status IN (
            'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'
        )
        AND order_item.b2b_order = FALSE
),
order_features AS (
    SELECT
        account_id,
        product_id,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN order_id END) AS INT) AS pid_n_orders_3d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN order_id END) AS INT) AS pid_n_orders_7d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN order_id END) AS INT) AS pid_n_orders_14d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN order_id END) AS INT) AS pid_n_orders_28d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 60 DAYS THEN order_id END) AS INT) AS pid_n_orders_60d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN order_id END) AS INT) AS pid_n_orders_90d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS pid_gmv_3d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS pid_gmv_7d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS pid_gmv_14d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS pid_gmv_28d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 60 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS pid_gmv_60d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS pid_gmv_90d,
        MAX(generated_at) AS last_purchase_at
    FROM filtered_orders
    GROUP BY account_id, product_id
),
base_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        COALESCE(actions.account_id, orders.account_id) AS account_id,
        COALESCE(actions.product_id, orders.product_id) AS product_id,
        COALESCE(actions.pid_n_clicks_3d, 0) AS pid_n_clicks_3d,
        COALESCE(actions.pid_n_clicks_7d, 0) AS pid_n_clicks_7d,
        COALESCE(actions.pid_n_clicks_14d, 0) AS pid_n_clicks_14d,
        COALESCE(actions.pid_n_clicks_28d, 0) AS pid_n_clicks_28d,
        COALESCE(actions.pid_n_atcs_3d, 0) AS pid_n_atcs_3d,
        COALESCE(actions.pid_n_atcs_7d, 0) AS pid_n_atcs_7d,
        COALESCE(actions.pid_n_atcs_14d, 0) AS pid_n_atcs_14d,
        COALESCE(actions.pid_n_atcs_28d, 0) AS pid_n_atcs_28d,
        COALESCE(actions.pid_n_atfs_3d, 0) AS pid_n_atfs_3d,
        COALESCE(actions.pid_n_atfs_7d, 0) AS pid_n_atfs_7d,
        COALESCE(actions.pid_n_atfs_14d, 0) AS pid_n_atfs_14d,
        COALESCE(actions.pid_n_atfs_28d, 0) AS pid_n_atfs_28d,
        COALESCE(orders.pid_n_orders_3d, 0) AS pid_n_orders_3d,
        COALESCE(orders.pid_n_orders_7d, 0) AS pid_n_orders_7d,
        COALESCE(orders.pid_n_orders_14d, 0) AS pid_n_orders_14d,
        COALESCE(orders.pid_n_orders_28d, 0) AS pid_n_orders_28d,
        COALESCE(orders.pid_n_orders_60d, 0) AS pid_n_orders_60d,
        COALESCE(orders.pid_n_orders_90d, 0) AS pid_n_orders_90d,
        COALESCE(orders.pid_gmv_3d, 0.0D) AS pid_gmv_3d,
        COALESCE(orders.pid_gmv_7d, 0.0D) AS pid_gmv_7d,
        COALESCE(orders.pid_gmv_14d, 0.0D) AS pid_gmv_14d,
        COALESCE(orders.pid_gmv_28d, 0.0D) AS pid_gmv_28d,
        COALESCE(orders.pid_gmv_60d, 0.0D) AS pid_gmv_60d,
        COALESCE(orders.pid_gmv_90d, 0.0D) AS pid_gmv_90d,
        actions.last_click_at,
        orders.last_purchase_at
    FROM action_features actions
    FULL OUTER JOIN order_features orders
        ON actions.account_id = orders.account_id
        AND actions.product_id = orders.product_id
),
features_with_ratios AS (
    SELECT
        *,
        CASE WHEN SUM(pid_n_clicks_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_clicks_3d AS DOUBLE) / SUM(pid_n_clicks_3d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_clicks_3d_ratio,
        CASE WHEN SUM(pid_n_clicks_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_clicks_7d AS DOUBLE) / SUM(pid_n_clicks_7d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_clicks_7d_ratio,
        CASE WHEN SUM(pid_n_clicks_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_clicks_14d AS DOUBLE) / SUM(pid_n_clicks_14d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_clicks_14d_ratio,
        CASE WHEN SUM(pid_n_clicks_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_clicks_28d AS DOUBLE) / SUM(pid_n_clicks_28d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_clicks_28d_ratio,
        CASE WHEN SUM(pid_n_atcs_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_atcs_3d AS DOUBLE) / SUM(pid_n_atcs_3d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_atcs_3d_ratio,
        CASE WHEN SUM(pid_n_atcs_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_atcs_7d AS DOUBLE) / SUM(pid_n_atcs_7d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_atcs_7d_ratio,
        CASE WHEN SUM(pid_n_atcs_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_atcs_14d AS DOUBLE) / SUM(pid_n_atcs_14d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_atcs_14d_ratio,
        CASE WHEN SUM(pid_n_atcs_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_atcs_28d AS DOUBLE) / SUM(pid_n_atcs_28d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_atcs_28d_ratio,
        CASE WHEN SUM(pid_n_atfs_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_atfs_3d AS DOUBLE) / SUM(pid_n_atfs_3d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_atfs_3d_ratio,
        CASE WHEN SUM(pid_n_atfs_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_atfs_7d AS DOUBLE) / SUM(pid_n_atfs_7d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_atfs_7d_ratio,
        CASE WHEN SUM(pid_n_atfs_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_atfs_14d AS DOUBLE) / SUM(pid_n_atfs_14d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_atfs_14d_ratio,
        CASE WHEN SUM(pid_n_atfs_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_atfs_28d AS DOUBLE) / SUM(pid_n_atfs_28d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_atfs_28d_ratio,
        CASE WHEN SUM(pid_n_orders_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_orders_3d AS DOUBLE) / SUM(pid_n_orders_3d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_orders_3d_ratio,
        CASE WHEN SUM(pid_n_orders_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_orders_7d AS DOUBLE) / SUM(pid_n_orders_7d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_orders_7d_ratio,
        CASE WHEN SUM(pid_n_orders_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_orders_14d AS DOUBLE) / SUM(pid_n_orders_14d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_orders_14d_ratio,
        CASE WHEN SUM(pid_n_orders_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_orders_28d AS DOUBLE) / SUM(pid_n_orders_28d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_orders_28d_ratio,
        CASE WHEN SUM(pid_n_orders_60d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_orders_60d AS DOUBLE) / SUM(pid_n_orders_60d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_orders_60d_ratio,
        CASE WHEN SUM(pid_n_orders_90d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(pid_n_orders_90d AS DOUBLE) / SUM(pid_n_orders_90d) OVER (PARTITION BY calculated_at, account_id) END AS pid_n_orders_90d_ratio,
        CASE WHEN SUM(pid_gmv_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN pid_gmv_3d / SUM(pid_gmv_3d) OVER (PARTITION BY calculated_at, account_id) END AS pid_gmv_3d_ratio,
        CASE WHEN SUM(pid_gmv_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN pid_gmv_7d / SUM(pid_gmv_7d) OVER (PARTITION BY calculated_at, account_id) END AS pid_gmv_7d_ratio,
        CASE WHEN SUM(pid_gmv_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN pid_gmv_14d / SUM(pid_gmv_14d) OVER (PARTITION BY calculated_at, account_id) END AS pid_gmv_14d_ratio,
        CASE WHEN SUM(pid_gmv_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN pid_gmv_28d / SUM(pid_gmv_28d) OVER (PARTITION BY calculated_at, account_id) END AS pid_gmv_28d_ratio,
        CASE WHEN SUM(pid_gmv_60d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN pid_gmv_60d / SUM(pid_gmv_60d) OVER (PARTITION BY calculated_at, account_id) END AS pid_gmv_60d_ratio,
        CASE WHEN SUM(pid_gmv_90d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN pid_gmv_90d / SUM(pid_gmv_90d) OVER (PARTITION BY calculated_at, account_id) END AS pid_gmv_90d_ratio,
        CASE WHEN last_click_at IS NOT NULL THEN -CAST(UNIX_TIMESTAMP(TIMESTAMP '{calculated_at_local}') - UNIX_TIMESTAMP(last_click_at) AS DOUBLE) / 3600.0 END AS pid_neg_n_hours_since_last_click,
        CASE WHEN pid_n_orders_90d > 0 THEN CAST(pid_n_orders_28d AS DOUBLE) / pid_n_orders_90d END AS pid_n_orders_28d_over_90d,
        CASE WHEN last_purchase_at IS NOT NULL THEN CAST(-CEIL(CAST(UNIX_TIMESTAMP(TIMESTAMP '{calculated_at_utc}') - UNIX_TIMESTAMP(last_purchase_at) AS DOUBLE) / 86400.0) AS INT) END AS pid_neg_n_days_since_last_purchase,
        CASE
            WHEN last_click_at IS NULL OR last_purchase_at IS NULL THEN NULL
            WHEN TO_UTC_TIMESTAMP(last_click_at, '{settings.business_timezone}') > last_purchase_at THEN 1
            ELSE 0
        END AS last_click_before_last_purchase
    FROM base_features
),
features_with_relative_recency AS (
    SELECT
        *,
        pid_neg_n_hours_since_last_click - MAX(pid_neg_n_hours_since_last_click) OVER (PARTITION BY calculated_at, account_id) AS pid_neg_n_hours_since_last_click_rel
    FROM features_with_ratios
)
SELECT
    calculated_at,
    account_id,
    product_id,
    pid_n_clicks_3d, pid_n_clicks_7d, pid_n_clicks_14d, pid_n_clicks_28d,
    pid_n_clicks_3d_ratio, pid_n_clicks_7d_ratio, pid_n_clicks_14d_ratio, pid_n_clicks_28d_ratio,
    pid_n_atcs_3d, pid_n_atcs_7d, pid_n_atcs_14d, pid_n_atcs_28d,
    pid_n_atcs_3d_ratio, pid_n_atcs_7d_ratio, pid_n_atcs_14d_ratio, pid_n_atcs_28d_ratio,
    pid_n_atfs_3d, pid_n_atfs_7d, pid_n_atfs_14d, pid_n_atfs_28d,
    pid_n_atfs_3d_ratio, pid_n_atfs_7d_ratio, pid_n_atfs_14d_ratio, pid_n_atfs_28d_ratio,
    pid_neg_n_hours_since_last_click, pid_neg_n_hours_since_last_click_rel,
    pid_n_orders_3d, pid_n_orders_7d, pid_n_orders_14d, pid_n_orders_28d, pid_n_orders_60d, pid_n_orders_90d,
    pid_n_orders_3d_ratio, pid_n_orders_7d_ratio, pid_n_orders_14d_ratio, pid_n_orders_28d_ratio, pid_n_orders_60d_ratio, pid_n_orders_90d_ratio,
    pid_n_orders_28d_over_90d,
    pid_gmv_3d, pid_gmv_7d, pid_gmv_14d, pid_gmv_28d, pid_gmv_60d, pid_gmv_90d,
    pid_gmv_3d_ratio, pid_gmv_7d_ratio, pid_gmv_14d_ratio, pid_gmv_28d_ratio, pid_gmv_60d_ratio, pid_gmv_90d_ratio,
    pid_neg_n_days_since_last_purchase,
    last_click_before_last_purchase
FROM features_with_relative_recency
"""


def build_account_product_features_merge_query(
    target_table: str, settings: SourceSettings, calculated_at: datetime
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at, settings.business_timezone
    )
    return f"""
MERGE INTO {target_table} AS target
USING account_product_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
