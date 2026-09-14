from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo


class SourceSettings(Protocol):
    product_metadata_table: str
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


def _local_day_start_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    local_value = value.astimezone(ZoneInfo(timezone_name))
    return local_value.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def build_account_category_features_query(
    settings: SourceSettings, calculated_at: datetime
) -> str:
    calculated_at_utc = _utc_timestamp_literal(calculated_at)
    calculated_at_local = _local_timestamp_literal(
        calculated_at, settings.business_timezone
    )
    metadata_dt_local = _local_day_start_literal(
        calculated_at, settings.business_timezone
    )
    return f"""
WITH product_categories AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        CAST(l3_category_id AS INT) AS l3_category_id
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_dt_local}'
        AND l3_category_id IS NOT NULL
),
deduplicated_actions AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        session_id,
        CAST(product_id AS INT) AS product_id,
        event_type,
        MAX(last_received_at) AS last_received_at
    FROM {settings.action_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND last_received_at < TIMESTAMP '{calculated_at_local}'
    GROUP BY account_id, session_id, product_id, event_type
),
mapped_actions AS (
    SELECT
        action.account_id,
        action.event_type,
        action.last_received_at,
        product.l3_category_id
    FROM deduplicated_actions action
    INNER JOIN product_categories product ON action.product_id = product.product_id
),
action_features AS (
    SELECT
        account_id,
        l3_category_id,
        CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_clicks_3d,
        CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_clicks_7d,
        CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_clicks_14d,
        CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_clicks_28d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_atcs_3d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_atcs_7d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_atcs_14d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_atcs_28d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_atfs_3d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_atfs_7d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_atfs_14d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN 1 ELSE 0 END) AS INT) AS l3_n_atfs_28d,
        MAX(CASE WHEN event_type = 'PRODUCT_VIEW' THEN last_received_at END) AS last_click_at
    FROM mapped_actions
    GROUP BY account_id, l3_category_id
),
sku_mapping AS (
    SELECT CAST(id AS INT) AS sku_id, CAST(MIN(product_id) AS INT) AS product_id
    FROM {settings.sku_table}
    GROUP BY id
),
filtered_order_lines AS (
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
mapped_order_lines AS (
    SELECT
        order_line.account_id,
        order_line.order_id,
        order_line.generated_at,
        order_line.line_gmv,
        product.l3_category_id
    FROM filtered_order_lines order_line
    INNER JOIN product_categories product ON order_line.product_id = product.product_id
),
order_features AS (
    SELECT
        account_id,
        l3_category_id,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN order_id END) AS INT) AS l3_n_orders_3d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN order_id END) AS INT) AS l3_n_orders_7d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN order_id END) AS INT) AS l3_n_orders_14d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN order_id END) AS INT) AS l3_n_orders_28d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 60 DAYS THEN order_id END) AS INT) AS l3_n_orders_60d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN order_id END) AS INT) AS l3_n_orders_90d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS l3_gmv_3d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS l3_gmv_7d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS l3_gmv_14d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS l3_gmv_28d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 60 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS l3_gmv_60d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS l3_gmv_90d
    FROM mapped_order_lines
    GROUP BY account_id, l3_category_id
),
entity_keys AS (
    SELECT account_id, l3_category_id FROM action_features
    UNION
    SELECT account_id, l3_category_id FROM order_features
),
base_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        entity.account_id,
        entity.l3_category_id,
        COALESCE(actions.l3_n_clicks_3d, 0) AS l3_n_clicks_3d,
        COALESCE(actions.l3_n_clicks_7d, 0) AS l3_n_clicks_7d,
        COALESCE(actions.l3_n_clicks_14d, 0) AS l3_n_clicks_14d,
        COALESCE(actions.l3_n_clicks_28d, 0) AS l3_n_clicks_28d,
        COALESCE(actions.l3_n_atcs_3d, 0) AS l3_n_atcs_3d,
        COALESCE(actions.l3_n_atcs_7d, 0) AS l3_n_atcs_7d,
        COALESCE(actions.l3_n_atcs_14d, 0) AS l3_n_atcs_14d,
        COALESCE(actions.l3_n_atcs_28d, 0) AS l3_n_atcs_28d,
        COALESCE(actions.l3_n_atfs_3d, 0) AS l3_n_atfs_3d,
        COALESCE(actions.l3_n_atfs_7d, 0) AS l3_n_atfs_7d,
        COALESCE(actions.l3_n_atfs_14d, 0) AS l3_n_atfs_14d,
        COALESCE(actions.l3_n_atfs_28d, 0) AS l3_n_atfs_28d,
        COALESCE(orders.l3_n_orders_3d, 0) AS l3_n_orders_3d,
        COALESCE(orders.l3_n_orders_7d, 0) AS l3_n_orders_7d,
        COALESCE(orders.l3_n_orders_14d, 0) AS l3_n_orders_14d,
        COALESCE(orders.l3_n_orders_28d, 0) AS l3_n_orders_28d,
        COALESCE(orders.l3_n_orders_60d, 0) AS l3_n_orders_60d,
        COALESCE(orders.l3_n_orders_90d, 0) AS l3_n_orders_90d,
        COALESCE(orders.l3_gmv_3d, 0.0D) AS l3_gmv_3d,
        COALESCE(orders.l3_gmv_7d, 0.0D) AS l3_gmv_7d,
        COALESCE(orders.l3_gmv_14d, 0.0D) AS l3_gmv_14d,
        COALESCE(orders.l3_gmv_28d, 0.0D) AS l3_gmv_28d,
        COALESCE(orders.l3_gmv_60d, 0.0D) AS l3_gmv_60d,
        COALESCE(orders.l3_gmv_90d, 0.0D) AS l3_gmv_90d,
        actions.last_click_at
    FROM entity_keys entity
    LEFT JOIN action_features actions USING (account_id, l3_category_id)
    LEFT JOIN order_features orders USING (account_id, l3_category_id)
),
features_with_ratios AS (
    SELECT
        *,
        CASE WHEN SUM(l3_n_clicks_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_clicks_3d AS DOUBLE) / SUM(l3_n_clicks_3d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_clicks_3d_ratio,
        CASE WHEN SUM(l3_n_clicks_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_clicks_7d AS DOUBLE) / SUM(l3_n_clicks_7d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_clicks_7d_ratio,
        CASE WHEN SUM(l3_n_clicks_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_clicks_14d AS DOUBLE) / SUM(l3_n_clicks_14d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_clicks_14d_ratio,
        CASE WHEN SUM(l3_n_clicks_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_clicks_28d AS DOUBLE) / SUM(l3_n_clicks_28d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_clicks_28d_ratio,
        CASE WHEN SUM(l3_n_atcs_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_atcs_3d AS DOUBLE) / SUM(l3_n_atcs_3d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_atcs_3d_ratio,
        CASE WHEN SUM(l3_n_atcs_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_atcs_7d AS DOUBLE) / SUM(l3_n_atcs_7d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_atcs_7d_ratio,
        CASE WHEN SUM(l3_n_atcs_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_atcs_14d AS DOUBLE) / SUM(l3_n_atcs_14d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_atcs_14d_ratio,
        CASE WHEN SUM(l3_n_atcs_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_atcs_28d AS DOUBLE) / SUM(l3_n_atcs_28d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_atcs_28d_ratio,
        CASE WHEN SUM(l3_n_atfs_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_atfs_3d AS DOUBLE) / SUM(l3_n_atfs_3d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_atfs_3d_ratio,
        CASE WHEN SUM(l3_n_atfs_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_atfs_7d AS DOUBLE) / SUM(l3_n_atfs_7d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_atfs_7d_ratio,
        CASE WHEN SUM(l3_n_atfs_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_atfs_14d AS DOUBLE) / SUM(l3_n_atfs_14d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_atfs_14d_ratio,
        CASE WHEN SUM(l3_n_atfs_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_atfs_28d AS DOUBLE) / SUM(l3_n_atfs_28d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_atfs_28d_ratio,
        CASE WHEN SUM(l3_n_orders_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_orders_3d AS DOUBLE) / SUM(l3_n_orders_3d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_orders_3d_ratio,
        CASE WHEN SUM(l3_n_orders_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_orders_7d AS DOUBLE) / SUM(l3_n_orders_7d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_orders_7d_ratio,
        CASE WHEN SUM(l3_n_orders_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_orders_14d AS DOUBLE) / SUM(l3_n_orders_14d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_orders_14d_ratio,
        CASE WHEN SUM(l3_n_orders_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_orders_28d AS DOUBLE) / SUM(l3_n_orders_28d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_orders_28d_ratio,
        CASE WHEN SUM(l3_n_orders_60d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_orders_60d AS DOUBLE) / SUM(l3_n_orders_60d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_orders_60d_ratio,
        CASE WHEN SUM(l3_n_orders_90d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN CAST(l3_n_orders_90d AS DOUBLE) / SUM(l3_n_orders_90d) OVER (PARTITION BY calculated_at, account_id) END AS l3_n_orders_90d_ratio,
        CASE WHEN SUM(l3_gmv_3d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN l3_gmv_3d / SUM(l3_gmv_3d) OVER (PARTITION BY calculated_at, account_id) END AS l3_gmv_3d_ratio,
        CASE WHEN SUM(l3_gmv_7d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN l3_gmv_7d / SUM(l3_gmv_7d) OVER (PARTITION BY calculated_at, account_id) END AS l3_gmv_7d_ratio,
        CASE WHEN SUM(l3_gmv_14d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN l3_gmv_14d / SUM(l3_gmv_14d) OVER (PARTITION BY calculated_at, account_id) END AS l3_gmv_14d_ratio,
        CASE WHEN SUM(l3_gmv_28d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN l3_gmv_28d / SUM(l3_gmv_28d) OVER (PARTITION BY calculated_at, account_id) END AS l3_gmv_28d_ratio,
        CASE WHEN SUM(l3_gmv_60d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN l3_gmv_60d / SUM(l3_gmv_60d) OVER (PARTITION BY calculated_at, account_id) END AS l3_gmv_60d_ratio,
        CASE WHEN SUM(l3_gmv_90d) OVER (PARTITION BY calculated_at, account_id) > 0 THEN l3_gmv_90d / SUM(l3_gmv_90d) OVER (PARTITION BY calculated_at, account_id) END AS l3_gmv_90d_ratio,
        CASE WHEN last_click_at IS NOT NULL THEN -CAST(CEIL(CAST(UNIX_TIMESTAMP(TIMESTAMP '{calculated_at_local}') - UNIX_TIMESTAMP(last_click_at) AS DOUBLE) / 86400.0) AS INT) END AS l3_neg_n_days_since_last_click
    FROM base_features
),
features_with_recency AS (
    SELECT
        *,
        l3_neg_n_days_since_last_click - MAX(l3_neg_n_days_since_last_click) OVER (PARTITION BY calculated_at, account_id) AS l3_neg_n_days_since_last_click_rel
    FROM features_with_ratios
)
SELECT
    calculated_at,
    account_id,
    l3_category_id,
    l3_n_clicks_3d, l3_n_clicks_7d, l3_n_clicks_14d, l3_n_clicks_28d,
    l3_n_clicks_3d_ratio, l3_n_clicks_7d_ratio, l3_n_clicks_14d_ratio, l3_n_clicks_28d_ratio,
    l3_n_atcs_3d, l3_n_atcs_7d, l3_n_atcs_14d, l3_n_atcs_28d,
    l3_n_atcs_3d_ratio, l3_n_atcs_7d_ratio, l3_n_atcs_14d_ratio, l3_n_atcs_28d_ratio,
    l3_n_atfs_3d, l3_n_atfs_7d, l3_n_atfs_14d, l3_n_atfs_28d,
    l3_n_atfs_3d_ratio, l3_n_atfs_7d_ratio, l3_n_atfs_14d_ratio, l3_n_atfs_28d_ratio,
    l3_n_orders_3d, l3_n_orders_7d, l3_n_orders_14d, l3_n_orders_28d, l3_n_orders_60d, l3_n_orders_90d,
    l3_n_orders_3d_ratio, l3_n_orders_7d_ratio, l3_n_orders_14d_ratio, l3_n_orders_28d_ratio, l3_n_orders_60d_ratio, l3_n_orders_90d_ratio,
    l3_gmv_3d, l3_gmv_7d, l3_gmv_14d, l3_gmv_28d, l3_gmv_60d, l3_gmv_90d,
    l3_gmv_3d_ratio, l3_gmv_7d_ratio, l3_gmv_14d_ratio, l3_gmv_28d_ratio, l3_gmv_60d_ratio, l3_gmv_90d_ratio,
    l3_neg_n_days_since_last_click,
    l3_neg_n_days_since_last_click_rel
FROM features_with_recency
"""


def build_account_category_features_merge_query(
    target_table: str, settings: SourceSettings, calculated_at: datetime
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at, settings.business_timezone
    )
    return f"""
MERGE INTO {target_table} AS target
USING account_category_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.l3_category_id = source.l3_category_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
