from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo


class SourceSettings(Protocol):
    product_metadata_table: str
    action_counts_table: str
    impression_counts_table: str
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
        CAST(l2_category_id AS INT) AS l2_category_id
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_dt_local}'
        AND l2_category_id IS NOT NULL
),
mapped_actions AS (
    SELECT
        CAST(action.account_id AS INT) AS account_id,
        action.event_type,
        action.last_received_at,
        product.l2_category_id
    FROM {settings.action_counts_table} action
    INNER JOIN product_categories product
        ON CAST(action.product_id AS INT) = product.product_id
    WHERE action.calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND action.calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND action.last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND action.last_received_at < TIMESTAMP '{calculated_at_local}'
),
action_features AS (
    SELECT
        account_id,
        l2_category_id,
        CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN 1 ELSE 0 END) AS INT) AS n_clicks_3d,
        CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS INT) AS n_clicks_7d,
        CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS INT) AS n_clicks_14d,
        CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN 1 ELSE 0 END) AS INT) AS n_clicks_28d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atcs_3d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atcs_7d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atcs_14d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_CART' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atcs_28d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atfs_3d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atfs_7d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atfs_14d,
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atfs_28d
    FROM mapped_actions
    GROUP BY account_id, l2_category_id
),
impression_features AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        CAST(l2_category_id AS INT) AS l2_category_id,
        CAST(SUM(CASE WHEN calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN n_impressions ELSE 0 END) AS INT) AS n_imps_3d,
        CAST(SUM(CASE WHEN calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN n_impressions ELSE 0 END) AS INT) AS n_imps_7d,
        CAST(SUM(CASE WHEN calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN n_impressions ELSE 0 END) AS INT) AS n_imps_14d,
        CAST(SUM(CASE WHEN calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN n_impressions ELSE 0 END) AS INT) AS n_imps_28d
    FROM {settings.impression_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
    GROUP BY account_id, l2_category_id
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
        product.l2_category_id
    FROM filtered_order_lines order_line
    INNER JOIN product_categories product ON order_line.product_id = product.product_id
),
order_features AS (
    SELECT
        account_id,
        l2_category_id,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN order_id END) AS INT) AS n_orders_3d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN order_id END) AS INT) AS n_orders_7d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN order_id END) AS INT) AS n_orders_14d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN order_id END) AS INT) AS n_orders_28d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 60 DAYS THEN order_id END) AS INT) AS n_orders_60d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN order_id END) AS INT) AS n_orders_90d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_3d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_7d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_14d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_28d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 60 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_60d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_90d
    FROM mapped_order_lines
    GROUP BY account_id, l2_category_id
),
account_order_features AS (
    SELECT
        account_id,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN order_id END) AS INT) AS account_n_orders_3d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN order_id END) AS INT) AS account_n_orders_7d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN order_id END) AS INT) AS account_n_orders_14d,
        CAST(COUNT(DISTINCT CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN order_id END) AS INT) AS account_n_orders_28d
    FROM filtered_order_lines
    GROUP BY account_id
),
entity_keys AS (
    SELECT account_id, l2_category_id FROM impression_features
    UNION
    SELECT account_id, l2_category_id FROM action_features
    UNION
    SELECT account_id, l2_category_id FROM order_features
),
base_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        entity.account_id,
        entity.l2_category_id,
        COALESCE(impressions.n_imps_3d, 0) AS n_imps_3d,
        COALESCE(impressions.n_imps_7d, 0) AS n_imps_7d,
        COALESCE(impressions.n_imps_14d, 0) AS n_imps_14d,
        COALESCE(impressions.n_imps_28d, 0) AS n_imps_28d,
        COALESCE(actions.n_clicks_3d, 0) AS n_clicks_3d,
        COALESCE(actions.n_clicks_7d, 0) AS n_clicks_7d,
        COALESCE(actions.n_clicks_14d, 0) AS n_clicks_14d,
        COALESCE(actions.n_clicks_28d, 0) AS n_clicks_28d,
        COALESCE(actions.n_atcs_3d, 0) AS n_atcs_3d,
        COALESCE(actions.n_atcs_7d, 0) AS n_atcs_7d,
        COALESCE(actions.n_atcs_14d, 0) AS n_atcs_14d,
        COALESCE(actions.n_atcs_28d, 0) AS n_atcs_28d,
        COALESCE(actions.n_atfs_3d, 0) AS n_atfs_3d,
        COALESCE(actions.n_atfs_7d, 0) AS n_atfs_7d,
        COALESCE(actions.n_atfs_14d, 0) AS n_atfs_14d,
        COALESCE(actions.n_atfs_28d, 0) AS n_atfs_28d,
        COALESCE(orders.n_orders_3d, 0) AS n_orders_3d,
        COALESCE(orders.n_orders_7d, 0) AS n_orders_7d,
        COALESCE(orders.n_orders_14d, 0) AS n_orders_14d,
        COALESCE(orders.n_orders_28d, 0) AS n_orders_28d,
        COALESCE(orders.n_orders_60d, 0) AS n_orders_60d,
        COALESCE(orders.n_orders_90d, 0) AS n_orders_90d,
        COALESCE(orders.gmv_3d, 0.0D) AS gmv_3d,
        COALESCE(orders.gmv_7d, 0.0D) AS gmv_7d,
        COALESCE(orders.gmv_14d, 0.0D) AS gmv_14d,
        COALESCE(orders.gmv_28d, 0.0D) AS gmv_28d,
        COALESCE(orders.gmv_60d, 0.0D) AS gmv_60d,
        COALESCE(orders.gmv_90d, 0.0D) AS gmv_90d
    FROM entity_keys entity
    LEFT JOIN impression_features impressions USING (account_id, l2_category_id)
    LEFT JOIN action_features actions USING (account_id, l2_category_id)
    LEFT JOIN order_features orders USING (account_id, l2_category_id)
),
features AS (
    SELECT
        base.*,
        CASE WHEN SUM(n_imps_3d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_imps_3d AS DOUBLE) / SUM(n_imps_3d) OVER (PARTITION BY account_id) END AS n_imps_3d_ratio,
        CASE WHEN SUM(n_imps_7d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_imps_7d AS DOUBLE) / SUM(n_imps_7d) OVER (PARTITION BY account_id) END AS n_imps_7d_ratio,
        CASE WHEN SUM(n_imps_14d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_imps_14d AS DOUBLE) / SUM(n_imps_14d) OVER (PARTITION BY account_id) END AS n_imps_14d_ratio,
        CASE WHEN SUM(n_imps_28d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_imps_28d AS DOUBLE) / SUM(n_imps_28d) OVER (PARTITION BY account_id) END AS n_imps_28d_ratio,
        CASE WHEN SUM(n_clicks_3d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_clicks_3d AS DOUBLE) / SUM(n_clicks_3d) OVER (PARTITION BY account_id) END AS n_clicks_3d_ratio,
        CASE WHEN SUM(n_clicks_7d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_clicks_7d AS DOUBLE) / SUM(n_clicks_7d) OVER (PARTITION BY account_id) END AS n_clicks_7d_ratio,
        CASE WHEN SUM(n_clicks_14d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_clicks_14d AS DOUBLE) / SUM(n_clicks_14d) OVER (PARTITION BY account_id) END AS n_clicks_14d_ratio,
        CASE WHEN SUM(n_clicks_28d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_clicks_28d AS DOUBLE) / SUM(n_clicks_28d) OVER (PARTITION BY account_id) END AS n_clicks_28d_ratio,
        CASE WHEN SUM(n_atcs_3d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_atcs_3d AS DOUBLE) / SUM(n_atcs_3d) OVER (PARTITION BY account_id) END AS n_atcs_3d_ratio,
        CASE WHEN SUM(n_atcs_7d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_atcs_7d AS DOUBLE) / SUM(n_atcs_7d) OVER (PARTITION BY account_id) END AS n_atcs_7d_ratio,
        CASE WHEN SUM(n_atcs_14d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_atcs_14d AS DOUBLE) / SUM(n_atcs_14d) OVER (PARTITION BY account_id) END AS n_atcs_14d_ratio,
        CASE WHEN SUM(n_atcs_28d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_atcs_28d AS DOUBLE) / SUM(n_atcs_28d) OVER (PARTITION BY account_id) END AS n_atcs_28d_ratio,
        CASE WHEN SUM(n_atfs_3d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_atfs_3d AS DOUBLE) / SUM(n_atfs_3d) OVER (PARTITION BY account_id) END AS n_atfs_3d_ratio,
        CASE WHEN SUM(n_atfs_7d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_atfs_7d AS DOUBLE) / SUM(n_atfs_7d) OVER (PARTITION BY account_id) END AS n_atfs_7d_ratio,
        CASE WHEN SUM(n_atfs_14d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_atfs_14d AS DOUBLE) / SUM(n_atfs_14d) OVER (PARTITION BY account_id) END AS n_atfs_14d_ratio,
        CASE WHEN SUM(n_atfs_28d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_atfs_28d AS DOUBLE) / SUM(n_atfs_28d) OVER (PARTITION BY account_id) END AS n_atfs_28d_ratio,
        CASE WHEN SUM(n_orders_3d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_orders_3d AS DOUBLE) / SUM(n_orders_3d) OVER (PARTITION BY account_id) END AS n_orders_3d_ratio,
        CASE WHEN SUM(n_orders_7d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_orders_7d AS DOUBLE) / SUM(n_orders_7d) OVER (PARTITION BY account_id) END AS n_orders_7d_ratio,
        CASE WHEN SUM(n_orders_14d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_orders_14d AS DOUBLE) / SUM(n_orders_14d) OVER (PARTITION BY account_id) END AS n_orders_14d_ratio,
        CASE WHEN SUM(n_orders_28d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_orders_28d AS DOUBLE) / SUM(n_orders_28d) OVER (PARTITION BY account_id) END AS n_orders_28d_ratio,
        CASE WHEN SUM(n_orders_60d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_orders_60d AS DOUBLE) / SUM(n_orders_60d) OVER (PARTITION BY account_id) END AS n_orders_60d_ratio,
        CASE WHEN SUM(n_orders_90d) OVER (PARTITION BY account_id) > 0 THEN CAST(n_orders_90d AS DOUBLE) / SUM(n_orders_90d) OVER (PARTITION BY account_id) END AS n_orders_90d_ratio,
        CASE WHEN SUM(gmv_3d) OVER (PARTITION BY account_id) > 0 THEN gmv_3d / SUM(gmv_3d) OVER (PARTITION BY account_id) END AS gmv_3d_ratio,
        CASE WHEN SUM(gmv_7d) OVER (PARTITION BY account_id) > 0 THEN gmv_7d / SUM(gmv_7d) OVER (PARTITION BY account_id) END AS gmv_7d_ratio,
        CASE WHEN SUM(gmv_14d) OVER (PARTITION BY account_id) > 0 THEN gmv_14d / SUM(gmv_14d) OVER (PARTITION BY account_id) END AS gmv_14d_ratio,
        CASE WHEN SUM(gmv_28d) OVER (PARTITION BY account_id) > 0 THEN gmv_28d / SUM(gmv_28d) OVER (PARTITION BY account_id) END AS gmv_28d_ratio,
        CASE WHEN SUM(gmv_60d) OVER (PARTITION BY account_id) > 0 THEN gmv_60d / SUM(gmv_60d) OVER (PARTITION BY account_id) END AS gmv_60d_ratio,
        CASE WHEN SUM(gmv_90d) OVER (PARTITION BY account_id) > 0 THEN gmv_90d / SUM(gmv_90d) OVER (PARTITION BY account_id) END AS gmv_90d_ratio,
        CASE WHEN n_imps_3d > 0 THEN CAST(n_clicks_3d AS DOUBLE) / n_imps_3d END AS account_conv_imp2click_3d,
        CASE WHEN n_imps_7d > 0 THEN CAST(n_clicks_7d AS DOUBLE) / n_imps_7d END AS account_conv_imp2click_7d,
        CASE WHEN n_imps_14d > 0 THEN CAST(n_clicks_14d AS DOUBLE) / n_imps_14d END AS account_conv_imp2click_14d,
        CASE WHEN n_imps_28d > 0 THEN CAST(n_clicks_28d AS DOUBLE) / n_imps_28d END AS account_conv_imp2click_28d,
        CASE WHEN n_imps_3d > 0 THEN CAST(n_atcs_3d AS DOUBLE) / n_imps_3d END AS account_conv_imp2atc_3d,
        CASE WHEN n_imps_7d > 0 THEN CAST(n_atcs_7d AS DOUBLE) / n_imps_7d END AS account_conv_imp2atc_7d,
        CASE WHEN n_imps_14d > 0 THEN CAST(n_atcs_14d AS DOUBLE) / n_imps_14d END AS account_conv_imp2atc_14d,
        CASE WHEN n_imps_28d > 0 THEN CAST(n_atcs_28d AS DOUBLE) / n_imps_28d END AS account_conv_imp2atc_28d,
        CASE WHEN n_imps_3d > 0 THEN CAST(n_atfs_3d AS DOUBLE) / n_imps_3d END AS account_conv_imp2atf_3d,
        CASE WHEN n_imps_7d > 0 THEN CAST(n_atfs_7d AS DOUBLE) / n_imps_7d END AS account_conv_imp2atf_7d,
        CASE WHEN n_imps_14d > 0 THEN CAST(n_atfs_14d AS DOUBLE) / n_imps_14d END AS account_conv_imp2atf_14d,
        CASE WHEN n_imps_28d > 0 THEN CAST(n_atfs_28d AS DOUBLE) / n_imps_28d END AS account_conv_imp2atf_28d,
        CASE WHEN n_imps_3d > 0 THEN CAST(n_orders_3d AS DOUBLE) / n_imps_3d END AS account_conv_imp2order_3d,
        CASE WHEN n_imps_7d > 0 THEN CAST(n_orders_7d AS DOUBLE) / n_imps_7d END AS account_conv_imp2order_7d,
        CASE WHEN n_imps_14d > 0 THEN CAST(n_orders_14d AS DOUBLE) / n_imps_14d END AS account_conv_imp2order_14d,
        CASE WHEN n_imps_28d > 0 THEN CAST(n_orders_28d AS DOUBLE) / n_imps_28d END AS account_conv_imp2order_28d
    FROM base_features base
),
category_baselines AS (
    SELECT
        l2_category_id,
        SUM(n_clicks_3d) / NULLIF(SUM(n_imps_3d), 0) AS click_3d,
        SUM(n_clicks_7d) / NULLIF(SUM(n_imps_7d), 0) AS click_7d,
        SUM(n_clicks_14d) / NULLIF(SUM(n_imps_14d), 0) AS click_14d,
        SUM(n_clicks_28d) / NULLIF(SUM(n_imps_28d), 0) AS click_28d,
        SUM(n_atcs_3d) / NULLIF(SUM(n_imps_3d), 0) AS atc_3d,
        SUM(n_atcs_7d) / NULLIF(SUM(n_imps_7d), 0) AS atc_7d,
        SUM(n_atcs_14d) / NULLIF(SUM(n_imps_14d), 0) AS atc_14d,
        SUM(n_atcs_28d) / NULLIF(SUM(n_imps_28d), 0) AS atc_28d,
        SUM(n_atfs_3d) / NULLIF(SUM(n_imps_3d), 0) AS atf_3d,
        SUM(n_atfs_7d) / NULLIF(SUM(n_imps_7d), 0) AS atf_7d,
        SUM(n_atfs_14d) / NULLIF(SUM(n_imps_14d), 0) AS atf_14d,
        SUM(n_atfs_28d) / NULLIF(SUM(n_imps_28d), 0) AS atf_28d,
        SUM(n_orders_3d) / NULLIF(SUM(n_imps_3d), 0) AS order_3d,
        SUM(n_orders_7d) / NULLIF(SUM(n_imps_7d), 0) AS order_7d,
        SUM(n_orders_14d) / NULLIF(SUM(n_imps_14d), 0) AS order_14d,
        SUM(n_orders_28d) / NULLIF(SUM(n_imps_28d), 0) AS order_28d
    FROM base_features
    GROUP BY l2_category_id
),
account_baselines AS (
    SELECT
        base.account_id,
        SUM(n_clicks_3d) / NULLIF(SUM(n_imps_3d), 0) AS click_3d,
        SUM(n_clicks_7d) / NULLIF(SUM(n_imps_7d), 0) AS click_7d,
        SUM(n_clicks_14d) / NULLIF(SUM(n_imps_14d), 0) AS click_14d,
        SUM(n_clicks_28d) / NULLIF(SUM(n_imps_28d), 0) AS click_28d,
        SUM(n_atcs_3d) / NULLIF(SUM(n_imps_3d), 0) AS atc_3d,
        SUM(n_atcs_7d) / NULLIF(SUM(n_imps_7d), 0) AS atc_7d,
        SUM(n_atcs_14d) / NULLIF(SUM(n_imps_14d), 0) AS atc_14d,
        SUM(n_atcs_28d) / NULLIF(SUM(n_imps_28d), 0) AS atc_28d,
        SUM(n_atfs_3d) / NULLIF(SUM(n_imps_3d), 0) AS atf_3d,
        SUM(n_atfs_7d) / NULLIF(SUM(n_imps_7d), 0) AS atf_7d,
        SUM(n_atfs_14d) / NULLIF(SUM(n_imps_14d), 0) AS atf_14d,
        SUM(n_atfs_28d) / NULLIF(SUM(n_imps_28d), 0) AS atf_28d,
        MAX(account_orders.account_n_orders_3d) / NULLIF(SUM(n_imps_3d), 0) AS order_3d,
        MAX(account_orders.account_n_orders_7d) / NULLIF(SUM(n_imps_7d), 0) AS order_7d,
        MAX(account_orders.account_n_orders_14d) / NULLIF(SUM(n_imps_14d), 0) AS order_14d,
        MAX(account_orders.account_n_orders_28d) / NULLIF(SUM(n_imps_28d), 0) AS order_28d
    FROM base_features base
    LEFT JOIN account_order_features account_orders ON base.account_id = account_orders.account_id
    GROUP BY base.account_id
)
SELECT
    features.*,
    CASE WHEN category.click_3d > 0 THEN account_conv_imp2click_3d / category.click_3d END AS conv_imp2click_3d,
    CASE WHEN category.click_7d > 0 THEN account_conv_imp2click_7d / category.click_7d END AS conv_imp2click_7d,
    CASE WHEN category.click_14d > 0 THEN account_conv_imp2click_14d / category.click_14d END AS conv_imp2click_14d,
    CASE WHEN category.click_28d > 0 THEN account_conv_imp2click_28d / category.click_28d END AS conv_imp2click_28d,
    CASE WHEN category.atc_3d > 0 THEN account_conv_imp2atc_3d / category.atc_3d END AS conv_imp2atc_3d,
    CASE WHEN category.atc_7d > 0 THEN account_conv_imp2atc_7d / category.atc_7d END AS conv_imp2atc_7d,
    CASE WHEN category.atc_14d > 0 THEN account_conv_imp2atc_14d / category.atc_14d END AS conv_imp2atc_14d,
    CASE WHEN category.atc_28d > 0 THEN account_conv_imp2atc_28d / category.atc_28d END AS conv_imp2atc_28d,
    CASE WHEN category.atf_3d > 0 THEN account_conv_imp2atf_3d / category.atf_3d END AS conv_imp2atf_3d,
    CASE WHEN category.atf_7d > 0 THEN account_conv_imp2atf_7d / category.atf_7d END AS conv_imp2atf_7d,
    CASE WHEN category.atf_14d > 0 THEN account_conv_imp2atf_14d / category.atf_14d END AS conv_imp2atf_14d,
    CASE WHEN category.atf_28d > 0 THEN account_conv_imp2atf_28d / category.atf_28d END AS conv_imp2atf_28d,
    CASE WHEN category.order_3d > 0 THEN account_conv_imp2order_3d / category.order_3d END AS conv_imp2order_3d,
    CASE WHEN category.order_7d > 0 THEN account_conv_imp2order_7d / category.order_7d END AS conv_imp2order_7d,
    CASE WHEN category.order_14d > 0 THEN account_conv_imp2order_14d / category.order_14d END AS conv_imp2order_14d,
    CASE WHEN category.order_28d > 0 THEN account_conv_imp2order_28d / category.order_28d END AS conv_imp2order_28d,
    CASE WHEN account.click_3d > 0 THEN account_conv_imp2click_3d / account.click_3d END AS conv_imp2click_vs_account_3d,
    CASE WHEN account.click_7d > 0 THEN account_conv_imp2click_7d / account.click_7d END AS conv_imp2click_vs_account_7d,
    CASE WHEN account.click_14d > 0 THEN account_conv_imp2click_14d / account.click_14d END AS conv_imp2click_vs_account_14d,
    CASE WHEN account.click_28d > 0 THEN account_conv_imp2click_28d / account.click_28d END AS conv_imp2click_vs_account_28d,
    CASE WHEN account.atc_3d > 0 THEN account_conv_imp2atc_3d / account.atc_3d END AS conv_imp2atc_vs_account_3d,
    CASE WHEN account.atc_7d > 0 THEN account_conv_imp2atc_7d / account.atc_7d END AS conv_imp2atc_vs_account_7d,
    CASE WHEN account.atc_14d > 0 THEN account_conv_imp2atc_14d / account.atc_14d END AS conv_imp2atc_vs_account_14d,
    CASE WHEN account.atc_28d > 0 THEN account_conv_imp2atc_28d / account.atc_28d END AS conv_imp2atc_vs_account_28d,
    CASE WHEN account.atf_3d > 0 THEN account_conv_imp2atf_3d / account.atf_3d END AS conv_imp2atf_vs_account_3d,
    CASE WHEN account.atf_7d > 0 THEN account_conv_imp2atf_7d / account.atf_7d END AS conv_imp2atf_vs_account_7d,
    CASE WHEN account.atf_14d > 0 THEN account_conv_imp2atf_14d / account.atf_14d END AS conv_imp2atf_vs_account_14d,
    CASE WHEN account.atf_28d > 0 THEN account_conv_imp2atf_28d / account.atf_28d END AS conv_imp2atf_vs_account_28d,
    CASE WHEN account.order_3d > 0 THEN account_conv_imp2order_3d / account.order_3d END AS conv_imp2order_vs_account_3d,
    CASE WHEN account.order_7d > 0 THEN account_conv_imp2order_7d / account.order_7d END AS conv_imp2order_vs_account_7d,
    CASE WHEN account.order_14d > 0 THEN account_conv_imp2order_14d / account.order_14d END AS conv_imp2order_vs_account_14d,
    CASE WHEN account.order_28d > 0 THEN account_conv_imp2order_28d / account.order_28d END AS conv_imp2order_vs_account_28d
FROM features
INNER JOIN category_baselines category USING (l2_category_id)
INNER JOIN account_baselines account USING (account_id)
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
    AND target.l2_category_id = source.l2_category_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
