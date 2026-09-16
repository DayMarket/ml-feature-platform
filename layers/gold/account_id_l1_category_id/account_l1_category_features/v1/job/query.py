from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

ACTION_WINDOWS = (3, 7, 14, 28)
ORDER_WINDOWS = (3, 7, 14, 28, 60, 90)
RATIO_COLUMNS = tuple(
    f"n_{signal}_{window}d"
    for signal in ("imps", "clicks", "atcs", "atfs")
    for window in ACTION_WINDOWS
) + tuple(
    f"{metric}_{window}d" for metric in ("n_orders", "gmv") for window in ORDER_WINDOWS
)
CONVERSION_SIGNALS = (
    ("click", "n_clicks"),
    ("atc", "n_atcs"),
    ("atf", "n_atfs"),
    ("order", "n_orders"),
)


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


def _account_ratio_expressions() -> str:
    expressions = []
    for column in RATIO_COLUMNS:
        numerator = f"CAST({column} AS DOUBLE)" if column.startswith("n_") else column
        denominator = f"SUM({column}) OVER (PARTITION BY account_id)"
        expressions.append(
            f"CASE WHEN {denominator} > 0 THEN {numerator} / {denominator} "
            f"END AS {column}_ratio"
        )
    return ",\n        ".join(expressions)


def _raw_conversion_expressions() -> str:
    return ",\n        ".join(
        f"CASE WHEN n_imps_{window}d > 0 "
        f"THEN CAST({source}_{window}d AS DOUBLE) / n_imps_{window}d "
        f"END AS conv_imp2{signal}_raw_{window}d"
        for signal, source in CONVERSION_SIGNALS
        for window in ACTION_WINDOWS
    )


def _relative_conversion_expressions() -> str:
    return ",\n    ".join(
        f"CASE WHEN {baseline}.{signal}_{window}d > 0 "
        f"THEN conv_imp2{signal}_raw_{window}d / "
        f"{baseline}.{signal}_{window}d END AS "
        f"conv_imp2{signal}_div_total_{baseline}_conv_{window}d"
        for baseline in ("category", "account")
        for signal, _ in CONVERSION_SIGNALS
        for window in ACTION_WINDOWS
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
    account_ratio_expressions = _account_ratio_expressions()
    raw_conversion_expressions = _raw_conversion_expressions()
    relative_conversion_expressions = _relative_conversion_expressions()
    return f"""
WITH product_categories AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        CAST(l1_category_id AS INT) AS l1_category_id
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_dt_local}'
        AND l1_category_id IS NOT NULL
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
        product.l1_category_id
    FROM deduplicated_actions action
    INNER JOIN product_categories product
        ON action.product_id = product.product_id
),
action_features AS (
    SELECT
        account_id,
        l1_category_id,
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
        CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN 1 ELSE 0 END) AS INT) AS n_atfs_28d,
        MAX(CASE WHEN event_type = 'PRODUCT_VIEW' THEN last_received_at END) AS last_click_at
    FROM mapped_actions
    GROUP BY account_id, l1_category_id
),
impression_features AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        CAST(l1_category_id AS INT) AS l1_category_id,
        CAST(SUM(CASE WHEN calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN n_impressions ELSE 0 END) AS INT) AS n_imps_3d,
        CAST(SUM(CASE WHEN calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN n_impressions ELSE 0 END) AS INT) AS n_imps_7d,
        CAST(SUM(CASE WHEN calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN n_impressions ELSE 0 END) AS INT) AS n_imps_14d,
        CAST(SUM(CASE WHEN calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN n_impressions ELSE 0 END) AS INT) AS n_imps_28d
    FROM {settings.impression_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
    GROUP BY account_id, l1_category_id
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
        product.l1_category_id
    FROM filtered_order_lines order_line
    INNER JOIN product_categories product ON order_line.product_id = product.product_id
),
order_features AS (
    SELECT
        account_id,
        l1_category_id,
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
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_90d,
        MAX(generated_at) AS last_purchase_at
    FROM mapped_order_lines
    GROUP BY account_id, l1_category_id
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
    SELECT account_id, l1_category_id FROM impression_features
    UNION
    SELECT account_id, l1_category_id FROM action_features
    UNION
    SELECT account_id, l1_category_id FROM order_features
),
recency_features AS (
    SELECT
        account_id,
        l1_category_id,
        neg_n_days_since_last_click,
        neg_n_days_since_last_click
            - MAX(neg_n_days_since_last_click) OVER (PARTITION BY account_id)
            AS neg_n_days_since_last_click_rel
    FROM (
        SELECT
            account_id,
            l1_category_id,
            CASE WHEN last_click_at IS NOT NULL THEN -CAST(UNIX_TIMESTAMP(TIMESTAMP '{calculated_at_local}') - UNIX_TIMESTAMP(last_click_at) AS DOUBLE) / 86400.0 END AS neg_n_days_since_last_click
        FROM action_features
    ) recency
),
base_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        entity.account_id,
        entity.l1_category_id,
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
        COALESCE(orders.gmv_90d, 0.0D) AS gmv_90d,
        recency.neg_n_days_since_last_click,
        recency.neg_n_days_since_last_click_rel,
        CAST(
            UNIX_TIMESTAMP(orders.last_purchase_at)
            - UNIX_TIMESTAMP(TO_UTC_TIMESTAMP(actions.last_click_at, '{settings.business_timezone}'))
            AS DOUBLE
        ) / 86400.0 AS n_days_between_last_click_and_last_purchase
    FROM entity_keys entity
    LEFT JOIN impression_features impressions USING (account_id, l1_category_id)
    LEFT JOIN action_features actions USING (account_id, l1_category_id)
    LEFT JOIN order_features orders USING (account_id, l1_category_id)
    LEFT JOIN recency_features recency USING (account_id, l1_category_id)
),
features AS (
    SELECT
        base.*,
        {account_ratio_expressions},
        {raw_conversion_expressions}
    FROM base_features base
),
category_baselines AS (
    SELECT
        l1_category_id,
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
    GROUP BY l1_category_id
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
    {relative_conversion_expressions}
FROM features
INNER JOIN category_baselines category USING (l1_category_id)
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
    AND target.l1_category_id = source.l1_category_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
