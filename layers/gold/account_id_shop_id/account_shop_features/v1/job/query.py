from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

ACTION_WINDOWS = (3, 7, 14, 28)
ORDER_WINDOWS = (3, 7, 14, 28, 60, 90)
FEATURE_NAMESPACE = "ACCOUNT_SHOP"
ACTION_EVENT_TYPES = {
    "clicks": "PRODUCT_VIEW",
    "atcs": "ADD_TO_CART",
    "atfs": "ADD_TO_FAVORITES",
}

ACTION_COLUMNS = tuple(
    f"n_{signal}_{window}d"
    for signal in ACTION_EVENT_TYPES
    for window in ACTION_WINDOWS
)
ORDER_COLUMNS = tuple(f"n_orders_{window}d" for window in ORDER_WINDOWS)
GMV_COLUMNS = tuple(f"gmv_{window}d" for window in ORDER_WINDOWS)
BASE_FEATURE_COLUMNS = ACTION_COLUMNS + ORDER_COLUMNS + GMV_COLUMNS
UNPREFIXED_FEATURE_COLUMNS = BASE_FEATURE_COLUMNS + tuple(
    f"{column}_ratio" for column in BASE_FEATURE_COLUMNS
)
FEATURE_COLUMNS = tuple(
    f"{FEATURE_NAMESPACE}__{column}" for column in UNPREFIXED_FEATURE_COLUMNS
)


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


def _local_day_start_utc_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    local_value = value.astimezone(ZoneInfo(timezone_name))
    local_day_start = local_value.replace(hour=0, minute=0, second=0, microsecond=0)
    return local_day_start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _action_feature_expressions(calculated_at_local: str) -> str:
    return ",\n        ".join(
        "CAST(SUM(CASE "
        f"WHEN event_type = '{event_type}' "
        f"AND last_received_at >= TIMESTAMP '{calculated_at_local}' "
        f"- INTERVAL {window} DAYS "
        "THEN 1 ELSE 0 END) AS INT) "
        f"AS n_{signal}_{window}d"
        for signal, event_type in ACTION_EVENT_TYPES.items()
        for window in ACTION_WINDOWS
    )


def _order_count_expressions(calculated_at_utc: str) -> str:
    return ",\n        ".join(
        "CAST(COUNT(DISTINCT CASE "
        f"WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' "
        f"- INTERVAL {window} DAYS "
        "THEN order_id END) AS INT) "
        f"AS n_orders_{window}d"
        for window in ORDER_WINDOWS
    )


def _gmv_expressions(calculated_at_utc: str) -> str:
    return ",\n        ".join(
        "CAST(SUM(CASE "
        f"WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' "
        f"- INTERVAL {window} DAYS "
        "THEN line_gmv ELSE 0.0 END) AS DOUBLE) "
        f"AS gmv_{window}d"
        for window in ORDER_WINDOWS
    )


def _base_feature_expressions() -> str:
    expressions = [
        *(f"COALESCE(actions.{column}, 0) AS {column}" for column in ACTION_COLUMNS),
        *(f"COALESCE(orders.{column}, 0) AS {column}" for column in ORDER_COLUMNS),
        *(f"COALESCE(orders.{column}, 0.0D) AS {column}" for column in GMV_COLUMNS),
    ]
    return ",\n        ".join(expressions)


def _account_ratio_expressions() -> str:
    return ",\n    ".join(
        "CASE WHEN "
        f"SUM({column}) OVER (PARTITION BY calculated_at, account_id) > 0 "
        f"THEN CAST({column} AS DOUBLE) / "
        f"SUM({column}) OVER (PARTITION BY calculated_at, account_id) "
        f"END AS {column}_ratio"
        for column in BASE_FEATURE_COLUMNS
    )


def _namespaced_feature_select(source_alias: str) -> str:
    return ",\n    ".join(
        f"{source_alias}.{column} AS {FEATURE_NAMESPACE}__{column}"
        for column in UNPREFIXED_FEATURE_COLUMNS
    )


def build_account_shop_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_utc = _utc_timestamp_literal(calculated_at)
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    metadata_dt_utc = _local_day_start_utc_literal(
        calculated_at,
        settings.business_timezone,
    )

    action_feature_expressions = _action_feature_expressions(calculated_at_local)
    order_count_expressions = _order_count_expressions(calculated_at_utc)
    gmv_expressions = _gmv_expressions(calculated_at_utc)
    base_feature_expressions = _base_feature_expressions()
    account_ratio_expressions = _account_ratio_expressions()
    namespaced_feature_select = _namespaced_feature_select("unprefixed_features")

    return f"""
WITH product_shops AS (
    SELECT
        product_id,
        shop_id
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_dt_utc}'
        AND shop_id IS NOT NULL
),
deduplicated_actions AS (
    SELECT
        account_id,
        session_id,
        product_id,
        event_type,
        MAX(last_received_at) AS last_received_at
    FROM {settings.action_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND last_received_at < TIMESTAMP '{calculated_at_local}'
        AND event_type IN (
            'PRODUCT_VIEW', 'ADD_TO_CART', 'ADD_TO_FAVORITES'
        )
    GROUP BY account_id, session_id, product_id, event_type
),
mapped_actions AS (
    SELECT
        action.account_id,
        product.shop_id,
        action.event_type,
        action.last_received_at
    FROM deduplicated_actions action
    INNER JOIN product_shops product
        ON action.product_id = product.product_id
),
action_features AS (
    SELECT
        account_id,
        shop_id,
        {action_feature_expressions}
    FROM mapped_actions
    GROUP BY account_id, shop_id
),
sku_mapping AS (
    SELECT
        id AS sku_id,
        CAST(MIN(product_id) AS INT) AS product_id
    FROM {settings.sku_table}
    GROUP BY id
),
filtered_order_lines AS (
    SELECT
        CAST(order_item.account_id AS INT) AS account_id,
        order_item.order_id,
        sku.product_id,
        CAST(order_item.generated_at AS TIMESTAMP) AS generated_at,
        CAST(order_item.payment_price AS DOUBLE)
            * CAST(order_item.item_quantity AS DOUBLE) AS line_gmv
    FROM {settings.order_items_table} order_item
    INNER JOIN sku_mapping sku
        ON order_item.sku_id = sku.sku_id
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
        product.shop_id,
        order_line.order_id,
        order_line.generated_at,
        order_line.line_gmv
    FROM filtered_order_lines order_line
    INNER JOIN product_shops product
        ON order_line.product_id = product.product_id
),
order_features AS (
    SELECT
        account_id,
        shop_id,
        {order_count_expressions},
        {gmv_expressions}
    FROM mapped_order_lines
    GROUP BY account_id, shop_id
),
entity_keys AS (
    SELECT account_id, shop_id FROM action_features
    UNION
    SELECT account_id, shop_id FROM order_features
),
base_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        entity.account_id,
        entity.shop_id,
        {base_feature_expressions}
    FROM entity_keys entity
    LEFT JOIN action_features actions
        ON entity.account_id = actions.account_id
        AND entity.shop_id = actions.shop_id
    LEFT JOIN order_features orders
        ON entity.account_id = orders.account_id
        AND entity.shop_id = orders.shop_id
),
unprefixed_features AS (
    SELECT
        base.*,
        {account_ratio_expressions}
    FROM base_features base
)
SELECT
    calculated_at,
    account_id,
    shop_id,
    {namespaced_feature_select}
FROM unprefixed_features
"""


def build_account_shop_features_merge_query(
    target_table: str,
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    return f"""
MERGE INTO {target_table} AS target
USING account_shop_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.shop_id = source.shop_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
