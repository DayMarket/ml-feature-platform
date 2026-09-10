from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

ACTION_EVENT_TYPES = {
    "clicks": "PRODUCT_VIEW",
    "atcs": "ADD_TO_CART",
    "atfs": "ADD_TO_FAVORITES",
}


class SourceSettings(Protocol):
    action_counts_table: str
    order_items_table: str
    sku_table: str
    business_timezone: str
    event_windows_days: tuple[int, ...]
    order_windows_days: tuple[int, ...]
    successful_order_statuses: tuple[str, ...]


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        normalized = value.replace(tzinfo=timezone.utc)
    else:
        normalized = value.astimezone(timezone.utc)
    return normalized.strftime("%Y-%m-%d %H:%M:%S")


def _local_timestamp_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M:%S")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def feature_columns(settings: SourceSettings) -> tuple[str, ...]:
    columns: list[str] = []
    for signal in ACTION_EVENT_TYPES:
        columns.extend(
            f"pid_n_{signal}_{window}d" for window in settings.event_windows_days
        )
        columns.extend(
            f"pid_n_{signal}_{window}d_ratio" for window in settings.event_windows_days
        )

    columns.extend(
        (
            "pid_neg_n_hours_since_last_click",
            "pid_neg_n_hours_since_last_click_rel",
        )
    )
    columns.extend(f"pid_n_orders_{window}d" for window in settings.order_windows_days)
    columns.extend(
        f"pid_n_orders_{window}d_ratio" for window in settings.order_windows_days
    )
    columns.append("pid_n_orders_28d_over_90d")
    columns.extend(f"pid_gmv_{window}d" for window in settings.order_windows_days)
    columns.extend(f"pid_gmv_{window}d_ratio" for window in settings.order_windows_days)
    columns.extend(
        (
            "pid_neg_n_days_since_last_purchase",
            "last_click_before_last_purchase",
        )
    )
    return tuple(columns)


def _conditional_action_counts(
    settings: SourceSettings,
    calculated_at_local: str,
) -> str:
    expressions = []
    for signal, event_type in ACTION_EVENT_TYPES.items():
        for window in settings.event_windows_days:
            expressions.append(
                "CAST(COUNT(DISTINCT CASE "
                f"WHEN event_type = {_sql_string(event_type)} "
                f"AND last_received_at >= TIMESTAMP '{calculated_at_local}' "
                f"- INTERVAL {window} DAYS "
                "THEN session_id END) AS BIGINT) "
                f"AS pid_n_{signal}_{window}d"
            )
    return ",\n        ".join(expressions)


def _conditional_order_features(
    settings: SourceSettings,
    calculated_at_utc: str,
) -> str:
    expressions = []
    for window in settings.order_windows_days:
        expressions.append(
            "CAST(COUNT(DISTINCT CASE "
            f"WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' "
            f"- INTERVAL {window} DAYS "
            "THEN order_id END) AS BIGINT) "
            f"AS pid_n_orders_{window}d"
        )
    for window in settings.order_windows_days:
        expressions.append(
            "CAST(SUM(CASE "
            f"WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' "
            f"- INTERVAL {window} DAYS "
            "THEN line_gmv ELSE 0.0 END) AS DOUBLE) "
            f"AS pid_gmv_{window}d"
        )
    return ",\n        ".join(expressions)


def _coalesced_base_features(settings: SourceSettings) -> str:
    expressions = []
    for signal in ACTION_EVENT_TYPES:
        for window in settings.event_windows_days:
            column = f"pid_n_{signal}_{window}d"
            expressions.append(f"COALESCE(actions.{column}, 0L) AS {column}")
    for window in settings.order_windows_days:
        column = f"pid_n_orders_{window}d"
        expressions.append(f"COALESCE(orders.{column}, 0L) AS {column}")
    for window in settings.order_windows_days:
        column = f"pid_gmv_{window}d"
        expressions.append(f"COALESCE(orders.{column}, 0.0D) AS {column}")
    return ",\n        ".join(expressions)


def _ratio_expressions(settings: SourceSettings) -> str:
    metric_columns = []
    for signal in ACTION_EVENT_TYPES:
        metric_columns.extend(
            f"pid_n_{signal}_{window}d" for window in settings.event_windows_days
        )
    metric_columns.extend(
        f"pid_n_orders_{window}d" for window in settings.order_windows_days
    )
    metric_columns.extend(
        f"pid_gmv_{window}d" for window in settings.order_windows_days
    )

    expressions = []
    for column in metric_columns:
        expressions.append(
            "CASE WHEN SUM(" + column + ") OVER ("
            "PARTITION BY calculated_at, account_id) > 0 "
            "THEN CAST(" + column + " AS DOUBLE) / SUM(" + column + ") OVER ("
            "PARTITION BY calculated_at, account_id) END "
            f"AS {column}_ratio"
        )
    return ",\n        ".join(expressions)


def build_account_product_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_utc = _utc_timestamp_literal(calculated_at)
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    max_event_window = max(settings.event_windows_days)
    max_order_window = max(settings.order_windows_days)
    statuses_sql = ", ".join(
        _sql_string(status) for status in settings.successful_order_statuses
    )
    selected_features = ",\n    ".join(feature_columns(settings))

    return f"""
WITH deduplicated_actions AS (
    SELECT
        account_id,
        product_id,
        event_type,
        session_id,
        MAX(last_received_at) AS last_received_at
    FROM {settings.action_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}'
            - INTERVAL {max_event_window} DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND last_received_at >= TIMESTAMP '{calculated_at_local}'
            - INTERVAL {max_event_window} DAYS
        AND last_received_at < TIMESTAMP '{calculated_at_local}'
    GROUP BY
        account_id,
        product_id,
        event_type,
        session_id
),
action_features AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        CAST(product_id AS INT) AS product_id,
        {_conditional_action_counts(settings, calculated_at_local)},
        MAX(CASE
            WHEN event_type = 'PRODUCT_VIEW' THEN last_received_at
        END) AS last_click_at
    FROM deduplicated_actions
    GROUP BY account_id, product_id
),
sku_mapping AS (
    SELECT
        CAST(id AS BIGINT) AS sku_id,
        CAST(MIN(product_id) AS INT) AS product_id
    FROM {settings.sku_table}
    GROUP BY id
),
filtered_orders AS (
    SELECT
        CAST(order_item.account_id AS INT) AS account_id,
        sku.product_id,
        CAST(order_item.order_id AS BIGINT) AS order_id,
        CAST(order_item.generated_at AS TIMESTAMP) AS generated_at,
        CAST(order_item.payment_price AS DOUBLE)
            * CAST(order_item.item_quantity AS DOUBLE) AS line_gmv
    FROM {settings.order_items_table} order_item
    INNER JOIN sku_mapping sku
        ON CAST(order_item.sku_id AS BIGINT) = sku.sku_id
    WHERE order_item.generated_at >= TIMESTAMP '{calculated_at_utc}'
            - INTERVAL {max_order_window} DAYS
        AND order_item.generated_at < TIMESTAMP '{calculated_at_utc}'
        AND order_item.order_item_status IN ({statuses_sql})
),
order_features AS (
    SELECT
        account_id,
        product_id,
        {_conditional_order_features(settings, calculated_at_utc)},
        MAX(generated_at) AS last_purchase_at
    FROM filtered_orders
    GROUP BY account_id, product_id
),
base_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        COALESCE(actions.account_id, orders.account_id) AS account_id,
        COALESCE(actions.product_id, orders.product_id) AS product_id,
        {_coalesced_base_features(settings)},
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
        {_ratio_expressions(settings)},
        CASE
            WHEN last_click_at IS NOT NULL THEN
                -CAST(
                    UNIX_TIMESTAMP(TIMESTAMP '{calculated_at_local}')
                    - UNIX_TIMESTAMP(last_click_at)
                    AS DOUBLE
                ) / 3600.0
        END AS pid_neg_n_hours_since_last_click,
        CASE
            WHEN pid_n_orders_90d > 0 THEN
                CAST(pid_n_orders_28d AS DOUBLE) / pid_n_orders_90d
        END AS pid_n_orders_28d_over_90d,
        CASE
            WHEN last_purchase_at IS NOT NULL THEN
                CAST(-CEIL(
                    CAST(
                        UNIX_TIMESTAMP(TIMESTAMP '{calculated_at_utc}')
                        - UNIX_TIMESTAMP(last_purchase_at)
                        AS DOUBLE
                    ) / 86400.0
                ) AS INT)
        END AS pid_neg_n_days_since_last_purchase,
        CASE
            WHEN last_click_at IS NULL OR last_purchase_at IS NULL THEN NULL
            WHEN TO_UTC_TIMESTAMP(last_click_at, {_sql_string(settings.business_timezone)})
                    > last_purchase_at THEN 1
            ELSE 0
        END AS last_click_before_last_purchase
    FROM base_features
),
features_with_relative_recency AS (
    SELECT
        *,
        pid_neg_n_hours_since_last_click
            - MAX(pid_neg_n_hours_since_last_click) OVER (
                PARTITION BY calculated_at, account_id
            ) AS pid_neg_n_hours_since_last_click_rel
    FROM features_with_ratios
)
SELECT
    calculated_at,
    account_id,
    product_id,
    {selected_features}
FROM features_with_relative_recency
"""


def build_account_product_features_merge_query(
    target_table: str,
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    columns = feature_columns(settings)
    update_assignments = ",\n    ".join(
        f"target.{column} = source.{column}" for column in columns
    )
    insert_columns = ",\n    ".join(
        ("calculated_at", "account_id", "product_id", *columns)
    )
    insert_values = ",\n    ".join(
        f"source.{column}"
        for column in ("calculated_at", "account_id", "product_id", *columns)
    )

    return f"""
MERGE INTO {target_table} AS target
USING account_product_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    {update_assignments}
WHEN NOT MATCHED THEN INSERT (
    {insert_columns}
) VALUES (
    {insert_values}
)
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
