from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo


class SourceSettings(Protocol):
    product_metadata_table: str
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


def _local_day_start_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    local_value = value.astimezone(ZoneInfo(timezone_name))
    return local_value.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def feature_columns(settings: SourceSettings) -> tuple[str, ...]:
    columns = [
        f"bid_n_clicks_{window}d" for window in settings.event_windows_days
    ]
    columns.extend(f"bid_gmv_{window}d" for window in settings.order_windows_days)
    columns.extend(
        f"bid_gmv_{window}d_ratio" for window in settings.order_windows_days
    )
    return tuple(columns)


def _click_count_expressions(
    settings: SourceSettings,
    calculated_at_local: str,
) -> str:
    return ",\n        ".join(
        "CAST(SUM(CASE "
        f"WHEN last_received_at >= TIMESTAMP '{calculated_at_local}' "
        f"- INTERVAL {window} DAYS "
        "THEN 1 ELSE 0 END) AS INT) "
        f"AS bid_n_clicks_{window}d"
        for window in settings.event_windows_days
    )


def _gmv_expressions(
    settings: SourceSettings,
    calculated_at_utc: str,
    prefix: str,
) -> str:
    return ",\n        ".join(
        "CAST(SUM(CASE "
        f"WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' "
        f"- INTERVAL {window} DAYS "
        "THEN line_gmv ELSE 0.0 END) AS DOUBLE) "
        f"AS {prefix}_{window}d"
        for window in settings.order_windows_days
    )


def _base_feature_expressions(settings: SourceSettings) -> str:
    expressions = [
        f"COALESCE(clicks.bid_n_clicks_{window}d, 0) AS bid_n_clicks_{window}d"
        for window in settings.event_windows_days
    ]
    expressions.extend(
        f"COALESCE(brand_gmv.bid_gmv_{window}d, 0.0D) AS bid_gmv_{window}d"
        for window in settings.order_windows_days
    )
    expressions.extend(
        "CASE "
        f"WHEN account_gmv.account_gmv_{window}d > 0 "
        f"THEN COALESCE(brand_gmv.bid_gmv_{window}d, 0.0D) "
        f"/ account_gmv.account_gmv_{window}d END "
        f"AS bid_gmv_{window}d_ratio"
        for window in settings.order_windows_days
    )
    return ",\n        ".join(expressions)


def build_account_brand_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_utc = _utc_timestamp_literal(calculated_at)
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    metadata_dt_local = _local_day_start_literal(
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
WITH product_brands AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        CAST(brand_id AS INT) AS brand_id
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_dt_local}'
),
deduplicated_clicks AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        session_id,
        CAST(product_id AS INT) AS product_id,
        MAX(last_received_at) AS last_received_at
    FROM {settings.action_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}'
            - INTERVAL {max_event_window} DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND last_received_at >= TIMESTAMP '{calculated_at_local}'
            - INTERVAL {max_event_window} DAYS
        AND last_received_at < TIMESTAMP '{calculated_at_local}'
        AND event_type = 'PRODUCT_VIEW'
    GROUP BY
        account_id,
        session_id,
        product_id
),
click_features AS (
    SELECT
        click.account_id,
        product.brand_id,
        {_click_count_expressions(settings, calculated_at_local)}
    FROM deduplicated_clicks click
    INNER JOIN product_brands product
        ON click.product_id = product.product_id
    WHERE product.brand_id IS NOT NULL
    GROUP BY click.account_id, product.brand_id
),
sku_mapping AS (
    SELECT
        CAST(id AS INT) AS sku_id,
        CAST(MIN(product_id) AS INT) AS product_id
    FROM {settings.sku_table}
    GROUP BY id
),
filtered_order_lines AS (
    SELECT
        CAST(order_item.account_id AS INT) AS account_id,
        sku.product_id,
        CAST(order_item.generated_at AS TIMESTAMP) AS generated_at,
        CAST(order_item.payment_price AS DOUBLE)
            * CAST(order_item.item_quantity AS DOUBLE) AS line_gmv
    FROM {settings.order_items_table} order_item
    INNER JOIN sku_mapping sku
        ON CAST(order_item.sku_id AS INT) = sku.sku_id
    WHERE order_item.generated_at >= TIMESTAMP '{calculated_at_utc}'
            - INTERVAL {max_order_window} DAYS
        AND order_item.generated_at < TIMESTAMP '{calculated_at_utc}'
        AND order_item.order_item_status IN ({statuses_sql})
        AND order_item.b2b_order = FALSE
),
mapped_order_lines AS (
    SELECT
        order_line.account_id,
        product.brand_id,
        order_line.generated_at,
        order_line.line_gmv
    FROM filtered_order_lines order_line
    LEFT JOIN product_brands product
        ON order_line.product_id = product.product_id
),
brand_gmv_features AS (
    SELECT
        account_id,
        brand_id,
        {_gmv_expressions(settings, calculated_at_utc, 'bid_gmv')}
    FROM mapped_order_lines
    WHERE brand_id IS NOT NULL
    GROUP BY account_id, brand_id
),
account_gmv_features AS (
    SELECT
        account_id,
        {_gmv_expressions(settings, calculated_at_utc, 'account_gmv')}
    FROM mapped_order_lines
    GROUP BY account_id
),
entity_keys AS (
    SELECT account_id, brand_id FROM click_features
    UNION
    SELECT account_id, brand_id FROM brand_gmv_features
),
features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        entity.account_id,
        entity.brand_id,
        {_base_feature_expressions(settings)}
    FROM entity_keys entity
    LEFT JOIN click_features clicks
        ON entity.account_id = clicks.account_id
        AND entity.brand_id = clicks.brand_id
    LEFT JOIN brand_gmv_features brand_gmv
        ON entity.account_id = brand_gmv.account_id
        AND entity.brand_id = brand_gmv.brand_id
    LEFT JOIN account_gmv_features account_gmv
        ON entity.account_id = account_gmv.account_id
)
SELECT
    calculated_at,
    account_id,
    brand_id,
    {selected_features}
FROM features
"""


def build_account_brand_features_merge_query(
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
        ("calculated_at", "account_id", "brand_id", *columns)
    )
    insert_values = ",\n    ".join(
        f"source.{column}"
        for column in ("calculated_at", "account_id", "brand_id", *columns)
    )

    return f"""
MERGE INTO {target_table} AS target
USING account_brand_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.brand_id = source.brand_id
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
