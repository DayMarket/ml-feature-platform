from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

GMV_WINDOWS = (3, 7, 14, 28, 60, 90)


FEATURE_COLUMNS = (
    "n_clicks_3d",
    "n_clicks_7d",
    "n_clicks_14d",
    "n_clicks_28d",
    "gmv_3d",
    "gmv_7d",
    "gmv_14d",
    "gmv_28d",
    "gmv_60d",
    "gmv_90d",
    "gmv_3d_ratio",
    "gmv_7d_ratio",
    "gmv_14d_ratio",
    "gmv_28d_ratio",
    "gmv_60d_ratio",
    "gmv_90d_ratio",
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


def _local_day_start_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    local_value = value.astimezone(ZoneInfo(timezone_name))
    return local_value.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _gmv_ratio_expressions() -> str:
    return ",\n    ".join(
        f"CASE WHEN account_gmv.account_gmv_{window}d > 0 "
        f"THEN COALESCE(brand_gmv.gmv_{window}d, 0.0D) / "
        f"account_gmv.account_gmv_{window}d END AS gmv_{window}d_ratio"
        for window in GMV_WINDOWS
    )


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
    gmv_ratio_expressions = _gmv_ratio_expressions()

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
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
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
        CAST(SUM(CASE WHEN last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 3 DAYS THEN 1 ELSE 0 END) AS INT) AS n_clicks_3d,
        CAST(SUM(CASE WHEN last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 7 DAYS THEN 1 ELSE 0 END) AS INT) AS n_clicks_7d,
        CAST(SUM(CASE WHEN last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 14 DAYS THEN 1 ELSE 0 END) AS INT) AS n_clicks_14d,
        CAST(SUM(CASE WHEN last_received_at >= TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS THEN 1 ELSE 0 END) AS INT) AS n_clicks_28d
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
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_3d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_7d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_14d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_28d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 60 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_60d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS gmv_90d
    FROM mapped_order_lines
    WHERE brand_id IS NOT NULL
    GROUP BY account_id, brand_id
),
account_gmv_features AS (
    SELECT
        account_id,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 3 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS account_gmv_3d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 7 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS account_gmv_7d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 14 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS account_gmv_14d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 28 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS account_gmv_28d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 60 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS account_gmv_60d,
        CAST(SUM(CASE WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS THEN line_gmv ELSE 0.0 END) AS DOUBLE) AS account_gmv_90d
    FROM mapped_order_lines
    GROUP BY account_id
),
entity_keys AS (
    SELECT account_id, brand_id FROM click_features
    UNION
    SELECT account_id, brand_id FROM brand_gmv_features
)
SELECT
    TIMESTAMP '{calculated_at_local}' AS calculated_at,
    entity.account_id,
    entity.brand_id,
    COALESCE(clicks.n_clicks_3d, 0) AS n_clicks_3d,
    COALESCE(clicks.n_clicks_7d, 0) AS n_clicks_7d,
    COALESCE(clicks.n_clicks_14d, 0) AS n_clicks_14d,
    COALESCE(clicks.n_clicks_28d, 0) AS n_clicks_28d,
    COALESCE(brand_gmv.gmv_3d, 0.0D) AS gmv_3d,
    COALESCE(brand_gmv.gmv_7d, 0.0D) AS gmv_7d,
    COALESCE(brand_gmv.gmv_14d, 0.0D) AS gmv_14d,
    COALESCE(brand_gmv.gmv_28d, 0.0D) AS gmv_28d,
    COALESCE(brand_gmv.gmv_60d, 0.0D) AS gmv_60d,
    COALESCE(brand_gmv.gmv_90d, 0.0D) AS gmv_90d,
    {gmv_ratio_expressions}
FROM entity_keys entity
LEFT JOIN click_features clicks
    ON entity.account_id = clicks.account_id
    AND entity.brand_id = clicks.brand_id
LEFT JOIN brand_gmv_features brand_gmv
    ON entity.account_id = brand_gmv.account_id
    AND entity.brand_id = brand_gmv.brand_id
LEFT JOIN account_gmv_features account_gmv
    ON entity.account_id = account_gmv.account_id
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
    return f"""
MERGE INTO {target_table} AS target
USING account_brand_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.brand_id = source.brand_id
WHEN MATCHED THEN UPDATE SET
    target.n_clicks_3d = source.n_clicks_3d,
    target.n_clicks_7d = source.n_clicks_7d,
    target.n_clicks_14d = source.n_clicks_14d,
    target.n_clicks_28d = source.n_clicks_28d,
    target.gmv_3d = source.gmv_3d,
    target.gmv_7d = source.gmv_7d,
    target.gmv_14d = source.gmv_14d,
    target.gmv_28d = source.gmv_28d,
    target.gmv_60d = source.gmv_60d,
    target.gmv_90d = source.gmv_90d,
    target.gmv_3d_ratio = source.gmv_3d_ratio,
    target.gmv_7d_ratio = source.gmv_7d_ratio,
    target.gmv_14d_ratio = source.gmv_14d_ratio,
    target.gmv_28d_ratio = source.gmv_28d_ratio,
    target.gmv_60d_ratio = source.gmv_60d_ratio,
    target.gmv_90d_ratio = source.gmv_90d_ratio
WHEN NOT MATCHED THEN INSERT (
    calculated_at,
    account_id,
    brand_id,
    n_clicks_3d,
    n_clicks_7d,
    n_clicks_14d,
    n_clicks_28d,
    gmv_3d,
    gmv_7d,
    gmv_14d,
    gmv_28d,
    gmv_60d,
    gmv_90d,
    gmv_3d_ratio,
    gmv_7d_ratio,
    gmv_14d_ratio,
    gmv_28d_ratio,
    gmv_60d_ratio,
    gmv_90d_ratio
) VALUES (
    source.calculated_at,
    source.account_id,
    source.brand_id,
    source.n_clicks_3d,
    source.n_clicks_7d,
    source.n_clicks_14d,
    source.n_clicks_28d,
    source.gmv_3d,
    source.gmv_7d,
    source.gmv_14d,
    source.gmv_28d,
    source.gmv_60d,
    source.gmv_90d,
    source.gmv_3d_ratio,
    source.gmv_7d_ratio,
    source.gmv_14d_ratio,
    source.gmv_28d_ratio,
    source.gmv_60d_ratio,
    source.gmv_90d_ratio
)
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
