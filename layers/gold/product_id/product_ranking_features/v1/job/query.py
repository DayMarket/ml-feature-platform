from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

FEATURE_NAMESPACE = "PRODUCT"
SMOOTHING_ALPHA = 10.0
RETURN_WINDOWS = (28, 90)

RANK_AND_PERCENTILE_COLUMNS = (
    "price_percentile",
    "price_percentile_in_cat",
    "popularity_by_orders_neg_rank",
    "popularity_by_orders_neg_rank_in_cat",
    "popularity_by_clicks_neg_rank_3d",
    "popularity_by_clicks_neg_rank_28d",
    "popularity_by_clicks_neg_rank_in_cat_3d",
    "popularity_by_clicks_neg_rank_in_cat_28d",
    "rating_percentile_in_cat",
    "discount_percentile_in_cat",
    "feedback_quantity_percentile_in_cat",
    "feedback_lte_3_to_orders_rate_smoothed",
    "feedback_lte_3_to_orders_rate_percentile_in_cat",
)
RETURN_FEATURE_COLUMNS = tuple(
    f"{family}_{window}d"
    for window in RETURN_WINDOWS
    for family in (
        "category_return_rate_neg",
        "return_rate_neg_smoothed",
        "return_rate_to_category_return_neg",
        "return_rate_smoothed_to_category_return_neg",
    )
)
BASE_FEATURE_COLUMNS = (
    *RANK_AND_PERCENTILE_COLUMNS,
    *RETURN_FEATURE_COLUMNS,
)
FEATURE_COLUMNS = tuple(
    f"{FEATURE_NAMESPACE}__{column}" for column in BASE_FEATURE_COLUMNS
)


class SourceSettings(Protocol):
    product_metadata_table: str
    product_base_features_table: str
    business_timezone: str


def _local_timestamp_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M:%S")


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _window_clause(partition_columns: tuple[str, ...], order_by: str) -> str:
    partition_clause = ""
    if partition_columns:
        partition_clause = f"PARTITION BY {', '.join(partition_columns)} "
    return f"{partition_clause}ORDER BY {order_by}"


def _average_rank_expression(
    metric: str,
    alias: str,
    *,
    partition_columns: tuple[str, ...] = (),
    descending: bool = False,
    percentile: bool = False,
) -> str:
    direction = "DESC" if descending else "ASC"
    rank_window = _window_clause(
        partition_columns,
        f"{metric} {direction} NULLS LAST",
    )
    tie_partition = (*partition_columns, metric)
    tie_count = f"COUNT(*) OVER (PARTITION BY {', '.join(tie_partition)})"
    average_rank = (
        f"CAST(RANK() OVER ({rank_window}) AS DOUBLE) "
        f"+ (CAST({tie_count} AS DOUBLE) - 1.0D) / 2.0D"
    )
    conditions = [f"{metric} IS NOT NULL"]
    conditions.extend(f"{column} IS NOT NULL" for column in partition_columns)

    if percentile:
        count_partition = ""
        if partition_columns:
            count_partition = f"PARTITION BY {', '.join(partition_columns)}"
        value = (
            f"({average_rank}) / NULLIF("
            f"CAST(COUNT({metric}) OVER ({count_partition}) AS DOUBLE), 0.0D)"
        )
    else:
        value = f"-({average_rank})"

    return f"CASE WHEN {' AND '.join(conditions)} THEN {value} END AS {alias}"


def _rank_and_percentile_expressions() -> str:
    expressions = [
        _average_rank_expression(
            "min_sell_price_eod",
            "price_percentile",
            percentile=True,
        ),
        _average_rank_expression(
            "min_sell_price_eod",
            "price_percentile_in_cat",
            partition_columns=("category_id",),
            percentile=True,
        ),
        _average_rank_expression(
            "orders_28d",
            "popularity_by_orders_neg_rank",
            descending=True,
        ),
        _average_rank_expression(
            "orders_28d",
            "popularity_by_orders_neg_rank_in_cat",
            partition_columns=("category_id",),
            descending=True,
        ),
    ]
    for window in (3, 28):
        expressions.extend(
            (
                _average_rank_expression(
                    f"clicks_{window}d",
                    f"popularity_by_clicks_neg_rank_{window}d",
                    descending=True,
                ),
                _average_rank_expression(
                    f"clicks_{window}d",
                    f"popularity_by_clicks_neg_rank_in_cat_{window}d",
                    partition_columns=("category_id",),
                    descending=True,
                ),
            )
        )
    expressions.extend(
        (
            _average_rank_expression(
                "rating",
                "rating_percentile_in_cat",
                partition_columns=("category_id",),
                percentile=True,
            ),
            _average_rank_expression(
                "discount",
                "discount_percentile_in_cat",
                partition_columns=("category_id",),
                percentile=True,
            ),
            _average_rank_expression(
                "feedback_quantity",
                "feedback_quantity_percentile_in_cat",
                partition_columns=("category_id",),
                percentile=True,
            ),
            _average_rank_expression(
                "feedback_lte_3_to_orders_rate_smoothed",
                "feedback_lte_3_to_orders_rate_percentile_in_cat",
                partition_columns=("category_id",),
                percentile=True,
            ),
        )
    )
    return ",\n        ".join(expressions)


def _return_count_expressions(calculated_at_utc: str) -> str:
    return ",\n        ".join(
        expression
        for window in RETURN_WINDOWS
        for expression in (
            (
                "CAST(SUM(CASE WHEN order_item.generated_at >= "
                f"TIMESTAMP '{calculated_at_utc}' - INTERVAL {window} DAYS "
                "AND order_item.order_item_status IN "
                "('COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY') "
                "THEN 1 ELSE 0 END) AS INT) "
                f"AS n_completed_{window}d"
            ),
            (
                "CAST(SUM(CASE WHEN order_item.generated_at >= "
                f"TIMESTAMP '{calculated_at_utc}' - INTERVAL {window} DAYS "
                "AND order_item.order_item_status = 'RETURNED' "
                "THEN 1 ELSE 0 END) AS INT) "
                f"AS n_returned_{window}d"
            ),
        )
    )


def _category_return_rate_expressions() -> str:
    return ",\n        ".join(
        (
            f"CASE WHEN category_id IS NOT NULL THEN -"
            f"CAST(SUM(n_returned_{window}d) OVER (PARTITION BY category_id) "
            f"AS DOUBLE) / NULLIF(CAST(SUM(n_completed_{window}d + "
            f"n_returned_{window}d) OVER (PARTITION BY category_id) "
            f"AS DOUBLE), 0.0D) END AS category_return_rate_neg_{window}d"
        )
        for window in RETURN_WINDOWS
    )


def _return_input_passthrough() -> str:
    expressions = []
    for window in RETURN_WINDOWS:
        expressions.extend(
            (
                f"n_completed_{window}d",
                f"n_returned_{window}d",
                (
                    f"-CAST(n_returned_{window}d AS DOUBLE) / NULLIF("
                    f"CAST(n_completed_{window}d + n_returned_{window}d AS DOUBLE), "
                    f"0.0D) AS return_rate_neg_{window}d"
                ),
            )
        )
    return ",\n        ".join(expressions)


def _smoothed_return_rate_expressions() -> str:
    return ",\n        ".join(
        (
            f"-(CAST(n_returned_{window}d AS DOUBLE) "
            f"- {SMOOTHING_ALPHA}D * category_return_rate_neg_{window}d) "
            f"/ NULLIF(CAST(n_completed_{window}d + n_returned_{window}d "
            f"AS DOUBLE) + {SMOOTHING_ALPHA}D, 0.0D) "
            f"AS return_rate_neg_smoothed_{window}d"
        )
        for window in RETURN_WINDOWS
    )


def _return_feature_expressions() -> str:
    expressions: list[str] = []
    for window in RETURN_WINDOWS:
        category_rate = f"category_return_rate_neg_{window}d"
        smoothed_rate = f"return_rate_neg_smoothed_{window}d"
        expressions.extend(
            (
                category_rate,
                smoothed_rate,
                (
                    f"-return_rate_neg_{window}d / NULLIF({category_rate}, 0.0D) "
                    f"AS return_rate_to_category_return_neg_{window}d"
                ),
                (
                    f"-{smoothed_rate} / NULLIF({category_rate}, 0.0D) AS "
                    f"return_rate_smoothed_to_category_return_neg_{window}d"
                ),
            )
        )
    return ",\n        ".join(expressions)


def _namespaced_feature_select(source_alias: str) -> str:
    return ",\n    ".join(
        f"{source_alias}.{column} AS {FEATURE_NAMESPACE}__{column}"
        for column in BASE_FEATURE_COLUMNS
    )


def build_product_ranking_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    calculated_at_utc = _utc_timestamp_literal(calculated_at)
    return_count_expressions = _return_count_expressions(calculated_at_utc)
    category_return_rates = _category_return_rate_expressions()
    return_passthrough = _return_input_passthrough()
    smoothed_return_rates = _smoothed_return_rate_expressions()
    return_features = _return_feature_expressions()
    rank_and_percentiles = _rank_and_percentile_expressions()
    namespaced_features = _namespaced_feature_select("unprefixed_features")

    return f"""
WITH latest_metadata_dt AS (
    SELECT MAX(dt) AS dt
    FROM {settings.product_metadata_table}
    WHERE dt <= TIMESTAMP '{calculated_at_utc}'
),
product_category_mapping AS (
    SELECT
        CAST(metadata.product_id AS INT) AS product_id,
        CAST(metadata.category_id AS INT) AS category_id
    FROM {settings.product_metadata_table} metadata
    INNER JOIN latest_metadata_dt latest
        ON metadata.dt = latest.dt
),
sku_mapping AS (
    SELECT
        id AS sku_id,
        CAST(MIN(product_id) AS INT) AS product_id
    FROM {settings.sku_table}
    GROUP BY id
),
return_counts AS (
    SELECT
        sku.product_id,
        {return_count_expressions}
    FROM {settings.order_items_table} order_item
    INNER JOIN sku_mapping sku
        ON order_item.sku_id = sku.sku_id
    WHERE order_item.generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS
        AND order_item.generated_at < TIMESTAMP '{calculated_at_utc}'
        AND order_item.b2b_order = FALSE
    GROUP BY sku.product_id
),
g7_snapshot AS (
    SELECT
        base.calculated_at,
        CAST(base.product_id AS INT) AS product_id,
        mapping.category_id,
        base.PRODUCT__min_sell_price_eod AS min_sell_price_eod,
        base.PRODUCT__orders_28d AS orders_28d,
        base.PRODUCT__clicks_3d AS clicks_3d,
        base.PRODUCT__clicks_28d AS clicks_28d,
        base.PRODUCT__rating AS rating,
        base.PRODUCT__cheapest_sku_discount_pct AS discount,
        base.PRODUCT__feedback_quantity AS feedback_quantity,
        base.PRODUCT__feedback_lte_3 AS feedback_lte_3,
        base.PRODUCT__feedback_lte_3_28d AS feedback_lte_3_28d,
        COALESCE(returns.n_completed_28d, 0) AS n_completed_28d,
        COALESCE(returns.n_returned_28d, 0) AS n_returned_28d,
        COALESCE(returns.n_completed_90d, 0) AS n_completed_90d,
        COALESCE(returns.n_returned_90d, 0) AS n_returned_90d
    FROM {settings.product_base_features_table} base
    LEFT JOIN product_category_mapping mapping
        ON base.product_id = mapping.product_id
    LEFT JOIN return_counts returns
        ON base.product_id = returns.product_id
    WHERE base.calculated_at = TIMESTAMP '{calculated_at_local}'
),
global_feedback_prior AS (
    SELECT
        CAST(SUM(feedback_lte_3_28d) AS DOUBLE)
            / NULLIF(CAST(SUM(orders_28d) AS DOUBLE), 0.0D)
            AS global_feedback_lte_3_to_orders_rate
    FROM g7_snapshot
),
category_baselines AS (
    SELECT
        snapshot.*,
        {category_return_rates}
    FROM g7_snapshot snapshot
),
smoothed_inputs AS (
    SELECT
        category.calculated_at,
        category.product_id,
        category.category_id,
        category.min_sell_price_eod,
        category.orders_28d,
        category.clicks_3d,
        category.clicks_28d,
        category.rating,
        category.discount,
        category.feedback_quantity,
        category.feedback_lte_3,
        category.feedback_lte_3_28d,
        (CAST(category.feedback_lte_3_28d AS DOUBLE)
            + {SMOOTHING_ALPHA}D * prior.global_feedback_lte_3_to_orders_rate)
            / NULLIF(CAST(category.orders_28d AS DOUBLE)
                + {SMOOTHING_ALPHA}D, 0.0D)
            AS feedback_lte_3_to_orders_rate_smoothed,
        {return_passthrough},
        {", ".join(f"category_return_rate_neg_{window}d" for window in RETURN_WINDOWS)},
        {smoothed_return_rates}
    FROM category_baselines category
    CROSS JOIN global_feedback_prior prior
),
derived_rates AS (
    SELECT
        calculated_at,
        product_id,
        category_id,
        min_sell_price_eod,
        orders_28d,
        clicks_3d,
        clicks_28d,
        rating,
        discount,
        feedback_quantity,
        feedback_lte_3_to_orders_rate_smoothed,
        {return_features}
    FROM smoothed_inputs
),
unprefixed_features AS (
    SELECT
        calculated_at,
        product_id,
        {rank_and_percentiles},
        feedback_lte_3_to_orders_rate_smoothed,
        {", ".join(RETURN_FEATURE_COLUMNS)}
    FROM derived_rates
)
SELECT
    calculated_at,
    product_id,
    {namespaced_features}
FROM unprefixed_features
"""


def build_product_ranking_features_merge_query(
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
USING product_ranking_features_for_calculated_at AS source
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
