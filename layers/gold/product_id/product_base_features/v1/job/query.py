from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

FEATURE_NAMESPACE = "PRODUCT"

PRICE_COLUMNS = (
    "min_sell_price_eod",
    "avg_sell_price_eod",
    "max_sell_price_eod",
    "weighted_price",
    "min_full_price_eod",
    "max_full_price_eod",
    "min_active_sku_sell_price_eod",
    "avg_active_sku_sell_price_eod",
    "max_active_sku_sell_price_eod",
    "minimal_sell_price",
    "minimal_full_price",
    "discount",
    "age_in_days",
)
ACTION_COLUMNS = (
    "clicks_3d",
    "clicks_28d",
    "favorites_1d",
    "favorites_last_3d",
    "favorites_last_7d",
    "favorites_last_14d",
    "favorites_last_21d",
    "favorites_last_28d",
)
ORDER_COLUMNS = (
    "orders_quantity_1d",
    "items_purchased_quantity_1d",
    "orders_total",
    "has_orders",
    "orders_7d",
    "orders_28d",
    "orders_90d",
    "category_orders_7d",
    "category_orders_28d",
    "category_orders_90d",
    "orders_share_in_category_7d",
    "orders_share_in_category_28d",
    "orders_share_in_category_90d",
)
ORDER_INPUT_COLUMNS = ORDER_COLUMNS[:-3]
DERIVED_COLUMNS = ("favorites_to_orders_rate",)
FEEDBACK_WINDOWS = (3, 7, 14, 21, 28)
ROLLING_FEEDBACK_COLUMNS = (
    "feedback_quantity_1d",
    "sum_rating_1d",
    *tuple(
        f"{family}_{window}d"
        for window in FEEDBACK_WINDOWS
        for family in (
            "feedback_last",
            "sum_rating_last",
            "feedback_gte_4",
            "feedback_lte_3",
            "feedback_gte_4_ratio",
            "feedback_lte_3_ratio",
            "feedback_avg_rating",
        )
    ),
)
ALL_TIME_FEEDBACK_COLUMNS = (
    "rating",
    "feedback_quantity",
    "feedback_gte_4",
    "feedback_lte_3",
    "feedback_gte_4_ratio",
    "feedback_lte_3_ratio",
    "log_feedback_quantity",
    "feedback_to_orders_rate_raw",
    "feedback_gte_4_to_orders_rate_raw",
    "feedback_lte_3_to_orders_rate_raw",
    "feedback_lte_3_to_orders_rate_28d",
)
RETURN_WINDOWS = (3, 28)
RETURN_COLUMNS = tuple(
    f"{family}_{window}d"
    for window in RETURN_WINDOWS
    for family in ("n_completed", "n_returned", "return_rate_neg")
)
GENDER_COLUMNS = (
    "n_unique_clickers_category_28d",
    "n_unique_clickers_with_age_category_28d",
    "known_age_clicker_share_category_28d",
    "clicker_age_p10_category_28d",
    "clicker_age_p50_category_28d",
    "clicker_age_p90_category_28d",
    "female_unique_clicker_share_category_28d",
    "male_unique_clicker_share_category_28d",
    "gender_balance_category_28d",
    "candidate_female_click_share_28d",
    "candidate_male_click_share_28d",
    "is_female_category",
    "is_male_category",
)
BASE_FEATURE_COLUMNS = (
    *PRICE_COLUMNS,
    *ACTION_COLUMNS,
    *ORDER_COLUMNS,
    *DERIVED_COLUMNS,
    *ROLLING_FEEDBACK_COLUMNS,
    *ALL_TIME_FEEDBACK_COLUMNS,
    *RETURN_COLUMNS,
    *GENDER_COLUMNS,
)
FEATURE_COLUMNS = tuple(
    f"{FEATURE_NAMESPACE}__{column}" for column in BASE_FEATURE_COLUMNS
)


class SourceSettings(Protocol):
    product_metadata_table: str
    product_prices_table: str
    sku_cm2_inputs_table: str
    action_counts_table: str
    feedback_counts_table: str
    order_items_table: str
    sku_table: str
    product_feedback_base_stats_table: str
    category_demographic_features_table: str
    business_timezone: str


def _timestamp_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d %H:%M:%S")


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _utc_date_literal(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _namespaced_feature_select(source_alias: str) -> str:
    return ",\n    ".join(
        f"{source_alias}.{column} AS {FEATURE_NAMESPACE}__{column}"
        for column in BASE_FEATURE_COLUMNS
    )


def _action_aggregate_expressions(calculated_at_local: str) -> str:
    expressions = []
    for window in (3, 28):
        expressions.append(
            "CAST(SUM(CASE WHEN event_type = 'PRODUCT_VIEW' "
            f"AND calculated_at > TIMESTAMP '{calculated_at_local}' "
            f"- INTERVAL {window} DAYS THEN n_events ELSE 0 END) AS INT) "
            f"AS clicks_{window}d"
        )
    expressions.append(
        "CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' "
        f"AND calculated_at > TIMESTAMP '{calculated_at_local}' "
        "- INTERVAL 1 DAY THEN n_events ELSE 0 END) AS favorites_1d"
    )
    for window in (3, 7, 14, 21, 28):
        expressions.append(
            "CAST(SUM(CASE WHEN event_type = 'ADD_TO_FAVORITES' "
            f"AND calculated_at > TIMESTAMP '{calculated_at_local}' "
            f"- INTERVAL {window} DAYS THEN n_events ELSE 0 END) AS INT) "
            f"AS favorites_last_{window}d"
        )
    return ",\n        ".join(expressions)


def _order_aggregate_expressions(calculated_at_utc: str) -> str:
    successful_status = (
        "order_item_status IN ('COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY')"
    )
    expressions = [
        (
            "CAST(COUNT(DISTINCT CASE WHEN "
            f"{successful_status} AND generated_at >= TIMESTAMP "
            f"'{calculated_at_utc}' - INTERVAL 1 DAY THEN order_id END) AS INT) "
            "AS orders_quantity_1d"
        ),
        (
            "CAST(SUM(CASE WHEN "
            f"{successful_status} AND generated_at >= TIMESTAMP "
            f"'{calculated_at_utc}' - INTERVAL 1 DAY THEN item_quantity "
            "ELSE 0 END) AS INT) AS items_purchased_quantity_1d"
        ),
        (
            "CAST(COUNT(DISTINCT CASE WHEN "
            f"{successful_status} THEN order_id END) AS INT) "
            "AS orders_total"
        ),
    ]
    for window in (7, 28, 90):
        expressions.append(
            "CAST(COUNT(DISTINCT CASE WHEN "
            f"{successful_status} AND generated_at >= TIMESTAMP "
            f"'{calculated_at_utc}' - INTERVAL {window} DAYS THEN order_id END) "
            f"AS INT) AS orders_{window}d"
        )
    return ",\n        ".join(expressions)


def _feedback_count_expressions(calculated_at_local: str) -> str:
    def bucket_sum(window: int, buckets: tuple[tuple[int, int], ...]) -> str:
        terms = [f"{weight} * n_feedbacks_{rating}" for rating, weight in buckets]
        return (
            "SUM(CASE WHEN calculated_at > TIMESTAMP "
            f"'{calculated_at_local}' - INTERVAL {window} DAYS THEN "
            f"{' + '.join(terms)} ELSE 0 END)"
        )

    all_buckets = tuple((rating, 1) for rating in range(1, 6))
    rating_buckets = tuple((rating, rating) for rating in range(1, 6))
    low_buckets = tuple((rating, 1) for rating in range(1, 4))
    high_buckets = ((4, 1), (5, 1))

    expressions = [
        f"CAST({bucket_sum(1, all_buckets)} AS INT) AS feedback_quantity_1d",
        f"CAST({bucket_sum(1, rating_buckets)} AS INT) AS sum_rating_1d",
    ]
    for window in FEEDBACK_WINDOWS:
        expressions.extend(
            (
                (
                    f"CAST({bucket_sum(window, all_buckets)} AS INT) "
                    f"AS feedback_last_{window}d"
                ),
                (
                    f"CAST({bucket_sum(window, rating_buckets)} AS INT) "
                    f"AS sum_rating_last_{window}d"
                ),
                (
                    f"CAST({bucket_sum(window, high_buckets)} AS INT) "
                    f"AS feedback_gte_4_{window}d"
                ),
                (
                    f"CAST({bucket_sum(window, low_buckets)} AS INT) "
                    f"AS feedback_lte_3_{window}d"
                ),
            )
        )
    return ",\n        ".join(expressions)


def _rolling_feedback_rate_expressions() -> str:
    expressions = []
    for window in FEEDBACK_WINDOWS:
        denominator = f"NULLIF(CAST(feedback_last_{window}d AS DOUBLE), 0.0D)"
        expressions.extend(
            (
                (
                    f"CAST(feedback_gte_4_{window}d AS DOUBLE) / {denominator} "
                    f"AS feedback_gte_4_ratio_{window}d"
                ),
                (
                    f"CAST(feedback_lte_3_{window}d AS DOUBLE) / {denominator} "
                    f"AS feedback_lte_3_ratio_{window}d"
                ),
                (
                    f"CAST(sum_rating_last_{window}d AS DOUBLE) / {denominator} "
                    f"AS feedback_avg_rating_{window}d"
                ),
            )
        )
    return ",\n        ".join(expressions)


def _return_count_expressions(calculated_at_utc: str) -> str:
    successful_status = (
        "order_item_status IN ('COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY')"
    )
    expressions = []
    for window in RETURN_WINDOWS:
        condition = (
            f"generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL {window} DAYS"
        )
        expressions.extend(
            (
                (
                    "CAST(SUM(CASE WHEN "
                    f"{condition} AND {successful_status} THEN 1 ELSE 0 END) "
                    f"AS INT) AS n_completed_{window}d"
                ),
                (
                    "CAST(SUM(CASE WHEN "
                    f"{condition} AND order_item_status = 'RETURNED' THEN 1 ELSE 0 END) AS INT) "
                    f"AS n_returned_{window}d"
                ),
            )
        )
    return ",\n        ".join(expressions)


def build_product_base_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    calculated_at_utc = _utc_timestamp_literal(calculated_at)
    feedback_base_date = _utc_date_literal(calculated_at)
    action_aggregates = _action_aggregate_expressions(calculated_at_local)
    order_aggregates = _order_aggregate_expressions(calculated_at_utc)
    feedback_counts = _feedback_count_expressions(calculated_at_local)
    feedback_rates = _rolling_feedback_rate_expressions()
    return_counts = _return_count_expressions(calculated_at_utc)
    order_input_select = ",\n        ".join(
        f"order_returns.{column}" for column in ORDER_INPUT_COLUMNS
    )
    rolling_feedback_select = ",\n        ".join(
        f"rolling.{column}" for column in ROLLING_FEEDBACK_COLUMNS
    )
    return_select = ",\n        ".join(
        f"order_returns.{column}" for column in RETURN_COLUMNS
    )
    return_population_select = ",\n        ".join(
        expression
        for window in RETURN_WINDOWS
        for expression in (
            f"COALESCE(orders.n_completed_{window}d, 0) AS n_completed_{window}d",
            f"COALESCE(orders.n_returned_{window}d, 0) AS n_returned_{window}d",
        )
    )
    return_feature_select = ",\n        ".join(
        expression
        for window in RETURN_WINDOWS
        for expression in (
            f"n_completed_{window}d",
            f"n_returned_{window}d",
            (
                f"-CAST(n_returned_{window}d AS DOUBLE) / NULLIF("
                f"CAST(n_completed_{window}d + n_returned_{window}d AS DOUBLE), "
                f"0.0D) AS return_rate_neg_{window}d"
            ),
        )
    )
    rolling_feedback_output_select = ",\n        ".join(
        expression
        for window in FEEDBACK_WINDOWS
        for expression in (
            f"COALESCE(feedback_last_{window}d, 0) AS feedback_last_{window}d",
            f"COALESCE(sum_rating_last_{window}d, 0) AS sum_rating_last_{window}d",
            f"COALESCE(feedback_gte_4_{window}d, 0) AS feedback_gte_4_{window}d",
            f"COALESCE(feedback_lte_3_{window}d, 0) AS feedback_lte_3_{window}d",
            f"feedback_gte_4_ratio_{window}d",
            f"feedback_lte_3_ratio_{window}d",
            f"feedback_avg_rating_{window}d",
        )
    )
    return_output_select = ",\n        ".join(
        expression
        for window in RETURN_WINDOWS
        for expression in (
            f"COALESCE(n_completed_{window}d, 0) AS n_completed_{window}d",
            f"COALESCE(n_returned_{window}d, 0) AS n_returned_{window}d",
            f"return_rate_neg_{window}d",
        )
    )
    namespaced_feature_select = _namespaced_feature_select("unprefixed_features")

    return f"""
WITH latest_metadata_dt AS (
    SELECT MAX(dt) AS dt
    FROM {settings.product_metadata_table}
    WHERE dt <= TIMESTAMP '{calculated_at_utc}'
),
product_population AS (
    SELECT
        CAST(metadata.product_id AS INT) AS product_id,
        CAST(metadata.category_id AS INT) AS category_id,
        CAST(metadata.created_at AS TIMESTAMP) AS created_at
    FROM {settings.product_metadata_table} metadata
    INNER JOIN latest_metadata_dt latest
        ON metadata.dt = latest.dt
),
latest_price_dt AS (
    SELECT MAX(dt) AS dt
    FROM {settings.product_prices_table}
    WHERE dt <= TIMESTAMP '{calculated_at_local}'
),
product_prices AS (
    SELECT
        CAST(prices.product_id AS INT) AS product_id,
        prices.min_sell_price_eod,
        prices.avg_sell_price_eod,
        prices.max_sell_price_eod,
        prices.min_full_price_eod,
        prices.max_full_price_eod,
        prices.min_active_sku_sell_price_eod,
        prices.avg_active_sku_sell_price_eod,
        prices.max_active_sku_sell_price_eod
    FROM {settings.product_prices_table} prices
    INNER JOIN latest_price_dt latest
        ON prices.dt = latest.dt
),
latest_cm2_inputs_dt AS (
    SELECT MAX(dt) AS dt
    FROM {settings.sku_cm2_inputs_table}
    WHERE dt <= TIMESTAMP '{calculated_at_local}'
),
product_weighted_prices AS (
    SELECT
        CAST(inputs.product_id AS INT) AS product_id,
        CASE
            WHEN SUM(inputs.n_orders_28d) < 5
                THEN AVG(inputs.sell_price_uzs)
            ELSE SUM(
                inputs.sell_price_uzs * CAST(inputs.n_orders_28d AS DOUBLE)
            ) / NULLIF(
                SUM(CAST(inputs.n_orders_28d AS DOUBLE)),
                0.0D
            )
        END AS weighted_price
    FROM {settings.sku_cm2_inputs_table} inputs
    INNER JOIN latest_cm2_inputs_dt latest
        ON inputs.dt = latest.dt
    WHERE inputs.sell_price_uzs IS NOT NULL
        AND inputs.commission_pct IS NOT NULL
    GROUP BY inputs.product_id
),
product_action_features AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        {action_aggregates}
    FROM {settings.action_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND event_type IN ('PRODUCT_VIEW', 'ADD_TO_FAVORITES')
    GROUP BY product_id
),
sku_mapping AS (
    SELECT
        CAST(id AS INT) AS sku_id,
        CAST(MIN(product_id) AS INT) AS product_id
    FROM {settings.sku_table}
    GROUP BY id
),
mapped_order_lines AS (
    SELECT
        order_item.order_id,
        sku.product_id,
        CAST(order_item.generated_at AS TIMESTAMP) AS generated_at,
        CAST(COALESCE(order_item.item_quantity, 0) AS INT) AS item_quantity,
        CAST(COALESCE(order_item.returned_quantity, 0) AS INT) AS returned_quantity,
        order_item.order_item_status
    FROM {settings.order_items_table} order_item
    INNER JOIN sku_mapping sku
        ON CAST(order_item.sku_id AS INT) = sku.sku_id
    WHERE order_item.generated_at < TIMESTAMP '{calculated_at_utc}'
        AND order_item.b2b_order = FALSE
        AND order_item.order_item_status IN (
            'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY', 'RETURNED'
        )
),
product_order_and_return_counts AS (
    SELECT
        product_id,
        {order_aggregates},
        {return_counts}
    FROM mapped_order_lines
    GROUP BY product_id
),
orders_for_population AS (
    SELECT
        population.product_id,
        population.category_id,
        COALESCE(orders.orders_quantity_1d, 0) AS orders_quantity_1d,
        COALESCE(orders.items_purchased_quantity_1d, 0)
            AS items_purchased_quantity_1d,
        COALESCE(orders.orders_total, 0) AS orders_total,
        COALESCE(orders.orders_7d, 0) AS orders_7d,
        COALESCE(orders.orders_28d, 0) AS orders_28d,
        COALESCE(orders.orders_90d, 0) AS orders_90d,
        {return_population_select}
    FROM product_population population
    LEFT JOIN product_order_and_return_counts orders
        ON population.product_id = orders.product_id
),
order_and_return_features AS (
    SELECT
        product_id,
        orders_quantity_1d,
        items_purchased_quantity_1d,
        orders_total,
        CAST(orders_total > 0 AS INT) AS has_orders,
        orders_7d,
        orders_28d,
        orders_90d,
        CASE WHEN category_id IS NOT NULL THEN CAST(
            SUM(orders_7d) OVER (PARTITION BY category_id) AS INT
        ) END AS category_orders_7d,
        CASE WHEN category_id IS NOT NULL THEN CAST(
            SUM(orders_28d) OVER (PARTITION BY category_id) AS INT
        ) END AS category_orders_28d,
        CASE WHEN category_id IS NOT NULL THEN CAST(
            SUM(orders_90d) OVER (PARTITION BY category_id) AS INT
        ) END AS category_orders_90d,
        {return_feature_select}
    FROM orders_for_population
),
rolling_feedback_counts AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        {feedback_counts}
    FROM {settings.feedback_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}' - INTERVAL 28 DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
    GROUP BY product_id
),
rolling_feedback_features AS (
    SELECT
        counts.*,
        {feedback_rates}
    FROM rolling_feedback_counts counts
),
latest_feedback_base_date AS (
    SELECT MAX(date) AS date
    FROM {settings.product_feedback_base_stats_table}
    WHERE date <= DATE '{feedback_base_date}'
),
all_time_feedback_counts AS (
    SELECT
        CAST(feedback.product_id AS INT) AS product_id,
        CAST(
            COALESCE(feedback.reviews_mark_one_count, 0)
            + COALESCE(feedback.reviews_mark_two_count, 0)
            + COALESCE(feedback.reviews_mark_three_count, 0)
            + COALESCE(feedback.reviews_mark_four_count, 0)
            + COALESCE(feedback.reviews_mark_five_count, 0)
            AS INT
        ) AS feedback_quantity,
        CAST(
            COALESCE(feedback.reviews_mark_four_count, 0)
            + COALESCE(feedback.reviews_mark_five_count, 0)
            AS INT
        ) AS feedback_gte_4,
        CAST(
            COALESCE(feedback.reviews_mark_one_count, 0)
            + COALESCE(feedback.reviews_mark_two_count, 0)
            + COALESCE(feedback.reviews_mark_three_count, 0)
            AS INT
        ) AS feedback_lte_3,
        CAST(
            COALESCE(feedback.reviews_mark_one_count, 0)
            + 2 * COALESCE(feedback.reviews_mark_two_count, 0)
            + 3 * COALESCE(feedback.reviews_mark_three_count, 0)
            + 4 * COALESCE(feedback.reviews_mark_four_count, 0)
            + 5 * COALESCE(feedback.reviews_mark_five_count, 0)
            AS DOUBLE
        ) AS sum_rating
    FROM {settings.product_feedback_base_stats_table} feedback
    INNER JOIN latest_feedback_base_date latest
        ON feedback.date = latest.date
),
all_time_feedback_features AS (
    SELECT
        product_id,
        sum_rating / NULLIF(
            CAST(feedback_quantity AS DOUBLE),
            0.0D
        ) AS rating,
        feedback_quantity,
        feedback_gte_4,
        feedback_lte_3,
        CAST(feedback_gte_4 AS DOUBLE) / NULLIF(
            CAST(feedback_quantity AS DOUBLE),
            0.0D
        ) AS feedback_gte_4_ratio,
        CAST(feedback_lte_3 AS DOUBLE) / NULLIF(
            CAST(feedback_quantity AS DOUBLE),
            0.0D
        ) AS feedback_lte_3_ratio,
        LN(1.0D + CAST(feedback_quantity AS DOUBLE))
            AS log_feedback_quantity
    FROM all_time_feedback_counts
),
category_demographic_features AS (
    SELECT
        CAST(category_id AS INT) AS category_id,
        CATEGORY_DEMOGRAPHICS__n_unique_clickers_28d
            AS n_unique_clickers_category_28d,
        CATEGORY_DEMOGRAPHICS__n_unique_clickers_with_age_28d
            AS n_unique_clickers_with_age_category_28d,
        CATEGORY_DEMOGRAPHICS__known_age_clicker_share_28d
            AS known_age_clicker_share_category_28d,
        CATEGORY_DEMOGRAPHICS__clicker_age_p10_28d
            AS clicker_age_p10_category_28d,
        CATEGORY_DEMOGRAPHICS__clicker_age_p50_28d
            AS clicker_age_p50_category_28d,
        CATEGORY_DEMOGRAPHICS__clicker_age_p90_28d
            AS clicker_age_p90_category_28d,
        CATEGORY_DEMOGRAPHICS__female_unique_clicker_share_28d
            AS female_unique_clicker_share_category_28d,
        CATEGORY_DEMOGRAPHICS__male_unique_clicker_share_28d
            AS male_unique_clicker_share_category_28d,
        CATEGORY_DEMOGRAPHICS__gender_balance_28d
            AS gender_balance_category_28d,
        CATEGORY_DEMOGRAPHICS__female_product_session_share_28d
            AS category_female_product_session_share_28d,
        CATEGORY_DEMOGRAPHICS__male_product_session_share_28d
            AS category_male_product_session_share_28d,
        CATEGORY_DEMOGRAPHICS__gender AS category_gender
    FROM {settings.category_demographic_features_table}
    WHERE calculated_at = TIMESTAMP '{calculated_at_local}'
),
feature_inputs AS (
    SELECT
        population.product_id,
        population.created_at,
        prices.min_sell_price_eod,
        prices.avg_sell_price_eod,
        prices.max_sell_price_eod,
        weighted.weighted_price,
        prices.min_full_price_eod,
        prices.max_full_price_eod,
        prices.min_active_sku_sell_price_eod,
        prices.avg_active_sku_sell_price_eod,
        prices.max_active_sku_sell_price_eod,
        COALESCE(actions.clicks_3d, 0) AS clicks_3d,
        COALESCE(actions.clicks_28d, 0) AS clicks_28d,
        COALESCE(actions.favorites_1d, 0) AS favorites_1d,
        COALESCE(actions.favorites_last_3d, 0) AS favorites_last_3d,
        COALESCE(actions.favorites_last_7d, 0) AS favorites_last_7d,
        COALESCE(actions.favorites_last_14d, 0) AS favorites_last_14d,
        COALESCE(actions.favorites_last_21d, 0) AS favorites_last_21d,
        COALESCE(actions.favorites_last_28d, 0) AS favorites_last_28d,
        {order_input_select},
        {rolling_feedback_select},
        COALESCE(all_time.feedback_quantity, 0)
            AS feedback_quantity,
        COALESCE(all_time.feedback_gte_4, 0) AS feedback_gte_4,
        COALESCE(all_time.feedback_lte_3, 0) AS feedback_lte_3,
        all_time.rating,
        all_time.feedback_gte_4_ratio,
        all_time.feedback_lte_3_ratio,
        COALESCE(all_time.log_feedback_quantity, 0.0D)
            AS log_feedback_quantity,
        {return_select},
        gender.n_unique_clickers_category_28d,
        gender.n_unique_clickers_with_age_category_28d,
        gender.known_age_clicker_share_category_28d,
        gender.clicker_age_p10_category_28d,
        gender.clicker_age_p50_category_28d,
        gender.clicker_age_p90_category_28d,
        gender.female_unique_clicker_share_category_28d,
        gender.male_unique_clicker_share_category_28d,
        gender.gender_balance_category_28d,
        gender.category_female_product_session_share_28d,
        gender.category_male_product_session_share_28d,
        gender.category_gender
    FROM product_population population
    LEFT JOIN product_prices prices
        ON population.product_id = prices.product_id
    LEFT JOIN product_weighted_prices weighted
        ON population.product_id = weighted.product_id
    LEFT JOIN product_action_features actions
        ON population.product_id = actions.product_id
    LEFT JOIN order_and_return_features order_returns
        ON population.product_id = order_returns.product_id
    LEFT JOIN rolling_feedback_features rolling
        ON population.product_id = rolling.product_id
    LEFT JOIN all_time_feedback_features all_time
        ON population.product_id = all_time.product_id
    LEFT JOIN category_demographic_features gender
        ON population.category_id = gender.category_id
),
unprefixed_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        product_id,
        min_sell_price_eod,
        avg_sell_price_eod,
        max_sell_price_eod,
        weighted_price,
        min_full_price_eod,
        max_full_price_eod,
        min_active_sku_sell_price_eod,
        avg_active_sku_sell_price_eod,
        max_active_sku_sell_price_eod,
        min_sell_price_eod AS minimal_sell_price,
        min_full_price_eod AS minimal_full_price,
        CASE
            WHEN min_sell_price_eod IS NULL
                THEN NULL
            WHEN min_full_price_eod IS NULL OR min_full_price_eod <= 0.0D
                THEN 0.0D
            ELSE LEAST(
                100.0D,
                GREATEST(
                    0.0D,
                    100.0D * (1.0D - min_sell_price_eod / min_full_price_eod)
                )
            )
        END AS discount,
        CASE
            WHEN created_at IS NULL
              OR created_at > TIMESTAMP '{calculated_at_utc}'
                THEN NULL
            ELSE CAST(
                DATEDIFF(
                    TO_DATE(TIMESTAMP '{calculated_at_local}'),
                    TO_DATE(FROM_UTC_TIMESTAMP(created_at, '{settings.business_timezone}'))
                ) AS INT
            )
        END AS age_in_days,
        clicks_3d,
        clicks_28d,
        favorites_1d,
        favorites_last_3d,
        favorites_last_7d,
        favorites_last_14d,
        favorites_last_21d,
        favorites_last_28d,
        orders_quantity_1d,
        items_purchased_quantity_1d,
        orders_total,
        has_orders,
        orders_7d,
        orders_28d,
        orders_90d,
        CAST(favorites_last_28d AS DOUBLE)
            / NULLIF(CAST(orders_28d AS DOUBLE), 0.0D)
            AS favorites_to_orders_rate,
        category_orders_7d,
        category_orders_28d,
        category_orders_90d,
        CAST(orders_7d AS DOUBLE)
            / NULLIF(CAST(category_orders_7d AS DOUBLE), 0.0D)
            AS orders_share_in_category_7d,
        CAST(orders_28d AS DOUBLE)
            / NULLIF(CAST(category_orders_28d AS DOUBLE), 0.0D)
            AS orders_share_in_category_28d,
        CAST(orders_90d AS DOUBLE)
            / NULLIF(CAST(category_orders_90d AS DOUBLE), 0.0D)
            AS orders_share_in_category_90d,
        COALESCE(feedback_quantity_1d, 0) AS feedback_quantity_1d,
        COALESCE(sum_rating_1d, 0) AS sum_rating_1d,
        {rolling_feedback_output_select},
        rating,
        feedback_quantity,
        feedback_gte_4,
        feedback_lte_3,
        feedback_gte_4_ratio,
        feedback_lte_3_ratio,
        log_feedback_quantity,
        CAST(feedback_quantity AS DOUBLE)
            / NULLIF(CAST(orders_total AS DOUBLE), 0.0D)
            AS feedback_to_orders_rate_raw,
        CAST(feedback_gte_4 AS DOUBLE)
            / NULLIF(CAST(orders_total AS DOUBLE), 0.0D)
            AS feedback_gte_4_to_orders_rate_raw,
        CAST(feedback_lte_3 AS DOUBLE)
            / NULLIF(CAST(orders_total AS DOUBLE), 0.0D)
            AS feedback_lte_3_to_orders_rate_raw,
        CAST(feedback_lte_3_28d AS DOUBLE)
            / NULLIF(CAST(orders_28d AS DOUBLE), 0.0D)
            AS feedback_lte_3_to_orders_rate_28d,
        {return_output_select},
        n_unique_clickers_category_28d,
        n_unique_clickers_with_age_category_28d,
        known_age_clicker_share_category_28d,
        clicker_age_p10_category_28d,
        clicker_age_p50_category_28d,
        clicker_age_p90_category_28d,
        female_unique_clicker_share_category_28d,
        male_unique_clicker_share_category_28d,
        gender_balance_category_28d,
        category_female_product_session_share_28d
            AS candidate_female_click_share_28d,
        category_male_product_session_share_28d
            AS candidate_male_click_share_28d,
        CAST(category_gender = 'F' AS INT) AS is_female_category,
        CAST(category_gender = 'M' AS INT) AS is_male_category
    FROM feature_inputs
)
SELECT
    calculated_at,
    product_id,
    {namespaced_feature_select}
FROM unprefixed_features
"""


def build_product_base_features_merge_query(
    target_table: str,
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
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
USING product_base_features_for_calculated_at AS source
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
