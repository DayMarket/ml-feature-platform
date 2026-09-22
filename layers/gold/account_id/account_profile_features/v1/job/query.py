from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

ORDER_WINDOWS = (7, 28, 90)
FEATURE_NAMESPACE = "ACCOUNT"

DEMOGRAPHIC_COLUMNS = (
    "gender",
    "gender_is_female",
    "age",
    "age_bucket",
    "city_name",
    "platform",
)

ORDER_FEATURE_TEMPLATES = (
    "median_order_total_{window}d",
    "max_order_total_{window}d",
    "min_order_total_{window}d",
    "sum_order_total_{window}d",
    "avg_item_price_from_orders_{window}d",
    "n_last_purchased_products_{window}d",
    "last_purchased_avg_discount_{window}d",
    "last_purchased_median_discount_{window}d",
    "last_purchased_p10_discount_{window}d",
    "last_purchased_null_rating_share_{window}d",
    "last_purchased_avg_rating_{window}d",
    "last_purchased_median_rating_{window}d",
    "last_purchased_p10_rating_{window}d",
    "last_purchased_avg_popularity_neg_rank_{window}d",
    "last_purchased_median_popularity_neg_rank_{window}d",
    "last_purchased_neg_p90_popularity_rank_{window}d",
    "last_purchased_neg_p10_popularity_rank_{window}d",
    "last_purchased_avg_popularity_neg_rank_in_category_{window}d",
    "last_purchased_median_popularity_neg_rank_in_category_{window}d",
    "last_purchased_neg_p90_popularity_rank_in_category_{window}d",
    "last_purchased_neg_p10_popularity_rank_in_category_{window}d",
    "last_purchased_male_cat_share_{window}d",
    "last_purchased_female_cat_share_{window}d",
    "last_purchased_unisex_cat_share_{window}d",
)
ORDER_FEATURE_COLUMNS = tuple(
    template.format(window=window)
    for window in ORDER_WINDOWS
    for template in ORDER_FEATURE_TEMPLATES
)

LAST_CLICKED_RAW_COLUMNS = (
    "last_clicked_avg_price",
    "last_clicked_median_price",
    "last_clicked_90th_pct_price",
    "last_clicked_avg_discount",
    "last_clicked_median_discount",
    "last_clicked_p10_discount",
    "last_clicked_male_cat_share_raw",
    "last_clicked_female_cat_share_raw",
    "last_clicked_null_rating_share",
    "last_clicked_p10_rating",
    "last_clicked_avg_popularity_neg_rank_by_orders",
    "last_clicked_p10_popularity_neg_rank_by_orders",
    "last_clicked_female_cat_share_among_gendered",
    "last_clicked_male_cat_share_among_gendered",
)
LAST_CLICKED_PERCENTILE_COLUMNS = (
    "last_clicked_avg_price_pctl",
    "last_clicked_median_price_pctl",
    "last_clicked_90th_pct_price_pctl",
)
LAST_CLICKED_COLUMNS = LAST_CLICKED_RAW_COLUMNS + LAST_CLICKED_PERCENTILE_COLUMNS

BASE_PROFILE_COLUMNS = (
    DEMOGRAPHIC_COLUMNS + ORDER_FEATURE_COLUMNS + LAST_CLICKED_RAW_COLUMNS
)
UNPREFIXED_FEATURE_COLUMNS = BASE_PROFILE_COLUMNS + LAST_CLICKED_PERCENTILE_COLUMNS
FEATURE_COLUMNS = tuple(
    f"{FEATURE_NAMESPACE}__{column}" for column in UNPREFIXED_FEATURE_COLUMNS
)


class SourceSettings(Protocol):
    demographics_table: str
    product_metadata_table: str
    product_prices_table: str
    action_counts_table: str
    order_items_table: str
    sku_table: str
    category_demographic_features_table: str
    product_base_features_table: str
    product_ranking_features_table: str
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


def _local_day_start_utc_literal(value: datetime, timezone_name: str) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    local_value = value.astimezone(ZoneInfo(timezone_name))
    local_day_start = local_value.replace(hour=0, minute=0, second=0, microsecond=0)
    return local_day_start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _order_total_expressions(calculated_at_utc: str) -> str:
    expressions: list[str] = []
    for window in ORDER_WINDOWS:
        condition = (
            f"generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL {window} DAYS"
        )
        expressions.extend(
            (
                (
                    "PERCENTILE_APPROX("
                    f"CASE WHEN {condition} THEN order_total END, 0.5"
                    f") AS median_order_total_{window}d"
                ),
                (
                    f"MAX(CASE WHEN {condition} THEN order_total END) "
                    f"AS max_order_total_{window}d"
                ),
                (
                    f"MIN(CASE WHEN {condition} THEN order_total END) "
                    f"AS min_order_total_{window}d"
                ),
                (
                    f"SUM(CASE WHEN {condition} THEN order_total END) "
                    f"AS sum_order_total_{window}d"
                ),
            )
        )
    return ",\n        ".join(expressions)


def _order_line_expressions(calculated_at_utc: str) -> str:
    expressions: list[str] = []
    for window in ORDER_WINDOWS:
        condition = (
            f"generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL {window} DAYS"
        )
        line_count = f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END)"
        category_label_denominator = (
            "SUM(CASE WHEN "
            f"{condition} AND category_id IS NOT NULL THEN 1 ELSE 0 END)"
        )
        expressions.extend(
            (
                (
                    "CAST(SUM(CASE WHEN "
                    f"{condition} THEN line_gmv END) AS DOUBLE) "
                    "/ NULLIF(CAST(SUM(CASE WHEN "
                    f"{condition} THEN item_quantity END) AS DOUBLE), 0.0D) "
                    f"AS avg_item_price_from_orders_{window}d"
                ),
                (
                    "CAST(COUNT(DISTINCT CASE WHEN "
                    f"{condition} THEN product_id END) AS INT) "
                    f"AS n_last_purchased_products_{window}d"
                ),
                (
                    f"AVG(CASE WHEN {condition} THEN discount END) "
                    f"AS last_purchased_avg_discount_{window}d"
                ),
                (
                    "PERCENTILE_APPROX("
                    f"CASE WHEN {condition} THEN discount END, 0.5"
                    f") AS last_purchased_median_discount_{window}d"
                ),
                (
                    "PERCENTILE_APPROX("
                    f"CASE WHEN {condition} THEN discount END, 0.1"
                    f") AS last_purchased_p10_discount_{window}d"
                ),
                (
                    "CAST(SUM(CASE WHEN "
                    f"{condition} AND rating IS NULL THEN 1 ELSE 0 END) "
                    f"AS DOUBLE) / NULLIF(CAST({line_count} AS DOUBLE), 0.0D) "
                    f"AS last_purchased_null_rating_share_{window}d"
                ),
                (
                    f"AVG(CASE WHEN {condition} THEN rating END) "
                    f"AS last_purchased_avg_rating_{window}d"
                ),
                (
                    "PERCENTILE_APPROX("
                    f"CASE WHEN {condition} THEN rating END, 0.5"
                    f") AS last_purchased_median_rating_{window}d"
                ),
                (
                    "PERCENTILE_APPROX("
                    f"CASE WHEN {condition} THEN rating END, 0.1"
                    f") AS last_purchased_p10_rating_{window}d"
                ),
                (
                    "AVG(CASE WHEN "
                    f"{condition} THEN popularity_by_orders_neg_rank END) "
                    f"AS last_purchased_avg_popularity_neg_rank_{window}d"
                ),
                (
                    "PERCENTILE_APPROX(CASE WHEN "
                    f"{condition} THEN popularity_by_orders_neg_rank END, 0.5) "
                    f"AS last_purchased_median_popularity_neg_rank_{window}d"
                ),
                (
                    "PERCENTILE_APPROX(CASE WHEN "
                    f"{condition} THEN popularity_by_orders_neg_rank END, 0.1) "
                    f"AS last_purchased_neg_p90_popularity_rank_{window}d"
                ),
                (
                    "PERCENTILE_APPROX(CASE WHEN "
                    f"{condition} THEN popularity_by_orders_neg_rank END, 0.9) "
                    f"AS last_purchased_neg_p10_popularity_rank_{window}d"
                ),
                (
                    "AVG(CASE WHEN "
                    f"{condition} THEN popularity_by_orders_neg_rank_in_cat END) "
                    f"AS last_purchased_avg_popularity_neg_rank_in_category_{window}d"
                ),
                (
                    "PERCENTILE_APPROX(CASE WHEN "
                    f"{condition} THEN popularity_by_orders_neg_rank_in_cat END, "
                    "0.5) AS "
                    f"last_purchased_median_popularity_neg_rank_in_category_{window}d"
                ),
                (
                    "PERCENTILE_APPROX(CASE WHEN "
                    f"{condition} THEN popularity_by_orders_neg_rank_in_cat END, "
                    "0.1) AS "
                    f"last_purchased_neg_p90_popularity_rank_in_category_{window}d"
                ),
                (
                    "PERCENTILE_APPROX(CASE WHEN "
                    f"{condition} THEN popularity_by_orders_neg_rank_in_cat END, "
                    "0.9) AS "
                    f"last_purchased_neg_p10_popularity_rank_in_category_{window}d"
                ),
                (
                    "AVG(CASE WHEN "
                    f"{condition} THEN male_click_share_28d END) "
                    f"AS last_purchased_male_cat_share_{window}d"
                ),
                (
                    "AVG(CASE WHEN "
                    f"{condition} THEN female_click_share_28d END) "
                    f"AS last_purchased_female_cat_share_{window}d"
                ),
                (
                    "CAST(SUM(CASE WHEN "
                    f"{condition} AND category_id IS NOT NULL "
                    "AND (category_gender IS NULL OR category_gender NOT IN ('M', 'F')) "
                    f"THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(CAST({category_label_denominator} "
                f"AS DOUBLE), 0.0D) AS last_purchased_unisex_cat_share_{window}d"
                ),
            )
        )
    return ",\n        ".join(expressions)


def _select_columns(alias: str, columns: tuple[str, ...]) -> str:
    return ",\n        ".join(f"{alias}.{column}" for column in columns)


def _nullable_select_columns(alias: str, columns: tuple[str, ...]) -> str:
    return ",\n        ".join(f"{alias}.{column} AS {column}" for column in columns)


def _price_rank_inputs() -> str:
    expressions: list[str] = []
    for column in (
        "last_clicked_avg_price",
        "last_clicked_median_price",
        "last_clicked_90th_pct_price",
    ):
        expressions.extend(
            (
                f"RANK() OVER (ORDER BY {column} ASC NULLS LAST) AS {column}_rank",
                f"COUNT(*) OVER (PARTITION BY {column}) AS {column}_tie_count",
                f"COUNT({column}) OVER () AS {column}_non_null_count",
            )
        )
    return ",\n        ".join(expressions)


def _price_percentile_expressions() -> str:
    mappings = (
        ("last_clicked_avg_price", "last_clicked_avg_price_pctl"),
        ("last_clicked_median_price", "last_clicked_median_price_pctl"),
        ("last_clicked_90th_pct_price", "last_clicked_90th_pct_price_pctl"),
    )
    expressions = []
    for source, target in mappings:
        expressions.append(
            f"CASE WHEN {source} IS NOT NULL THEN "
            f"(CAST({source}_rank AS DOUBLE) "
            f"+ (CAST({source}_tie_count AS DOUBLE) - 1.0D) / 2.0D) "
            f"/ NULLIF(CAST({source}_non_null_count AS DOUBLE), 0.0D) "
            f"END AS {target}"
        )
    return ",\n    ".join(expressions)


def _namespaced_feature_select(source_alias: str) -> str:
    return ",\n    ".join(
        f"{source_alias}.{column} AS {FEATURE_NAMESPACE}__{column}"
        for column in UNPREFIXED_FEATURE_COLUMNS
    )


def build_account_profile_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_utc = _utc_timestamp_literal(calculated_at)
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    daily_snapshot_local = _local_day_start_literal(
        calculated_at,
        settings.business_timezone,
    )
    metadata_snapshot_utc = _local_day_start_utc_literal(
        calculated_at,
        settings.business_timezone,
    )

    order_total_expressions = _order_total_expressions(calculated_at_utc)
    order_line_expressions = _order_line_expressions(calculated_at_utc)
    order_columns_from_totals = _select_columns(
        "totals",
        tuple(column for column in ORDER_FEATURE_COLUMNS if "order_total" in column),
    )
    order_columns_from_lines = _select_columns(
        "lines",
        tuple(
            column for column in ORDER_FEATURE_COLUMNS if "order_total" not in column
        ),
    )
    demographic_select = _nullable_select_columns("demographics", DEMOGRAPHIC_COLUMNS)
    order_profile_select = _nullable_select_columns("orders", ORDER_FEATURE_COLUMNS)
    last_clicked_select = _nullable_select_columns(
        "clicks",
        LAST_CLICKED_RAW_COLUMNS,
    )
    final_base_select = ",\n    ".join(BASE_PROFILE_COLUMNS)
    price_rank_inputs = _price_rank_inputs()
    price_percentile_expressions = _price_percentile_expressions()
    namespaced_feature_select = _namespaced_feature_select("unprefixed_features")

    return f"""
WITH demographics AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        gender,
        CASE
            WHEN gender = 'FEMALE' THEN 1
            WHEN gender = 'MALE' THEN 0
        END AS gender_is_female,
        CAST(age AS INT) AS age,
        CASE
            WHEN age IS NULL THEN 'UNKNOWN'
            WHEN age < 18 THEN 'LT_18'
            WHEN age <= 24 THEN '18_24'
            WHEN age <= 34 THEN '25_34'
            WHEN age <= 44 THEN '35_44'
            WHEN age <= 54 THEN '45_54'
            ELSE '55_PLUS'
        END AS age_bucket,
        city_name,
        platform
    FROM {settings.demographics_table}
    WHERE dt = TIMESTAMP '{daily_snapshot_local}'
),
product_metadata AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        CAST(category_id AS INT) AS category_id
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_snapshot_utc}'
),
product_prices AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        min_sell_price_eod
    FROM {settings.product_prices_table}
    WHERE dt = TIMESTAMP '{daily_snapshot_local}'
),
category_demographics AS (
    SELECT
        CAST(category_id AS INT) AS category_id,
        CATEGORY__gender AS category_gender,
        CATEGORY__male_click_share_28d
            AS male_click_share_28d,
        CATEGORY__female_click_share_28d
            AS female_click_share_28d
    FROM {settings.category_demographic_features_table}
    WHERE calculated_at = TIMESTAMP '{calculated_at_local}'
),
product_base_features AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        PRODUCT__discount AS discount,
        PRODUCT__rating AS rating
    FROM {settings.product_base_features_table}
    WHERE calculated_at = TIMESTAMP '{calculated_at_local}'
),
product_ranking_features AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        PRODUCT_STATS__popularity_by_orders_neg_rank
            AS popularity_by_orders_neg_rank,
        PRODUCT_STATS__popularity_by_orders_neg_rank_in_cat
            AS popularity_by_orders_neg_rank_in_cat
    FROM {settings.product_ranking_features_table}
    WHERE calculated_at = TIMESTAMP '{calculated_at_local}'
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
        CAST(order_item.item_quantity AS INT) AS item_quantity,
        CAST(order_item.payment_price AS DOUBLE)
            * CAST(order_item.item_quantity AS DOUBLE) AS line_gmv
    FROM {settings.order_items_table} order_item
    LEFT JOIN sku_mapping sku
        ON order_item.sku_id = sku.sku_id
    WHERE order_item.generated_at >= TIMESTAMP '{calculated_at_utc}' - INTERVAL 90 DAYS
        AND order_item.generated_at < TIMESTAMP '{calculated_at_utc}'
        AND order_item.order_item_status IN (
            'COMPLETED', 'PAID', 'DELIVERED', 'IN_DELIVERY'
        )
        AND order_item.b2b_order = FALSE
),
enriched_order_lines AS (
    SELECT
        orders.account_id,
        orders.order_id,
        orders.product_id,
        orders.generated_at,
        orders.item_quantity,
        orders.line_gmv,
        metadata.category_id,
        category.category_gender,
        category.male_click_share_28d,
        category.female_click_share_28d,
        base.discount,
        base.rating,
        ranking.popularity_by_orders_neg_rank,
        ranking.popularity_by_orders_neg_rank_in_cat
    FROM filtered_order_lines orders
    LEFT JOIN product_metadata metadata
        ON orders.product_id = metadata.product_id
    LEFT JOIN category_demographics category
        ON metadata.category_id = category.category_id
    LEFT JOIN product_base_features base
        ON orders.product_id = base.product_id
    LEFT JOIN product_ranking_features ranking
        ON orders.product_id = ranking.product_id
),
order_totals AS (
    SELECT
        account_id,
        order_id,
        MAX(generated_at) AS generated_at,
        CAST(SUM(line_gmv) AS DOUBLE) AS order_total
    FROM enriched_order_lines
    GROUP BY account_id, order_id
),
order_total_features AS (
    SELECT
        account_id,
        {order_total_expressions}
    FROM order_totals
    GROUP BY account_id
),
order_line_features AS (
    SELECT
        account_id,
        {order_line_expressions}
    FROM enriched_order_lines
    GROUP BY account_id
),
order_profile AS (
    SELECT
        totals.account_id,
        {order_columns_from_totals},
        {order_columns_from_lines}
    FROM order_total_features totals
    INNER JOIN order_line_features lines
        ON totals.account_id = lines.account_id
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
    GROUP BY account_id, session_id, product_id
),
ranked_clicks AS (
    SELECT
        clicks.*,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY last_received_at DESC, product_id, session_id
        ) AS click_row_number
    FROM deduplicated_clicks clicks
),
selected_clicks AS (
    SELECT
        account_id,
        session_id,
        product_id,
        last_received_at
    FROM ranked_clicks
    WHERE click_row_number <= 75
),
enriched_last_clicks AS (
    SELECT
        clicks.account_id,
        clicks.session_id,
        clicks.product_id,
        clicks.last_received_at,
        prices.min_sell_price_eod,
        category.category_gender,
        category.male_click_share_28d,
        category.female_click_share_28d,
        base.discount,
        base.rating,
        ranking.popularity_by_orders_neg_rank
    FROM selected_clicks clicks
    INNER JOIN product_prices prices
        ON clicks.product_id = prices.product_id
    LEFT JOIN product_metadata metadata
        ON clicks.product_id = metadata.product_id
    LEFT JOIN category_demographics category
        ON metadata.category_id = category.category_id
    LEFT JOIN product_base_features base
        ON clicks.product_id = base.product_id
    LEFT JOIN product_ranking_features ranking
        ON clicks.product_id = ranking.product_id
    WHERE prices.min_sell_price_eod IS NOT NULL
),
last_clicked_raw_profile AS (
    SELECT
        account_id,
        AVG(min_sell_price_eod) AS last_clicked_avg_price,
        PERCENTILE_APPROX(min_sell_price_eod, 0.5)
            AS last_clicked_median_price,
        PERCENTILE_APPROX(min_sell_price_eod, 0.9)
            AS last_clicked_90th_pct_price,
        AVG(discount) AS last_clicked_avg_discount,
        PERCENTILE_APPROX(discount, 0.5)
            AS last_clicked_median_discount,
        PERCENTILE_APPROX(discount, 0.1)
            AS last_clicked_p10_discount,
        AVG(male_click_share_28d)
            AS last_clicked_male_cat_share_raw,
        AVG(female_click_share_28d)
            AS last_clicked_female_cat_share_raw,
        CAST(SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END) AS DOUBLE)
            / NULLIF(CAST(COUNT(*) AS DOUBLE), 0.0D)
            AS last_clicked_null_rating_share,
        PERCENTILE_APPROX(rating, 0.1) AS last_clicked_p10_rating,
        AVG(popularity_by_orders_neg_rank)
            AS last_clicked_avg_popularity_neg_rank_by_orders,
        PERCENTILE_APPROX(popularity_by_orders_neg_rank, 0.1)
            AS last_clicked_p10_popularity_neg_rank_by_orders
    FROM enriched_last_clicks
    GROUP BY account_id
),
last_clicked_profile AS (
    SELECT
        raw.*,
        raw.last_clicked_female_cat_share_raw
            / NULLIF(
                raw.last_clicked_female_cat_share_raw
                    + raw.last_clicked_male_cat_share_raw,
                0.0D
            ) AS last_clicked_female_cat_share_among_gendered,
        raw.last_clicked_male_cat_share_raw
            / NULLIF(
                raw.last_clicked_female_cat_share_raw
                    + raw.last_clicked_male_cat_share_raw,
                0.0D
            ) AS last_clicked_male_cat_share_among_gendered
    FROM last_clicked_raw_profile raw
),
account_population AS (
    SELECT account_id FROM demographics
    UNION
    SELECT account_id FROM order_profile
    UNION
    SELECT account_id FROM last_clicked_profile
),
profile_base AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        population.account_id,
        {demographic_select},
        {order_profile_select},
        {last_clicked_select}
    FROM account_population population
    LEFT JOIN demographics
        ON population.account_id = demographics.account_id
    LEFT JOIN order_profile orders
        ON population.account_id = orders.account_id
    LEFT JOIN last_clicked_profile clicks
        ON population.account_id = clicks.account_id
),
price_rank_inputs AS (
    SELECT
        profile_base.*,
        {price_rank_inputs}
    FROM profile_base
),
unprefixed_features AS (
    SELECT
        calculated_at,
        account_id,
        {final_base_select},
        {price_percentile_expressions}
    FROM price_rank_inputs
)
SELECT
    calculated_at,
    account_id,
    {namespaced_feature_select}
FROM unprefixed_features
"""


def build_account_profile_features_merge_query(
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
USING account_profile_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_local}'
THEN DELETE
"""
