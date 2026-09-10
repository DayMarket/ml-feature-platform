from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

ACTION_EVENT_TYPES = {
    "clicks": "PRODUCT_VIEW",
    "atcs": "ADD_TO_CART",
    "atfs": "ADD_TO_FAVORITES",
}
CONVERSION_SIGNALS = {
    "click": "clicks",
    "atc": "atcs",
    "atf": "atfs",
    "order": "orders",
}


class SourceSettings(Protocol):
    category_level: int
    category_column: str
    product_metadata_table: str
    action_counts_table: str
    impression_counts_table: str | None
    order_items_table: str
    sku_table: str
    business_timezone: str
    event_windows_days: tuple[int, ...]
    order_windows_days: tuple[int, ...]
    successful_order_statuses: tuple[str, ...]
    has_impressions: bool
    has_recency: bool


def _prefix(settings: SourceSettings) -> str:
    return f"l{settings.category_level}"


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
    prefix = _prefix(settings)
    columns: list[str] = []

    if settings.has_impressions:
        columns.extend(
            f"{prefix}_n_imps_{window}d"
            for window in settings.event_windows_days
        )
        columns.extend(
            f"{prefix}_n_imps_{window}d_ratio"
            for window in settings.event_windows_days
        )

    for signal in ACTION_EVENT_TYPES:
        columns.extend(
            f"{prefix}_n_{signal}_{window}d"
            for window in settings.event_windows_days
        )
        columns.extend(
            f"{prefix}_n_{signal}_{window}d_ratio"
            for window in settings.event_windows_days
        )

    if settings.has_impressions:
        for output_signal in CONVERSION_SIGNALS:
            columns.extend(
                f"{prefix}_account_conv_imp2{output_signal}_{window}d"
                for window in settings.event_windows_days
            )
        for output_signal in CONVERSION_SIGNALS:
            columns.extend(
                f"{prefix}_conv_imp2{output_signal}_{window}d"
                for window in settings.event_windows_days
            )
        for output_signal in CONVERSION_SIGNALS:
            columns.extend(
                f"{prefix}_conv_imp2{output_signal}_vs_account_{window}d"
                for window in settings.event_windows_days
            )

    columns.extend(
        f"{prefix}_n_orders_{window}d"
        for window in settings.order_windows_days
    )
    columns.extend(
        f"{prefix}_n_orders_{window}d_ratio"
        for window in settings.order_windows_days
    )
    columns.extend(
        f"{prefix}_gmv_{window}d"
        for window in settings.order_windows_days
    )
    columns.extend(
        f"{prefix}_gmv_{window}d_ratio"
        for window in settings.order_windows_days
    )

    if settings.has_recency:
        columns.extend(
            (
                f"{prefix}_neg_n_days_since_last_click",
                f"{prefix}_neg_n_days_since_last_click_rel",
            )
        )
    return tuple(columns)


def _action_count_expressions(
    settings: SourceSettings,
    calculated_at_local: str,
) -> str:
    prefix = _prefix(settings)
    expressions: list[str] = []
    for signal, event_type in ACTION_EVENT_TYPES.items():
        for window in settings.event_windows_days:
            expressions.append(
                "CAST(SUM(CASE "
                f"WHEN event_type = {_sql_string(event_type)} "
                f"AND last_received_at >= TIMESTAMP '{calculated_at_local}' "
                f"- INTERVAL {window} DAYS "
                "THEN 1 ELSE 0 END) AS BIGINT) "
                f"AS {prefix}_n_{signal}_{window}d"
            )
    return ",\n        ".join(expressions)


def _impression_count_expressions(
    settings: SourceSettings,
    calculated_at_local: str,
) -> str:
    prefix = _prefix(settings)
    return ",\n        ".join(
        "CAST(SUM(CASE "
        f"WHEN source_calculated_at > TIMESTAMP '{calculated_at_local}' "
        f"- INTERVAL {window} DAYS "
        "THEN n_impressions ELSE 0 END) AS BIGINT) "
        f"AS {prefix}_n_imps_{window}d"
        for window in settings.event_windows_days
    )


def _order_feature_expressions(
    settings: SourceSettings,
    calculated_at_utc: str,
) -> str:
    prefix = _prefix(settings)
    expressions: list[str] = []
    for window in settings.order_windows_days:
        expressions.append(
            "CAST(COUNT(DISTINCT CASE "
            f"WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' "
            f"- INTERVAL {window} DAYS "
            "THEN order_id END) AS BIGINT) "
            f"AS {prefix}_n_orders_{window}d"
        )
    for window in settings.order_windows_days:
        expressions.append(
            "CAST(SUM(CASE "
            f"WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' "
            f"- INTERVAL {window} DAYS "
            "THEN line_gmv ELSE 0.0 END) AS DOUBLE) "
            f"AS {prefix}_gmv_{window}d"
        )
    return ",\n        ".join(expressions)


def _account_order_count_expressions(
    settings: SourceSettings,
    calculated_at_utc: str,
) -> str:
    return ",\n        ".join(
        "CAST(COUNT(DISTINCT CASE "
        f"WHEN generated_at >= TIMESTAMP '{calculated_at_utc}' "
        f"- INTERVAL {window} DAYS "
        "THEN order_id END) AS BIGINT) "
        f"AS account_n_orders_{window}d"
        for window in settings.event_windows_days
    )


def _base_feature_expressions(settings: SourceSettings) -> str:
    prefix = _prefix(settings)
    expressions: list[str] = []
    if settings.has_impressions:
        for window in settings.event_windows_days:
            column = f"{prefix}_n_imps_{window}d"
            expressions.append(f"COALESCE(impressions.{column}, 0L) AS {column}")

    for signal in ACTION_EVENT_TYPES:
        for window in settings.event_windows_days:
            column = f"{prefix}_n_{signal}_{window}d"
            expressions.append(f"COALESCE(actions.{column}, 0L) AS {column}")

    for window in settings.order_windows_days:
        column = f"{prefix}_n_orders_{window}d"
        expressions.append(f"COALESCE(orders.{column}, 0L) AS {column}")
    for window in settings.order_windows_days:
        column = f"{prefix}_gmv_{window}d"
        expressions.append(f"COALESCE(orders.{column}, 0.0D) AS {column}")
    return ",\n        ".join(expressions)


def _ratio_expressions(settings: SourceSettings) -> str:
    prefix = _prefix(settings)
    metric_columns: list[str] = []
    if settings.has_impressions:
        metric_columns.extend(
            f"{prefix}_n_imps_{window}d"
            for window in settings.event_windows_days
        )
    for signal in ACTION_EVENT_TYPES:
        metric_columns.extend(
            f"{prefix}_n_{signal}_{window}d"
            for window in settings.event_windows_days
        )
    metric_columns.extend(
        f"{prefix}_n_orders_{window}d"
        for window in settings.order_windows_days
    )
    metric_columns.extend(
        f"{prefix}_gmv_{window}d"
        for window in settings.order_windows_days
    )

    return ",\n        ".join(
        "CASE WHEN SUM(" + column + ") OVER ("
        "PARTITION BY calculated_at, account_id) > 0 "
        "THEN CAST(" + column + " AS DOUBLE) / SUM(" + column + ") OVER ("
        "PARTITION BY calculated_at, account_id) END "
        f"AS {column}_ratio"
        for column in metric_columns
    )


def _account_conversion_expressions(settings: SourceSettings) -> str:
    prefix = _prefix(settings)
    expressions: list[str] = []
    for output_signal, count_signal in CONVERSION_SIGNALS.items():
        for window in settings.event_windows_days:
            count_column = f"{prefix}_n_{count_signal}_{window}d"
            impressions_column = f"{prefix}_n_imps_{window}d"
            expressions.append(
                f"CASE WHEN {impressions_column} > 0 "
                f"THEN CAST({count_column} AS DOUBLE) / {impressions_column} END "
                f"AS {prefix}_account_conv_imp2{output_signal}_{window}d"
            )
    return ",\n        ".join(expressions)


def _conversion_baseline_expressions(settings: SourceSettings) -> str:
    prefix = _prefix(settings)
    category_column = settings.category_column
    expressions: list[str] = []
    for output_signal, count_signal in CONVERSION_SIGNALS.items():
        for window in settings.event_windows_days:
            count_column = f"{prefix}_n_{count_signal}_{window}d"
            impressions_column = f"{prefix}_n_imps_{window}d"
            expressions.append(
                f"CASE WHEN SUM({impressions_column}) OVER ("
                f"PARTITION BY calculated_at, {category_column}) > 0 "
                f"THEN CAST(SUM({count_column}) OVER ("
                f"PARTITION BY calculated_at, {category_column}) AS DOUBLE) "
                f"/ SUM({impressions_column}) OVER ("
                f"PARTITION BY calculated_at, {category_column}) END "
                f"AS category_{output_signal}_conversion_{window}d"
            )

            if output_signal == "order":
                account_numerator = f"account_n_orders_{window}d"
            else:
                account_numerator = (
                    f"SUM({count_column}) OVER "
                    "(PARTITION BY calculated_at, account_id)"
                )
            expressions.append(
                f"CASE WHEN SUM({impressions_column}) OVER ("
                "PARTITION BY calculated_at, account_id) > 0 "
                f"THEN CAST({account_numerator} AS DOUBLE) "
                f"/ SUM({impressions_column}) OVER ("
                "PARTITION BY calculated_at, account_id) END "
                f"AS account_total_{output_signal}_conversion_{window}d"
            )
    return ",\n        ".join(expressions)


def _relative_conversion_expressions(settings: SourceSettings) -> str:
    prefix = _prefix(settings)
    expressions: list[str] = []
    for output_signal in CONVERSION_SIGNALS:
        for window in settings.event_windows_days:
            account_conversion = (
                f"{prefix}_account_conv_imp2{output_signal}_{window}d"
            )
            category_baseline = (
                f"category_{output_signal}_conversion_{window}d"
            )
            account_baseline = (
                f"account_total_{output_signal}_conversion_{window}d"
            )
            expressions.append(
                f"CASE WHEN {category_baseline} > 0 "
                f"THEN {account_conversion} / {category_baseline} END "
                f"AS {prefix}_conv_imp2{output_signal}_{window}d"
            )
            expressions.append(
                f"CASE WHEN {account_baseline} > 0 "
                f"THEN {account_conversion} / {account_baseline} END "
                f"AS {prefix}_conv_imp2{output_signal}_vs_account_{window}d"
            )
    return ",\n        ".join(expressions)


def build_account_category_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    prefix = _prefix(settings)
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

    ctes = [
        f"""product_categories AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        CAST({settings.category_column} AS INT) AS category_id
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_dt_local}'
        AND {settings.category_column} IS NOT NULL
)""",
        f"""raw_actions AS (
    SELECT
        calculated_at AS source_calculated_at,
        CAST(account_id AS INT) AS account_id,
        session_id,
        CAST(product_id AS INT) AS product_id,
        event_type,
        last_received_at
    FROM {settings.action_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}'
            - INTERVAL {max_event_window} DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND last_received_at >= TIMESTAMP '{calculated_at_local}'
            - INTERVAL {max_event_window} DAYS
        AND last_received_at < TIMESTAMP '{calculated_at_local}'
)""",
    ]

    if settings.has_impressions:
        ctes.append(
            """actions_for_aggregation AS (
    SELECT
        source_calculated_at,
        account_id,
        session_id,
        product_id,
        event_type,
        last_received_at
    FROM raw_actions
)"""
        )
    else:
        ctes.append(
            """actions_for_aggregation AS (
    SELECT
        MAX(source_calculated_at) AS source_calculated_at,
        account_id,
        session_id,
        product_id,
        event_type,
        MAX(last_received_at) AS last_received_at
    FROM raw_actions
    GROUP BY
        account_id,
        session_id,
        product_id,
        event_type
)"""
        )

    ctes.extend(
        [
            """mapped_actions AS (
    SELECT
        action.account_id,
        action.session_id,
        action.product_id,
        action.event_type,
        action.last_received_at,
        product.category_id
    FROM actions_for_aggregation action
    INNER JOIN product_categories product
        ON action.product_id = product.product_id
)""",
            f"""action_features AS (
    SELECT
        account_id,
        category_id,
        {_action_count_expressions(settings, calculated_at_local)},
        MAX(CASE
            WHEN event_type = 'PRODUCT_VIEW' THEN last_received_at
        END) AS last_click_at
    FROM mapped_actions
    GROUP BY account_id, category_id
)""",
        ]
    )

    if settings.has_impressions:
        ctes.extend(
            [
                f"""filtered_impressions AS (
    SELECT
        calculated_at AS source_calculated_at,
        CAST(account_id AS INT) AS account_id,
        CAST({settings.category_column} AS INT) AS category_id,
        CAST(n_impressions AS BIGINT) AS n_impressions
    FROM {settings.impression_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}'
            - INTERVAL {max_event_window} DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
)""",
                f"""impression_features AS (
    SELECT
        account_id,
        category_id,
        {_impression_count_expressions(settings, calculated_at_local)}
    FROM filtered_impressions
    GROUP BY account_id, category_id
)""",
            ]
        )

    ctes.extend(
        [
            f"""sku_mapping AS (
    SELECT
        CAST(id AS BIGINT) AS sku_id,
        CAST(MIN(product_id) AS INT) AS product_id
    FROM {settings.sku_table}
    GROUP BY id
)""",
            f"""filtered_order_lines AS (
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
)""",
            """mapped_order_lines AS (
    SELECT
        order_line.account_id,
        order_line.product_id,
        order_line.order_id,
        order_line.generated_at,
        order_line.line_gmv,
        product.category_id
    FROM filtered_order_lines order_line
    INNER JOIN product_categories product
        ON order_line.product_id = product.product_id
)""",
            f"""order_features AS (
    SELECT
        account_id,
        category_id,
        {_order_feature_expressions(settings, calculated_at_utc)}
    FROM mapped_order_lines
    GROUP BY account_id, category_id
)""",
        ]
    )

    if settings.has_impressions:
        ctes.append(
            f"""account_order_features AS (
    SELECT
        account_id,
        {_account_order_count_expressions(settings, calculated_at_utc)}
    FROM filtered_order_lines
    GROUP BY account_id
)"""
        )

    key_sources = [
        "SELECT account_id, category_id FROM action_features",
        "SELECT account_id, category_id FROM order_features",
    ]
    if settings.has_impressions:
        key_sources.insert(
            0,
            "SELECT account_id, category_id FROM impression_features",
        )
    ctes.append(
        "entity_keys AS (\n    "
        + "\n    UNION\n    ".join(key_sources)
        + "\n)"
    )

    joins = [
        """LEFT JOIN action_features actions
        ON entity.account_id = actions.account_id
        AND entity.category_id = actions.category_id""",
        """LEFT JOIN order_features orders
        ON entity.account_id = orders.account_id
        AND entity.category_id = orders.category_id""",
    ]
    account_order_columns = ""
    if settings.has_impressions:
        joins.insert(
            0,
            """LEFT JOIN impression_features impressions
        ON entity.account_id = impressions.account_id
        AND entity.category_id = impressions.category_id""",
        )
        joins.append(
            """LEFT JOIN account_order_features account_orders
        ON entity.account_id = account_orders.account_id"""
        )
        account_order_columns = ",\n        " + ",\n        ".join(
            f"COALESCE(account_orders.account_n_orders_{window}d, 0L) "
            f"AS account_n_orders_{window}d"
            for window in settings.event_windows_days
        )

    ctes.append(
        f"""base_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        entity.account_id,
        entity.category_id AS {settings.category_column},
        {_base_feature_expressions(settings)},
        actions.last_click_at{account_order_columns}
    FROM entity_keys entity
    {chr(10).join(joins)}
)"""
    )

    ratio_and_recency = _ratio_expressions(settings)
    if settings.has_recency:
        ratio_and_recency += (
            ",\n        CASE WHEN last_click_at IS NOT NULL THEN "
            "-CAST(CEIL(CAST("
            f"UNIX_TIMESTAMP(TIMESTAMP '{calculated_at_local}') "
            "- UNIX_TIMESTAMP(last_click_at) AS DOUBLE"
            ") / 86400.0) AS INT) END "
            f"AS {prefix}_neg_n_days_since_last_click"
        )

    ctes.append(
        f"""features_with_ratios AS (
    SELECT
        *,
        {ratio_and_recency}
    FROM base_features
)"""
    )

    ready_cte = "features_with_ratios"
    if settings.has_recency:
        ctes.append(
            f"""features_with_recency AS (
    SELECT
        *,
        {prefix}_neg_n_days_since_last_click
            - MAX({prefix}_neg_n_days_since_last_click) OVER (
                PARTITION BY calculated_at, account_id
            ) AS {prefix}_neg_n_days_since_last_click_rel
    FROM features_with_ratios
)"""
        )
        ready_cte = "features_with_recency"

    if settings.has_impressions:
        ctes.extend(
            [
                f"""features_with_account_conversions AS (
    SELECT
        *,
        {_account_conversion_expressions(settings)}
    FROM {ready_cte}
)""",
                f"""features_with_conversion_baselines AS (
    SELECT
        *,
        {_conversion_baseline_expressions(settings)}
    FROM features_with_account_conversions
)""",
                f"""features_with_relative_conversions AS (
    SELECT
        *,
        {_relative_conversion_expressions(settings)}
    FROM features_with_conversion_baselines
)""",
            ]
        )
        ready_cte = "features_with_relative_conversions"

    selected_features = ",\n    ".join(feature_columns(settings))
    return (
        "WITH "
        + ",\n".join(ctes)
        + f"""
SELECT
    calculated_at,
    account_id,
    {settings.category_column},
    {selected_features}
FROM {ready_cte}
"""
    )


def build_account_category_features_merge_query(
    target_table: str,
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    columns = feature_columns(settings)
    key_columns = (
        "calculated_at",
        "account_id",
        settings.category_column,
    )
    update_assignments = ",\n    ".join(
        f"target.{column} = source.{column}" for column in columns
    )
    insert_columns = ",\n    ".join((*key_columns, *columns))
    insert_values = ",\n    ".join(
        f"source.{column}" for column in (*key_columns, *columns)
    )

    return f"""
MERGE INTO {target_table} AS target
USING account_category_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.{settings.category_column} = source.{settings.category_column}
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
