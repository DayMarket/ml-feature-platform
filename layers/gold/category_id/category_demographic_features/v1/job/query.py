from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

FEATURE_NAMESPACE = "CATEGORY_DEMOGRAPHICS"
BASE_FEATURE_COLUMNS = (
    "female_product_session_share_28d",
    "male_product_session_share_28d",
    "female_unique_clicker_share_28d",
    "male_unique_clicker_share_28d",
    "gender_balance_28d",
    "n_unique_clickers_28d",
    "n_unique_known_gender_clickers_28d",
    "n_unique_female_clickers_28d",
    "n_unique_male_clickers_28d",
    "n_unique_clickers_with_age_28d",
    "known_age_clicker_share_28d",
    "clicker_age_p10_28d",
    "clicker_age_p50_28d",
    "clicker_age_p90_28d",
    "gender",
)
FEATURE_COLUMNS = tuple(
    f"{FEATURE_NAMESPACE}__{column}" for column in BASE_FEATURE_COLUMNS
)


class SourceSettings(Protocol):
    product_metadata_table: str
    action_counts_table: str
    demographics_table: str
    business_timezone: str
    lookback_days: int
    min_valid_age: int
    max_valid_age: int


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


def _namespaced_feature_select(source_alias: str) -> str:
    return ",\n    ".join(
        f"{source_alias}.{column} AS {FEATURE_NAMESPACE}__{column}"
        for column in BASE_FEATURE_COLUMNS
    )


def build_category_demographic_features_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    demographics_snapshot_local = _local_day_start_literal(
        calculated_at,
        settings.business_timezone,
    )
    metadata_snapshot_utc = _local_day_start_utc_literal(
        calculated_at,
        settings.business_timezone,
    )
    namespaced_feature_select = _namespaced_feature_select("unprefixed_features")

    return f"""
WITH product_metadata AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        CAST(category_id AS INT) AS category_id,
        category_gender
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_snapshot_utc}'
),
demographics AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        gender AS account_gender,
        CAST(age AS INT) AS age
    FROM {settings.demographics_table}
    WHERE dt = TIMESTAMP '{demographics_snapshot_local}'
),
deduplicated_product_views AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        session_id,
        CAST(product_id AS INT) AS product_id,
        MAX(last_received_at) AS last_received_at
    FROM {settings.action_counts_table}
    WHERE calculated_at > TIMESTAMP '{calculated_at_local}'
            - INTERVAL {settings.lookback_days} DAYS
        AND calculated_at <= TIMESTAMP '{calculated_at_local}'
        AND last_received_at >= TIMESTAMP '{calculated_at_local}'
            - INTERVAL {settings.lookback_days} DAYS
        AND last_received_at < TIMESTAMP '{calculated_at_local}'
        AND event_type = 'PRODUCT_VIEW'
    GROUP BY
        account_id,
        session_id,
        product_id
),
enriched_product_views AS (
    SELECT
        views.account_id,
        views.session_id,
        views.product_id,
        metadata.category_id,
        metadata.category_gender,
        demographics.account_gender,
        demographics.age
    FROM deduplicated_product_views views
    INNER JOIN product_metadata metadata
        ON views.product_id = metadata.product_id
    LEFT JOIN demographics
        ON views.account_id = demographics.account_id
    WHERE metadata.category_id IS NOT NULL
),
product_session_statistics AS (
    SELECT
        category_id,
        CAST(
            SUM(CASE WHEN account_gender = 'FEMALE' THEN 1 ELSE 0 END)
            AS DOUBLE
        ) / NULLIF(
            CAST(
                SUM(
                    CASE
                        WHEN account_gender IN ('MALE', 'FEMALE') THEN 1
                        ELSE 0
                    END
                ) AS DOUBLE
            ),
            0.0D
        ) AS female_product_session_share_28d,
        CAST(
            SUM(CASE WHEN account_gender = 'MALE' THEN 1 ELSE 0 END)
            AS DOUBLE
        ) / NULLIF(
            CAST(
                SUM(
                    CASE
                        WHEN account_gender IN ('MALE', 'FEMALE') THEN 1
                        ELSE 0
                    END
                ) AS DOUBLE
            ),
            0.0D
        ) AS male_product_session_share_28d,
        MAX(category_gender) AS category_gender
    FROM enriched_product_views
    GROUP BY category_id
),
unique_category_clickers AS (
    SELECT
        category_id,
        account_id,
        MAX(account_gender) AS account_gender,
        MAX(age) AS age
    FROM enriched_product_views
    GROUP BY
        category_id,
        account_id
),
unique_clicker_statistics AS (
    SELECT
        category_id,
        CAST(COUNT(*) AS INT) AS n_unique_clickers_28d,
        CAST(
            COUNT(CASE WHEN account_gender IN ('MALE', 'FEMALE') THEN 1 END)
            AS INT
        ) AS n_unique_known_gender_clickers_28d,
        CAST(
            COUNT(CASE WHEN account_gender = 'FEMALE' THEN 1 END)
            AS INT
        ) AS n_unique_female_clickers_28d,
        CAST(
            COUNT(CASE WHEN account_gender = 'MALE' THEN 1 END)
            AS INT
        ) AS n_unique_male_clickers_28d,
        CAST(
            SUM(CASE WHEN account_gender = 'FEMALE' THEN 1 ELSE 0 END)
            AS DOUBLE
        ) / NULLIF(
            CAST(
                SUM(
                    CASE
                        WHEN account_gender IN ('MALE', 'FEMALE') THEN 1
                        ELSE 0
                    END
                ) AS DOUBLE
            ),
            0.0D
        ) AS female_unique_clicker_share_28d,
        CAST(
            SUM(CASE WHEN account_gender = 'MALE' THEN 1 ELSE 0 END)
            AS DOUBLE
        ) / NULLIF(
            CAST(
                SUM(
                    CASE
                        WHEN account_gender IN ('MALE', 'FEMALE') THEN 1
                        ELSE 0
                    END
                ) AS DOUBLE
            ),
            0.0D
        ) AS male_unique_clicker_share_28d,
        CAST(
            COUNT(
                CASE
                    WHEN age BETWEEN {settings.min_valid_age}
                        AND {settings.max_valid_age}
                    THEN 1
                END
            ) AS INT
        ) AS n_unique_clickers_with_age_28d,
        CAST(
            COUNT(
                CASE
                    WHEN age BETWEEN {settings.min_valid_age}
                        AND {settings.max_valid_age}
                    THEN 1
                END
            ) AS DOUBLE
        ) / NULLIF(CAST(COUNT(*) AS DOUBLE), 0.0D)
            AS known_age_clicker_share_28d,
        PERCENTILE(
            CASE
                WHEN age BETWEEN {settings.min_valid_age}
                    AND {settings.max_valid_age}
                THEN CAST(age AS DOUBLE)
            END,
            0.1D
        ) AS clicker_age_p10_28d,
        PERCENTILE(
            CASE
                WHEN age BETWEEN {settings.min_valid_age}
                    AND {settings.max_valid_age}
                THEN CAST(age AS DOUBLE)
            END,
            0.5D
        ) AS clicker_age_p50_28d,
        PERCENTILE(
            CASE
                WHEN age BETWEEN {settings.min_valid_age}
                    AND {settings.max_valid_age}
                THEN CAST(age AS DOUBLE)
            END,
            0.9D
        ) AS clicker_age_p90_28d
    FROM unique_category_clickers
    GROUP BY category_id
),
unprefixed_features AS (
    SELECT
        TIMESTAMP '{calculated_at_local}' AS calculated_at,
        product_sessions.category_id,
        product_sessions.female_product_session_share_28d,
        product_sessions.male_product_session_share_28d,
        unique_clickers.female_unique_clicker_share_28d,
        unique_clickers.male_unique_clicker_share_28d,
        CASE
            WHEN product_sessions.female_product_session_share_28d IS NULL
                THEN NULL
            ELSE 1.0D - 2.0D * ABS(
                product_sessions.female_product_session_share_28d - 0.5D
            )
        END AS gender_balance_28d,
        unique_clickers.n_unique_clickers_28d,
        unique_clickers.n_unique_known_gender_clickers_28d,
        unique_clickers.n_unique_female_clickers_28d,
        unique_clickers.n_unique_male_clickers_28d,
        unique_clickers.n_unique_clickers_with_age_28d,
        unique_clickers.known_age_clicker_share_28d,
        unique_clickers.clicker_age_p10_28d,
        unique_clickers.clicker_age_p50_28d,
        unique_clickers.clicker_age_p90_28d,
        product_sessions.category_gender AS gender
    FROM product_session_statistics product_sessions
    INNER JOIN unique_clicker_statistics unique_clickers
        ON product_sessions.category_id = unique_clickers.category_id
)
SELECT
    calculated_at,
    category_id,
    {namespaced_feature_select}
FROM unprefixed_features
"""


def build_category_demographic_features_merge_query(
    target_table: str,
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    calculated_at_local = _local_timestamp_literal(
        calculated_at,
        settings.business_timezone,
    )
    update_columns = ",\n    ".join(
        f"target.{column} = source.{column}" for column in FEATURE_COLUMNS
    )
    insert_columns = ",\n    ".join(
        ("calculated_at", "category_id", *FEATURE_COLUMNS)
    )
    source_columns = ",\n    ".join(
        f"source.{column}"
        for column in ("calculated_at", "category_id", *FEATURE_COLUMNS)
    )

    return f"""
MERGE INTO {target_table} AS target
USING category_demographic_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.category_id = source.category_id
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
