from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

FEATURE_COLUMNS = (
    "category_female_product_session_share_28d",
    "category_male_product_session_share_28d",
    "n_unique_known_gender_clickers_28d",
    "n_unique_female_clickers_28d",
    "n_unique_male_clickers_28d",
    "category_gender",
)


class SourceSettings(Protocol):
    product_metadata_table: str
    action_counts_table: str
    demographics_table: str
    category_genders_table: str
    business_timezone: str
    lookback_days: int


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


def build_l6_category_gender_features_query(
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

    return f"""
WITH product_metadata AS (
    SELECT
        CAST(product_id AS INT) AS product_id,
        CAST(l6_category_id AS INT) AS l6_category_id
    FROM {settings.product_metadata_table}
    WHERE dt = TIMESTAMP '{metadata_snapshot_utc}'
),
demographics AS (
    SELECT
        CAST(account_id AS INT) AS account_id,
        gender AS account_gender
    FROM {settings.demographics_table}
    WHERE dt = TIMESTAMP '{demographics_snapshot_local}'
),
category_genders AS (
    SELECT
        CAST(category_id AS INT) AS l6_category_id,
        CASE
            WHEN dominant_gender IN ('M', 'F', 'U') THEN dominant_gender
        END AS category_gender
    FROM {settings.category_genders_table}
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
        metadata.l6_category_id,
        demographics.account_gender
    FROM deduplicated_product_views views
    INNER JOIN product_metadata metadata
        ON views.product_id = metadata.product_id
    LEFT JOIN demographics
        ON views.account_id = demographics.account_id
    WHERE metadata.l6_category_id IS NOT NULL
),
category_statistics AS (
    SELECT
        l6_category_id,
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
        ) AS category_female_product_session_share_28d,
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
        ) AS category_male_product_session_share_28d,
        CAST(
            COUNT(
                DISTINCT CASE
                    WHEN account_gender IN ('MALE', 'FEMALE') THEN account_id
                END
            ) AS INT
        ) AS n_unique_known_gender_clickers_28d,
        CAST(
            COUNT(DISTINCT CASE WHEN account_gender = 'FEMALE' THEN account_id END)
            AS INT
        ) AS n_unique_female_clickers_28d,
        CAST(
            COUNT(DISTINCT CASE WHEN account_gender = 'MALE' THEN account_id END)
            AS INT
        ) AS n_unique_male_clickers_28d
    FROM enriched_product_views
    GROUP BY l6_category_id
)
SELECT
    TIMESTAMP '{calculated_at_local}' AS calculated_at,
    statistics.l6_category_id,
    statistics.category_female_product_session_share_28d,
    statistics.category_male_product_session_share_28d,
    statistics.n_unique_known_gender_clickers_28d,
    statistics.n_unique_female_clickers_28d,
    statistics.n_unique_male_clickers_28d,
    genders.category_gender
FROM category_statistics statistics
LEFT JOIN category_genders genders
    ON statistics.l6_category_id = genders.l6_category_id
"""


def build_l6_category_gender_features_merge_query(
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
        ("calculated_at", "l6_category_id", *FEATURE_COLUMNS)
    )
    source_columns = ",\n    ".join(
        f"source.{column}"
        for column in ("calculated_at", "l6_category_id", *FEATURE_COLUMNS)
    )

    return f"""
MERGE INTO {target_table} AS target
USING l6_category_gender_features_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.l6_category_id = source.l6_category_id
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
