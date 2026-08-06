from datetime import datetime, timedelta, timezone
from typing import Protocol


class SourceSettings(Protocol):
    feedback_table: str
    published_status: str
    window_hours: int
    min_rating: int
    max_rating: int
    positive_rating_min: int
    negative_rating_max: int


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        normalized = value.replace(tzinfo=timezone.utc)
    else:
        normalized = value.astimezone(timezone.utc)
    return normalized.strftime("%Y-%m-%d %H:%M:%S")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_product_feedback_counts_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    window_start = calculated_at - timedelta(hours=settings.window_hours)
    calculated_at_sql = _utc_timestamp_literal(calculated_at)
    window_start_sql = _utc_timestamp_literal(window_start)
    published_status_sql = _sql_string(settings.published_status)

    return f"""
SELECT
    TIMESTAMP '{calculated_at_sql}' AS calculated_at,
    CAST(product_id AS BIGINT) AS product_id,
    CAST(COUNT(rating) AS BIGINT) AS feedback_count,
    CAST(COALESCE(SUM(CAST(rating AS BIGINT)), 0) AS BIGINT) AS rating_sum,
    CAST(
        COUNT(*) FILTER (
            WHERE CAST(rating AS INT) >= {settings.positive_rating_min}
        ) AS BIGINT
    ) AS feedback_gte_4,
    CAST(
        COUNT(*) FILTER (
            WHERE CAST(rating AS INT) <= {settings.negative_rating_max}
        ) AS BIGINT
    ) AS feedback_lte_3
FROM {settings.feedback_table}
WHERE status = {published_status_sql}
    AND CAST(product_id AS BIGINT) > 0
    AND date_published >= TIMESTAMP '{window_start_sql}'
    AND date_published < TIMESTAMP '{calculated_at_sql}'
GROUP BY CAST(product_id AS BIGINT)
"""
