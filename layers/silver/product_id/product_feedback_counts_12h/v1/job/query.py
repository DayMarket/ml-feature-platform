from datetime import datetime, timedelta, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

TASHKENT_TIME_ZONE = ZoneInfo("Asia/Tashkent")


class SourceSettings(Protocol):
    feedback_table: str


def _tashkent_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    normalized = value.astimezone(TASHKENT_TIME_ZONE)
    return normalized.strftime("%Y-%m-%d %H:%M:%S")


def build_product_feedback_counts_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    window_start = calculated_at - timedelta(hours=12)
    calculated_at_sql = _tashkent_timestamp_literal(calculated_at)
    window_start_sql = _tashkent_timestamp_literal(window_start)

    return f"""
SELECT
    TIMESTAMP '{calculated_at_sql}' AS calculated_at,
    CAST(product_id AS INT) AS product_id,
    COUNT(rating) AS feedback_count,
    COALESCE(SUM(CAST(rating AS INT)), 0) AS rating_sum,
    COUNT(*) FILTER (
        WHERE CAST(rating AS INT) >= 4
    ) AS feedback_gte_4,
    COUNT(*) FILTER (
        WHERE CAST(rating AS INT) <= 3
    ) AS feedback_lte_3
FROM {settings.feedback_table}
WHERE status = 'PUBLISHED'
    AND CAST(product_id AS INT) > 0
    AND date_published >= TIMESTAMP '{window_start_sql}'
    AND date_published < TIMESTAMP '{calculated_at_sql}'
GROUP BY CAST(product_id AS INT)
"""
