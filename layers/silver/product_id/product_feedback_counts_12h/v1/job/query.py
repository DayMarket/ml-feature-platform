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
    COUNT(*) FILTER (WHERE rating = 1) AS n_feedbacks_1,
    COUNT(*) FILTER (WHERE rating = 2) AS n_feedbacks_2,
    COUNT(*) FILTER (WHERE rating = 3) AS n_feedbacks_3,
    COUNT(*) FILTER (WHERE rating = 4) AS n_feedbacks_4,
    COUNT(*) FILTER (WHERE rating = 5) AS n_feedbacks_5,
    COUNT(rating) AS _n_feedbacks_with_rating
FROM {settings.feedback_table}
WHERE status = 'PUBLISHED'
    AND CAST(product_id AS INT) > 0
    AND date_published >= TIMESTAMP '{window_start_sql}'
    AND date_published < TIMESTAMP '{calculated_at_sql}'
GROUP BY CAST(product_id AS INT)
"""
