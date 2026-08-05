from datetime import datetime, timedelta, timezone
from typing import Protocol


class SourceSettings(Protocol):
    events_table: str
    event_types: tuple[str, ...]
    window_hours: int


def _utc_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        normalized = value.replace(tzinfo=timezone.utc)
    else:
        normalized = value.astimezone(timezone.utc)
    return normalized.strftime("%Y-%m-%d %H:%M:%S")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_account_product_session_action_counts_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    window_start = calculated_at - timedelta(hours=settings.window_hours)
    calculated_at_sql = _utc_timestamp_literal(calculated_at)
    window_start_sql = _utc_timestamp_literal(window_start)
    event_types_sql = ",\n            ".join(
        _sql_string(event_type)
        for event_type in settings.event_types
    )

    return f"""
WITH filtered_events AS (
    SELECT
        CAST(event.account_id AS BIGINT) AS account_id,
        CAST(event.session_id AS STRING) AS session_id,
        CAST(event.product_id AS BIGINT) AS product_id,
        CAST(event.event_type AS STRING) AS event_type,
        CAST(event.received_at AS TIMESTAMP) AS received_at
    FROM {settings.events_table} event
    WHERE event.received_at >= TIMESTAMP '{window_start_sql}'
        AND event.received_at < TIMESTAMP '{calculated_at_sql}'
        AND event.event_type IN (
            {event_types_sql}
        )
        AND CAST(event.account_id AS BIGINT) > 0
        AND CAST(event.product_id AS BIGINT) > 0
        AND event.session_id IS NOT NULL
)
SELECT
    TIMESTAMP '{calculated_at_sql}' AS calculated_at,
    account_id,
    session_id,
    product_id,
    event_type,
    CAST(COUNT(*) AS BIGINT) AS n_events,
    CAST(MAX(received_at) AS TIMESTAMP) AS last_received_at
FROM filtered_events
GROUP BY
    account_id,
    session_id,
    product_id,
    event_type
"""
