from datetime import datetime, timedelta, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

TASHKENT_TIME_ZONE = ZoneInfo("Asia/Tashkent")


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


def _tashkent_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(TASHKENT_TIME_ZONE).strftime("%Y-%m-%d %H:%M:%S")


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_account_product_session_action_counts_query(
    settings: SourceSettings,
    calculated_at: datetime,
) -> str:
    window_start = calculated_at - timedelta(hours=settings.window_hours)
    calculated_at_utc_sql = _utc_timestamp_literal(calculated_at)
    calculated_at_tashkent_sql = _tashkent_timestamp_literal(calculated_at)
    window_start_utc_sql = _utc_timestamp_literal(window_start)
    event_types_sql = ",\n            ".join(
        _sql_string(event_type)
        for event_type in settings.event_types
    )

    return f"""
WITH filtered_events AS (
    SELECT
        CAST(event.account_id AS INT) AS account_id,
        event.session_id,
        CAST(event.product_id AS INT) AS product_id,
        event.event_type,
        event.received_at
    FROM {settings.events_table} event
    WHERE event.received_at >= TIMESTAMP '{window_start_utc_sql}'
        AND event.received_at < TIMESTAMP '{calculated_at_utc_sql}'
        AND event.event_type IN (
            {event_types_sql}
        )
        AND event.account_id > 0
        AND event.product_id > 0
        AND event.session_id IS NOT NULL
)
SELECT
    TIMESTAMP '{calculated_at_tashkent_sql}' AS calculated_at,
    account_id,
    session_id,
    product_id,
    event_type,
    COUNT(*) AS n_events,
    FROM_UTC_TIMESTAMP(
        MAX(received_at),
        'Asia/Tashkent'
    ) AS last_received_at
FROM filtered_events
GROUP BY
    account_id,
    session_id,
    product_id,
    event_type
"""


def build_account_product_session_action_counts_merge_query(
    target_table: str,
    calculated_at: datetime,
) -> str:
    calculated_at_sql = _tashkent_timestamp_literal(calculated_at)

    return f"""
MERGE INTO {target_table} AS target
USING account_product_session_action_counts_for_calculated_at AS source
    ON target.calculated_at = source.calculated_at
    AND target.account_id = source.account_id
    AND target.session_id = source.session_id
    AND target.product_id = source.product_id
    AND target.event_type = source.event_type
WHEN MATCHED THEN UPDATE SET
    target.n_events = source.n_events,
    target.last_received_at = source.last_received_at
WHEN NOT MATCHED THEN INSERT (
    calculated_at,
    account_id,
    session_id,
    product_id,
    event_type,
    n_events,
    last_received_at
) VALUES (
    source.calculated_at,
    source.account_id,
    source.session_id,
    source.product_id,
    source.event_type,
    source.n_events,
    source.last_received_at
)
WHEN NOT MATCHED BY SOURCE
    AND target.calculated_at = TIMESTAMP '{calculated_at_sql}'
THEN DELETE
"""
