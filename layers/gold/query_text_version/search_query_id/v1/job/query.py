"""Trino query for search queries of the trailing window that have no query_id yet."""

from __future__ import annotations

from datetime import date, timedelta


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _utc_timestamp(day: date) -> str:
    """Literal with an explicit zone: logged_at is `timestamp with time zone`, and the
    Trino session runs in Europe/Moscow, so a bare TIMESTAMP silently shifts the window."""
    return f"TIMESTAMP '{day.isoformat()} 00:00:00 UTC'"


def build_new_queries_query(
    partition_date: date,
    search_logs_table: str,
    query_id_table: str,
    version: str,
    lookback_days: int,
    short_query_max_length: int,
    short_query_min_installs: int,
    long_query_min_installs: int,
) -> str:
    """Candidates are distinct service queries of the trailing window that pass the
    install thresholds; the anti-join keeps already normalized queries out."""
    if lookback_days < 1:
        raise ValueError("lookback_days must be at least 1")
    if short_query_max_length < 1:
        raise ValueError("short_query_max_length must be at least 1")
    if short_query_min_installs < 1 or long_query_min_installs < 1:
        raise ValueError("install thresholds must be at least 1")

    # Окно закрытое и считается от партиции, а не от now(): перезапуск за ту же дату
    # обязан дать тот же набор кандидатов.
    window_end = partition_date + timedelta(days=1)
    window_start = window_end - timedelta(days=int(lookback_days))

    return f"""
WITH
service_queries AS (
    SELECT
        install_id,
        CASE
            WHEN corrected_query_text IS NULL OR corrected_query_text = ''
                THEN query_text
            ELSE corrected_query_text
        END AS service_query
    FROM {search_logs_table}
    WHERE logged_at >= {_utc_timestamp(window_start)}
      AND logged_at < {_utc_timestamp(window_end)}
      AND query_text != ''
      AND pagination_offset = 0
),
candidates AS (
    SELECT
        service_query,
        COUNT(DISTINCT install_id) AS installs
    FROM service_queries
    WHERE service_query IS NOT NULL
      AND service_query <> ''
    GROUP BY service_query
)
SELECT candidate.service_query AS original_query
FROM candidates AS candidate
LEFT JOIN {query_id_table} AS known_query
    ON known_query.query_text = candidate.service_query
   AND known_query.version = {_sql_string(version)}
WHERE known_query.query_text IS NULL
  AND (
        (LENGTH(candidate.service_query) <= {int(short_query_max_length)}
         AND candidate.installs > {int(short_query_min_installs)})
     OR (LENGTH(candidate.service_query) > {int(short_query_max_length)}
         AND candidate.installs > {int(long_query_min_installs)})
  )
"""
