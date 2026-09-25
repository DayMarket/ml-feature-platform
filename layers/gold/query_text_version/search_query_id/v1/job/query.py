"""Trino query for search queries without a query_id yet.

Источников два: дневная партиция silver-предагрегата поисковых событий и окно
ranking analytics events за последние `lookback_days` суток. Второй источник
нужен, потому что предагрегат к текущему моменту почти исчерпан как поставщик
новизны (единицы-сотни новых формулировок в день против десятков тысяч у
ranking-логов), а нахлёст — потому что у ranking analytics events нет своего
DQ-контракта и поздние данные ловить больше нечем.
"""

from __future__ import annotations

from datetime import date, timedelta


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _timestamp_literal(day: date) -> str:
    return _sql_string(f"{day.isoformat()} 00:00:00")


def build_new_queries_query(
    partition_date: date,
    install_query_table: str,
    ranking_events_table: str,
    query_id_table: str,
    space: str,
    model_name_like: str,
    lookback_days: int,
    version: str,
) -> str:
    """Anti-join against the gold table so already normalized queries are not re-analyzed."""
    lookback_days = int(lookback_days)
    if lookback_days < 1:
        raise ValueError(f"lookback_days must be at least 1, got {lookback_days}")

    # Окно включает сам день интервала, поэтому lookback_days=1 — это ровно он.
    window_start = partition_date - timedelta(days=lookback_days - 1)
    window_end = partition_date + timedelta(days=1)

    return f"""
WITH source_queries AS (
    SELECT DISTINCT install_query.uniqs AS original_query
    FROM {install_query_table} AS install_query
    WHERE install_query.date = DATE {_sql_string(partition_date.isoformat())}
      AND install_query.space = {_sql_string(space)}
      AND install_query.uniqs IS NOT NULL
UNION
    SELECT DISTINCT ranking_events.search_query AS original_query
    FROM {ranking_events_table} AS ranking_events
    WHERE ranking_events.fired_at >= TIMESTAMP {_timestamp_literal(window_start)}
      AND ranking_events.fired_at < TIMESTAMP {_timestamp_literal(window_end)}
      AND ranking_events.model_name LIKE {_sql_string(model_name_like)}
      AND ranking_events.search_query IS NOT NULL
      AND ranking_events.search_query <> ''
)
SELECT source_queries.original_query
FROM source_queries
LEFT JOIN {query_id_table} AS known_query
    ON known_query.query_text = source_queries.original_query
   AND known_query.version = {_sql_string(version)}
WHERE known_query.query_text IS NULL
"""
