"""Trino query for SEARCH_RESULTS queries of one day that have no query_id yet."""

from __future__ import annotations

from datetime import date


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_new_queries_query(
    partition_date: date,
    install_query_table: str,
    query_id_table: str,
    space: str,
    version: str,
) -> str:
    """Anti-join against the gold table so already normalized queries are not re-analyzed."""
    return f"""
SELECT DISTINCT install_query.uniqs AS original_query
FROM {install_query_table} AS install_query
LEFT JOIN {query_id_table} AS known_query
    ON known_query.query_text = install_query.uniqs
   AND known_query.version = {_sql_string(version)}
WHERE install_query.date = DATE {_sql_string(partition_date.isoformat())}
  AND install_query.space = {_sql_string(space)}
  AND install_query.uniqs IS NOT NULL
  AND known_query.query_text IS NULL
"""
