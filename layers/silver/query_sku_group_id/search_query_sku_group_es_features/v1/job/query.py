"""Trino query for search query/SKU group Elasticsearch candidates."""

from __future__ import annotations

from datetime import date, timedelta


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(
    partition_date: date,
    clickstream_events_table: str,
    search_logs_table: str,
    min_result_query_installs: int = 2,
) -> str:
    if min_result_query_installs < 1:
        raise ValueError("min_result_query_installs must be at least 1")

    date_sql = _sql_string(partition_date.isoformat())
    previous_log_date = partition_date - timedelta(days=1)
    next_date = partition_date + timedelta(days=1)
    previous_log_date_sql = _sql_string(previous_log_date.isoformat())
    next_date_sql = _sql_string(next_date.isoformat())
    min_result_query_installs_sql = int(min_result_query_installs)

    return f"""
WITH
clickstream_queries AS (
    SELECT
        lower(trim(replace(query, 'ё', 'е'))) AS query,
        CAST(sku_group_id AS BIGINT) AS sku_group_id
    FROM {clickstream_events_table}
    WHERE logged_at >= CAST({date_sql} AS TIMESTAMP(6))
      AND logged_at < CAST({next_date_sql} AS TIMESTAMP(6))
      AND received_at >= CAST({date_sql} AS TIMESTAMP(6))
      AND received_at < CAST({next_date_sql} AS TIMESTAMP(6))
      AND event_type = 'PRODUCT_IMPRESSION'
      AND widget_space_name = 'SEARCH_RESULTS'
      AND widget_section_name = 'SEARCH_RESULTS'
      AND query IS NOT NULL
      AND trim(query) <> ''
      AND COALESCE(is_full_catpred, false) = false
      AND sku_group_id IS NOT NULL
    GROUP BY
        lower(trim(replace(query, 'ё', 'е'))),
        CAST(sku_group_id AS BIGINT)
),
stats AS (
    SELECT
        query_text,
        corrected_query_text,
        CASE
            WHEN corrected_query_text = '' OR corrected_query_text IS NULL
                THEN lower(trim(replace(query_text, 'ё', 'е')))
            ELSE lower(trim(replace(corrected_query_text, 'ё', 'е')))
        END AS search_query,
        result_query_text,
        install_id
    FROM {search_logs_table}
    WHERE logged_at >= CAST({previous_log_date_sql} AS TIMESTAMP(6))
      AND logged_at < CAST({next_date_sql} AS TIMESTAMP(6))
      AND query_text != ''
),
final_stats AS (
    SELECT
        search_query,
        result_query_text,
        COUNT(DISTINCT install_id) AS installs
    FROM stats
    GROUP BY
        search_query,
        result_query_text
    HAVING
        COUNT(DISTINCT install_id) >= {min_result_query_installs_sql}
)
SELECT
    CAST({date_sql} AS DATE) AS date,
    fs.result_query_text AS query,
    array_agg(DISTINCT cq.sku_group_id) AS sku_group_ids
FROM clickstream_queries cq
INNER JOIN final_stats fs
    ON cq.query = fs.search_query
WHERE fs.result_query_text IS NOT NULL
  AND trim(fs.result_query_text) <> ''
GROUP BY
    fs.result_query_text
"""
