"""Trino query for search query/SKU group Elasticsearch candidates."""

from __future__ import annotations

from datetime import date, timedelta


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(
    partition_date: date,
    dataset_table: str,
    search_logs_table: str,
) -> str:
    date_sql = _sql_string(partition_date.isoformat())
    previous_date = partition_date - timedelta(days=1)
    next_date = partition_date + timedelta(days=1)
    previous_date_sql = _sql_string(previous_date.isoformat())
    next_date_sql = _sql_string(next_date.isoformat())

    return f"""
WITH
dataset_queries AS (
    SELECT DISTINCT
        lower(trim(replace(query, 'ё', 'е'))) AS query,
        CAST(sku_group_id AS BIGINT) AS sku_group_id
    FROM {dataset_table}
    WHERE event_date = CAST({date_sql} AS DATE)
      AND query IS NOT NULL
      AND trim(query) <> ''
      AND sku_group_id IS NOT NULL
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
    WHERE logged_at >= CAST({previous_date_sql} AS TIMESTAMP(6))
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
)
SELECT
    CAST({date_sql} AS DATE) AS date,
    fs.result_query_text AS query,
    array_agg(DISTINCT dq.sku_group_id) AS sku_group_ids
FROM dataset_queries dq
LEFT JOIN final_stats fs
    ON dq.query = fs.search_query
WHERE fs.result_query_text IS NOT NULL
  AND trim(fs.result_query_text) <> ''
GROUP BY
    fs.result_query_text
"""
