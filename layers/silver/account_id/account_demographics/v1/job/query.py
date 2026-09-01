"""Build the daily account demographics query."""

from __future__ import annotations

from datetime import date, datetime, timedelta


def _date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _timestamp_literal(value: datetime) -> str:
    return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='seconds')}'"


def _tashkent_timestamp_literal(value: date) -> str:
    return f"TIMESTAMP '{value.isoformat()} 00:00:00 Asia/Tashkent'"


def build_query(
    dt: datetime,
    customer_table: str,
    ecosystem_users_table: str,
    geo_events_table: str,
    platform_sessions_table: str,
    city_table: str,
    lookback_days: int,
) -> str:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    calculation_date = dt.date()
    dt_sql = _timestamp_literal(dt)
    calculation_date_sql = _date_literal(calculation_date)
    window_start_date = calculation_date - timedelta(days=lookback_days)
    window_start_date_sql = _date_literal(window_start_date)
    window_start_sql = _tashkent_timestamp_literal(window_start_date)
    window_end_sql = _tashkent_timestamp_literal(calculation_date)

    return f"""
WITH params AS (
    SELECT
        {dt_sql} AS dt,
        {calculation_date_sql} AS calculation_date,
        {window_start_date_sql} AS window_start_date,
        {window_start_sql} AS window_start,
        {window_end_sql} AS window_end
),
um AS (
    SELECT
        account_id,
        CASE sex
            WHEN 'MAN' THEN 'MALE'
            WHEN 'WOMAN' THEN 'FEMALE'
        END AS gender,
        NULLIF(
            CAST(AT_TIMEZONE(birth_date, 'Asia/Tashkent') AS DATE),
            DATE '1970-01-01'
        ) AS birth_date
    FROM {customer_table}
    WHERE account_id > 0
),
ecosystem AS (
    SELECT
        last_user_id_m AS account_id,
        CASE last_gender_ub
            WHEN 'M' THEN 'MALE'
            WHEN 'F' THEN 'FEMALE'
        END AS gender
    FROM {ecosystem_users_table}
    WHERE last_user_id_m > 0
),
demographics AS (
    SELECT
        COALESCE(um.account_id, ecosystem.account_id) AS account_id,
        COALESCE(um.gender, ecosystem.gender) AS gender,
        um.birth_date
    FROM um
    FULL OUTER JOIN ecosystem
        ON um.account_id = ecosystem.account_id
),
geo_events_raw AS (
    SELECT
        event.event_id,
        event.account_id,
        AT_TIMEZONE(
            event.received_at,
            'Asia/Tashkent'
        ) AS received_at_tashkent,
        TRY_CAST(
            JSON_EXTRACT_SCALAR(
                CAST(event.event_properties AS VARCHAR),
                '$.event_parameters.latitude'
            ) AS DOUBLE
        ) AS latitude,
        TRY_CAST(
            JSON_EXTRACT_SCALAR(
                CAST(event.event_properties AS VARCHAR),
                '$.event_parameters.longitude'
            ) AS DOUBLE
        ) AS longitude
    FROM {geo_events_table} event
    CROSS JOIN params
    WHERE event.event_type = 'GEO_INFO'
      AND event.event_id IS NOT NULL
      AND event.account_id > 0
      AND AT_TIMEZONE(
            event.received_at,
            'Asia/Tashkent'
          ) >= params.window_start
      AND AT_TIMEZONE(
            event.received_at,
            'Asia/Tashkent'
          ) < params.window_end
),
valid_geo_events AS (
    SELECT
        event_id,
        account_id,
        received_at_tashkent,
        latitude,
        longitude
    FROM geo_events_raw
    WHERE latitude BETWEEN -90 AND 90
      AND longitude BETWEEN -180 AND 180
),
city_locations AS (
    SELECT
        CAST(city_ru_name AS VARCHAR) AS city_name,
        TRY_CAST(city_latitude AS DOUBLE) AS city_latitude,
        TRY_CAST(city_longitude AS DOUBLE) AS city_longitude
    FROM {city_table}
    WHERE city_ru_name IS NOT NULL
      AND TRY_CAST(city_latitude AS DOUBLE) BETWEEN -90 AND 90
      AND TRY_CAST(city_longitude AS DOUBLE) BETWEEN -180 AND 180
),
city_candidates AS (
    SELECT
        geo.account_id,
        geo.event_id,
        geo.received_at_tashkent,
        city.city_name,
        ROW_NUMBER() OVER (
            PARTITION BY
                geo.account_id,
                geo.event_id
            ORDER BY
                GREAT_CIRCLE_DISTANCE(
                    geo.latitude,
                    geo.longitude,
                    city.city_latitude,
                    city.city_longitude
                ),
                city.city_name
        ) AS distance_rank
    FROM valid_geo_events geo
    CROSS JOIN city_locations city
),
city_counts AS (
    SELECT
        account_id,
        city_name,
        COUNT(*) AS geo_event_count,
        MAX(received_at_tashkent) AS last_received_at
    FROM city_candidates
    WHERE distance_rank = 1
    GROUP BY
        account_id,
        city_name
),
ranked_cities AS (
    SELECT
        account_id,
        city_name,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY
                geo_event_count DESC,
                last_received_at DESC,
                city_name
        ) AS city_rank
    FROM city_counts
),
account_city AS (
    SELECT
        account_id,
        city_name
    FROM ranked_cities
    WHERE city_rank = 1
),
platform_events AS (
    SELECT
        session.account_id,
        session.session_id,
        UPPER(TRIM(session.platform)) AS platform,
        AT_TIMEZONE(
            session.started_at,
            'Asia/Tashkent'
        ) AS received_at_tashkent
    FROM {platform_sessions_table} session
    CROSS JOIN params
    WHERE session.account_id > 0
      AND session.session_id IS NOT NULL
      AND UPPER(TRIM(session.platform)) IN (
            'IOS',
            'ANDROID',
            'WEB'
          )
      AND session.date_uz >= params.window_start_date
      AND session.date_uz < params.calculation_date
      AND AT_TIMEZONE(
            session.started_at,
            'Asia/Tashkent'
          ) >= params.window_start
      AND AT_TIMEZONE(
            session.started_at,
            'Asia/Tashkent'
          ) < params.window_end
),
platform_counts AS (
    SELECT
        account_id,
        platform,
        COUNT(DISTINCT session_id) AS session_count,
        MAX(received_at_tashkent) AS last_received_at
    FROM platform_events
    GROUP BY
        account_id,
        platform
),
account_platform AS (
    SELECT
        account_id,
        ARRAY_AGG(
            platform
            ORDER BY session_count DESC, last_received_at DESC, platform
        )[1] AS platform
    FROM platform_counts
    GROUP BY account_id
)
SELECT
    params.dt AS dt,
    demographics.account_id,
    demographics.gender,
    CASE
        WHEN demographics.birth_date IS NULL
          OR demographics.birth_date > params.calculation_date
        THEN NULL
        ELSE CAST(
            DATE_DIFF('year', demographics.birth_date, params.calculation_date)
            AS INTEGER
        )
    END AS age,
    account_city.city_name,
    account_platform.platform
FROM demographics
CROSS JOIN params
LEFT JOIN account_city
    ON demographics.account_id = account_city.account_id
LEFT JOIN account_platform
    ON demographics.account_id = account_platform.account_id
"""
