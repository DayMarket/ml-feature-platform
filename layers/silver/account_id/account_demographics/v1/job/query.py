"""Build the daily account demographics query."""

from __future__ import annotations

from datetime import date, timedelta


def _date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _tashkent_timestamp_literal(value: date) -> str:
    return f"TIMESTAMP '{value.isoformat()} 00:00:00 Asia/Tashkent'"


def build_query(
    dt: date,
    customer_table: str,
    ecosystem_users_table: str,
    geo_events_table: str,
    platform_events_table: str,
    city_table: str,
) -> str:
    dt_sql = _date_literal(dt)
    window_start_sql = _tashkent_timestamp_literal(dt - timedelta(days=28))
    window_end_sql = _tashkent_timestamp_literal(dt)

    return f"""
WITH params AS (
    SELECT
        {dt_sql} AS dt,
        {window_start_sql} AS window_start,
        {window_end_sql} AS window_end
),
um AS (
    SELECT
        TRY_CAST(account_id AS INTEGER) AS account_id,
        CASE UPPER(TRIM(CAST(sex AS VARCHAR)))
            WHEN 'MAN' THEN 'M'
            WHEN 'WOMAN' THEN 'F'
        END AS gender
    FROM {customer_table}
    WHERE TRY_CAST(account_id AS INTEGER) > 0
),
ecosystem AS (
    SELECT
        TRY_CAST(last_user_id_m AS INTEGER) AS account_id,
        CASE UPPER(TRIM(CAST(last_gender_ub AS VARCHAR)))
            WHEN 'M' THEN 'M'
            WHEN 'F' THEN 'F'
        END AS gender,
        NULLIF(
            TRY_CAST(birth_year_UB AS DATE),
            DATE '1970-01-01'
        ) AS birth_date
    FROM {ecosystem_users_table}
    WHERE TRY_CAST(last_user_id_m AS INTEGER) > 0
),
demographics AS (
    SELECT
        COALESCE(um.account_id, ecosystem.account_id) AS account_id,
        COALESCE(um.gender, ecosystem.gender) AS gender,
        ecosystem.birth_date
    FROM um
    FULL OUTER JOIN ecosystem
        ON um.account_id = ecosystem.account_id
),
geo_events_raw AS (
    SELECT
        TRY_CAST(event.account_id AS INTEGER) AS account_id,
        AT_TIMEZONE(
            WITH_TIMEZONE(CAST(event.received_at AS TIMESTAMP), 'UTC'),
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
      AND TRY_CAST(event.account_id AS INTEGER) > 0
      AND AT_TIMEZONE(
            WITH_TIMEZONE(CAST(event.received_at AS TIMESTAMP), 'UTC'),
            'Asia/Tashkent'
          ) >= params.window_start
      AND AT_TIMEZONE(
            WITH_TIMEZONE(CAST(event.received_at AS TIMESTAMP), 'UTC'),
            'Asia/Tashkent'
          ) < params.window_end
),
geo_points AS (
    SELECT
        account_id,
        latitude,
        longitude,
        COUNT(*) AS geo_event_count,
        MAX(received_at_tashkent) AS last_received_at
    FROM geo_events_raw
    WHERE latitude BETWEEN -90 AND 90
      AND longitude BETWEEN -180 AND 180
    GROUP BY
        account_id,
        latitude,
        longitude
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
        geo.latitude,
        geo.longitude,
        geo.geo_event_count,
        geo.last_received_at,
        city.city_name,
        ROW_NUMBER() OVER (
            PARTITION BY
                geo.account_id,
                geo.latitude,
                geo.longitude
            ORDER BY
                GREAT_CIRCLE_DISTANCE(
                    geo.latitude,
                    geo.longitude,
                    city.city_latitude,
                    city.city_longitude
                ),
                city.city_name
        ) AS distance_rank
    FROM geo_points geo
    CROSS JOIN city_locations city
),
city_counts AS (
    SELECT
        account_id,
        city_name,
        SUM(geo_event_count) AS geo_event_count,
        MAX(last_received_at) AS last_received_at
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
        TRY_CAST(event.account_id AS INTEGER) AS account_id,
        event.session_id,
        UPPER(TRIM(CAST(event.platform AS VARCHAR))) AS platform,
        AT_TIMEZONE(
            WITH_TIMEZONE(CAST(event.received_at AS TIMESTAMP), 'UTC'),
            'Asia/Tashkent'
        ) AS received_at_tashkent
    FROM {platform_events_table} event
    CROSS JOIN params
    WHERE TRY_CAST(event.account_id AS INTEGER) > 0
      AND event.session_id IS NOT NULL
      AND UPPER(TRIM(CAST(event.platform AS VARCHAR))) IN (
            'IOS',
            'ANDROID',
            'WEB'
          )
      AND AT_TIMEZONE(
            WITH_TIMEZONE(CAST(event.received_at AS TIMESTAMP), 'UTC'),
            'Asia/Tashkent'
          ) >= params.window_start
      AND AT_TIMEZONE(
            WITH_TIMEZONE(CAST(event.received_at AS TIMESTAMP), 'UTC'),
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
ranked_platforms AS (
    SELECT
        account_id,
        platform,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY
                session_count DESC,
                last_received_at DESC,
                platform
        ) AS platform_rank
    FROM platform_counts
),
account_platform AS (
    SELECT
        account_id,
        platform
    FROM ranked_platforms
    WHERE platform_rank = 1
)
SELECT
    params.dt AS dt,
    demographics.account_id,
    demographics.gender,
    CASE
        WHEN demographics.birth_date IS NULL
          OR demographics.birth_date > params.dt
        THEN NULL
        ELSE CAST(
            DATE_DIFF('year', demographics.birth_date, params.dt)
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
