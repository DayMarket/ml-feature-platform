"""Build the daily account demographics query."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


TASHKENT_TIME_ZONE = ZoneInfo("Asia/Tashkent")
GEO_H3_RESOLUTION = 7
GEO_CITY_CANDIDATES = 5


def _date_literal(value: date) -> str:
    return f"DATE '{value.isoformat()}'"


def _timestamp_literal(value: datetime) -> str:
    return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='seconds')}'"


def _tashkent_timestamp_literal(value: date) -> str:
    return f"TIMESTAMP '{value.isoformat()} 00:00:00 Asia/Tashkent'"


def _clickhouse_utc_datetime(value: date) -> str:
    local_midnight = datetime.combine(
        value,
        time.min,
        tzinfo=TASHKENT_TIME_ZONE,
    )
    utc_value = local_midnight.astimezone(timezone.utc)
    formatted = utc_value.strftime("%Y-%m-%d %H:%M:%S")
    return f"toDateTime('{formatted}', 'UTC')"


def _trino_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_city_counts_query(
    geo_events_table: str,
    city_table: str,
    window_start_date: date,
    calculation_date: date,
) -> str:
    window_start = _clickhouse_utc_datetime(window_start_date)
    window_end = _clickhouse_utc_datetime(calculation_date)
    return f"""
WITH geo_events AS (
    SELECT
        account_id,
        received_at,
        assumeNotNull(latitude) AS latitude,
        assumeNotNull(longitude) AS longitude,
        geoToH3(
            assumeNotNull(latitude),
            assumeNotNull(longitude),
            {GEO_H3_RESOLUTION}
        ) AS h3
    FROM (
        SELECT
            event_id,
            account_id,
            received_at,
            toFloat64OrNull(
                JSONExtractString(
                    event_properties,
                    'event_parameters',
                    'latitude'
                )
            ) AS latitude,
            toFloat64OrNull(
                JSONExtractString(
                    event_properties,
                    'event_parameters',
                    'longitude'
                )
            ) AS longitude
        FROM {geo_events_table}
        PREWHERE event_type = 'GEO_INFO'
          AND received_at >= {window_start}
          AND received_at < {window_end}
    ) AS parsed
    WHERE event_id IS NOT NULL
      AND account_id > 0
      AND latitude BETWEEN -90 AND 90
      AND longitude BETWEEN -180 AND 180
),
cities AS (
    SELECT
        city_ru_name,
        toFloat64(city_latitude) AS city_latitude,
        toFloat64(city_longitude) AS city_longitude
    FROM {city_table}
    WHERE city_ru_name != ''
      AND city_latitude BETWEEN -90 AND 90
      AND city_longitude BETWEEN -180 AND 180
),
candidate_rows AS (
    SELECT
        cells.h3,
        cities.city_ru_name,
        cities.city_latitude,
        cities.city_longitude
    FROM (
        SELECT DISTINCT h3
        FROM geo_events
    ) AS cells
    CROSS JOIN cities
    ORDER BY
        cells.h3,
        greatCircleDistance(
            h3ToGeo(cells.h3).2,
            h3ToGeo(cells.h3).1,
            cities.city_longitude,
            cities.city_latitude
        ),
        cities.city_ru_name
    LIMIT {GEO_CITY_CANDIDATES} BY cells.h3
),
h3_candidates AS (
    SELECT
        h3,
        groupArray(
            (city_ru_name, city_latitude, city_longitude)
        ) AS candidates
    FROM candidate_rows
    GROUP BY h3
),
event_cities AS (
    SELECT
        geo.account_id,
        geo.received_at,
        arrayMin(
            arrayMap(
                city -> (
                    greatCircleDistance(
                        geo.longitude,
                        geo.latitude,
                        city.3,
                        city.2
                    ),
                    city.1
                ),
                map.candidates
            )
        ).2 AS city_name
    FROM geo_events AS geo
    INNER JOIN h3_candidates AS map
        ON geo.h3 = map.h3
),
city_counts AS (
    SELECT
        account_id,
        city_name,
        count() AS geo_event_count,
        max(received_at) AS last_received_at
    FROM event_cities
    GROUP BY
        account_id,
        city_name
)
SELECT
    account_id,
    city_name,
    geo_event_count,
    last_received_at
FROM city_counts
""".strip()


def _date_folds(
    window_start_date: date,
    calculation_date: date,
    fold_days: int,
) -> list[tuple[date, date]]:
    folds = []
    fold_start = window_start_date
    while fold_start < calculation_date:
        fold_end = min(
            fold_start + timedelta(days=fold_days),
            calculation_date,
        )
        folds.append((fold_start, fold_end))
        fold_start = fold_end
    return folds


def _build_account_city_ctes(
    clickhouse_catalog: str,
    geo_events_table: str,
    city_table: str,
    window_start_date: date,
    calculation_date: date,
    geo_fold_days: int,
) -> str:
    fold_names = []
    fold_ctes = []
    for index, (fold_start, fold_end) in enumerate(
        _date_folds(
            window_start_date,
            calculation_date,
            geo_fold_days,
        )
    ):
        fold_name = f"geo_fold_{index}"
        fold_query = _build_city_counts_query(
            geo_events_table=geo_events_table,
            city_table=city_table,
            window_start_date=fold_start,
            calculation_date=fold_end,
        )
        fold_names.append(fold_name)
        fold_ctes.append(
            f"""{fold_name} AS (
    SELECT
        account_id,
        CAST(city_name AS VARCHAR) AS city_name,
        geo_event_count,
        last_received_at
    FROM TABLE(
        {clickhouse_catalog}.system.query(
            query => {_trino_string_literal(fold_query)}
        )
    )
)"""
        )

    union_query = "\n    UNION ALL\n    ".join(
        f"SELECT * FROM {fold_name}" for fold_name in fold_names
    )
    fold_ctes_sql = ",\n".join(fold_ctes)
    return f"""
{fold_ctes_sql},
geo_city_counts AS (
    SELECT
        account_id,
        city_name,
        SUM(geo_event_count) AS geo_event_count,
        MAX(last_received_at) AS last_received_at
    FROM (
        {union_query}
    ) AS folds
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
    FROM geo_city_counts
),
account_city AS (
    SELECT
        account_id,
        city_name
    FROM ranked_cities
    WHERE city_rank = 1
)
""".strip()


def build_query(
    dt: datetime,
    customer_table: str,
    ecosystem_users_table: str,
    clickhouse_catalog: str,
    geo_events_table: str,
    platform_sessions_table: str,
    city_table: str,
    history_table: str,
    lookback_days: int,
    geo_fold_days: int,
) -> str:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if geo_fold_days <= 0:
        raise ValueError("geo_fold_days must be positive")

    calculation_date = dt.date()
    dt_sql = _timestamp_literal(dt)
    calculation_date_sql = _date_literal(calculation_date)
    window_start_date = calculation_date - timedelta(days=lookback_days)
    window_start_date_sql = _date_literal(window_start_date)
    window_start_sql = _tashkent_timestamp_literal(window_start_date)
    window_end_sql = _tashkent_timestamp_literal(calculation_date)
    account_city_ctes = _build_account_city_ctes(
        clickhouse_catalog=clickhouse_catalog,
        geo_events_table=geo_events_table,
        city_table=city_table,
        window_start_date=window_start_date,
        calculation_date=calculation_date,
        geo_fold_days=geo_fold_days,
    )

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
{account_city_ctes},
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
),
previous_dt AS (
    SELECT
        MAX(history.dt) AS dt
    FROM {history_table} history
    CROSS JOIN params
    WHERE history.dt < params.dt
),
previous_context AS (
    SELECT
        history.account_id,
        history.city_name,
        history.platform
    FROM {history_table} history
    INNER JOIN previous_dt
        ON history.dt = previous_dt.dt
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
    COALESCE(
        account_city.city_name,
        previous_context.city_name
    ) AS city_name,
    COALESCE(
        account_platform.platform,
        previous_context.platform
    ) AS platform
FROM demographics
CROSS JOIN params
LEFT JOIN account_city
    ON demographics.account_id = account_city.account_id
LEFT JOIN account_platform
    ON demographics.account_id = account_platform.account_id
LEFT JOIN previous_context
    ON demographics.account_id = previous_context.account_id
"""
