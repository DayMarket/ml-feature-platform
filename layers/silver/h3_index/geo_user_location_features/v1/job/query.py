"""ClickHouse query for silver.feature_platform_geo_user_location_features.

Per H3 hex (res 9), number of users by ring 0..5 on the latest available
geo_client_hist snapshot at or before the calc date.

Source (external, ClickHouse): gold.geo_client_hist.

Mirrors the inline inference query ``user_location_features_full`` (already a
full ring 0..5 snapshot); the actual snapshot date is exposed as report_date.
"""

from __future__ import annotations

from typing import Dict, Tuple

RINGS = [0, 1, 2, 3, 4, 5]


def _ring_aggregations() -> str:
    return ",\n".join(
        f"            toInt64(sumIf(coalesce(ud.users, 0), hr.ring <= {r})) AS users_r{r}"
        for r in RINGS
    )


SQL = f"""
WITH
    9 AS hex_resolution,
    toDate(%(date_to)s) AS calc_date,
    latest_report_date AS (
        SELECT max(report_date)
        FROM gold.geo_client_hist
        WHERE report_date <= calc_date
    ),
    all_center_hexes AS (
        SELECT DISTINCT h3_9 AS center_h3
        FROM gold.geo_client_hist
        WHERE report_date = (SELECT * FROM latest_report_date)
    ),
    hex_rings AS (
        SELECT
            center_h3,
            ring_h3 AS h3_index,
            h3Distance(center_h3, ring_h3) - 1 AS ring
        FROM all_center_hexes
        ARRAY JOIN h3kRing(center_h3, 5) AS ring_h3
    ),
    users_daily AS (
        SELECT
            h3_9 AS h3_index,
            count(1) AS users
        FROM gold.geo_client_hist
        WHERE report_date = (SELECT * FROM latest_report_date)
        GROUP BY h3_9
    ),
    daily_ring_features AS (
        SELECT
            hr.center_h3,
{_ring_aggregations()}
        FROM hex_rings hr
        LEFT JOIN users_daily ud ON ud.h3_index = hr.h3_index
        GROUP BY hr.center_h3
    )
    SELECT
        toInt64(center_h3) AS h3_index,
        (SELECT * FROM latest_report_date) AS report_date,
        users_r0,
        users_r1,
        users_r2,
        users_r3,
        users_r4,
        users_r5
    FROM daily_ring_features
"""


def build_query(calc_date: str) -> Tuple[str, Dict[str, str]]:
    return SQL, {"date_to": calc_date}
