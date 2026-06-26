"""ClickHouse query for silver.feature_platform_geo_user_activity_features.

Per H3 hex (res 9), user activity (views/orders) aggregated over rings 0..5 and
trailing windows 30/60/90 days ending at the calc date.

Source (external, ClickHouse): silver.client_geo_activity_hex_9.

Extended variant of the inline inference query ``user_activity_features_full``:
the production query kept only a few ring/window combinations; here we compute
the full grid rings 0..5 x windows 30/60/90 for both views and orders.
"""

from __future__ import annotations

from typing import Dict, Tuple

RINGS = [0, 1, 2, 3, 4, 5]
WINDOWS = [30, 60, 90]


def _ring_day_aggregations() -> str:
    lines = []
    for r in RINGS:
        lines.append(
            f"                sumIf(coalesce(sd.daily_views, 0),  hr.ring <= {r}) AS views_r{r}_day"
        )
    for r in RINGS:
        lines.append(
            f"                sumIf(coalesce(sd.daily_orders, 0), hr.ring <= {r}) AS orders_r{r}_day"
        )
    return ",\n".join(lines)


def _window_aggregations() -> str:
    lines = []
    for r in RINGS:
        for w in WINDOWS:
            lines.append(
                f"        toInt64(sumIf(views_r{r}_day, day_diff BETWEEN 0 AND {w - 1})) AS views_r{r}_{w}d"
            )
    for r in RINGS:
        for w in WINDOWS:
            lines.append(
                f"        toInt64(sumIf(orders_r{r}_day, day_diff BETWEEN 0 AND {w - 1})) AS orders_r{r}_{w}d"
            )
    return ",\n".join(lines)


SQL = f"""
WITH
    9 AS hex_resolution,
    toDate(%(date_to)s) AS calc_date,
    all_center_hexes AS (
        SELECT DISTINCT h3_index AS center_h3
        FROM silver.client_geo_activity_hex_9
        WHERE day BETWEEN addDays(calc_date, -89) AND calc_date
    ),
    hex_rings AS (
        SELECT
            center_h3,
            ring_h3,
            h3Distance(center_h3, ring_h3) - 1 AS ring
        FROM all_center_hexes
        ARRAY JOIN h3kRing(center_h3, 5) AS ring_h3
    ),
    sandbox_daily AS (
        SELECT
            sg.day,
            sg.h3_index,
            sum(sg.daily_views) AS daily_views,
            sum(sg.daily_orders) AS daily_orders
        FROM silver.client_geo_activity_hex_9 sg
        WHERE sg.day BETWEEN addDays(calc_date, -89) AND calc_date
        GROUP BY sg.day, sg.h3_index
    ),
    daily_ring_features AS (
        SELECT
            hr.center_h3,
            sd.day,
            dateDiff('day', sd.day, calc_date) AS day_diff,
{_ring_day_aggregations()}
        FROM sandbox_daily sd
        INNER JOIN hex_rings hr ON hr.ring_h3 = sd.h3_index
        GROUP BY hr.center_h3, sd.day, day_diff
    )
    SELECT
        toInt64(center_h3) AS h3_index,
{_window_aggregations()}
    FROM daily_ring_features
    GROUP BY center_h3
"""


def build_query(calc_date: str) -> Tuple[str, Dict[str, str]]:
    return SQL, {"date_to": calc_date}
