"""ClickHouse query for silver.feature_platform_dp_neighbor_order_features.

Per H3 hex (res 9), neighbouring delivery-point order/GMV aggregates and the
physical distance to the nearest competitor PVZ / in-shop point.

Source (external, ClickHouse): marts.order_items, dict.delivery_point.

This is the "extended" variant of the inline inference query
(``dp_orders_features_full``): the production query materialised only a
hand-picked subset of ring/window combinations; here we compute the full
grid rings 0..5 x windows 30/60/90 for orders, GMV and unique competitor counts.
"""

from __future__ import annotations

from typing import Dict, Tuple

RINGS = [0, 1, 2, 3, 4, 5]
WINDOWS = [30, 60, 90]


def _aggregations() -> str:
    lines = []
    for r in RINGS:
        for w in WINDOWS:
            cond = f"p.h3_dist <= {r} AND dateDiff('day', o.date, p.t_features_date) <= {w}"
            lines.append(
                f"            toInt64(sumIf(o.orders_fact, {cond})) AS orders_r{r}_h{w}"
            )
    for r in RINGS:
        for w in WINDOWS:
            cond = f"p.h3_dist <= {r} AND dateDiff('day', o.date, p.t_features_date) <= {w}"
            lines.append(
                f"            toFloat64(sumIf(o.gmv_usd_fact, {cond})) AS gmv_r{r}_h{w}"
            )
    for r in RINGS:
        for w in WINDOWS:
            cond = (
                f"p.h3_dist <= {r} AND dateDiff('day', o.date, p.t_features_date) <= {w} "
                "AND p.n_is_dp = 1"
            )
            lines.append(
                f"            toInt64(uniqExactIf(p.neighbor_id, {cond})) AS unique_dp_r{r}_h{w}"
            )
    return ",\n".join(lines)


def _final_features() -> str:
    lines = []
    for r in RINGS:
        for w in WINDOWS:
            lines.append(f"        ifNull(sf.orders_r{r}_h{w}, 0) AS orders_r{r}_h{w}")
    for r in RINGS:
        for w in WINDOWS:
            lines.append(f"        ifNull(sf.gmv_r{r}_h{w}, 0.0) AS gmv_r{r}_h{w}")
    for r in RINGS:
        for w in WINDOWS:
            lines.append(
                f"        ifNull(sf.unique_dp_r{r}_h{w}, 0) AS unique_dp_r{r}_h{w}"
            )
    return ",\n".join(lines)


SQL = f"""
WITH
    9 AS hex_resolution,
    toDate(%(date_to)s) AS calc_date,
    open_datest AS (
        SELECT
            delivery_point_id,
            toDate(min(order_date_issued)) AS open_date
        FROM marts.order_items
        WHERE toDate(order_date_issued) != '1970-01-01'
          AND delivery_point_id != 0
        GROUP BY delivery_point_id
    ),
    delivery_points AS (
        SELECT
            id AS delivery_point_id,
            latitude,
            longitude,
            geoToH3(toFloat64(latitude), toFloat64(longitude), hex_resolution) AS dp_h3,
            od.open_date,
            type IN ('DELIVERY_POINT', 'FRANCHISE') AS is_dp,
            type = 'UZ_POST' AS is_inshop
        FROM dict.delivery_point dp
        LEFT JOIN open_datest od ON dp.id = od.delivery_point_id
    ),
    dp_orders AS (
        SELECT
            oi.delivery_point_id,
            toDate(oi.order_date_created) AS date,
            uniqExact(order_id) AS orders_fact,
            sum(daily_uzs_to_usd(order_date_created, gmv_final)) AS gmv_usd_fact
        FROM marts.order_items oi
        WHERE oi.order_status IN ('COMPLETED', 'RETURNED')
          AND delivery_point_id != 0
          AND order_date_created != '1970-01-01'
          AND order_date_created BETWEEN addDays(calc_date, -89) AND calc_date
        GROUP BY date, delivery_point_id
    ),
    targets AS (
        SELECT DISTINCT
            calc_date AS t_features_date,
            search_h3_index AS t_h3,
            h3ToGeo(t_h3).1 AS t_lat,
            h3ToGeo(t_h3).2 AS t_lon
        FROM delivery_points
        ARRAY JOIN h3kRing(dp_h3, 5) AS search_h3_index
    ),
    neighbors AS (
        SELECT
            delivery_point_id AS neighbor_id,
            open_date AS n_open_date,
            geoToH3(toFloat64(latitude), toFloat64(longitude), hex_resolution) AS n_h3,
            toFloat64(latitude) AS n_lat,
            toFloat64(longitude) AS n_lon,
            is_dp AS n_is_dp,
            is_inshop AS n_is_inshop
        FROM delivery_points
    ),
    targets_with_ring AS (
        SELECT
            t_features_date,
            t_h3,
            t_lat,
            t_lon,
            search_h3_index
        FROM targets
        ARRAY JOIN h3kRing(t_h3, 5) AS search_h3_index
    ),
    close_pairs AS (
        SELECT
            t.t_h3,
            t.t_features_date,
            t.t_lat,
            t.t_lon,
            n.neighbor_id,
            n.n_lat,
            n.n_lon,
            n.n_is_dp,
            n.n_is_inshop,
            h3Distance(t.t_h3, n.n_h3) - 1 AS h3_dist
        FROM targets_with_ring t
        JOIN neighbors n ON n.n_h3 = t.search_h3_index
        WHERE n.n_open_date <= t.t_features_date
    ),
    min_distances AS (
        SELECT
            t_h3,
            if(countIf(n_is_dp) > 0, minIf(geoDistance(t_lon, t_lat, n_lon, n_lat), n_is_dp), 10000) AS min_dist_to_dp_m,
            if(countIf(n_is_inshop) > 0, minIf(geoDistance(t_lon, t_lat, n_lon, n_lat), n_is_inshop), 10000) AS min_dist_to_inshop_m
        FROM close_pairs
        GROUP BY t_h3
    ),
    sales_features AS (
        SELECT
            p.t_h3,
{_aggregations()}
        FROM close_pairs p
        JOIN dp_orders o ON p.neighbor_id = o.delivery_point_id
        WHERE o.date < p.t_features_date
          AND dateDiff('day', o.date, p.t_features_date) BETWEEN 1 AND 90
        GROUP BY p.t_h3
    )
    SELECT
        toInt64(t.t_h3) AS h3_index,
        h3ToString(t.t_h3) AS h3_string,
        toFloat64(if(md.min_dist_to_dp_m = 0, 10000, md.min_dist_to_dp_m)) AS min_dist_to_dp_m,
        toFloat64(if(md.min_dist_to_inshop_m = 0, 10000, md.min_dist_to_inshop_m)) AS min_dist_to_inshop_m,
{_final_features()}
    FROM targets t
    LEFT JOIN min_distances md ON t.t_h3 = md.t_h3
    LEFT JOIN sales_features sf ON t.t_h3 = sf.t_h3
"""


def build_query(calc_date: str) -> Tuple[str, Dict[str, str]]:
    return SQL, {"date_to": calc_date}
