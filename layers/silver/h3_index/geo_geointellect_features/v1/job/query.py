"""ClickHouse query for silver.feature_platform_geo_geointellect_features.

Per H3 hex (res 9), demographic population and pedestrian-traffic index
aggregated over rings 0..5, plus parent-hex (levels 5..8) roll-ups, plus the
hex region label.

Source (external, ClickHouse): silver.h3_l9_geointellect.

Mirrors the inline query ``geointellect_features``; the ring-cumulative columns
are made uniform (r0..r5) so the ring-2-only feature (``traffic_ring_1_2`` in
the model) is derived downstream as r2 - r1 in the gold layer.
"""

from __future__ import annotations

from typing import Dict, Tuple

RINGS = [0, 1, 2, 3, 4, 5]
PARENT_LEVELS = [5, 6, 7, 8]


def _ring_aggregations() -> str:
    lines = []
    for r in RINGS:
        lines.append(
            f"        toFloat64(sumIf(g.population, n.ring <= {r})) AS population_r{r}"
        )
    for r in RINGS:
        lines.append(
            f"        toFloat64(sumIf(g.pedestrian_traffic_index, n.ring <= {r})) AS pedestrian_traffic_index_r{r}"
        )
    for l in PARENT_LEVELS:
        lines.append(
            f"        toFloat64(any(ifNull(p{l}.population_l{l}, 0))) AS population_l{l}"
        )
    for l in PARENT_LEVELS:
        lines.append(
            f"        toFloat64(any(ifNull(p{l}.pedestrian_traffic_index_l{l}, 0))) AS pedestrian_traffic_index_l{l}"
        )
    return ",\n".join(lines)


def _parent_ctes() -> str:
    blocks = []
    for l in PARENT_LEVELS:
        blocks.append(
            f"""    parent_l{l} AS (
        SELECT
            h3ToParent(h3_index, {l}) AS parent_h3,
            sum(population) AS population_l{l},
            sum(pedestrian_traffic_index) AS pedestrian_traffic_index_l{l}
        FROM base
        GROUP BY parent_h3
    )"""
        )
    return ",\n".join(blocks)


def _parent_joins() -> str:
    return "\n".join(
        f"    LEFT JOIN parent_l{l} p{l} ON p{l}.parent_h3 = h3ToParent(n.center_h3, {l})"
        for l in PARENT_LEVELS
    )


SQL = f"""
WITH base AS (
        SELECT
            toUInt64(h3_index) AS h3_index,
            population,
            pedestrian_traffic_index
        FROM silver.h3_l9_geointellect
        WHERE population > 0
           OR pedestrian_traffic_index > 0
    ),
{_parent_ctes()},
    candidates AS (
        SELECT DISTINCT arrayJoin(h3kRing(b.h3_index, 5)) AS h3_index
        FROM base b
    ),
    nbrs AS (
        SELECT
            c.h3_index AS center_h3,
            arrayJoin(h3kRing(c.h3_index, 5)) AS nbr_h3,
            h3Distance(c.h3_index, nbr_h3) - 1 AS ring
        FROM candidates c
    )
    SELECT
        toInt64(n.center_h3) AS h3_index,
        h3ToString(n.center_h3) AS h3_string,
        toString(geopoint2region((h3ToGeo(n.center_h3).2, h3ToGeo(n.center_h3).1))) AS region,
{_ring_aggregations()}
    FROM nbrs n
    LEFT JOIN base g ON g.h3_index = n.nbr_h3
{_parent_joins()}
    GROUP BY n.center_h3
"""


def build_query(calc_date: str) -> Tuple[str, Dict[str, str]]:
    return SQL, {"date_to": calc_date}
