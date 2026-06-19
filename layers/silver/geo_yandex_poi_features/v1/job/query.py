"""ClickHouse query for silver.feature_platform_geo_yandex_poi_features.

Per H3 hex (res 9), counts of Yandex POIs by business category aggregated over
rings 0..5.

Source (external, ClickHouse): silver.organizations_yandex (latest inserted_at
snapshot).

Extended variant of the inline query ``yandex_geo_features``: the production
query materialised only one hand-picked ring per category; here we compute the
full grid (each category x rings 0..5). The category list itself is a source
business-coded enum and is kept exactly as in the original contract.
"""

from __future__ import annotations

from typing import Dict, Tuple

TABLE_IDENTIFIER = "silver.feature_platform_geo_yandex_poi_features"
CLICKHOUSE_CONN_ID = "clickhouse_dwh_team_logistics"

RINGS = [0, 1, 2, 3, 4, 5]
# (output prefix, source category_sub value)
CATEGORIES = [
    ("atms", "Банкоматы"),
    ("banks", "Банки"),
    ("retail_points", "Торговые точки"),
    ("car_dealers_services", "Автосалоны, Автосервисы"),
    ("mixed_goods", "Смешанные товары"),
    ("fast_food_coffee", "Быстрое питание, Кофейни"),
    ("bakeries", "Пекарни"),
]


def _category_list_sql() -> str:
    return ",\n".join(f"                  '{rus}'" for _, rus in CATEGORIES)


def _aggregations() -> str:
    lines = []
    for key, rus in CATEGORIES:
        for r in RINGS:
            lines.append(
                f"        toInt64(sumIf(oc.cnt, n.ring <= {r} AND oc.category_sub = '{rus}')) AS {key}_r{r}"
            )
    return ",\n".join(lines)


SQL = f"""
WITH org_base AS (
        SELECT
            toUInt64(h3_index) AS h3_index,
            category_sub
        FROM silver.organizations_yandex
        WHERE toDate(inserted_at) = (SELECT max(toDate(inserted_at)) FROM silver.organizations_yandex)
          AND category_sub IN (
{_category_list_sql()}
          )
    ),
    org_counts AS (
        SELECT
            h3_index,
            category_sub,
            count() AS cnt
        FROM org_base
        GROUP BY h3_index, category_sub
    ),
    candidates AS (
        SELECT DISTINCT arrayJoin(h3kRing(h3_index, 5)) AS h3_index
        FROM org_counts
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
{_aggregations()}
    FROM nbrs n
    LEFT JOIN org_counts oc ON oc.h3_index = n.nbr_h3
    GROUP BY n.center_h3
"""


def build_query(calc_date: str) -> Tuple[str, Dict[str, str]]:
    return SQL, {"date_to": calc_date}
