"""Gold assembly for gold.feature_platform_location_h3_forecast_features.

Joins the five silver feature tables on (date, h3_index) for one partition date
and projects them onto the stable feature contract consumed by the
``location_forecast`` model (the names in ``train_model_dag`` METADATA
``features_to_use``, minus the per-prediction time features the model derives at
scoring time). Source-side defaulting (region -> UNKNOWN, distance NULL -> 10000,
remaining numeric NULL -> 0) is applied here so the model reads a clean matrix.
"""

from __future__ import annotations

from datetime import date

GOLD_IDENTIFIER = "gold.feature_platform_location_h3_forecast_features"

SILVER = {
    "geo": "silver.feature_platform_geo_geointellect_features",
    "dp": "silver.feature_platform_dp_neighbor_order_features",
    "act": "silver.feature_platform_geo_user_activity_features",
    "loc": "silver.feature_platform_geo_user_location_features",
    "poi": "silver.feature_platform_geo_yandex_poi_features",
}

_DP_COLUMNS = [
    "h3_index",
    "min_dist_to_dp_m",
    "min_dist_to_inshop_m",
    "unique_dp_r5_h60",
    "unique_dp_r3_h60",
    "orders_r3_h90",
    "orders_r3_h30",
    "gmv_r2_h30",
    "gmv_r2_h60",
    "gmv_r3_h30",
    "orders_r5_h90",
    "unique_dp_r5_h90",
]
_ACT_COLUMNS = ["h3_index", "views_r1_30d", "orders_r0_30d", "orders_r3_90d", "orders_r4_30d"]
_LOC_COLUMNS = ["h3_index", "users_r0", "users_r1", "users_r2", "users_r3", "users_r4", "users_r5"]
_POI_COLUMNS = [
    "h3_index",
    "atms_r1",
    "banks_r2",
    "retail_points_r1",
    "car_dealers_services_r2",
    "mixed_goods_r2",
    "fast_food_coffee_r5",
    "bakeries_r1",
]

_RENAMES = {
    "population_r0": "population",
    "pedestrian_traffic_index_r0": "pedestrian_traffic_index",
    "views_r1_30d": "users_views_r1_30d",
    "orders_r0_30d": "users_orders_30d",
    "orders_r3_90d": "users_orders_r3_90d",
    "orders_r4_30d": "users_orders_r4_30d",
    "atms_r1": "atms_rad_1",
    "banks_r2": "banks_rad_2",
    "retail_points_r1": "retail_points_rad_1",
    "car_dealers_services_r2": "car_dealers_services_rad_2",
    "mixed_goods_r2": "mixed_goods_rad_2",
    "fast_food_coffee_r5": "fast_food_coffee_rad_5",
    "bakeries_r1": "bakeries_rad_1",
}


def build_gold(read_partition, partition_date: date):
    """``read_partition(identifier, partition_date) -> pandas.DataFrame``."""
    geo = read_partition(SILVER["geo"], partition_date)
    dp = read_partition(SILVER["dp"], partition_date)[_DP_COLUMNS]
    act = read_partition(SILVER["act"], partition_date)[_ACT_COLUMNS]
    loc = read_partition(SILVER["loc"], partition_date)[_LOC_COLUMNS]
    poi = read_partition(SILVER["poi"], partition_date)[_POI_COLUMNS]

    # geointellect is the base grid (every candidate hex), others are left-joined.
    df = (
        geo.drop(columns=["date"], errors="ignore")
        .merge(dp, on="h3_index", how="left")
        .merge(act, on="h3_index", how="left")
        .merge(loc, on="h3_index", how="left")
        .merge(poi, on="h3_index", how="left")
        .rename(columns=_RENAMES)
    )

    df["traffic_ring_1_2"] = (
        df["pedestrian_traffic_index_r2"] - df["pedestrian_traffic_index_r1"]
    )
    df["orders_per_dp_r5_h90"] = df["orders_r5_h90"] / (df["unique_dp_r5_h90"] + 1e-5)

    df["region"] = df["region"].fillna("UNKNOWN")
    df["min_dist_to_dp_m"] = df["min_dist_to_dp_m"].fillna(10000)
    df["min_dist_to_inshop_m"] = df["min_dist_to_inshop_m"].fillna(10000)
    df = df.fillna(0)
    return df


def run(catalog, partition_date: date) -> None:
    import sys
    import os

    sys.path.insert(0, os.path.join(_repo_root(), "layers", "_common"))
    from clickhouse_iceberg import read_iceberg_date, write_daily_snapshot

    def read_partition(identifier, day):
        return read_iceberg_date(catalog, identifier, day)

    frame = build_gold(read_partition, partition_date)
    write_daily_snapshot(catalog, GOLD_IDENTIFIER, frame, partition_date)


def _repo_root() -> str:
    import os

    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
    )
