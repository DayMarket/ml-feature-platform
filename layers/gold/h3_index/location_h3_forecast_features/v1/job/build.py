"""Gold assembly for gold.feature_platform_location_h3_forecast_features.

Wide h3 feature mart: full-outer union of every silver feature table on
(date, h3_index) so no hex carrying any feature is dropped, plus the stable
model-contract columns the ``location_forecast`` model consumes (renamed copies
and derived features). Raw silver columns are preserved alongside their
contract copies so a single mart can back different model feature subsets.
Source-side defaulting (region -> UNKNOWN, distance NULL -> 10000, remaining
numeric NULL -> 0) is applied here; string/date metadata columns keep NULL.
"""

from __future__ import annotations

from datetime import date

# Silver entity aliases whose full column sets are unioned into the wide mart.
_SILVER_ALIASES = ("geo", "dp", "act", "loc", "poi")

# Metadata columns present in more than one silver table; keep a single owner so
# the outer join does not produce _x/_y duplicates. geo owns h3_string/region.
_DROP_DUPLICATE_METADATA = {"dp": ["h3_string"]}

# Model-contract copies: stable model feature name -> raw silver column. Copies
# (not renames) so the raw silver column stays in the mart for other consumers.
_CONTRACT_COPIES = {
    "population": "population_r0",
    "pedestrian_traffic_index": "pedestrian_traffic_index_r0",
    "users_views_r1_30d": "views_r1_30d",
    "users_orders_30d": "orders_r0_30d",
    "users_orders_r3_90d": "orders_r3_90d",
    "users_orders_r4_30d": "orders_r4_30d",
    "atms_rad_1": "atms_r1",
    "banks_rad_2": "banks_r2",
    "retail_points_rad_1": "retail_points_r1",
    "car_dealers_services_rad_2": "car_dealers_services_r2",
    "mixed_goods_rad_2": "mixed_goods_r2",
    "fast_food_coffee_rad_5": "fast_food_coffee_r5",
    "bakeries_rad_1": "bakeries_r1",
}


def build_gold(read_partition, partition_date: date):
    """Build the wide gold mart from silver aliases resolved by the DAG."""
    df = None
    for alias in _SILVER_ALIASES:
        frame = read_partition(alias, partition_date).drop(columns=["date"], errors="ignore")
        frame = frame.drop(columns=_DROP_DUPLICATE_METADATA.get(alias, []), errors="ignore")
        df = frame if df is None else df.merge(frame, on="h3_index", how="outer")

    # Keep the raw silver column and expose the stable model-contract name.
    for target, source in _CONTRACT_COPIES.items():
        df[target] = df[source]

    df["traffic_ring_1_2"] = (
        df["pedestrian_traffic_index_r2"] - df["pedestrian_traffic_index_r1"]
    )
    df["orders_per_dp_r5_h90"] = df["orders_r5_h90"] / (df["unique_dp_r5_h90"] + 1e-5)

    df["region"] = df["region"].fillna("UNKNOWN")
    df["min_dist_to_dp_m"] = df["min_dist_to_dp_m"].fillna(10000)
    df["min_dist_to_inshop_m"] = df["min_dist_to_inshop_m"].fillna(10000)

    # Fill numeric features only; h3_string/report_date stay NULL for hexes a
    # source did not cover (blanket fillna(0) would corrupt those dtypes).
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    return df
