"""Build dynamic-pricing final price features from SKU and discount snapshots."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence


def _empty_discount_frame():
    import pandas as pd

    return pd.DataFrame(
        columns=[
            "date",
            "sku_id",
            "promotion_id",
            "discount_amount",
            "calculated_for_price",
            "created_at",
        ]
    )


def _latest_discounts(historical_discounts, today_discounts):
    import pandas as pd

    frames = []
    for frame in (historical_discounts, today_discounts):
        if frame is not None and not frame.empty:
            frames.append(frame.copy())

    if not frames:
        return _empty_discount_frame()

    discounts = pd.concat(frames, ignore_index=True)
    discounts["created_at"] = pd.to_datetime(discounts["created_at"], errors="coerce")
    discounts = discounts.dropna(subset=["sku_id", "promotion_id", "created_at"])
    if discounts.empty:
        return _empty_discount_frame()

    discounts = discounts.sort_values(
        ["sku_id", "promotion_id", "created_at"],
        ascending=[True, True, False],
    )
    return discounts.drop_duplicates(["sku_id", "promotion_id"], keep="first")


def build_gold(
    actual_sku,
    historical_discounts,
    today_discounts,
    promotion_ids: Sequence[str],
    calculated_at: datetime,
):
    import numpy as np
    import pandas as pd

    sku = actual_sku.loc[:, ["sku_id", "sku_group_id", "product_id", "sell_price"]].copy()
    sku["sell_price"] = pd.to_numeric(sku["sell_price"], errors="coerce")
    promotions = pd.DataFrame({"promotion_id": list(promotion_ids)})
    base = sku.merge(promotions, how="cross")

    latest = _latest_discounts(historical_discounts, today_discounts)
    latest = latest.loc[
        :,
        [
            "sku_id",
            "promotion_id",
            "discount_amount",
            "calculated_for_price",
            "created_at",
        ],
    ].copy()
    latest["discount_amount"] = pd.to_numeric(
        latest["discount_amount"],
        errors="coerce",
    )
    latest["calculated_for_price"] = pd.to_numeric(
        latest["calculated_for_price"],
        errors="coerce",
    )

    frame = base.merge(latest, on=["sku_id", "promotion_id"], how="left")
    price_matched = frame["sell_price"].eq(frame["calculated_for_price"])
    discount_amount = frame["discount_amount"].fillna(0.0)
    frame["discount"] = np.where(price_matched, discount_amount, 0.0)
    frame["sell_price"] = np.where(
        price_matched,
        frame["sell_price"] - discount_amount,
        frame["sell_price"],
    )
    frame["discount_fraction"] = np.where(
        frame["sell_price"].ne(0.0),
        frame["discount"] / frame["sell_price"],
        np.nan,
    )
    frame["calculated_at"] = calculated_at
    frame = frame.rename(columns={"created_at": "dynamic_discount_created_at"})

    return frame.loc[
        :,
        [
            "calculated_at",
            "sku_id",
            "promotion_id",
            "sku_group_id",
            "product_id",
            "discount",
            "sell_price",
            "discount_fraction",
            "dynamic_discount_created_at",
        ],
    ]
