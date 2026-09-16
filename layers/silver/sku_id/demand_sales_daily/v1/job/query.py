"""Trino SQL свёртки seller-продаж до SKU-дня."""

from datetime import date

CHANNELS = ("fbo", "fbs", "dbs", "other", "unknown")
MONEY = (
    "sales_gmv", "sales_payment_value", "sales_full_value",
    "sales_seller_promo_value", "sales_marketplace_promo_value",
    *(f"sales_gmv_{channel}" for channel in CHANNELS),
)
ADDITIVE = ("sales_units", *(f"sales_units_{channel}" for channel in CHANNELS), *MONEY,
            *(f"{name}_usd" for name in MONEY))
# Одинаковы во всех строках дня: курс фиксируется на день.
DAY_CONSTANT = ("fx_rate_date", "fx_rate_uzs_per_usd", "fx_rate_source", "fx_captured_at")


def quote(*parts: str) -> str:
    return ".".join('"' + part.replace('"', '""') + '"' for part in parts)


def source_query(source_table: str, day: date) -> str:
    """Сумма seller-вкладов по SKU.

    NULL хотя бы одного вклада даёт NULL итога. Уники заказов и позиций не
    суммируются: берутся точные SKU-итоги, повторённые в каждой seller-строке.
    """
    sums = [f"IF(count({name}) = count(*), sum({name})) AS {name}" for name in ADDITIVE]
    constants = [f"max({name}) AS {name}" for name in DAY_CONSTANT]
    columns = ",\n    ".join([
        '"date"',
        "sku_id",
        *sums,
        "max(sku_sales_order_items) AS sales_order_items",
        "max(sku_sales_orders) AS sales_orders",
        "max(source_updated_at) AS source_updated_at",
        *constants,
    ])
    return (
        f"SELECT\n    {columns}\n"
        f"FROM {source_table}\n"
        f"WHERE \"date\" = DATE '{day.isoformat()}'\n"
        'GROUP BY "date", sku_id'
    )
