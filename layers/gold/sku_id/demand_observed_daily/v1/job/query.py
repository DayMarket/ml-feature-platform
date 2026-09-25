"""Trino SQL дневной observed-панели: FULL JOIN продаж и EOD-наличия."""

from datetime import date

SALES_COLUMNS = (
    "sales_units", "sales_order_items", "sales_orders",
    "sales_gmv", "sales_payment_value", "sales_full_value",
    "sales_seller_promo_value", "sales_marketplace_promo_value",
    "sales_gmv_fbo", "sales_gmv_fbs", "sales_gmv_dbs", "sales_gmv_other", "sales_gmv_unknown",
    "sales_units_fbo", "sales_units_fbs", "sales_units_dbs", "sales_units_other", "sales_units_unknown",
    "sales_gmv_usd", "sales_payment_value_usd", "sales_full_value_usd",
    "sales_seller_promo_value_usd", "sales_marketplace_promo_value_usd",
    "sales_gmv_fbo_usd", "sales_gmv_fbs_usd", "sales_gmv_dbs_usd", "sales_gmv_other_usd",
    "sales_gmv_unknown_usd",
)
# Цены на конец дня берутся из stock-строки; у SKU без положительного остатка — NULL.
STOCK_COLUMNS = ("purchase_price_eod", "sell_price_eod", "full_price_eod")
# Технические поля sales переносятся с префиксом sales_.
SALES_PREFIXED = (
    "source_updated_at", "fx_rate_date", "fx_rate_uzs_per_usd", "fx_rate_source",
    "fx_captured_at", "source_manifest_id", "source_contract_version", "ingested_at",
)


def versioned(table: str, snapshot_id: int) -> str:
    return f"{table} FOR VERSION AS OF {int(snapshot_id)}"


def counts_query(sales: str, stock: str, day: date) -> str:
    literal = f"DATE '{day.isoformat()}'"
    return (
        f'SELECT (SELECT count(*) FROM {sales} WHERE "date" = {literal}),\n'
        f'       (SELECT count(*) FROM {stock} WHERE "date" = {literal})'
    )


def source_query(sales: str, stock: str, day: date) -> str:
    """Строка на каждый SKU, у которого за день есть продажи или положительный EOD; цены — из EOD."""
    literal = f"DATE '{day.isoformat()}'"
    columns = ",\n    ".join([
        f"{literal} AS \"date\"",
        "COALESCE(s.sku_id, k.sku_id) AS sku_id",
        *(f"s.{name}" for name in SALES_COLUMNS),
        *(f"s.{name} AS sales_{name}" for name in SALES_PREFIXED),
        "s.sku_id IS NOT NULL AS sales_component_present",
        "k.sku_id IS NOT NULL AS is_in_stock_eod",
        *(f"k.{name}" for name in STOCK_COLUMNS),
    ])
    stock_columns = ", ".join(STOCK_COLUMNS)
    return (
        f"SELECT\n    {columns}\n"
        f'FROM (SELECT * FROM {sales} WHERE "date" = {literal}) AS s\n'
        f'FULL OUTER JOIN (SELECT sku_id, {stock_columns} FROM {stock} WHERE "date" = {literal}) AS k\n'
        "  ON s.sku_id = k.sku_id"
    )
