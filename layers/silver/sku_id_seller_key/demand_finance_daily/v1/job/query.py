"""SQL дневных финансовых событий SKU × продавец."""

from datetime import date

QUANTITY_SOURCES = {
    "finance_units_generated": "items_generated",
    "finance_units_assembled": "items_assembled",
    "finance_units_delivered": "items_delivered",
    "finance_units_completed": "items_completed",
    "finance_units_returned": "items_returned",
    "finance_units_net": "net_items",
    "finance_returned_units_current_period": "items_returned_current_period",
    "finance_returned_units_previous_period": "items_returned_previous_period",
}
MONEY_SOURCES = {
    "finance_gmv_generated": "gmv_generated",
    "finance_gmv_assembled": "gmv_assembled",
    "finance_gmv_delivered": "gmv_delivered",
    "finance_gmv_completed": "gmv_completed",
    "finance_gmv_returned": "gmv_returned",
    "finance_gmv_net": "net_gmv",
    "finance_gmv_generated_without_promo": "gmv_generated_without_promo",
    "finance_gmv_delivered_without_promo": "gmv_delivered_without_promo",
    "finance_gmv_completed_without_promo": "gmv_completed_without_promo",
    "finance_gmv_returned_without_promo": "gmv_returned_without_promo",
    "finance_gmv_net_without_promo": "net_gmv_without_promo",
    "finance_promocodes_generated": "promocodes_generated",
    "finance_seller_discount_generated": "seller_discount_amount_generated",
    "finance_discount_generated": "discount_generated",
    "finance_promocodes_completed": "promocodes_completed",
    "finance_seller_discount_completed": "seller_discount_amount_completed",
    "finance_discount_completed": "discount_completed",
    "finance_promocodes_returned": "promocodes_returned",
    "finance_seller_discount_returned": "seller_discount_amount_returned",
    "finance_discount_returned": "discount_returned",
}


def fx_prefix(day: date) -> str:
    """Курс USD на дату: точный из dict.currency_rates, иначе последний официальный."""
    return f"""WITH
    toDate('{day.isoformat()}') AS fact_date,
    dictHas('dict.currency_rates', ('USD', fact_date)) AS has_exact,
    (SELECT argMax(tuple(toFloat64(rate), toDate(requested_dt)), requested_dt)
     FROM marts.currency_rates_official WHERE currency_name = 'USD') AS latest,
    if(has_exact, toFloat64(dictGetFloat32('dict.currency_rates', 'rate', ('USD', fact_date))), latest.1) AS raw_rate,
    if(isFinite(raw_rate) AND raw_rate > 0, raw_rate, CAST(NULL AS Nullable(Float64))) AS fx_rate"""


FX_COLUMNS = """if(fx_rate IS NULL, NULL, if(has_exact, fact_date, latest.2)) AS fx_rate_date,
    fx_rate AS fx_rate_uzs_per_usd,
    multiIf(fx_rate IS NULL, 'unavailable', has_exact, 'exact_date', 'latest_available') AS fx_rate_source,
    now64(6, 'UTC') AS fx_captured_at"""


def source_query(config: dict, day: date) -> str:
    """Сумма финансовых событий дня `dt` без cohort-фильтра.

    Источник — ReplicatedMergeTree: FINAL не нужен, повтор order_item_id в разных
    событиях не является дублем. Денежные Int64 суммируются в Decimal(38,0).
    """
    source = config["source"]
    aggregates = [
        "sku_id",
        "if(seller_id > 0, concat('seller:', toString(seller_id)), 'unknown') AS seller_key",
        "nullIf(seller_id, 0) AS seller_id",
        *(f"toInt64(sum(toDecimal128({src}, 0))) AS {name}" for name, src in QUANTITY_SOURCES.items()),
        *(f"sum(toDecimal128({src}, 0)) AS {name}" for name, src in MONEY_SOURCES.items()),
        "toInt64(count()) AS source_rows",
    ]
    usd = ",\n    ".join(f"toFloat64({name}) / fx_rate AS {name}_usd" for name in MONEY_SOURCES)
    select = ",\n        ".join(aggregates)
    return f"""{fx_prefix(day)}
SELECT
    fact_date AS date,
    facts.*,
    {usd},
    {FX_COLUMNS}
FROM (
    SELECT
        {select}
    FROM {source['database']}.{source['table']}
    WHERE dt = fact_date
    GROUP BY sku_id, seller_key, seller_id
) AS facts
SETTINGS max_threads = 2, max_execution_time = 1200"""
