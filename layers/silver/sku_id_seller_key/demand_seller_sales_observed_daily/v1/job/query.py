"""SQL дневной когорты продаж SKU × продавец из marts.order_items."""

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

CHANNELS = ("fbo", "fbs", "dbs", "other", "unknown")
MONEY_SOURCES = {
    "sales_gmv": "gmv_purchased",
    "sales_payment_value": "gmv_paid",
    "sales_full_value": "toDecimal128(order_item_full_price, 0) * order_item_amount",
    "sales_seller_promo_value": "seller_promo_value",
    "sales_marketplace_promo_value": "ke_promo_value",
}
MONEY_FIELDS = (*MONEY_SOURCES, *(f"sales_gmv_{channel}" for channel in CHANNELS))
CHANNEL_FILTERS = {
    "fbo": "order_type = 'FBO'",
    "fbs": "order_type = 'FBS'",
    "dbs": "order_type = 'DBS'",
    "other": "order_type NOT IN ('FBO', 'FBS', 'DBS', '')",
    "unknown": "order_type = '' OR order_type IS NULL",
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


def day_bounds_utc(day: date) -> tuple[str, str]:
    """Бизнес-день Asia/Tashkent как полуоткрытый интервал UTC."""
    start = datetime.combine(day, time.min, tzinfo=ZoneInfo("Asia/Tashkent"))
    return tuple(
        moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for moment in (start, start + timedelta(days=1))
    )


def source_query(config: dict, day: date) -> str:
    """Seller-вклады дня и точные distinct SKU-дня одним сканом order_items.

    GROUPING SETS считает группы (sku, seller) и итог SKU; окно переносит точные
    уники SKU-дня в `sku_sales_orders`/`sku_sales_order_items`, затем строки
    итогов отбрасываются.
    """
    source = config["source"]
    start, end = day_bounds_utc(day)
    aggregates = [
        "sku_id",
        "if(seller_id IS NULL OR seller_id = 0, 'unknown', concat('seller:', toString(seller_id))) AS seller_key",
        "GROUPING(seller_key) AS is_sku_total",
        "toInt64(sum(toUInt64(order_item_amount))) AS sales_units",
        "toInt64(uniqExact(order_item_id)) AS sales_order_items",
        "toInt64(uniqExact(order_id)) AS sales_orders",
        *(f"sum(toDecimal128({expr}, 0)) AS {name}" for name, expr in MONEY_SOURCES.items()),
    ]
    for channel, condition in CHANNEL_FILTERS.items():
        aggregates += [
            f"toInt64(sumIf(toUInt64(order_item_amount), {condition})) AS sales_units_{channel}",
            f"sumIf(toDecimal128(gmv_purchased, 0), {condition}) AS sales_gmv_{channel}",
        ]
    aggregates.append("toDateTime64(max(order_item_date_updated), 6, 'UTC') AS source_updated_at")
    usd = ",\n    ".join(f"toFloat64({name}) / fx_rate AS {name}_usd" for name in MONEY_FIELDS)
    return f"""{fx_prefix(day)}
SELECT
    fact_date AS date,
    facts.* EXCEPT (is_sku_total),
    if(seller_key = 'unknown', CAST(NULL AS Nullable(Int64)), toInt64(substring(seller_key, 8))) AS seller_id,
    {usd},
    {FX_COLUMNS}
FROM (
    SELECT grouped.*,
        toInt64(maxIf(sales_orders, is_sku_total = 1) OVER (PARTITION BY sku_id)) AS sku_sales_orders,
        toInt64(maxIf(sales_order_items, is_sku_total = 1) OVER (PARTITION BY sku_id)) AS sku_sales_order_items
    FROM (
        SELECT
            {(',' + chr(10) + '            ').join(aggregates)}
        FROM {source['database']}.{source['table']} FINAL
        WHERE order_date_created >= toDateTime('{start}', 'UTC')
          AND order_date_created < toDateTime('{end}', 'UTC')
          AND order_item_status NOT IN ('CREATED', 'NOT_CREATED')
          AND sku_id > 0
        GROUP BY GROUPING SETS ((sku_id, seller_key), (sku_id))
    ) AS grouped
) AS facts
WHERE is_sku_total = 0
SETTINGS max_threads = 2, max_execution_time = 1200"""
