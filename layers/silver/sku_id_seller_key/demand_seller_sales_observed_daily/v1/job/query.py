"""Считать seller-вклады и точные SKU-day distinct одним захватом order_items."""

from datetime import date, datetime, time, timedelta, timezone
import re
from zoneinfo import ZoneInfo

CHANNELS = ("fbo", "fbs", "dbs", "other", "unknown")
MONEY_SOURCES = {
    "sales_gmv": "gmv_purchased",
    "sales_payment_value": "gmv_paid",
    "sales_full_value": "toDecimal128(order_item_full_price, 0) * order_item_amount",
    "sales_seller_promo_value": "seller_promo_value",
    "sales_marketplace_promo_value": "ke_promo_value",
}
MONEY_FIELDS = tuple(MONEY_SOURCES) + tuple(f"sales_gmv_{c}" for c in CHANNELS)


def source_ref(config):
    source = config["source"]
    for key in ("database", "table"):
        if not isinstance(source.get(key), str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", source[key]):
            raise ValueError(f"Неверная компонента source.{key}")
    if source.get("time_policy") != "order_created_tashkent" or source.get("currency") != "UZS":
        raise ValueError("Не подтверждена временная ось или валюта sales")
    return f"`{source['database']}`.`{source['table']}`"


def day_bounds(day):
    """Полуоткрытый бизнес-день выражается UTC-границами исходного timestamp."""
    if type(day) is not date:
        raise ValueError("Нужна DATE, не строка или timestamp")
    start = datetime.combine(day, time.min, tzinfo=ZoneInfo("Asia/Tashkent"))
    return tuple(moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                 for moment in (start, start + timedelta(days=1)))


def source_filter(day):
    start, end = day_bounds(day)
    return (f"order_date_created >= toDateTime('{start}', 'UTC')\n"
            f"  AND order_date_created < toDateTime('{end}', 'UTC')\n"
            "  AND order_item_status NOT IN ('CREATED', 'NOT_CREATED') AND sku_id > 0")


def channel_filter(channel):
    if channel in ("fbo", "fbs", "dbs"):
        return f"order_type = '{channel.upper()}'"
    if channel == "other":
        return "order_type NOT IN ('FBO', 'FBS', 'DBS', '')"
    if channel == "unknown":
        return "order_type = '' OR order_type IS NULL"
    raise ValueError(f"Неизвестный канал {channel!r}")


def aggregate_query(config, day):
    """Не дополнять разреженный источник нулями и не подмешивать текущий каталог."""
    fields = [
        f"toDate('{day.isoformat()}') AS date" if type(day) is date else "",
        "sku_id",
        "if(seller_id IS NULL OR seller_id = 0, 'unknown', concat('seller:', toString(seller_id))) AS seller_key",
        "GROUPING(seller_key) AS is_sku_total",
        "sum(toUInt64(order_item_amount)) AS sales_units",
        "uniqExact(order_item_id) AS sales_order_items",
        "uniqExact(order_id) AS sales_orders",
    ]
    predicate = source_filter(day)
    fields += [f"sum(toDecimal128({expression}, 0)) AS {name}"
               for name, expression in MONEY_SOURCES.items()]
    for channel in CHANNELS:
        condition = channel_filter(channel)
        fields += [
            f"sumIf(toUInt64(order_item_amount), {condition}) AS sales_units_{channel}",
            f"sumIf(toDecimal128(gmv_purchased, 0), {condition}) AS sales_gmv_{channel}",
        ]
    fields.append("toDateTime64(max(order_item_date_updated), 6, 'UTC') AS source_updated_at")
    grouped = ("SELECT\n    " + ",\n    ".join(fields)
               + f"\nFROM {source_ref(config)} FINAL\nWHERE {predicate}\n"
               "GROUP BY GROUPING SETS ((sku_id, seller_key), (sku_id))")
    totals = ("SELECT grouped.*,\n"
              "    maxIf(sales_orders, is_sku_total = 1) OVER (PARTITION BY sku_id) AS sku_sales_orders,\n"
              "    maxIf(sales_order_items, is_sku_total = 1) OVER (PARTITION BY sku_id) AS sku_sales_order_items\n"
              f"FROM (\n{grouped}\n) AS grouped")
    return ("SELECT * EXCEPT (is_sku_total),\n"
            "    if(seller_key = 'unknown', CAST(NULL AS Nullable(Int64)), "
            "toInt64(substring(seller_key, 8))) AS seller_id\n"
            f"FROM (\n{totals}\n) AS with_totals\nWHERE is_sku_total = 0")


def source_query(config, day, *, fx_available):
    if type(fx_available) is not bool:
        raise ValueError("Нужен проверенный статус FX")
    inner = aggregate_query(config, day)
    usd = []
    for name in MONEY_FIELDS:
        expression = (f"daily_uzs_to_usd(date, {name})" if fx_available
                      else "CAST(NULL AS Nullable(Float64))")
        usd.append(f"{expression} AS {name}_usd")
    return ("SELECT facts.*,\n    " + ",\n    ".join(usd)
            + f"\nFROM (\n{inner}\n) AS facts\nORDER BY sku_id, seller_key\n"
            "SETTINGS max_threads=2, max_execution_time=300")


def day_literal(day):
    if type(day) is not date:
        raise ValueError('Нужна DATE, не SQL-строка')
    return f"toDate('{day.isoformat()}')"


def fx_query(day):
    value = day_literal(day)
    return f"""WITH
        {value} AS fact_date,
        dictHas('dict.currency_rates', ('USD', fact_date)) AS has_exact,
        (SELECT argMax(tuple(toFloat64(rate), toDate(requested_dt)), requested_dt)
         FROM marts.currency_rates_official WHERE currency_name = 'USD') AS latest,
        if(has_exact, toFloat64(dictGetFloat32('dict.currency_rates', 'rate', ('USD', fact_date))), latest.1) AS rate_value,
        isFinite(rate_value) AND rate_value > 0 AS valid_rate
    SELECT fact_date AS date,
        if(valid_rate, if(has_exact, fact_date, latest.2), NULL) AS fx_rate_date,
        if(valid_rate, rate_value, NULL) AS fx_rate_uzs_per_usd,
        if(valid_rate, if(has_exact, 'exact_date', 'latest_available'), 'unavailable') AS fx_rate_source,
        now64(6, 'UTC') AS fx_captured_at
    SETTINGS max_threads=1, max_execution_time=300"""


# Суммы повторённых controls нужны только для технической сверки потока, не как SKU distinct.
TOTAL_COLUMNS = ("sales_units", "sales_order_items", "sales_orders",
                 *(f"sales_units_{c}" for c in CHANNELS), *MONEY_FIELDS,
                 "sku_sales_orders", "sku_sales_order_items")


def coverage_totals_query(config, day):
    """Сверить число ключей и все исходные суммы, не заменяя upstream DQ."""
    fields = ["count() AS rows"]
    fields += [f"sum(toDecimal256({name}, 0)) AS {name}" for name in TOTAL_COLUMNS]
    fields.append("max(source_updated_at) AS source_updated_at")
    return ("SELECT " + ", ".join(fields) + f" FROM (\n{aggregate_query(config, day)}\n) AS facts "
            "SETTINGS max_threads=2, max_execution_time=300")


def coverage_query(config, day):
    """Количество позиций и SKU проверяется отдельно от готовности upstream."""
    return ("SELECT count() AS source_rows, uniqExact(order_item_id) AS unique_items,\n"
            "       uniqExact(tuple(sku_id, ifNull(seller_id, 0))) AS seller_sku_days, countIf(seller_id < 0) AS invalid_sellers,\n"
            "       sum(toUInt64(order_item_amount)) AS units,\n"
            "       sum(toDecimal128(gmv_purchased, 0)) AS raw_gmv\n"
            f"FROM {source_ref(config)} FINAL\nWHERE {source_filter(day)}\n"
            "SETTINGS max_threads=2, max_execution_time=300")
