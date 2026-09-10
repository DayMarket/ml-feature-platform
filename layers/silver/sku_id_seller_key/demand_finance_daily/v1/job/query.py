"""Свернуть финансовые события по дню, SKU и продавцу без cohort-фильтра."""

from datetime import date
import re

QUANTITY_SOURCES = {
    "finance_units_generated": "items_generated",
    "finance_units_assembled": "items_assembled",
    "finance_units_delivered": "items_delivered",
    "finance_units_completed": "items_completed",
    "finance_units_returned": "items_returned",
    "finance_units_net": "net_items",
    "finance_returned_units_current_period": "items_returned_current_period",
    "finance_returned_units_previous_period": "items_returned_previous_period"
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
    "finance_discount_returned": "discount_returned"
}
RAW_COLUMNS = ("date", "sku_id", "seller_key", "seller_id", *QUANTITY_SOURCES,
               *MONEY_SOURCES, *(f"{name}_usd" for name in MONEY_SOURCES), "source_rows")


def source_ref(config):
    source = config["source"]
    for key in ("database", "table"):
        if not isinstance(source.get(key), str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", source[key]):
            raise ValueError(f"Неверный source.{key}")
    if source.get("time_policy") != "finance_event_dt" or source.get("currency") != "UZS":
        raise ValueError("Не подтверждены дата или валюта финансового потока")
    return f"`{source['database']}`.`{source['table']}`"


def day_literal(day):
    if type(day) is not date:
        raise ValueError("Нужна DATE финансового события")
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


TOTAL_COLUMNS = ("source_rows", *QUANTITY_SOURCES, *MONEY_SOURCES)


def coverage_totals_query(config, day):
    """Сверить число ключей и все исходные суммы, не заменяя upstream DQ."""
    fields = ["count() AS rows"]
    fields += [f"sum(toDecimal256({name}, 0)) AS {name}" for name in TOTAL_COLUMNS]
    return ("SELECT " + ", ".join(fields) + f" FROM (\n{aggregate_query(config, day)}\n) AS facts "
            "SETTINGS max_threads=2, max_execution_time=300")


def source_audit_query(config, day):
    return ("SELECT count() AS source_rows, countIf(sku_id <= 0) AS invalid_sku, "
            "countIf(seller_id < 0) AS invalid_seller FROM "
            f"{source_ref(config)} WHERE dt = {day_literal(day)} "
            "SETTINGS max_threads=2, max_execution_time=300")


def aggregate_query(config, day):
    value = day_literal(day)
    fields = [f"{value} AS date", "sku_id",
              "if(seller_id > 0, concat('seller:', toString(seller_id)), 'unknown') AS seller_key",
              "nullIf(seller_id, 0) AS seller_id"]
    # Decimal до SUM исключает переполнение signed integer во время агрегации.
    fields += [f"sum(toDecimal128({source}, 0)) AS {name}"
               for name, source in {**QUANTITY_SOURCES, **MONEY_SOURCES}.items()]
    fields.append("count() AS source_rows")
    return ("SELECT\n    " + ",\n    ".join(fields)
            + f"\nFROM {source_ref(config)}\nWHERE dt = {value}\n"
            "GROUP BY sku_id, seller_key, seller_id")


def source_query(config, day, *, fx_available):
    if type(fx_available) is not bool:
        raise ValueError("Нужен подтверждённый FX status")
    fields = []
    for name in MONEY_SOURCES:
        usd = (f"daily_uzs_to_usd(date, {name})" if fx_available
               else "CAST(NULL AS Nullable(Float64))")
        fields.append(f"{usd} AS {name}_usd")
    return ("SELECT facts.*,\n    " + ",\n    ".join(fields)
            + f"\nFROM (\n{aggregate_query(config, day)}\n) AS facts\n"
            "ORDER BY sku_id, seller_key\nSETTINGS max_threads=2, max_execution_time=300")
