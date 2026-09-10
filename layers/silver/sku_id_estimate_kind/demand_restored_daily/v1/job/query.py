"""Читать конкретный CH E3-run без current/latest и без потери unavailable строк."""

from datetime import date
import re

SOURCE_COLUMNS = (
    "event_date AS date", "sku_id", "toString(estimate_kind) AS estimate_kind",
    "run_id", "prediction_date",
    "sales_units", "lost_units", "demand_units", "potential_units", "sales_gmv", "lost_gmv", "demand_gmv", "potential_gmv", "lost_unit_price", "p_active", "sigma",
    "currency_code", "price_model_version", "rate_ok", "settled_at", "method_version",
    "toString(quality_status) AS quality_status", "unavailable_reason",
    "toDateTime64(toTimeZone(updated_at, 'UTC'), 6, 'UTC') AS source_updated_at",
)


def source_ref(config, *, registry=False):
    source = config["source"]
    parts = [source.get("database"), source.get("registry_table" if registry else "table")]
    if any(not isinstance(v, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", v) for v in parts):
        raise ValueError("Неверное имя CH таблицы")
    return ".".join(f"`{value}`" for value in parts)


def selection(*, run_id, prediction_date, start, end):
    if not isinstance(run_id, str) or not run_id.strip() or run_id.strip().lower() in {"latest", "current"}:
        raise ValueError("Нужен точный run_id")
    if any(type(day) is not date for day in (prediction_date, start, end)):
        raise ValueError("Нужны DATE отсечки и диапазона")
    if not start < end <= prediction_date:
        raise ValueError("E3 диапазон должен быть непустым и строго до cutoff")
    return {"run_id": run_id, "prediction_date": prediction_date, "start": start, "end": end}


def registry_query(config):
    return (
        "SELECT run_id, prediction_date, state_version, stage, toString(status) AS status, "
        "model_version, code_version, catalog_version, input_manifest, output_manifest, "
        "finished_at, published_at FROM " + source_ref(config, registry=True)
        + " FINAL WHERE prediction_date = %(prediction_date)s AND run_id = %(run_id)s"
    )


def source_query(config):
    # PREWHERE использует префикс ORDER BY; значения передаются native driver params.
    return ("SELECT\n    " + ",\n    ".join(SOURCE_COLUMNS)
            + f"\nFROM {source_ref(config)} FINAL\n"
            "PREWHERE prediction_date = %(prediction_date)s AND run_id = %(run_id)s\n"
            "WHERE event_date >= %(start)s AND event_date < %(end)s\n"
            "ORDER BY event_date, sku_id, toString(estimate_kind)\n"
            "SETTINGS max_threads=2, max_execution_time=300")


def counts_query(config):
    return ("SELECT event_date AS date, toString(estimate_kind) AS estimate_kind, "
            "count() AS rows, uniqExact(sku_id) AS keys FROM " + source_ref(config)
            + " FINAL PREWHERE prediction_date = %(prediction_date)s AND run_id = %(run_id)s "
            "WHERE event_date >= %(start)s AND event_date < %(end)s "
            "GROUP BY event_date, estimate_kind ORDER BY event_date, estimate_kind "
            "SETTINGS max_threads=2, max_execution_time=300")


def source_schema_query(config):
    sql = source_query(config)
    marker = "\nSETTINGS "
    if sql.count(marker) != 1:
        raise ValueError("Неизвестная форма source SQL для E3 metadata preflight")
    return sql.replace(marker, "\nLIMIT 0\nSETTINGS ")
