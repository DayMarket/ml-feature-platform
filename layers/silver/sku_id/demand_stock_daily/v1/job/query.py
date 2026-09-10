"""Выбрать из EOD только SKU с положительным доступным остатком."""

from datetime import date
import re


def source_ref(config):
    source = config["source"]
    for key in ("database", "table"):
        value = source.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError("Неверная компонента source table")
    if source.get("time_policy") != "source_eod_date_accepted":
        raise ValueError("Не подтверждена семантика дня EOD")
    return f"`{source['database']}`.`{source['table']}`"


def day_literal(day):
    if type(day) is not date:
        raise ValueError("Нужна DATE, не SQL-строка")
    return f"toDate('{day.isoformat()}')"


def available():
    return "(`quantity_active_eod` > 0 OR `quantity_fbs_eod` > 0)"


def valid_key():
    return "sku_id > 0 AND sku_id <= 9223372036854775807"


def source_query(config, day):
    """Отсутствие ключа в успешно принятом дне кодирует нулевое наличие."""
    return (
        "SELECT dt AS date, toInt64(sku_id) AS sku_id "
        f"FROM {source_ref(config)} FINAL "
        f"WHERE dt = {day_literal(day)} AND {valid_key()} AND {available()} "
        "ORDER BY sku_id SETTINGS max_threads=2, max_execution_time=1200"
    )


def count_query(config, day):
    """Зафиксировать count и сигнатуру выбранного множества до чтения строк."""
    selected = f"{valid_key()} AND {available()}"
    invalid_values = (
        "sku_id > 0 AND (isNull(quantity_active_eod) OR quantity_active_eod < 0 "
        "OR isNull(quantity_fbs_eod) OR quantity_fbs_eod < 0 OR isNull(updated_at))"
    )
    return (
        f"SELECT countIf({selected}) AS rows, uniqExactIf(sku_id, {selected}) AS keys, "
        "countIf(isNull(sku_id) OR sku_id < 0 OR sku_id > 9223372036854775807) AS invalid_keys, "
        f"countIf({invalid_values}) AS invalid_values, "
        f"sumIf(cityHash64(toString(sku_id)), {selected}) AS key_hash, max(updated_at) AS source_updated_at "
        f"FROM {source_ref(config)} FINAL WHERE dt = {day_literal(day)} "
        "SETTINGS max_threads=2, max_execution_time=1200"
    )
