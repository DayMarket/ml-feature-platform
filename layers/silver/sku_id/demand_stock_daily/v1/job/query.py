"""SQL дневного EOD-наличия SKU."""

from datetime import date


def source_query(config: dict, day: date) -> str:
    """SKU с положительным active/FBS EOD-остатком за один день `dt` и его цены на конец дня.

    Отсутствие SKU в записанном дне означает нулевое наличие. Цена 0 в источнике
    (история до появления колонок, неизвестная цена) пишется как NULL.
    """
    source = config["source"]
    return (
        "SELECT dt AS date, toInt64(sku_id) AS sku_id,\n"
        "       toInt64(nullIf(purchase_price_eod, 0)) AS purchase_price_eod,\n"
        "       toInt64(nullIf(sell_price_eod, 0)) AS sell_price_eod,\n"
        "       toInt64(nullIf(full_price_eod, 0)) AS full_price_eod\n"
        f"FROM {source['database']}.{source['table']} FINAL\n"
        f"WHERE dt = toDate('{day.isoformat()}')\n"
        "  AND sku_id > 0\n"
        "  AND (quantity_active_eod > 0 OR quantity_fbs_eod > 0)\n"
        "SETTINGS max_threads = 2, max_execution_time = 1200"
    )
