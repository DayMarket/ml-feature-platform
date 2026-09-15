"""SQL дневного EOD-наличия SKU."""

from datetime import date


def source_query(config: dict, day: date) -> str:
    """SKU с положительным active/FBS EOD-остатком за один день `dt`.

    Отсутствие SKU в записанном дне означает нулевое наличие.
    """
    source = config["source"]
    return (
        "SELECT dt AS date, toInt64(sku_id) AS sku_id\n"
        f"FROM {source['database']}.{source['table']} FINAL\n"
        f"WHERE dt = toDate('{day.isoformat()}')\n"
        "  AND sku_id > 0\n"
        "  AND (quantity_active_eod > 0 OR quantity_fbs_eod > 0)\n"
        "SETTINGS max_threads = 2, max_execution_time = 1200"
    )
