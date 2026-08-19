"""Trino query for the online city CPI table of the buyout service.

Тонкая проекция silver-витрины CPI: тот же грейн, тот же состав колонок, никакой
новой семантики. Нужна как serving-контракт — сервис невыкупов читает `gold`,
а не `silver`, и не зависит от перекладок в предагрегате.

Источник: iceberg.silver.feature_platform_delivery_cpi_city_features (партиция date,
имя источника строится из его config.yaml).
"""

from __future__ import annotations

from datetime import date

# Состав колонок совпадает с silver-витриной один в один; перечислены явно,
# чтобы новая колонка в источнике не попадала в serving-контракт молча.
PROJECTED_COLUMNS = (
    "city_id",
    "dimensional_group",
    "region_id",
    "cpi_forward_uzs",
    "cpi_reverse_uzs",
    "n_items_delivered",
    "n_items_reverse",
    "cpi_forward_region_uzs",
    "cpi_reverse_region_uzs",
    "cpi_forward_country_uzs",
    "cpi_reverse_country_uzs",
)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(partition_date: date, source_table: str) -> str:
    """SQL проекции на дату партиции; source_table — Trino-имя silver-витрины."""
    partition_date_sql = f"DATE {_sql_string(partition_date.isoformat())}"
    columns_sql = ",\n    ".join(PROJECTED_COLUMNS)

    return f"""
SELECT
    {partition_date_sql} AS date,
    {columns_sql}
FROM {source_table}
WHERE date = {partition_date_sql}
"""
