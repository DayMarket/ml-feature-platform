"""Trino query for the online category table of the buyout service.

Подстановка для sku, которых нет в buyout_online_sku_features (нет заказов за
90 дней): модель невыкупов при обучении (cart_item_signal.sql, MAD-13227)
подставляла таким позициям сглаженную выкупаемость категории, а категории без
заказов — общую выкупаемость маркетплейса. Здесь обе величины по одной строке на
category_id; формулы совпадают с buyout_online_sku_features один в один:

  cat_smooth = (marketplace_rate * k + raw_rate * n) / (k + n), k = 30,
  marketplace_rate — по строкам key_type = 'category' сигнала.

Источник: iceberg.gold.feature_platform_buyout_item_signal_features (партиция date,
имя источника строится из его config.yaml).
"""

from __future__ import annotations

from datetime import date

# Сила стягивания к маркетплейсу: k = 30 «виртуальных» доставок, как в
# cart_item_signal.sql (MAD-13227) и в buyout_online_sku_features.
SHRINKAGE_K = 30


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(partition_date: date, signal_table: str) -> str:
    """SQL проекции на дату партиции; signal_table — Trino-имя gold-источника."""
    partition_date_sql = f"DATE {_sql_string(partition_date.isoformat())}"
    k = SHRINKAGE_K

    return f"""
WITH sig AS (
    SELECT key_id, n_delivered_90d, n_completed_90d, n_no_show_90d,
           buyout_rate_items_90d, no_show_rate_90d
    FROM {signal_table}
    WHERE date = {partition_date_sql}
      AND key_type = 'category'
),

global_rate AS (
    SELECT
        CAST(SUM(n_completed_90d) AS DOUBLE) / NULLIF(SUM(n_delivered_90d), 0) AS g_buyout,
        CAST(SUM(n_no_show_90d)   AS DOUBLE) / NULLIF(SUM(n_delivered_90d), 0) AS g_no_show
    FROM sig
)

SELECT
    {partition_date_sql}                                AS date,
    s.key_id                                            AS category_id,
    s.n_delivered_90d                                   AS cat_n_delivered_90d,
    s.buyout_rate_items_90d                             AS category_buyout_rate_raw_90d,
    s.no_show_rate_90d                                  AS category_no_show_rate_raw_90d,
    (g.g_buyout  * {k} + COALESCE(s.buyout_rate_items_90d, g.g_buyout)  * s.n_delivered_90d)
        / ({k} + s.n_delivered_90d)                     AS category_buyout_rate_90d,
    (g.g_no_show * {k} + COALESCE(s.no_show_rate_90d, g.g_no_show) * s.n_delivered_90d)
        / ({k} + s.n_delivered_90d)                     AS category_no_show_rate_90d,
    g.g_buyout                                          AS marketplace_buyout_rate_90d,
    g.g_no_show                                         AS marketplace_no_show_rate_90d
FROM sig s
CROSS JOIN global_rate g
"""
