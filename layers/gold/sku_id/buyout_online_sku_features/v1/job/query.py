"""Trino query for the online SKU table of the buyout service.

Одна строка на sku: свои признаки + родительские (product / category / shop /
brand) + сглаженные ставки. Serving-контракт: сервис невыкупов забирает
последнюю дату.

Источник: gold.feature_platform_buyout_item_signal_features (партиция date)
+ "dwh-iceberg".silver.sku (маппинг sku -> product/category/shop/brand;
текущий снимок атрибутов — допустимо, атрибуты медленные, как в MAD-13227).

Сглаживание — канон cart_item_signal.sql (MAD-13227), k = 30:
  категория стягивается к общей выкупаемости маркетплейса,
  sku и product — к сглаженной ставке своей категории:
  shrunk = (parent_rate * k + raw_rate * n) / (k + n).
Признак гипотезы MAD-13413 «размеры внутри карточки выкупаются по-разному»:
  sku_vs_product_gap_90d = shrunk_sku - shrunk_product.

Популяция: sku с историей за 90 дней (~1.44 млн строк). Холодные sku в таблице
отсутствуют — сервис для них падает на категорию/бренд из своих данных
(вопрос популяции «весь каталог против sku с историей» вынесен в PR).
"""

from __future__ import annotations

from datetime import date

SKU_TABLE = '"dwh-iceberg".silver.sku'

# Сила стягивания к родителю: k = 30 «виртуальных» доставок (канон MAD-13227).
SHRINKAGE_K = 30


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(partition_date: date, signal_table: str) -> str:
    """SQL проекции на дату партиции; signal_table — Trino-имя silver/gold источника."""
    partition_date_sql = f"DATE {_sql_string(partition_date.isoformat())}"
    k = SHRINKAGE_K

    return f"""
WITH sig AS (
    SELECT key_type, key_id,
           n_delivered_90d, n_completed_90d, n_no_show_90d, n_nonbuyout_client_90d,
           n_delivered_30d, n_completed_30d,
           buyout_rate_items_90d, no_show_rate_90d, nonbuyout_rate_90d,
           buyout_rate_money_90d, cancel_before_delivery_rate_90d,
           buyout_rate_items_30d, no_show_rate_30d
    FROM {signal_table}
    WHERE date = {partition_date_sql}
),

global_rate AS (
    SELECT
        CAST(SUM(n_completed_90d) AS DOUBLE) / NULLIF(SUM(n_delivered_90d), 0) AS g_buyout,
        CAST(SUM(n_no_show_90d)   AS DOUBLE) / NULLIF(SUM(n_delivered_90d), 0) AS g_no_show
    FROM sig WHERE key_type = 'category'
),

cat_smooth AS (
    SELECT s.key_id AS category_id,
           s.n_delivered_90d AS cat_n_delivered_90d,
           (g.g_buyout  * {k} + COALESCE(s.buyout_rate_items_90d, g.g_buyout)  * s.n_delivered_90d)
               / ({k} + s.n_delivered_90d) AS cat_buyout_90d,
           (g.g_no_show * {k} + COALESCE(s.no_show_rate_90d, g.g_no_show) * s.n_delivered_90d)
               / ({k} + s.n_delivered_90d) AS cat_no_show_90d
    FROM sig s CROSS JOIN global_rate g
    WHERE s.key_type = 'category'
),

sku_map AS (
    SELECT id AS sku_id, product_id, category_id, shop_id, brand_name_id
    FROM {SKU_TABLE}
)

SELECT
    {partition_date_sql}                                AS date,
    s.key_id                                            AS sku_id,
    m.product_id,
    m.category_id,
    m.shop_id,
    m.brand_name_id,

    -- собственный сигнал sku
    s.n_delivered_90d                                   AS sku_n_delivered_90d,
    s.n_delivered_30d                                   AS sku_n_delivered_30d,
    s.buyout_rate_items_90d                             AS sku_buyout_rate_90d,
    s.buyout_rate_items_30d                             AS sku_buyout_rate_30d,
    s.no_show_rate_90d                                  AS sku_no_show_rate_90d,
    s.nonbuyout_rate_90d                                AS sku_nonbuyout_rate_90d,
    s.buyout_rate_money_90d                             AS sku_buyout_rate_money_90d,
    s.cancel_before_delivery_rate_90d                   AS sku_cancel_before_delivery_rate_90d,

    -- родители (сырые ставки 90д + объёмы)
    p.n_delivered_90d                                   AS product_n_delivered_90d,
    p.buyout_rate_items_90d                             AS product_buyout_rate_90d,
    p.no_show_rate_90d                                  AS product_no_show_rate_90d,
    c.cat_n_delivered_90d,
    c.cat_buyout_90d                                    AS category_buyout_rate_90d,
    c.cat_no_show_90d                                   AS category_no_show_rate_90d,
    sh.n_delivered_90d                                  AS shop_n_delivered_90d,
    sh.buyout_rate_items_90d                            AS shop_buyout_rate_90d,
    b.n_delivered_90d                                   AS brand_n_delivered_90d,
    b.buyout_rate_items_90d                             AS brand_buyout_rate_90d,

    -- сглаженные ставки (канон k=30)
    (c.cat_buyout_90d * {k} + COALESCE(s.buyout_rate_items_90d, c.cat_buyout_90d) * s.n_delivered_90d)
        / ({k} + s.n_delivered_90d)                     AS sku_buyout_rate_shrunk_90d,
    (c.cat_no_show_90d * {k} + COALESCE(s.no_show_rate_90d, c.cat_no_show_90d) * s.n_delivered_90d)
        / ({k} + s.n_delivered_90d)                     AS sku_no_show_rate_shrunk_90d,
    (c.cat_buyout_90d * {k} + COALESCE(p.buyout_rate_items_90d, c.cat_buyout_90d) * COALESCE(p.n_delivered_90d, 0))
        / ({k} + COALESCE(p.n_delivered_90d, 0))        AS product_buyout_rate_shrunk_90d,

    -- гипотеза MAD-13413: разрыв sku против карточки (размерный эффект одежды)
    (c.cat_buyout_90d * {k} + COALESCE(s.buyout_rate_items_90d, c.cat_buyout_90d) * s.n_delivered_90d)
        / ({k} + s.n_delivered_90d)
    - (c.cat_buyout_90d * {k} + COALESCE(p.buyout_rate_items_90d, c.cat_buyout_90d) * COALESCE(p.n_delivered_90d, 0))
        / ({k} + COALESCE(p.n_delivered_90d, 0))        AS sku_vs_product_gap_90d

FROM sig s
JOIN sku_map m        ON m.sku_id = s.key_id
LEFT JOIN sig p       ON p.key_type = 'product'  AND p.key_id = m.product_id
LEFT JOIN cat_smooth c ON c.category_id = m.category_id
LEFT JOIN sig sh      ON sh.key_type = 'shop'    AND sh.key_id = m.shop_id
LEFT JOIN sig b       ON b.key_type = 'brand'    AND b.key_id = m.brand_name_id
WHERE s.key_type = 'sku'
"""
