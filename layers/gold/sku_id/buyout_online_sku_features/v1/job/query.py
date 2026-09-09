"""Trino query for the online SKU table of the buyout service.

Одна строка на sku: свои признаки + родительские (product / category / shop /
brand) + сглаженные выкупаемости. Сервис невыкупов забирает последнюю дату
одним поиском по sku_id.

Источник: gold.feature_platform_buyout_item_signal_features (партиция date)
+ "dwh-iceberg".silver.sku (маппинг sku -> product/category/shop/brand;
текущий снимок атрибутов — допустимо, атрибуты медленные, как в MAD-13227).

Сглаживание — как в обучающем запросе cart_item_signal.sql (MAD-13227), k = 30:
  категория стягивается к общей выкупаемости маркетплейса,
  sku и product — к сглаженной выкупаемости своей категории:
  shrunk = (parent_rate * k + raw_rate * n) / (k + n).
Признак гипотезы MAD-13413 «размеры внутри карточки выкупаются по-разному»:
  sku_vs_product_gap_90d = shrunk_sku - shrunk_product.
Магазин стягивается к общей выкупаемости маркетплейса (shop_buyout_rate_shrunk_90d),
сама общая выкупаемость отдаётся колонкой marketplace_buyout_rate_90d.

Население (MAD-13695): все активные sku в наличии (status = 'ACTIVE', остаток
quantity_active + quantity_additional + quantity_fbs > 0) плюс все sku с
доставками за 90 дней. У sku без доставок сырые доли NULL, число доставок 0,
сглаженные выкупаемости равны выкупаемости категории, а у категории без
доставок — маркетплейса: те же подстановки, что видела модель при обучении.
"""

from __future__ import annotations

from datetime import date

SKU_TABLE = '"dwh-iceberg".silver.sku'

# Сила стягивания к родителю: k = 30 «виртуальных» доставок, как в cart_item_signal.sql (MAD-13227).
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

-- население: активные sku в наличии плюс все sku с доставками за 90 дней
sku_map AS (
    SELECT id AS sku_id, product_id, category_id, shop_id, brand_name_id
    FROM {SKU_TABLE}
    WHERE (status = 'ACTIVE'
           AND COALESCE(quantity_active, 0) + COALESCE(quantity_additional, 0) + COALESCE(quantity_fbs, 0) > 0)
       OR id IN (SELECT key_id FROM sig WHERE key_type = 'sku')
),

-- родитель для sku и карточки: выкупаемость категории, а без доставок у категории — маркетплейса
parent AS (
    SELECT m.sku_id, m.product_id, m.category_id, m.shop_id, m.brand_name_id,
           COALESCE(c.cat_n_delivered_90d, 0)       AS cat_n_delivered_90d,
           COALESCE(c.cat_buyout_90d,  g.g_buyout)  AS cat_buyout_90d,
           COALESCE(c.cat_no_show_90d, g.g_no_show) AS cat_no_show_90d,
           g.g_buyout,
           g.g_no_show
    FROM sku_map m
    CROSS JOIN global_rate g
    LEFT JOIN cat_smooth c ON c.category_id = m.category_id
)

SELECT
    {partition_date_sql}                                AS date,
    m.sku_id,
    m.product_id,
    m.category_id,
    m.shop_id,
    m.brand_name_id,

    -- собственный сигнал sku: без доставок число доставок 0, сырые доли NULL
    COALESCE(s.n_delivered_90d, 0)                      AS sku_n_delivered_90d,
    COALESCE(s.n_delivered_30d, 0)                      AS sku_n_delivered_30d,
    s.buyout_rate_items_90d                             AS sku_buyout_rate_90d,
    s.buyout_rate_items_30d                             AS sku_buyout_rate_30d,
    s.no_show_rate_90d                                  AS sku_no_show_rate_90d,
    s.nonbuyout_rate_90d                                AS sku_nonbuyout_rate_90d,
    s.buyout_rate_money_90d                             AS sku_buyout_rate_money_90d,
    s.cancel_before_delivery_rate_90d                   AS sku_cancel_before_delivery_rate_90d,

    -- родители (сырые доли 90д + объёмы)
    COALESCE(p.n_delivered_90d, 0)                      AS product_n_delivered_90d,
    p.buyout_rate_items_90d                             AS product_buyout_rate_90d,
    p.no_show_rate_90d                                  AS product_no_show_rate_90d,
    m.cat_n_delivered_90d,
    m.cat_buyout_90d                                    AS category_buyout_rate_90d,
    m.cat_no_show_90d                                   AS category_no_show_rate_90d,
    COALESCE(sh.n_delivered_90d, 0)                     AS shop_n_delivered_90d,
    sh.buyout_rate_items_90d                            AS shop_buyout_rate_90d,
    COALESCE(b.n_delivered_90d, 0)                      AS brand_n_delivered_90d,
    b.buyout_rate_items_90d                             AS brand_buyout_rate_90d,

    -- общая выкупаемость маркетплейса (по строкам категорий) и сглаженная
    -- выкупаемость магазина: обучающий запрос cart_item_signal.sql (MAD-13227)
    -- стягивает магазин к маркетплейсу с k = 30 — модель обучена на этой величине (MAD-13695)
    m.g_buyout                                          AS marketplace_buyout_rate_90d,
    m.g_no_show                                         AS marketplace_no_show_rate_90d,
    (m.g_buyout * {k} + COALESCE(sh.buyout_rate_items_90d, m.g_buyout) * COALESCE(sh.n_delivered_90d, 0))
        / ({k} + COALESCE(sh.n_delivered_90d, 0))       AS shop_buyout_rate_shrunk_90d,

    -- сглаженные выкупаемости sku и карточки (k = 30): без доставок равны выкупаемости категории
    (m.cat_buyout_90d * {k} + COALESCE(s.buyout_rate_items_90d, m.cat_buyout_90d) * COALESCE(s.n_delivered_90d, 0))
        / ({k} + COALESCE(s.n_delivered_90d, 0))        AS sku_buyout_rate_shrunk_90d,
    (m.cat_no_show_90d * {k} + COALESCE(s.no_show_rate_90d, m.cat_no_show_90d) * COALESCE(s.n_delivered_90d, 0))
        / ({k} + COALESCE(s.n_delivered_90d, 0))        AS sku_no_show_rate_shrunk_90d,
    (m.cat_buyout_90d * {k} + COALESCE(p.buyout_rate_items_90d, m.cat_buyout_90d) * COALESCE(p.n_delivered_90d, 0))
        / ({k} + COALESCE(p.n_delivered_90d, 0))        AS product_buyout_rate_shrunk_90d,

    -- гипотеза MAD-13413: разрыв sku против карточки (размерный эффект одежды)
    (m.cat_buyout_90d * {k} + COALESCE(s.buyout_rate_items_90d, m.cat_buyout_90d) * COALESCE(s.n_delivered_90d, 0))
        / ({k} + COALESCE(s.n_delivered_90d, 0))
    - (m.cat_buyout_90d * {k} + COALESCE(p.buyout_rate_items_90d, m.cat_buyout_90d) * COALESCE(p.n_delivered_90d, 0))
        / ({k} + COALESCE(p.n_delivered_90d, 0))        AS sku_vs_product_gap_90d

FROM parent m
LEFT JOIN sig s       ON s.key_type = 'sku'      AND s.key_id = m.sku_id
LEFT JOIN sig p       ON p.key_type = 'product'  AND p.key_id = m.product_id
LEFT JOIN sig sh      ON sh.key_type = 'shop'    AND sh.key_id = m.shop_id
LEFT JOIN sig b       ON b.key_type = 'brand'    AND b.key_id = m.brand_name_id
"""
