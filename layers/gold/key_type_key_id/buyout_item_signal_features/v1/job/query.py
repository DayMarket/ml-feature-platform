"""Trino query for the buyout item signal table (long format).

Грейн: date x key_type x key_id, key_type = 'sku' | 'product' | 'category'
| 'shop' | 'brand'. Счётчики и ставки в двух окнах — 30 и 90 дней до date
(окно явно в имени колонки). Один скан 90-дневного окна, 30-дневное — FILTER.

Источник: срез "dwh-iceberg".silver.history_order_items (analyze_date = date,
публикуется в 19:00 UTC того же дня) + silver.sku (привязка к product / shop /
бренду). Классификация исходов — канон target_orders.sql (MAD-13227):
доставка по delivered_at ИЛИ (COURIER и issued_at); клиентский невыкуп =
RETURNED NO SHOW или (RETURNED и CANCELED); брак/пересорт — отдельным счётчиком.

Позиции ACTIVE исключены: их исход неизвестен, в знаменателе они занижали бы
выкупаемость свежих товаров. Ставки сырые, без сглаживания — сглаживание к
родителю (k=30) делает online-проекция buyout_online_sku_features.
"""

from __future__ import annotations

from datetime import date

SOURCE_TABLE = '"dwh-iceberg".silver.history_order_items'
SKU_TABLE = '"dwh-iceberg".silver.sku'

# Возвраты по вине маркетплейса или контента: не клиентский невыкуп,
# считаются отдельным счётчиком n_fair_return_90d.
FAIR_RETURN_CAUSES = (
    "MISSING",
    "DEFECTED",
    "BAD_QUALITY",
    "WRONG_ITEM",
    "PHOTO_MISMATCH",
    "CONTENT",
)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(analyze_date: date) -> str:
    """SQL среза на analyze_date; окна 30 и 90 дней зашиты в имена колонок."""
    analyze_date_sql = f"DATE {_sql_string(analyze_date.isoformat())}"
    fair_return_causes_sql = ", ".join(
        _sql_string(cause) for cause in FAIR_RETURN_CAUSES
    )

    return f"""
WITH src AS (
    SELECT
        h.sku_id,
        s.product_id,
        h.category_id,
        s.shop_id,
        s.brand_name_id,
        h.gmv_generated,
        h.real_order_item_status AS st,
        NULLIF(h.return_cause, '') AS return_cause,
        h.delivered_at,
        h.issued_at,
        h.returned_at,
        h.delivery_type,
        -- флаг 30-дневного окна; сканируем всегда 90 дней
        CASE WHEN h.generated_at >= with_timezone(
                 CAST(date_add('day', -30, {analyze_date_sql}) AS TIMESTAMP), 'Asia/Tashkent')
             THEN 1 ELSE 0 END AS in_30d
    FROM {SOURCE_TABLE} AS h
    LEFT JOIN {SKU_TABLE} AS s ON s.id = h.sku_id
    WHERE h.analyze_date = {analyze_date_sql}
      AND h.generated_at >= with_timezone(
              CAST(date_add('day', -90, {analyze_date_sql}) AS TIMESTAMP), 'Asia/Tashkent')
      AND h.real_order_item_status <> 'ACTIVE'
),

flagged AS (
    SELECT
        src.*,
        CASE WHEN src.delivered_at IS NOT NULL
                  OR (src.delivery_type = 'COURIER' AND src.issued_at IS NOT NULL)
             THEN 1 ELSE 0 END AS is_delivered,
        CASE WHEN src.st = 'COMPLETED' THEN 1 ELSE 0 END AS is_completed,
        CASE WHEN src.st = 'RETURNED NO SHOW' THEN 1 ELSE 0 END AS is_no_show,
        CASE WHEN src.st = 'RETURNED NO SHOW'
                  OR (src.st = 'RETURNED' AND src.return_cause = 'CANCELED')
             THEN 1 ELSE 0 END AS is_nonbuyout_client,
        CASE WHEN src.st = 'RETURNED AFTER COMPLETED' THEN 1 ELSE 0 END AS is_return_after_completed,
        CASE WHEN src.st = 'RETURNED BEFORE DELIVERY' THEN 1 ELSE 0 END AS is_cancel_before_delivery,
        CASE WHEN src.returned_at IS NOT NULL
                  AND src.return_cause IN ({fair_return_causes_sql})
             THEN 1 ELSE 0 END AS is_fair_return
    FROM src
),

agg AS (
    SELECT
        CASE
            WHEN GROUPING(sku_id)        = 0 THEN 'sku'
            WHEN GROUPING(product_id)    = 0 THEN 'product'
            WHEN GROUPING(category_id)   = 0 THEN 'category'
            WHEN GROUPING(shop_id)       = 0 THEN 'shop'
            ELSE 'brand'
        END AS key_type,
        -- явный разбор вместо COALESCE: при пропуске в silver.sku ключ соседнего
        -- уровня подставился бы молча и агрегаты смешались бы между гранулярностями
        CASE
            WHEN GROUPING(sku_id)      = 0 THEN sku_id
            WHEN GROUPING(product_id)  = 0 THEN product_id
            WHEN GROUPING(category_id) = 0 THEN category_id
            WHEN GROUPING(shop_id)     = 0 THEN shop_id
            ELSE brand_name_id
        END AS key_id,

        -- окно 90 дней
        COUNT(*)                                                          AS n_rows_90d,
        SUM(is_delivered)                                                 AS n_delivered_90d,
        SUM(CASE WHEN is_delivered = 1 THEN is_completed ELSE 0 END)      AS n_completed_90d,
        SUM(is_no_show)                                                   AS n_no_show_90d,
        SUM(is_nonbuyout_client)                                          AS n_nonbuyout_client_90d,
        SUM(is_return_after_completed)                                    AS n_return_after_completed_90d,
        SUM(is_fair_return)                                               AS n_fair_return_90d,
        SUM(is_cancel_before_delivery)                                    AS n_cancel_before_delivery_90d,
        CAST(SUM(CASE WHEN is_delivered = 1 THEN gmv_generated ELSE 0 END) AS DOUBLE)                    AS gmv_delivered_90d,
        CAST(SUM(CASE WHEN is_delivered = 1 AND is_completed = 1 THEN gmv_generated ELSE 0 END) AS DOUBLE) AS gmv_completed_90d,
        CAST(SUM(CASE WHEN is_fair_return = 1 THEN gmv_generated ELSE 0 END) AS DOUBLE)                  AS gmv_fair_return_90d,

        -- окно 30 дней (FILTER по in_30d)
        COUNT(*)                        FILTER (WHERE in_30d = 1)         AS n_rows_30d,
        SUM(is_delivered)               FILTER (WHERE in_30d = 1)         AS n_delivered_30d,
        SUM(CASE WHEN is_delivered = 1 THEN is_completed ELSE 0 END)
                                        FILTER (WHERE in_30d = 1)         AS n_completed_30d,
        SUM(is_no_show)                 FILTER (WHERE in_30d = 1)         AS n_no_show_30d,
        SUM(is_nonbuyout_client)        FILTER (WHERE in_30d = 1)         AS n_nonbuyout_client_30d,
        SUM(is_cancel_before_delivery)  FILTER (WHERE in_30d = 1)         AS n_cancel_before_delivery_30d
    FROM flagged
    GROUP BY GROUPING SETS ((sku_id), (product_id), (category_id), (shop_id), (brand_name_id))
)

SELECT
    {analyze_date_sql} AS date,
    key_type,
    key_id,
    n_rows_90d,
    n_delivered_90d,
    n_completed_90d,
    n_no_show_90d,
    n_nonbuyout_client_90d,
    n_return_after_completed_90d,
    n_fair_return_90d,
    n_cancel_before_delivery_90d,
    gmv_delivered_90d,
    gmv_completed_90d,
    gmv_fair_return_90d,
    n_rows_30d,
    n_delivered_30d,
    n_completed_30d,
    n_no_show_30d,
    n_nonbuyout_client_30d,
    n_cancel_before_delivery_30d,
    -- сырые ставки; сглаживание к родителю (k=30) — в online-проекции
    CASE WHEN n_delivered_90d > 0 THEN CAST(n_completed_90d AS DOUBLE) / n_delivered_90d END        AS buyout_rate_items_90d,
    CASE WHEN n_delivered_90d > 0 THEN CAST(n_no_show_90d AS DOUBLE) / n_delivered_90d END          AS no_show_rate_90d,
    CASE WHEN n_delivered_90d > 0 THEN CAST(n_nonbuyout_client_90d AS DOUBLE) / n_delivered_90d END AS nonbuyout_rate_90d,
    CASE WHEN (gmv_delivered_90d - gmv_fair_return_90d) > 0
         THEN gmv_completed_90d / (gmv_delivered_90d - gmv_fair_return_90d) END                     AS buyout_rate_money_90d,
    CASE WHEN n_rows_90d > 0 THEN CAST(n_cancel_before_delivery_90d AS DOUBLE) / n_rows_90d END     AS cancel_before_delivery_rate_90d,
    CASE WHEN n_delivered_30d > 0 THEN CAST(n_completed_30d AS DOUBLE) / n_delivered_30d END        AS buyout_rate_items_30d,
    CASE WHEN n_delivered_30d > 0 THEN CAST(n_no_show_30d AS DOUBLE) / n_delivered_30d END          AS no_show_rate_30d,
    CASE WHEN n_delivered_30d > 0 THEN CAST(n_nonbuyout_client_30d AS DOUBLE) / n_delivered_30d END AS nonbuyout_rate_30d,
    CASE WHEN n_rows_30d > 0 THEN CAST(n_cancel_before_delivery_30d AS DOUBLE) / n_rows_30d END     AS cancel_before_delivery_rate_30d
FROM agg
WHERE key_id IS NOT NULL
"""
