"""Trino query для витрины экономики корзины на грейне sku_id.

База строк — весь sku-универс silver.sku. Тип товара определяется антиджойном
к последней приёмке 1p-склада, а не UNION'ом двух веток: ветка 3p не фильтрует
продавца и содержит все sku ветки 1p, поэтому union задвоил бы их.

Категорийное дерево прокидывает вниз последний ненулевой уровень. Справочник
хранит нули вместо NULL, и обрыв всегда сплошной: пар вида «l3 = 0 при l4 <> 0»
в dict.category нет, поэтому пер-уровневый COALESCE эквивалентен каскаду.
"""

from __future__ import annotations

from datetime import date

SKU_TABLE = '"dwh-iceberg".silver.sku'
OLTP_SKU_TABLE = "kazanexpress.public.sku"
STOCK_FLOW_TABLE = '"dwh-clickhouse".marts.stock_flow_1p'
SELLER_TABLE = '"dwh-clickhouse".dict.seller'
CATEGORY_TABLE = '"dwh-clickhouse".dict.category'

ACCEPTANCE_TRANSACTION_TYPE = "EventType.ACCEPTANCE"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _range_predicate(column: str, lower: int | None, upper: int | None) -> str:
    """Предикат среза. Диапазон по sku_id, а не остаток от деления.

    Оба варианта пушдаунятся в Postgres-коннектор, но `id % n = k` не может
    воспользоваться индексом и вызывает полный seq scan OLTP на каждом срезе.
    """
    clauses = []
    if lower is not None:
        clauses.append(f"{column} >= {int(lower)}")
    if upper is not None:
        clauses.append(f"{column} < {int(upper)}")
    return " AND ".join(clauses) if clauses else "TRUE"


def build_query(
    partition_date: date,
    signal_table: str,
    lower: int | None,
    upper: int | None,
) -> str:
    """SQL одного среза партиции; signal_table — Trino-имя gold-источника."""
    partition_date_sql = f"DATE {_sql_string(partition_date.isoformat())}"
    acceptance = _sql_string(ACCEPTANCE_TRANSACTION_TYPE)

    return f"""
WITH one_p AS (
    SELECT sku_id, value_per_piece
    FROM (
        SELECT
            CAST(f.sku_id AS BIGINT) AS sku_id,
            f.value_per_piece,
            ROW_NUMBER() OVER (
                PARTITION BY f.sku_id ORDER BY f.stock_changed_at DESC
            ) AS row_order
        FROM {STOCK_FLOW_TABLE} f
        WHERE f.transaction_type = {acceptance}
          AND {_range_predicate("f.sku_id", lower, upper)}
    ) latest
    WHERE latest.row_order = 1
      AND latest.value_per_piece > 0
      AND latest.sku_id IN (
          SELECT s.id
          FROM {SKU_TABLE} s
          JOIN {SELLER_TABLE} sel ON sel.id = s.seller_id
          WHERE sel.is_1p = 1
            AND {_range_predicate("s.id", lower, upper)}
      )
),

base AS (
    SELECT
        s.id AS sku_id,
        s.product_id,
        s.seller_id,
        s.category_id,
        s.height,
        s."length" AS length_mm,
        s.width,
        s.dimensional_group,
        s.height + s."length" + s.width AS total_size
    FROM {SKU_TABLE} s
    WHERE {_range_predicate("s.id", lower, upper)}
),

dims AS (
    SELECT
        sku_id,
        product_id,
        seller_id,
        category_id,
        CASE
            WHEN total_size < 500 THEN 'SMALL'
            WHEN total_size >= 500 AND total_size < 1700
                 AND height < 500 AND length_mm < 500 AND width < 500 THEN 'MEDIUM'
            WHEN total_size IS NULL THEN NULLIF(dimensional_group, '')
            ELSE 'LARGE'
        END AS predicted_dimensional_group
    FROM base
),

cat AS (
    SELECT
        CAST(c.id AS BIGINT) AS category_id,
        CAST(c.l1_category AS BIGINT) AS l1_category,
        CAST(COALESCE(NULLIF(c.l2_category, 0), c.l1_category) AS BIGINT)
            AS l2_category,
        CAST(COALESCE(
            NULLIF(c.l3_category, 0),
            NULLIF(c.l2_category, 0),
            c.l1_category
        ) AS BIGINT) AS l3_category,
        CAST(COALESCE(
            NULLIF(c.l4_category, 0),
            NULLIF(c.l3_category, 0),
            NULLIF(c.l2_category, 0),
            c.l1_category
        ) AS BIGINT) AS l4_category,
        CAST(COALESCE(
            NULLIF(c.l5_category, 0),
            NULLIF(c.l4_category, 0),
            NULLIF(c.l3_category, 0),
            NULLIF(c.l2_category, 0),
            c.l1_category
        ) AS BIGINT) AS l5_category
    FROM {CATEGORY_TABLE} c
),

comm AS (
    SELECT ke.id AS sku_id, ke.commission
    FROM {OLTP_SKU_TABLE} ke
    WHERE {_range_predicate("ke.id", lower, upper)}
),

feat AS (
    SELECT
        sku_id,
        sku_buyout_rate_shrunk_90d,
        product_buyout_rate_shrunk_90d,
        shop_buyout_rate_shrunk_90d,
        category_buyout_rate_90d,
        category_no_show_rate_90d,
        sku_n_delivered_90d,
        product_n_delivered_90d
    FROM {signal_table}
    WHERE date = {partition_date_sql}
      AND {_range_predicate("sku_id", lower, upper)}
)

SELECT
    {partition_date_sql} AS date,
    dims.sku_id,
    dims.product_id,
    dims.seller_id,
    dims.category_id,
    cat.l1_category,
    cat.l2_category,
    cat.l3_category,
    cat.l4_category,
    cat.l5_category,
    CASE WHEN one_p.sku_id IS NOT NULL THEN '1p' ELSE '3p' END AS type,
    CASE WHEN one_p.sku_id IS NULL THEN comm.commission END AS commission,
    one_p.value_per_piece AS cost_price,
    false AS is_not_block,
    feat.sku_buyout_rate_shrunk_90d AS sku_buyout,
    feat.product_buyout_rate_shrunk_90d AS product_buyout,
    feat.category_buyout_rate_90d AS category_buyout,
    feat.shop_buyout_rate_shrunk_90d AS shop_buyout,
    feat.category_no_show_rate_90d AS category_no_show,
    feat.sku_n_delivered_90d AS sku_n_delivered,
    feat.product_n_delivered_90d AS product_n_delivered,
    dims.predicted_dimensional_group
FROM dims
LEFT JOIN one_p ON one_p.sku_id = dims.sku_id
LEFT JOIN comm ON comm.sku_id = dims.sku_id
LEFT JOIN cat ON cat.category_id = dims.category_id
LEFT JOIN feat ON feat.sku_id = dims.sku_id
"""
