"""Trino query for category-level order completion and no-show shares."""

from __future__ import annotations

from datetime import date

SOURCE_TABLE = '"dwh-iceberg".silver.history_order_items'
CATEGORY_DICT_TABLE = '"dwh-clickhouse".dict.category'

# Возвраты по вине маркетплейса/контента: по ним статус заказа не отражает
# поведение покупателя, поэтому такие позиции не участвуют ни в числителе,
# ни в знаменателе.
EXCLUDED_RETURN_CAUSES = (
    "MISSING",
    "DEFECTED",
    "BAD_QUALITY",
    "WRONG_ITEM",
    "PHOTO_MISMATCH",
    "CONTENT",
)

# Значения category_level и колонка иерархии, из которой берется category_id.
# 'leaf' - категория позиции как есть, без подъема по дереву.
CATEGORY_LEVELS = (
    ("leaf", "leaf_category"),
    ("l1", "l1_category"),
    ("l2", "l2_category"),
    ("l3", "l3_category"),
    ("l4", "l4_category"),
)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_query(analyze_date: date, lookback_days: int) -> str:
    analyze_date_sql = f"DATE {_sql_string(analyze_date.isoformat())}"
    excluded_causes_sql = ", ".join(
        _sql_string(cause) for cause in EXCLUDED_RETURN_CAUSES
    )
    levels_sql = ",\n        ".join(
        f"ROW({_sql_string(level)}, {column})" for level, column in CATEGORY_LEVELS
    )

    return f"""
WITH order_items AS (
    SELECT
        oi.order_id,
        oi.real_order_item_status,
        CAST(oi.category_id AS BIGINT) AS leaf_category,
        CAST(cat.l1_category AS BIGINT) AS l1_category,
        CAST(cat.l2_category AS BIGINT) AS l2_category,
        CAST(cat.l3_category AS BIGINT) AS l3_category,
        CAST(cat.l4_category AS BIGINT) AS l4_category
    FROM {SOURCE_TABLE} oi
    LEFT JOIN {CATEGORY_DICT_TABLE} cat
        ON cat.id = oi.category_id
    WHERE oi.analyze_date = {analyze_date_sql}
      AND oi.generated_at > {analyze_date_sql} - INTERVAL '{lookback_days}' DAY
      AND oi.return_cause NOT IN ({excluded_causes_sql})
      AND oi.category_id IS NOT NULL
),
level_items AS (
    SELECT
        order_items.order_id,
        order_items.real_order_item_status,
        levels.category_level,
        levels.category_id
    FROM order_items
    CROSS JOIN UNNEST(ARRAY[
        {levels_sql}
    ]) AS levels (category_level, category_id)
    -- У части категорий дерево мельче запрошенного уровня: в dict.category
    -- отсутствующий уровень записан нулем. Такие позиции в срез уровня не входят.
    WHERE levels.category_id IS NOT NULL
      AND levels.category_id > 0
),
category_stats AS (
    SELECT
        category_level,
        category_id,
        COUNT(DISTINCT order_id) FILTER (
            WHERE real_order_item_status != 'ACTIVE'
        ) AS total_orders,
        COUNT(DISTINCT order_id) FILTER (
            WHERE real_order_item_status = 'COMPLETED'
        ) AS completed_orders,
        COUNT(DISTINCT order_id) FILTER (
            WHERE real_order_item_status = 'RETURNED NO SHOW'
        ) AS returned_no_show_orders
    FROM level_items
    GROUP BY category_level, category_id
)
SELECT
    {analyze_date_sql} AS date,
    category_level,
    category_id,
    CAST(completed_orders AS DOUBLE) / total_orders AS part_completed_orders,
    CAST(returned_no_show_orders AS DOUBLE) / total_orders AS part_no_show_from_total
FROM category_stats
WHERE total_orders > 0
"""
