from datetime import date
from typing import Protocol


class SourceSettings(Protocol):
    product_table: str
    category_table: str
    sku_table: str
    category_gender_table: str
    technical_category_root_id: int
    excluded_brand_id: int
    max_category_depth: int


def _category_joins(settings: SourceSettings) -> str:
    return "\n".join(
        (
            f"LEFT JOIN {settings.category_table} c{level} "
            f"ON c{level - 1}.parent_id = c{level}.id"
        )
        for level in range(1, settings.max_category_depth)
    )


def build_category_depth_validation_query(settings: SourceSettings) -> str:
    last_alias = f"c{settings.max_category_depth - 1}"
    return f"""
SELECT c0.id
FROM {settings.category_table} c0
{_category_joins(settings)}
WHERE {last_alias}.parent_id IS NOT NULL
    AND {last_alias}.parent_id > 0
    AND {last_alias}.parent_id <> {settings.technical_category_root_id}
LIMIT 1
"""


def build_required_l6_validation_query(settings: SourceSettings) -> str:
    return f"""
SELECT CAST(product.id AS BIGINT) AS product_id
FROM {settings.product_table} product
LEFT JOIN {settings.category_table} category
    ON product.category_id = category.id
WHERE product.id IS NOT NULL
    AND (
        product.category_id IS NULL
        OR category.id IS NULL
        OR category.id = {settings.technical_category_root_id}
    )
LIMIT 1
"""


def _hierarchy_level_expression(level: int) -> str:
    candidates = ",\n                ".join(
        f"TRY_ELEMENT_AT(hierarchy, {candidate})"
        for candidate in range(level, 0, -1)
    )
    if level == 1:
        return f"CAST({candidates} AS BIGINT)"
    return f"""CAST(
            COALESCE(
                {candidates}
            ) AS BIGINT
        )"""


def build_product_attributes_snapshot_query(
    settings: SourceSettings,
    snapshot_date: str,
) -> str:
    normalized_snapshot_date = date.fromisoformat(snapshot_date).isoformat()
    category_ids = ",\n                    ".join(
        f"c{level}.id"
        for level in range(settings.max_category_depth)
    )
    hierarchy_columns = ",\n        ".join(
        (
            f"{_hierarchy_level_expression(level)} "
            f"AS l{level}_category_id"
        )
        for level in range(1, 6)
    )

    return f"""
WITH category_paths AS (
    SELECT
        CAST(c0.id AS BIGINT) AS category_id,
        REVERSE(
            FILTER(
                ARRAY(
                    {category_ids}
                ),
                value -> value IS NOT NULL
                    AND value <> {settings.technical_category_root_id}
            )
        ) AS hierarchy
    FROM {settings.category_table} c0
    {_category_joins(settings)}
),
category_hierarchy AS (
    SELECT
        category_id,
        {hierarchy_columns},
        CAST(TRY_ELEMENT_AT(hierarchy, -1) AS BIGINT) AS l6_category_id
    FROM category_paths
),
product_brands AS (
    SELECT
        CAST(product_id AS BIGINT) AS product_id,
        CAST(MIN(brand_name_id) AS BIGINT) AS brand_id
    FROM {settings.sku_table}
    WHERE product_id IS NOT NULL
        AND brand_name_id IS NOT NULL
        AND brand_name_id <> {settings.excluded_brand_id}
    GROUP BY product_id
),
category_genders AS (
    SELECT
        CAST(category_id AS BIGINT) AS category_id,
        CASE
            WHEN dominant_gender IN ('M', 'F', 'U') THEN dominant_gender
        END AS category_gender
    FROM {settings.category_gender_table}
)
SELECT
    DATE '{normalized_snapshot_date}' AS snapshot_date,
    CAST(product.id AS BIGINT) AS product_id,
    hierarchy.l1_category_id,
    hierarchy.l2_category_id,
    hierarchy.l3_category_id,
    hierarchy.l4_category_id,
    hierarchy.l5_category_id,
    hierarchy.l6_category_id,
    brands.brand_id,
    CAST(product.shop_id AS BIGINT) AS shop_id,
    CAST(product.created_at AS TIMESTAMP) AS created_at,
    genders.category_gender
FROM {settings.product_table} product
LEFT JOIN category_hierarchy hierarchy
    ON product.category_id = hierarchy.category_id
LEFT JOIN product_brands brands
    ON product.id = brands.product_id
LEFT JOIN category_genders genders
    ON product.category_id = genders.category_id
WHERE product.id IS NOT NULL
"""
