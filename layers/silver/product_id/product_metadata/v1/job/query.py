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
    return f"""
LEFT JOIN {settings.category_table} c1
    ON c0.parent_id = c1.id
LEFT JOIN {settings.category_table} c2
    ON c1.parent_id = c2.id
LEFT JOIN {settings.category_table} c3
    ON c2.parent_id = c3.id
LEFT JOIN {settings.category_table} c4
    ON c3.parent_id = c4.id
LEFT JOIN {settings.category_table} c5
    ON c4.parent_id = c5.id
"""


def build_category_depth_validation_query(settings: SourceSettings) -> str:
    return f"""
SELECT c0.id
FROM {settings.category_table} c0
{_category_joins(settings)}
WHERE c5.parent_id IS NOT NULL
    AND c5.parent_id > 0
    AND c5.parent_id != {settings.technical_category_root_id}
LIMIT 1
"""


def build_required_l6_validation_query(settings: SourceSettings) -> str:
    return f"""
SELECT CAST(product.id AS INT) AS product_id
FROM {settings.product_table} product
LEFT JOIN {settings.category_table} category
    ON product.category_id = category.id
WHERE product.id IS NOT NULL
    AND (
        product.category_id IS NULL
        OR product.category_id <= 0
        OR category.id IS NULL
        OR category.id = {settings.technical_category_root_id}
    )
LIMIT 1
"""


def build_product_metadata_query(
    settings: SourceSettings,
    dt: str,
) -> str:
    normalized_dt = date.fromisoformat(dt).isoformat()

    return f"""
WITH category_paths AS (
    SELECT
        c0.id AS category_id,
        REVERSE(
            FILTER(
                ARRAY(c0.id, c1.id, c2.id, c3.id, c4.id, c5.id),
                value -> value IS NOT NULL
                    AND value > 0
                    AND value != {settings.technical_category_root_id}
            )
        ) AS hierarchy
    FROM {settings.category_table} c0
    {_category_joins(settings)}
),
category_hierarchy AS (
    SELECT
        category_id,
        CAST(TRY_ELEMENT_AT(hierarchy, 1) AS INT) AS l1_category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS INT
        ) AS l2_category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 3),
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS INT
        ) AS l3_category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 4),
                TRY_ELEMENT_AT(hierarchy, 3),
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS INT
        ) AS l4_category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 5),
                TRY_ELEMENT_AT(hierarchy, 4),
                TRY_ELEMENT_AT(hierarchy, 3),
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS INT
        ) AS l5_category_id,
        CAST(category_id AS INT) AS l6_category_id
    FROM category_paths
),
product_brands AS (
    SELECT
        product_id,
        CAST(MIN(brand_name_id) AS INT) AS brand_id
    FROM {settings.sku_table}
    WHERE product_id IS NOT NULL
        AND brand_name_id IS NOT NULL
        AND brand_name_id != {settings.excluded_brand_id}
    GROUP BY product_id
),
category_genders AS (
    SELECT
        category_id,
        CASE
            WHEN dominant_gender IN ('M', 'F', 'U') THEN dominant_gender
        END AS category_gender
    FROM {settings.category_gender_table}
)
SELECT
    DATE '{normalized_dt}' AS dt,
    CAST(product.id AS INT) AS product_id,
    CAST(product.category_id AS INT) AS category_id,
    hierarchy.l1_category_id,
    hierarchy.l2_category_id,
    hierarchy.l3_category_id,
    hierarchy.l4_category_id,
    hierarchy.l5_category_id,
    hierarchy.l6_category_id,
    brands.brand_id,
    CAST(product.shop_id AS INT) AS shop_id,
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


def build_product_metadata_merge_query(target_table: str, dt: str) -> str:
    normalized_dt = date.fromisoformat(dt).isoformat()

    return f"""
MERGE INTO {target_table} AS target
USING product_metadata_for_dt AS source
    ON target.dt = source.dt
    AND target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    target.category_id = source.category_id,
    target.l1_category_id = source.l1_category_id,
    target.l2_category_id = source.l2_category_id,
    target.l3_category_id = source.l3_category_id,
    target.l4_category_id = source.l4_category_id,
    target.l5_category_id = source.l5_category_id,
    target.l6_category_id = source.l6_category_id,
    target.brand_id = source.brand_id,
    target.shop_id = source.shop_id,
    target.created_at = source.created_at,
    target.category_gender = source.category_gender
WHEN NOT MATCHED THEN INSERT (
    dt,
    product_id,
    category_id,
    l1_category_id,
    l2_category_id,
    l3_category_id,
    l4_category_id,
    l5_category_id,
    l6_category_id,
    brand_id,
    shop_id,
    created_at,
    category_gender
) VALUES (
    source.dt,
    source.product_id,
    source.category_id,
    source.l1_category_id,
    source.l2_category_id,
    source.l3_category_id,
    source.l4_category_id,
    source.l5_category_id,
    source.l6_category_id,
    source.brand_id,
    source.shop_id,
    source.created_at,
    source.category_gender
)
WHEN NOT MATCHED BY SOURCE
    AND target.dt = DATE '{normalized_dt}'
THEN DELETE
"""
