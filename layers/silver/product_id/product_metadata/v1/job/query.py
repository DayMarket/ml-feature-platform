from datetime import datetime
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
LEFT JOIN {settings.category_table} parent_1
    ON leaf_category.parent_id = parent_1.id
LEFT JOIN {settings.category_table} parent_2
    ON parent_1.parent_id = parent_2.id
LEFT JOIN {settings.category_table} parent_3
    ON parent_2.parent_id = parent_3.id
LEFT JOIN {settings.category_table} parent_4
    ON parent_3.parent_id = parent_4.id
LEFT JOIN {settings.category_table} parent_5
    ON parent_4.parent_id = parent_5.id
"""


def build_category_depth_validation_query(settings: SourceSettings) -> str:
    return f"""
SELECT leaf_category.id
FROM {settings.category_table} leaf_category
{_category_joins(settings)}
WHERE parent_5.parent_id IS NOT NULL
    AND parent_5.parent_id > 0
    AND parent_5.parent_id != {settings.technical_category_root_id}
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
    dt: datetime,
) -> str:
    normalized_dt = dt.isoformat(sep=" ", timespec="seconds")

    return f"""
WITH category_ancestors AS (
    SELECT
        leaf_category.id AS category_id,
        parent_1.id AS parent_1_id,
        parent_2.id AS parent_2_id,
        parent_3.id AS parent_3_id,
        parent_4.id AS parent_4_id,
        parent_5.id AS parent_5_id,
        CASE
            WHEN parent_5.id > 0
             AND parent_5.id != {settings.technical_category_root_id} THEN 6
            WHEN parent_4.id > 0
             AND parent_4.id != {settings.technical_category_root_id} THEN 5
            WHEN parent_3.id > 0
             AND parent_3.id != {settings.technical_category_root_id} THEN 4
            WHEN parent_2.id > 0
             AND parent_2.id != {settings.technical_category_root_id} THEN 3
            WHEN parent_1.id > 0
             AND parent_1.id != {settings.technical_category_root_id} THEN 2
            ELSE 1
        END AS category_depth
    FROM {settings.category_table} leaf_category
    {_category_joins(settings)}
    WHERE leaf_category.id > 0
      AND leaf_category.id != {settings.technical_category_root_id}
),
category_hierarchy AS (
    SELECT
        category_id,
        CASE category_depth
            WHEN 6 THEN parent_5_id
            WHEN 5 THEN parent_4_id
            WHEN 4 THEN parent_3_id
            WHEN 3 THEN parent_2_id
            WHEN 2 THEN parent_1_id
            ELSE category_id
        END AS l1_category_id,
        CASE category_depth
            WHEN 6 THEN parent_4_id
            WHEN 5 THEN parent_3_id
            WHEN 4 THEN parent_2_id
            WHEN 3 THEN parent_1_id
            ELSE category_id
        END AS l2_category_id,
        CASE category_depth
            WHEN 6 THEN parent_3_id
            WHEN 5 THEN parent_2_id
            WHEN 4 THEN parent_1_id
            ELSE category_id
        END AS l3_category_id,
        CASE category_depth
            WHEN 6 THEN parent_2_id
            WHEN 5 THEN parent_1_id
            ELSE category_id
        END AS l4_category_id,
        CASE category_depth
            WHEN 6 THEN parent_1_id
            ELSE category_id
        END AS l5_category_id,
        category_id AS l6_category_id
    FROM category_ancestors
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
    TIMESTAMP '{normalized_dt}' AS dt,
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


def build_product_metadata_merge_query(target_table: str, dt: datetime) -> str:
    normalized_dt = dt.isoformat(sep=" ", timespec="seconds")

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
    AND target.dt = TIMESTAMP '{normalized_dt}'
THEN DELETE
"""
