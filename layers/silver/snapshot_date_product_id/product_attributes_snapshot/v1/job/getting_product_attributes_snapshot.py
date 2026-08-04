from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments
from job.partition import snapshot_date_from_partition_end

PRODUCT_TABLE = "iceberg.silver.product"
CATEGORY_TABLE = "iceberg.silver_apidb_kazanexpress.public_category"
SKU_TABLE = "iceberg.silver.sku"
CATEGORY_GENDER_TABLE = "iceberg.silver.recsys_category_genders"
TECHNICAL_CATEGORY_ROOT_ID = 1
EXCLUDED_BRAND_ID = 160078

SOURCE_TABLES = (
    PRODUCT_TABLE,
    CATEGORY_TABLE,
    SKU_TABLE,
    CATEGORY_GENDER_TABLE,
)


def _require_tables(spark: SparkSession, table_names: tuple[str, ...]) -> None:
    missing_tables = [
        table_name
        for table_name in table_names
        if not spark.catalog.tableExists(table_name)
    ]
    if missing_tables:
        raise RuntimeError(
            "Required Iceberg tables are missing: "
            f"{', '.join(missing_tables)}"
        )


def _validate_category_depth(spark: SparkSession) -> None:
    categories_deeper_than_supported = spark.sql(
        f"""
SELECT c0.id
FROM {CATEGORY_TABLE} c0
LEFT JOIN {CATEGORY_TABLE} c1 ON c0.parent_id = c1.id
LEFT JOIN {CATEGORY_TABLE} c2 ON c1.parent_id = c2.id
LEFT JOIN {CATEGORY_TABLE} c3 ON c2.parent_id = c3.id
LEFT JOIN {CATEGORY_TABLE} c4 ON c3.parent_id = c4.id
LEFT JOIN {CATEGORY_TABLE} c5 ON c4.parent_id = c5.id
LEFT JOIN {CATEGORY_TABLE} c6 ON c5.parent_id = c6.id
LEFT JOIN {CATEGORY_TABLE} c7 ON c6.parent_id = c7.id
LEFT JOIN {CATEGORY_TABLE} c8 ON c7.parent_id = c8.id
LEFT JOIN {CATEGORY_TABLE} c9 ON c8.parent_id = c9.id
WHERE c9.parent_id IS NOT NULL
    AND c9.parent_id > 0
LIMIT 1
"""
    ).take(1)

    if categories_deeper_than_supported:
        category_id = categories_deeper_than_supported[0]["id"]
        raise RuntimeError(
            "Category hierarchy exceeds the supported depth of 10 levels; "
            f"example category_id={category_id}"
        )


def build_product_attributes_snapshot(
    spark: SparkSession,
    snapshot_date: str,
) -> DataFrame:
    return spark.sql(
        f"""
WITH category_paths AS (
    SELECT
        CAST(c0.id AS BIGINT) AS category_id,
        REVERSE(
            FILTER(
                ARRAY(
                    c0.id,
                    c1.id,
                    c2.id,
                    c3.id,
                    c4.id,
                    c5.id,
                    c6.id,
                    c7.id,
                    c8.id,
                    c9.id
                ),
                value -> value IS NOT NULL
                    AND value <> {TECHNICAL_CATEGORY_ROOT_ID}
            )
        ) AS hierarchy
    FROM {CATEGORY_TABLE} c0
    LEFT JOIN {CATEGORY_TABLE} c1 ON c0.parent_id = c1.id
    LEFT JOIN {CATEGORY_TABLE} c2 ON c1.parent_id = c2.id
    LEFT JOIN {CATEGORY_TABLE} c3 ON c2.parent_id = c3.id
    LEFT JOIN {CATEGORY_TABLE} c4 ON c3.parent_id = c4.id
    LEFT JOIN {CATEGORY_TABLE} c5 ON c4.parent_id = c5.id
    LEFT JOIN {CATEGORY_TABLE} c6 ON c5.parent_id = c6.id
    LEFT JOIN {CATEGORY_TABLE} c7 ON c6.parent_id = c7.id
    LEFT JOIN {CATEGORY_TABLE} c8 ON c7.parent_id = c8.id
    LEFT JOIN {CATEGORY_TABLE} c9 ON c8.parent_id = c9.id
),
category_hierarchy AS (
    SELECT
        category_id,
        CAST(TRY_ELEMENT_AT(hierarchy, 1) AS BIGINT) AS l1_category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS BIGINT
        ) AS l2_category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 3),
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS BIGINT
        ) AS l3_category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 4),
                TRY_ELEMENT_AT(hierarchy, 3),
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS BIGINT
        ) AS l4_category_id,
        CAST(
            COALESCE(
                TRY_ELEMENT_AT(hierarchy, 5),
                TRY_ELEMENT_AT(hierarchy, 4),
                TRY_ELEMENT_AT(hierarchy, 3),
                TRY_ELEMENT_AT(hierarchy, 2),
                TRY_ELEMENT_AT(hierarchy, 1)
            ) AS BIGINT
        ) AS l5_category_id
    FROM category_paths
),
product_brands AS (
    SELECT
        CAST(product_id AS BIGINT) AS product_id,
        CAST(MIN(brand_name_id) AS BIGINT) AS brand_id
    FROM {SKU_TABLE}
    WHERE product_id IS NOT NULL
        AND brand_name_id IS NOT NULL
        AND brand_name_id <> {EXCLUDED_BRAND_ID}
    GROUP BY product_id
),
category_genders AS (
    SELECT
        CAST(category_id AS BIGINT) AS category_id,
        CASE
            WHEN dominant_gender IN ('M', 'F', 'U') THEN dominant_gender
        END AS category_gender
    FROM {CATEGORY_GENDER_TABLE}
)
SELECT
    DATE '{snapshot_date}' AS snapshot_date,
    CAST(product.id AS BIGINT) AS product_id,
    hierarchy.l1_category_id,
    hierarchy.l2_category_id,
    hierarchy.l3_category_id,
    hierarchy.l4_category_id,
    hierarchy.l5_category_id,
    CAST(product.category_id AS BIGINT) AS l6_category_id,
    brands.brand_id,
    CAST(product.shop_id AS BIGINT) AS shop_id,
    CAST(product.created_at AS TIMESTAMP) AS created_at,
    genders.category_gender
FROM {PRODUCT_TABLE} product
LEFT JOIN category_hierarchy hierarchy
    ON product.category_id = hierarchy.category_id
LEFT JOIN product_brands brands
    ON product.id = brands.product_id
LEFT JOIN category_genders genders
    ON product.category_id = genders.category_id
WHERE product.id IS NOT NULL
"""
    )


def save_product_attributes_snapshot(
    spark: SparkSession,
    partition_end: str,
    target_table: str,
) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    _require_tables(spark, SOURCE_TABLES)
    _require_tables(spark, (target_table,))
    _validate_category_depth(spark)

    snapshot_date = snapshot_date_from_partition_end(partition_end).isoformat()
    snapshot = build_product_attributes_snapshot(spark, snapshot_date)
    snapshot.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments) -> None:
    save_product_attributes_snapshot(
        spark,
        arguments.partition_end,
        arguments.table_name,
    )
