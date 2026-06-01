from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_product_feedback_base_stats(
    spark: SparkSession,
    partition_date: str,
):
    return spark.sql(
        f"""
WITH stats AS (
    SELECT
        CAST(f.product_id AS BIGINT) AS product_id,
        AVG(CAST(f.rating AS DOUBLE)) AS product_rating,
        COUNT(f.id) FILTER (WHERE f.rating <= 3) AS bad_reviews_count,
        COUNT(f.id) FILTER (WHERE f.rating > 3) AS good_reviews_count,
        COUNT(f.id) AS total_reviews_count,
        COUNT(f.id) FILTER (WHERE f.rating = 1) AS reviews_mark_one_count,
        COUNT(f.id) FILTER (WHERE f.rating = 2) AS reviews_mark_two_count,
        COUNT(f.id) FILTER (WHERE f.rating = 3) AS reviews_mark_three_count,
        COUNT(f.id) FILTER (WHERE f.rating = 4) AS reviews_mark_four_count,
        COUNT(f.id) FILTER (WHERE f.rating = 5) AS reviews_mark_five_count,
        COUNT(f.id) FILTER (WHERE COALESCE(f.message, '') != '') AS total_reviews_with_text
    FROM foodback.public.feedback f
    INNER JOIN (
        SELECT
            id AS sku_id,
            product_id,
            sku_group_id
        FROM `dwh-iceberg`.silver.sku
    ) s ON s.sku_id = f.sku_id
    WHERE
        f.status = 'PUBLISHED'
        AND f.date_published < DATE '{partition_date}'
    GROUP BY f.product_id
)
SELECT
    DATE '{partition_date}' AS date,
    product_id,
    product_rating,
    CAST(bad_reviews_count AS BIGINT) AS bad_reviews_count,
    CAST(good_reviews_count AS BIGINT) AS good_reviews_count,
    CAST(total_reviews_count AS BIGINT) AS total_reviews_count,
    CAST(reviews_mark_one_count AS BIGINT) AS reviews_mark_one_count,
    CAST(reviews_mark_two_count AS BIGINT) AS reviews_mark_two_count,
    CAST(reviews_mark_three_count AS BIGINT) AS reviews_mark_three_count,
    CAST(reviews_mark_four_count AS BIGINT) AS reviews_mark_four_count,
    CAST(reviews_mark_five_count AS BIGINT) AS reviews_mark_five_count,
    CAST(total_reviews_with_text AS BIGINT) AS total_reviews_with_text,
    COALESCE(CAST(reviews_mark_one_count AS DOUBLE) / NULLIF(total_reviews_count, 0), 0.0) AS ratio_reviews_mark_one,
    COALESCE(CAST(reviews_mark_two_count AS DOUBLE) / NULLIF(total_reviews_count, 0), 0.0) AS ratio_reviews_mark_two,
    COALESCE(CAST(reviews_mark_three_count AS DOUBLE) / NULLIF(total_reviews_count, 0), 0.0) AS ratio_reviews_mark_three,
    COALESCE(CAST(reviews_mark_four_count AS DOUBLE) / NULLIF(total_reviews_count, 0), 0.0) AS ratio_reviews_mark_four,
    COALESCE(CAST(reviews_mark_five_count AS DOUBLE) / NULLIF(total_reviews_count, 0), 0.0) AS ratio_reviews_mark_five,
    COALESCE(CAST(bad_reviews_count AS DOUBLE) / NULLIF(total_reviews_count, 0), 0.0) AS ratio_reviews_bad,
    COALESCE(CAST(good_reviews_count AS DOUBLE) / NULLIF(total_reviews_count, 0), 0.0) AS ratio_reviews_good
FROM stats
"""
    )


def save_product_feedback_base_stats(
    spark: SparkSession,
    partition_date: str,
    target_table: str,
) -> None:
    features = build_product_feedback_base_stats(spark, partition_date)

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_product_feedback_base_stats(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
