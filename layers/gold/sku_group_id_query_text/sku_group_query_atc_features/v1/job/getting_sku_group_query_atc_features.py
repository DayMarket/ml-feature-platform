from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_search_sku_group_query_atc_features(
    spark: SparkSession,
    partition_date: str,
):
    return spark.sql(
        f"""
WITH final_stats AS (
    SELECT
        sku_group_id,
        lower(trim(uniqs)) AS query_text,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 90 DAY THEN sum_atc ELSE 0 END) AS atc_90_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 60 DAY THEN sum_atc ELSE 0 END) AS atc_60_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 30 DAY THEN sum_atc ELSE 0 END) AS atc_30_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 21 DAY THEN sum_atc ELSE 0 END) AS atc_21_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 14 DAY THEN sum_atc ELSE 0 END) AS atc_14_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 7 DAY THEN sum_atc ELSE 0 END) AS atc_7_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 3 DAY THEN sum_atc ELSE 0 END) AS atc_3_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 1 DAY THEN sum_atc ELSE 0 END) AS atc_1_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 90 DAY THEN sum_impressions ELSE 0 END) AS impressions_90_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 60 DAY THEN sum_impressions ELSE 0 END) AS impressions_60_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 30 DAY THEN sum_impressions ELSE 0 END) AS impressions_30_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 21 DAY THEN sum_impressions ELSE 0 END) AS impressions_21_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 14 DAY THEN sum_impressions ELSE 0 END) AS impressions_14_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 7 DAY THEN sum_impressions ELSE 0 END) AS impressions_7_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 3 DAY THEN sum_impressions ELSE 0 END) AS impressions_3_day,
        SUM(CASE WHEN date <= DATE '{partition_date}' AND date > DATE '{partition_date}' - INTERVAL 1 DAY THEN sum_impressions ELSE 0 END) AS impressions_1_day
    FROM iceberg.silver.feature_platform_search_sku_group_id_install_query
    WHERE
        space = 'SEARCH_RESULTS'
        AND date <= DATE '{partition_date}'
        AND date > DATE '{partition_date}' - INTERVAL 90 DAY
    GROUP BY
        sku_group_id,
        lower(trim(uniqs))
)
SELECT
    DATE '{partition_date}' AS date,
    CAST(sku_group_id AS BIGINT) AS sku_group_id,
    CAST(query_text AS STRING) AS query_text,
    COALESCE(CAST(atc_1_day AS DOUBLE) / NULLIF(impressions_1_day, 0), 0) AS query_skg_conv_imp2atc_1,
    COALESCE(CAST(atc_3_day AS DOUBLE) / NULLIF(impressions_3_day, 0), 0) AS query_skg_conv_imp2atc_3,
    COALESCE(CAST(atc_7_day AS DOUBLE) / NULLIF(impressions_7_day, 0), 0) AS query_skg_conv_imp2atc_7,
    COALESCE(CAST(atc_14_day AS DOUBLE) / NULLIF(impressions_14_day, 0), 0) AS query_skg_conv_imp2atc_14,
    COALESCE(CAST(atc_21_day AS DOUBLE) / NULLIF(impressions_21_day, 0), 0) AS query_skg_conv_imp2atc_21,
    COALESCE(CAST(atc_30_day AS DOUBLE) / NULLIF(impressions_30_day, 0), 0) AS query_skg_conv_imp2atc_30,
    COALESCE(CAST(atc_60_day AS DOUBLE) / NULLIF(impressions_60_day, 0), 0) AS query_skg_conv_imp2atc_60,
    COALESCE(CAST(atc_90_day AS DOUBLE) / NULLIF(impressions_60_day, 0), 0) AS query_skg_conv_imp2atc_90,
    COALESCE(CAST(atc_1_day AS DOUBLE) / NULLIF(SUM(atc_1_day) OVER (PARTITION BY query_text), 0), 0) AS share_of_atc_1,
    COALESCE(CAST(atc_3_day AS DOUBLE) / NULLIF(SUM(atc_3_day) OVER (PARTITION BY query_text), 0), 0) AS share_of_atc_3,
    COALESCE(CAST(atc_7_day AS DOUBLE) / NULLIF(SUM(atc_7_day) OVER (PARTITION BY query_text), 0), 0) AS share_of_atc_7,
    COALESCE(CAST(atc_14_day AS DOUBLE) / NULLIF(SUM(atc_14_day) OVER (PARTITION BY query_text), 0), 0) AS share_of_atc_14,
    COALESCE(CAST(atc_21_day AS DOUBLE) / NULLIF(SUM(atc_21_day) OVER (PARTITION BY query_text), 0), 0) AS share_of_atc_21,
    COALESCE(CAST(atc_30_day AS DOUBLE) / NULLIF(SUM(atc_30_day) OVER (PARTITION BY query_text), 0), 0) AS share_of_atc_30,
    COALESCE(CAST(atc_60_day AS DOUBLE) / NULLIF(SUM(atc_60_day) OVER (PARTITION BY query_text), 0), 0) AS share_of_atc_60,
    COALESCE(CAST(atc_90_day AS DOUBLE) / NULLIF(SUM(atc_90_day) OVER (PARTITION BY query_text), 0), 0) AS share_of_atc_90
FROM final_stats
"""
    )


def save_search_sku_group_query_atc_features(
    spark: SparkSession,
    partition_date: str,
    target_table: str,
) -> None:
    features = build_search_sku_group_query_atc_features(spark, partition_date)

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_search_sku_group_query_atc_features(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
