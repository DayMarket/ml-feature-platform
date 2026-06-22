from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from job.entities import Arguments


SOURCE_TABLE = "iceberg.silver.adv_funnel_daily"


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def build_sku_group_ad_revenue_daily(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    # adv_funnel_daily хранит числовые поля как строки, поэтому приводим их явно.
    # Источник содержит все рекламные показы CPC-funnel (выдача + категории) без
    # разреза по площадке, поэтому фильтр по space здесь не накладывается.
    return spark.sql(
        f"""
SELECT
    DATE('{run_date}') AS date,
    CAST(sku_group_id AS BIGINT) AS sku_group_id,
    CAST(SUM(CAST(impressions AS DOUBLE)) AS BIGINT) AS ad_impressions,
    CAST(SUM(CAST(clicks AS DOUBLE)) AS BIGINT) AS ad_clicks,
    SUM(CAST(adrev AS DOUBLE)) AS ad_revenue
FROM {SOURCE_TABLE}
WHERE date = DATE('{run_date}')
    AND sku_group_id IS NOT NULL
    AND CAST(sku_group_id AS BIGINT) > 0
GROUP BY
    DATE('{run_date}'),
    CAST(sku_group_id AS BIGINT)
"""
    )


def save_sku_group_ad_revenue_daily(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    features = build_sku_group_ad_revenue_daily(spark, run_date)

    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_ad_revenue_daily(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
