from pathlib import Path

from pyspark.sql import SparkSession

from job.entities import Arguments


PRICE_BOUND_COLUMNS = {
    "min_sell_price_eod",
    "max_sell_price_eod",
    "min_full_price_eod",
    "max_full_price_eod",
}


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _load_migration_statements(migration_name: str) -> list[str]:
    migration_query = _load_migration_query(migration_name)
    return [
        statement.strip()
        for statement in migration_query.split(";")
        if statement.strip()
    ]


def _ensure_table_schema(spark: SparkSession, target_table: str) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))
        return

    existing_columns = {column.lower() for column in spark.table(target_table).columns}
    missing_columns = PRICE_BOUND_COLUMNS - existing_columns
    if not missing_columns:
        return

    for statement in _load_migration_statements("20260602_add_price_min_max_columns.sql"):
        column_name = statement.split(" ADD COLUMN ", 1)[1].split(" ", 1)[0].lower()
        if column_name in missing_columns:
            spark.sql(statement.format(target_table=target_table))


def build_sku_group_id_prices(
    spark: SparkSession,
    partition_date: str,
):
    return spark.sql(
        f"""
SELECT
    DATE '{partition_date}' AS date,
    CAST(s.sku_group_id AS BIGINT) AS sku_group_id,
    AVG(CAST(se.sell_price_eod AS DOUBLE)) AS avg_sell_price_eod,
    percentile_approx(CAST(se.sell_price_eod AS DOUBLE), 0.5) AS median_sell_price_eod,
    MIN(CAST(se.sell_price_eod AS DOUBLE)) AS min_sell_price_eod,
    MAX(CAST(se.sell_price_eod AS DOUBLE)) AS max_sell_price_eod,
    AVG(CAST(se.full_price_eod AS DOUBLE)) AS avg_full_price_eod,
    percentile_approx(CAST(se.full_price_eod AS DOUBLE), 0.5) AS median_full_price_eod,
    MIN(CAST(se.full_price_eod AS DOUBLE)) AS min_full_price_eod,
    MAX(CAST(se.full_price_eod AS DOUBLE)) AS max_full_price_eod
FROM 
    iceberg.silver.sku_eod se
INNER JOIN (
    SELECT
        id AS sku_id,
        product_id,
        sku_group_id
    FROM iceberg.silver.sku
) s ON s.sku_id = se.sku_id
WHERE
    se.dt = DATE '{partition_date}'
    AND s.sku_group_id IS NOT NULL
GROUP BY
    s.sku_group_id
"""
    )


def save_sku_group_id_prices(
    spark: SparkSession,
    partition_date: str,
    target_table: str,
) -> None:
    _ensure_table_schema(spark, target_table)

    features = build_sku_group_id_prices(spark, partition_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_id_prices(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
