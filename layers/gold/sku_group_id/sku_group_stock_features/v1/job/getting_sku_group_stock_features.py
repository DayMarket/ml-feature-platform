from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)
SELECTED_COLUMNS = (
    "date",
    "sku_group_id",
    "skg_total_stock_1",
    "skg_total_stock_3",
    "skg_total_stock_7",
    "skg_total_stock_14",
    "skg_total_stock_21",
    "skg_total_stock_30",
    "skg_total_stock_60",
    "skg_total_stock_90",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _parse_partition_date(partition_start: str) -> str:
    value = partition_start.strip()

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        pass

    for date_format in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, date_format).date().isoformat()
        except ValueError:
            continue

    raise ValueError(
        f"Unsupported partition_start value for sku_group_stock_features: {partition_start}"
    )


def _window_start_dates(run_date: str) -> dict[int, str]:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return {
        window: (run_dt - timedelta(days=window)).isoformat()
        for window in WINDOWS
    }


def _sum_between(
    column_name: str,
    start_date: str,
    finish_date_exclusive: str,
) -> Column:
    return F.sum(
        F.when(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(finish_date_exclusive).cast("date")),
            F.col(column_name),
        ).otherwise(0.0)
    )


def _build_sku_group_daily_stock(
    spark: SparkSession,
    start_date: str,
    run_date: str,
) -> DataFrame:
    stock = (
        spark.table("iceberg.silver.feature_platform_sku_stock_daily")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(run_date).cast("date"))
        )
        .select(
            F.col("date"),
            F.col("sku_id").cast("long").alias("sku_id"),
            F.col("total_stock").cast("double").alias("total_stock"),
        )
    )

    sku_mapping = (
        spark.table("iceberg.silver.sku")
        .select(
            F.col("id").cast("long").alias("sku_id"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
        )
        .filter(F.col("sku_group_id").isNotNull())
        .dropDuplicates(["sku_id"])
    )

    return (
        stock.join(sku_mapping, on="sku_id", how="inner")
        .groupBy("date", "sku_group_id")
        .agg(F.sum("total_stock").alias("total_stock"))
    )


def _build_window_features(
    daily_stock: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    return daily_stock.groupBy("sku_group_id").agg(
        *[
            _sum_between("total_stock", window_dates[window], run_date).alias(
                f"skg_total_stock_{window}"
            )
            for window in WINDOWS
        ]
    )


def build_sku_group_stock_features(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    window_dates = _window_start_dates(run_date)
    start_date = window_dates[max(WINDOWS)]

    daily_stock = _build_sku_group_daily_stock(spark, start_date, run_date)

    return (
        _build_window_features(daily_stock, window_dates, run_date)
        .withColumn("date", F.lit(run_date).cast("date"))
        .select(*SELECTED_COLUMNS)
    )


def save_sku_group_stock_features(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_stock_features(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_stock_features(
        spark,
        _parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
