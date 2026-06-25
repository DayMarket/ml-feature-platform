from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)
SELECTED_COLUMNS = (
    "date",
    "query",
    "query_uniq_impressions_1",
    "query_uniq_atcs_1",
    "query_orders_1",
    "query_uniq_impressions_3",
    "query_uniq_atcs_3",
    "query_orders_3",
    "query_uniq_impressions_7",
    "query_uniq_atcs_7",
    "query_orders_7",
    "query_uniq_impressions_14",
    "query_uniq_atcs_14",
    "query_orders_14",
    "query_uniq_impressions_21",
    "query_uniq_atcs_21",
    "query_orders_21",
    "query_uniq_impressions_30",
    "query_uniq_atcs_30",
    "query_orders_30",
    "query_uniq_impressions_60",
    "query_uniq_atcs_60",
    "query_orders_60",
    "query_uniq_impressions_90",
    "query_uniq_atcs_90",
    "query_orders_90",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _parse_partition_date(partition_start: str) -> str:
    supported_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    )
    normalized_value = partition_start
    if normalized_value.endswith("Z"):
        normalized_value = f"{normalized_value[:-1]}+0000"
    else:
        normalized_value = normalized_value.replace("+00:00", "+0000")

    for date_format in supported_formats:
        try:
            return datetime.strptime(normalized_value, date_format).date().isoformat()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(partition_start).date().isoformat()
    except ValueError as error:
        raise ValueError(
            f"Unsupported partition_start value for search_query_atc_features: {partition_start}"
        ) from error


def _window_start_dates(run_date: str) -> dict[int, str]:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return {
        window: (run_dt - timedelta(days=window)).isoformat()
        for window in WINDOWS
    }


def _normalize_query_frame(frame: DataFrame) -> DataFrame:
    return (
        frame.withColumn("query", F.lower(F.col("query")))
        .withColumn("query", F.regexp_replace(F.col("query"), "ё", "е"))
        .withColumn("query", F.regexp_replace(F.col("query"), r"\s+", " "))
        .withColumn("query", F.trim(F.col("query")))
        .filter(F.col("query").isNotNull() & F.col("query").rlike(r"\S"))
    )


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


def _build_window_features(
    events: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    aggregations = []
    for window in WINDOWS:
        aggregations.extend(
            (
                _sum_between("sum_impressions", window_dates[window], run_date).alias(
                    f"query_uniq_impressions_{window}"
                ),
                _sum_between("sum_atc", window_dates[window], run_date).alias(
                    f"query_uniq_atcs_{window}"
                ),
            )
        )

    return events.groupBy("query").agg(*aggregations)


def _build_order_window_features(
    orders: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    return orders.groupBy("query").agg(
        *[
            _sum_between("orders_generated", window_dates[window], run_date).alias(
                f"query_orders_{window}"
            )
            for window in WINDOWS
        ]
    )


def build_search_query_atc_features(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    window_dates = _window_start_dates(run_date)
    start_date = window_dates[max(WINDOWS)]

    events = _normalize_query_frame(
        spark.table("iceberg.silver.feature_platform_search_sku_group_id_install_query")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(run_date).cast("date"))
        )
        .filter(F.col("space") == F.lit("SEARCH_RESULTS"))
        .select(
            F.col("date"),
            F.col("uniqs").alias("query"),
            F.col("sum_impressions").cast("double").alias("sum_impressions"),
            F.col("sum_atc").cast("double").alias("sum_atc"),
        )
    )

    orders = _normalize_query_frame(
        spark.table("iceberg.silver.feature_platform_sku_group_query_search_orders")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(run_date).cast("date"))
        )
        .select(
            F.col("date"),
            F.col("query"),
            F.col("orders_generated").cast("double").alias("orders_generated"),
        )
    )

    features = _build_window_features(events, window_dates, run_date).join(
        _build_order_window_features(orders, window_dates, run_date),
        on="query",
        how="left",
    )

    for window in WINDOWS:
        features = features.withColumn(
            f"query_orders_{window}",
            F.coalesce(F.col(f"query_orders_{window}"), F.lit(0.0)),
        )

    return (
        features
        .withColumn("date", F.lit(run_date).cast("date"))
        .select(*SELECTED_COLUMNS)
    )


def save_search_query_atc_features(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_search_query_atc_features(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_search_query_atc_features(
        spark,
        _parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
