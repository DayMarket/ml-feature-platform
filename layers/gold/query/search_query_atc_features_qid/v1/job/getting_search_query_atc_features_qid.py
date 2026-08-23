import re
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)

QUERY_ID_TABLE = "iceberg.gold.feature_platform_search_query_id"
QUERY_ID_VERSION = "v1"
NORMALIZATION_REPLACEMENTS = (("ё", "е"), (r"\s+", " "))

SELECTED_COLUMNS = (
    "date",
    "query",
    "query_id",
    "has_query_id",
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


def parse_partition_date(partition_start: str) -> str:
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
            "Unsupported partition_start value for search_query_atc_features_qid: "
            f"{partition_start}"
        ) from error


def normalize_query_value(value: str | None) -> str:
    """Чистый двойник normalize_query_column: та же цепочка шагов, но для тестов."""
    if value is None:
        return ""
    normalized = value.lower()
    for pattern, replacement in NORMALIZATION_REPLACEMENTS:
        normalized = re.sub(pattern, replacement, normalized)
    return normalized.strip()


def normalize_query_column(column: Column) -> Column:
    normalized = F.lower(column)
    for pattern, replacement in NORMALIZATION_REPLACEMENTS:
        normalized = F.regexp_replace(normalized, pattern, replacement)
    return F.trim(normalized)


def _normalize_query_frame(frame: DataFrame) -> DataFrame:
    return frame.withColumn("query", normalize_query_column(F.col("query"))).filter(
        F.col("query").isNotNull() & F.col("query").rlike(r"\S")
    )


def build_query_id_map(spark: SparkSession) -> DataFrame:
    """Справочник query -> query_id, схлопнутый до одной строки на нормализованный запрос.

    Справочник хранит сырой query_text, поэтому нормализация обязательна. PK там задан на
    сыром тексте, так что несколько сырых вариантов схлопываются в один нормализованный;
    min даёт детерминированный результат при перезапуске.
    """
    return (
        spark.table(QUERY_ID_TABLE)
        .filter(F.col("version") == F.lit(QUERY_ID_VERSION))
        .select(
            normalize_query_column(F.col("query_text")).alias("query"),
            F.trim(F.lower(F.col("query_id"))).alias("query_id"),
        )
        .filter(F.col("query").rlike(r"\S") & F.col("query_id").rlike(r"\S"))
        .groupBy("query")
        .agg(F.min("query_id").alias("query_id"))
    )


def attach_group_key(frame: DataFrame, query_id_map: DataFrame) -> DataFrame:
    """Ключ агрегации: каноничный query_id, либо сам запрос, если его нет в справочнике."""
    return (
        frame.join(query_id_map, on="query", how="left")
        .withColumn("has_query_id", F.col("query_id").isNotNull())
        .withColumn("group_key", F.coalesce(F.col("query_id"), F.col("query")))
        .drop("query_id")
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

    return events.groupBy("group_key").agg(*aggregations)


def _build_order_window_features(
    orders: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    return orders.groupBy("group_key").agg(
        *[
            _sum_between("orders_generated", window_dates[window], run_date).alias(
                f"query_orders_{window}"
            )
            for window in WINDOWS
        ]
    )


def build_search_query_atc_features_qid(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    window_dates = _window_start_dates(run_date)
    start_date = window_dates[max(WINDOWS)]

    query_id_map = build_query_id_map(spark)

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

    members = attach_group_key(events.select("query").distinct(), query_id_map)

    features = _build_window_features(
        attach_group_key(events, query_id_map),
        window_dates,
        run_date,
    ).join(
        _build_order_window_features(
            attach_group_key(orders, query_id_map),
            window_dates,
            run_date,
        ),
        on="group_key",
        how="left",
    )

    for window in WINDOWS:
        features = features.withColumn(
            f"query_orders_{window}",
            F.coalesce(F.col(f"query_orders_{window}"), F.lit(0.0)),
        )

    return (
        members.join(features, on="group_key", how="inner")
        .withColumn("date", F.lit(run_date).cast("date"))
        .withColumnRenamed("group_key", "query_id")
        .select(*SELECTED_COLUMNS)
    )


def save_search_query_atc_features_qid(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_search_query_atc_features_qid(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_search_query_atc_features_qid(
        spark,
        parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
