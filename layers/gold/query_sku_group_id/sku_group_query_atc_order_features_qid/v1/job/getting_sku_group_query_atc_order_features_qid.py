import re
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql import functions as F

from job.entities import Arguments


WINDOWS = (1, 3, 7, 14, 21, 30, 60, 90)
SMOOTHING_COEF = 100.0

QUERY_ID_TABLE = "iceberg.gold.feature_platform_search_query_id"
QUERY_ID_VERSION = "v1"
NORMALIZATION_REPLACEMENTS = (("ё", "е"), (r"\s+", " "))

SELECTED_COLUMNS = (
    "date",
    "query",
    "sku_group_id",
    "query_id",
    "has_query_id",
    "query_skg_smooth_conv_imp2atc_1",
    "query_skg_smooth_conv_imp2order_1",
    "query_skg_atc_frac_all_skg_atc_1",
    "query_skg_orders_frac_all_skg_orders_1",
    "query_skg_smooth_conv_imp2atc_3",
    "query_skg_smooth_conv_imp2order_3",
    "query_skg_atc_frac_all_skg_atc_3",
    "query_skg_orders_frac_all_skg_orders_3",
    "query_skg_uniq_orders_7",
    "query_skg_conv_imp2atc_7",
    "query_skg_smooth_conv_imp2atc_7",
    "query_skg_smooth_conv_imp2order_7",
    "query_skg_atc_frac_all_skg_atc_7",
    "query_skg_orders_frac_all_skg_orders_7",
    "query_skg_conv_imp2order_7",
    "query_skg_uniq_orders_14",
    "query_skg_conv_imp2atc_14",
    "query_skg_smooth_conv_imp2atc_14",
    "query_skg_smooth_conv_imp2order_14",
    "query_skg_atc_frac_all_skg_atc_14",
    "query_skg_orders_frac_all_skg_orders_14",
    "query_skg_conv_imp2order_14",
    "query_skg_uniq_orders_21",
    "query_skg_conv_imp2atc_21",
    "query_skg_smooth_conv_imp2atc_21",
    "query_skg_smooth_conv_imp2order_21",
    "query_skg_atc_frac_all_skg_atc_21",
    "query_skg_orders_frac_all_skg_orders_21",
    "query_skg_conv_imp2order_21",
    "query_skg_uniq_orders_30",
    "query_skg_conv_imp2atc_30",
    "query_skg_smooth_conv_imp2atc_30",
    "query_skg_smooth_conv_imp2order_30",
    "query_skg_atc_frac_all_skg_atc_30",
    "query_skg_orders_frac_all_skg_orders_30",
    "query_skg_conv_imp2order_30",
    "query_skg_imp2atc_3_to_1",
    "query_skg_imp2atc_7_to_3",
    "query_skg_imp2atc_14_to_7",
    "query_skg_imp2atc_30_to_14",
    "query_skg_imp2order_30_to_14",
    "query_skg_uniq_atcs_60",
    "query_skg_uniq_orders_60",
    "query_skg_conv_imp2atc_60",
    "query_skg_smooth_conv_imp2atc_60",
    "query_skg_smooth_conv_imp2order_60",
    "query_skg_atc_frac_all_skg_atc_60",
    "query_skg_orders_frac_all_skg_orders_60",
    "query_skg_conv_imp2order_60",
    "query_skg_uniq_atcs_90",
    "query_skg_uniq_orders_90",
    "query_skg_conv_imp2atc_90",
    "query_skg_smooth_conv_imp2atc_90",
    "query_skg_smooth_conv_imp2order_90",
    "query_skg_atc_frac_all_skg_atc_90",
    "query_skg_orders_frac_all_skg_orders_90",
    "query_skg_conv_imp2order_90",
    "query_skg_imp2atc_60_to_30",
    "query_skg_imp2order_60_to_30",
    "query_skg_imp2atc_90_to_60",
    "query_skg_imp2order_90_to_60",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _safe_div(num: Column, den: Column) -> Column:
    return num / den


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
            "Unsupported partition_start value for "
            f"sku_group_query_atc_order_features_qid: {partition_start}"
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


def _sum_since(column_name: str, start_date: str) -> F.Column:
    return F.sum(
        F.when(F.col("date") >= F.lit(start_date), F.col(column_name)).otherwise(0.0)
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


def _build_events_agg(events: DataFrame, window_dates: dict[int, str]) -> DataFrame:
    aggregations = []
    for window in WINDOWS:
        aggregations.extend(
            (
                _sum_since("sum_atc", window_dates[window]).alias(
                    f"query_skg_uniq_atcs_{window}"
                ),
                _sum_since("sum_impressions", window_dates[window]).alias(
                    f"query_skg_uniq_impressions_{window}"
                ),
            )
        )
    return events.groupBy("group_key", "sku_group_id").agg(*aggregations)


def _build_smoothed_pair_events_agg(
    events: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    aggregations = []
    for window in WINDOWS:
        aggregations.extend(
            (
                _sum_between("sum_atc", window_dates[window], run_date).alias(
                    f"query_skg_smooth_atcs_{window}"
                ),
                _sum_between("sum_impressions", window_dates[window], run_date).alias(
                    f"query_skg_smooth_impressions_{window}"
                ),
            )
        )
    return events.groupBy("group_key", "sku_group_id").agg(*aggregations)


def _build_smoothed_pair_orders_agg(
    orders: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    return orders.groupBy("group_key", "sku_group_id").agg(
        *[
            _sum_between("orders_generated", window_dates[window], run_date).alias(
                f"query_skg_smooth_orders_{window}"
            )
            for window in WINDOWS
        ]
    )


def _build_smoothed_skg_events_agg(
    spark: SparkSession,
    start_date: str,
    run_date: str,
    window_dates: dict[int, str],
) -> DataFrame:
    daily_events = (
        spark.table("iceberg.silver.feature_platform_search_sku_group_id_install_query")
        .filter(
            (F.col("date") >= F.lit(start_date).cast("date"))
            & (F.col("date") < F.lit(run_date).cast("date"))
        )
        .filter(F.col("space") == F.lit("SEARCH_RESULTS"))
        .filter(F.col("sku_group_id").isNotNull())
        .select(
            F.col("date"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("sum_atc").cast("double").alias("sum_atc"),
            F.col("sum_impressions").cast("double").alias("sum_impressions"),
        )
    )

    aggregations = []
    for window in WINDOWS:
        aggregations.extend(
            (
                _sum_between("sum_atc", window_dates[window], run_date).alias(
                    f"skg_smooth_atcs_{window}"
                ),
                _sum_between("sum_impressions", window_dates[window], run_date).alias(
                    f"skg_smooth_impressions_{window}"
                ),
            )
        )

    return daily_events.groupBy("sku_group_id").agg(*aggregations)


def _build_smoothed_skg_orders_agg(
    orders: DataFrame,
    window_dates: dict[int, str],
    run_date: str,
) -> DataFrame:
    return orders.groupBy("sku_group_id").agg(
        *[
            _sum_between("orders_generated", window_dates[window], run_date).alias(
                f"skg_smooth_orders_{window}"
            )
            for window in WINDOWS
        ]
    )


def _build_orders_agg(orders: DataFrame, window_dates: dict[int, str]) -> DataFrame:
    return orders.groupBy("group_key", "sku_group_id").agg(
        *[
            _sum_since("orders_generated", window_dates[window]).alias(
                f"query_skg_uniq_orders_{window}"
            )
            for window in WINDOWS
        ]
    )


def build_sku_group_query_atc_order_features_qid(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    window_dates = _window_start_dates(run_date)
    d90 = window_dates[90]
    query_id_map = build_query_id_map(spark)

    events = _normalize_query_frame(
        spark.table("iceberg.silver.feature_platform_search_sku_group_id_install_query")
        .filter((F.col("date") >= F.lit(d90)) & (F.col("date") <= F.lit(run_date)))
        .filter(F.col("space") == F.lit("SEARCH_RESULTS"))
        .select(
            F.col("date"),
            F.col("uniqs").alias("query"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("sum_atc").cast("double").alias("sum_atc"),
            F.col("sum_impressions").cast("double").alias("sum_impressions"),
        )
    )

    orders = _normalize_query_frame(
        spark.table("iceberg.silver.feature_platform_sku_group_query_search_orders")
        .filter((F.col("date") >= F.lit(d90)) & (F.col("date") <= F.lit(run_date)))
        .select(
            F.col("date"),
            F.col("query"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("orders_generated").cast("double").alias("orders_generated"),
        )
    )

    grouped_events = attach_group_key(events, query_id_map)
    grouped_orders = attach_group_key(orders, query_id_map)
    members = attach_group_key(events.select("query").distinct(), query_id_map)

    events_agg = _build_events_agg(grouped_events, window_dates)
    smoothed_pair_events_agg = _build_smoothed_pair_events_agg(
        grouped_events,
        window_dates,
        run_date,
    )
    smoothed_pair_orders_agg = _build_smoothed_pair_orders_agg(
        grouped_orders,
        window_dates,
        run_date,
    )
    smoothed_skg_events_agg = _build_smoothed_skg_events_agg(
        spark,
        window_dates[max(WINDOWS)],
        run_date,
        window_dates,
    )
    smoothed_skg_orders_agg = _build_smoothed_skg_orders_agg(
        orders,
        window_dates,
        run_date,
    )
    orders_agg = _build_orders_agg(grouped_orders, window_dates)

    features = events_agg.join(orders_agg, on=["group_key", "sku_group_id"], how="left")
    features = features.join(
        smoothed_pair_events_agg,
        on=["group_key", "sku_group_id"],
        how="left",
    )
    features = features.join(
        smoothed_pair_orders_agg,
        on=["group_key", "sku_group_id"],
        how="left",
    )
    features = features.join(
        smoothed_skg_events_agg,
        on=["sku_group_id"],
        how="left",
    )
    features = features.join(
        smoothed_skg_orders_agg,
        on=["sku_group_id"],
        how="left",
    )
    for column_name in features.columns:
        if column_name not in ("group_key", "sku_group_id"):
            features = features.withColumn(column_name, F.coalesce(F.col(column_name), F.lit(0.0)))

    features = features.filter(F.col("query_skg_uniq_impressions_14") >= F.lit(2.0))
    features = features.filter(
        (F.col("query_skg_uniq_atcs_90") > F.lit(0.0))
        | (F.col("query_skg_uniq_orders_90") > F.lit(0.0))
    )

    for window in WINDOWS:
        features = features.withColumn(
            f"query_skg_conv_imp2atc_{window}",
            _safe_div(
                F.col(f"query_skg_uniq_atcs_{window}"),
                F.col(f"query_skg_uniq_impressions_{window}"),
            ),
        )

    for window in WINDOWS:
        skg_conv = _safe_div(
            F.col(f"skg_smooth_atcs_{window}"),
            F.col(f"skg_smooth_impressions_{window}"),
        )
        features = features.withColumn(
            f"query_skg_smooth_conv_imp2atc_{window}",
            (
                F.col(f"query_skg_smooth_atcs_{window}")
                + F.lit(SMOOTHING_COEF) * skg_conv
            )
            / (
                F.col(f"query_skg_smooth_impressions_{window}")
                + F.lit(SMOOTHING_COEF)
            ),
        )

    for window in WINDOWS:
        skg_conv = _safe_div(
            F.col(f"skg_smooth_orders_{window}"),
            F.col(f"skg_smooth_impressions_{window}"),
        )
        features = features.withColumn(
            f"query_skg_smooth_conv_imp2order_{window}",
            (
                F.col(f"query_skg_smooth_orders_{window}")
                + F.lit(SMOOTHING_COEF) * skg_conv
            )
            / (
                F.col(f"query_skg_smooth_impressions_{window}")
                + F.lit(SMOOTHING_COEF)
            ),
        )

    for window in WINDOWS:
        features = features.withColumn(
            f"query_skg_atc_frac_all_skg_atc_{window}",
            _safe_div(
                F.col(f"query_skg_smooth_atcs_{window}"),
                F.col(f"skg_smooth_atcs_{window}"),
            ),
        )
        features = features.withColumn(
            f"query_skg_orders_frac_all_skg_orders_{window}",
            _safe_div(
                F.col(f"query_skg_smooth_orders_{window}"),
                F.col(f"skg_smooth_orders_{window}"),
            ),
        )

    for window in (7, 14, 21, 30, 60, 90):
        features = features.withColumn(
            f"query_skg_conv_imp2order_{window}",
            _safe_div(
                F.col(f"query_skg_uniq_orders_{window}"),
                F.col(f"query_skg_uniq_impressions_{window}"),
            ),
        )

    features = (
        features.withColumn(
            "query_skg_imp2atc_3_to_1",
            _safe_div(F.col("query_skg_conv_imp2atc_3"), F.col("query_skg_conv_imp2atc_1")),
        )
        .withColumn(
            "query_skg_imp2atc_7_to_3",
            _safe_div(F.col("query_skg_conv_imp2atc_7"), F.col("query_skg_conv_imp2atc_3")),
        )
        .withColumn(
            "query_skg_imp2atc_14_to_7",
            _safe_div(F.col("query_skg_conv_imp2atc_14"), F.col("query_skg_conv_imp2atc_7")),
        )
        .withColumn(
            "query_skg_imp2atc_30_to_14",
            _safe_div(F.col("query_skg_conv_imp2atc_30"), F.col("query_skg_conv_imp2atc_14")),
        )
        .withColumn(
            "query_skg_imp2atc_60_to_30",
            _safe_div(F.col("query_skg_conv_imp2atc_60"), F.col("query_skg_conv_imp2atc_30")),
        )
        .withColumn(
            "query_skg_imp2atc_90_to_60",
            _safe_div(F.col("query_skg_conv_imp2atc_90"), F.col("query_skg_conv_imp2atc_60")),
        )
        .withColumn(
            "query_skg_imp2order_30_to_14",
            _safe_div(F.col("query_skg_conv_imp2order_30"), F.col("query_skg_conv_imp2order_14")),
        )
        .withColumn(
            "query_skg_imp2order_60_to_30",
            _safe_div(F.col("query_skg_conv_imp2order_60"), F.col("query_skg_conv_imp2order_30")),
        )
        .withColumn(
            "query_skg_imp2order_90_to_60",
            _safe_div(F.col("query_skg_conv_imp2order_90"), F.col("query_skg_conv_imp2order_60")),
        )
    )

    return (
        members.join(features, on="group_key", how="inner")
        .withColumn("date", F.lit(run_date).cast("date"))
        .withColumnRenamed("group_key", "query_id")
        .select(*SELECTED_COLUMNS)
    )


def save_sku_group_query_atc_order_features_qid(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_query_atc_order_features_qid(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_query_atc_order_features_qid(
        spark,
        parse_partition_date(arguments.partition_start),
        arguments.table_name,
    )
