from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from job.entities import Arguments


AD_DAILY_TABLE = "iceberg.silver.feature_platform_sku_group_ad_revenue_daily"
FEEDBACK_TABLE = "iceberg.silver_bxappdb2_foodback.public_feedback"
SKU_TABLE = "iceberg.silver.sku"

WINDOW_DAYS = 30
# Вес дня показа: weight = 0.5 ** (age / HALF_LIFE_DAYS), age = ds - 1 - day.
HALF_LIFE_DAYS = 14.0

SELECTED_COLUMNS = (
    "date",
    "category_id",
    "avg_advertised_rating_30d_hl14",
    "advertised_sku_groups_30d",
    "rated_advertised_sku_groups_30d",
)


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _window_bounds(run_date: str) -> tuple[str, str]:
    run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    return (
        (run_dt - timedelta(days=WINDOW_DAYS)).isoformat(),
        (run_dt - timedelta(days=1)).isoformat(),
    )


def _build_advertised_days(
    spark: SparkSession,
    window_start: str,
    window_finish: str,
) -> DataFrame:
    return (
        spark.table(AD_DAILY_TABLE)
        .filter(
            (F.col("date") >= F.lit(window_start).cast("date"))
            & (F.col("date") <= F.lit(window_finish).cast("date"))
        )
        .filter(F.col("sku_group_id").isNotNull())
        .filter(F.col("ad_impressions").cast("double") > 0)
        .select(
            F.col("date").alias("ad_date"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
        )
        .distinct()
    )


def _build_reviews(
    spark: SparkSession,
    advertised_sku_groups: DataFrame,
    run_date: str,
) -> DataFrame:
    sku_to_group = (
        spark.table(SKU_TABLE)
        .select(
            F.col("id").alias("sku_id"),
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
        )
        .filter(F.col("sku_group_id").isNotNull())
    )

    return (
        spark.table(FEEDBACK_TABLE)
        .filter(F.col("status") == "PUBLISHED")
        .filter(F.col("date_published") < F.lit(run_date).cast("date"))
        .select(
            F.col("sku_id"),
            F.col("date_published").cast("date").alias("pub_date"),
            F.col("rating").cast("double").alias("rating"),
        )
        .join(sku_to_group, on="sku_id", how="inner")
        .join(advertised_sku_groups, on="sku_group_id", how="leftsemi")
        .select("sku_group_id", "pub_date", "rating")
    )


def _build_rating_as_of_ad_day(
    advertised_days: DataFrame,
    reviews: DataFrame,
    window_start: str,
) -> DataFrame:
    # Рейтинг товара на день показа d = средний рейтинг отзывов с pub_date < d.
    # Отзывы до начала окна сворачиваем в одну базовую сумму на sku_group,
    # внутри окна держим дневные суммы и докидываем их условным неравенством —
    # так не приходится джойнить всю историю отзывов на каждый день окна.
    base_stats = (
        reviews.filter(F.col("pub_date") < F.lit(window_start).cast("date"))
        .groupBy("sku_group_id")
        .agg(
            F.sum("rating").alias("base_rating_sum"),
            F.count("rating").alias("base_review_count"),
        )
    )

    in_window_daily = (
        reviews.filter(F.col("pub_date") >= F.lit(window_start).cast("date"))
        .groupBy("sku_group_id", "pub_date")
        .agg(
            F.sum("rating").alias("day_rating_sum"),
            F.count("rating").alias("day_review_count"),
        )
    )

    window_sums = (
        advertised_days.join(in_window_daily, on="sku_group_id", how="left")
        .withColumn(
            "counted",
            F.col("pub_date").isNotNull() & (F.col("pub_date") < F.col("ad_date")),
        )
        .groupBy("ad_date", "sku_group_id")
        .agg(
            F.sum(F.when(F.col("counted"), F.col("day_rating_sum"))).alias(
                "window_rating_sum"
            ),
            F.sum(F.when(F.col("counted"), F.col("day_review_count"))).alias(
                "window_review_count"
            ),
        )
    )

    return (
        window_sums.join(base_stats, on="sku_group_id", how="left")
        .withColumn(
            "review_count",
            F.coalesce(F.col("base_review_count"), F.lit(0))
            + F.coalesce(F.col("window_review_count"), F.lit(0)),
        )
        .withColumn(
            "rating_as_of_ad_day",
            F.when(
                F.col("review_count") > 0,
                (
                    F.coalesce(F.col("base_rating_sum"), F.lit(0.0))
                    + F.coalesce(F.col("window_rating_sum"), F.lit(0.0))
                )
                / F.col("review_count"),
            ),
        )
        .select("ad_date", "sku_group_id", "rating_as_of_ad_day")
    )


def _build_sku_group_categories(spark: SparkSession) -> DataFrame:
    return (
        spark.table(SKU_TABLE)
        .select(
            F.col("sku_group_id").cast("long").alias("sku_group_id"),
            F.col("category_id").cast("long").alias("category_id"),
        )
        .filter(F.col("sku_group_id").isNotNull())
        .filter(F.col("category_id").isNotNull())
        .distinct()
    )


def build_category_ad_rating_features(
    spark: SparkSession,
    run_date: str,
) -> DataFrame:
    window_start, window_finish = _window_bounds(run_date)

    advertised_days = _build_advertised_days(spark, window_start, window_finish)
    advertised_sku_groups = advertised_days.select("sku_group_id").distinct()
    reviews = _build_reviews(spark, advertised_sku_groups, run_date)

    rated_ad_days = _build_rating_as_of_ad_day(
        advertised_days, reviews, window_start
    ).withColumn(
        "decay_weight",
        F.pow(
            F.lit(0.5),
            (F.datediff(F.lit(run_date).cast("date"), F.col("ad_date")) - F.lit(1))
            / F.lit(HALF_LIFE_DAYS),
        ),
    )

    return (
        rated_ad_days.join(_build_sku_group_categories(spark), on="sku_group_id")
        .groupBy("category_id")
        .agg(
            (
                F.sum(
                    F.when(
                        F.col("rating_as_of_ad_day").isNotNull(),
                        F.col("decay_weight") * F.col("rating_as_of_ad_day"),
                    )
                )
                / F.sum(
                    F.when(
                        F.col("rating_as_of_ad_day").isNotNull(),
                        F.col("decay_weight"),
                    )
                )
            ).alias("avg_advertised_rating_30d_hl14"),
            F.countDistinct("sku_group_id").alias("advertised_sku_groups_30d"),
            F.countDistinct(
                F.when(
                    F.col("rating_as_of_ad_day").isNotNull(), F.col("sku_group_id")
                )
            ).alias("rated_advertised_sku_groups_30d"),
        )
        .withColumn("date", F.lit(run_date).cast("date"))
        .select(*SELECTED_COLUMNS)
    )


def save_category_ad_rating_features(
    spark: SparkSession,
    run_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_category_ad_rating_features(spark, run_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_category_ad_rating_features(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
