from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit

from job.entities import Arguments


PRICE_INDEX_BASE_PATH = "s3a://um-prod-airflow-fs/price_index_dag/dag_runs"
PRICE_INDEX_CLASSES = {
    "CHEAPEST_AMONG_CLUSTER": 0,
    "CHEAPEST_AMONG_COMPETITORS": 1,
    "CHEAPEST_AMONG_COMPETITORS_AND_CLUSTER": 2,
}


def _load_migration_query(migration_name: str) -> str:
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    return (migrations_dir / migration_name).read_text(encoding="utf-8")


def _get_price_index_path(partition_date: str) -> str:
    return f"{PRICE_INDEX_BASE_PATH}/{partition_date}/price_index_features.parquet"


def _ensure_path_exists(spark: SparkSession, path: str) -> None:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = spark.sparkContext._jvm.java.net.URI(path)
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hadoop_path):
        raise FileNotFoundError(
            f"Price index features file or directory does not exist: {path}"
        )


def build_sku_group_price_index_status(
    spark: SparkSession,
    partition_date: str,
):
    price_index_path = _get_price_index_path(partition_date)
    _ensure_path_exists(spark, price_index_path)

    mapping_expr = create_map(
        *[
            lit(value)
            for mapping_item in PRICE_INDEX_CLASSES.items()
            for value in mapping_item
        ]
    )

    return (
        spark.read.format("parquet")
        .load(price_index_path)
        .filter(col("price_index_status") != "NO_BOOST")
        .withColumn("price_index_status", mapping_expr[col("price_index_status")])
        .select(
            lit(partition_date).cast("date").alias("date"),
            col("sku_group_id").cast("bigint").alias("sku_group_id"),
            col("price_index_status").cast("int").alias("price_index_status"),
        )
    )


def save_sku_group_price_index_status(
    spark: SparkSession,
    partition_date: str,
    target_table: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        migration_query = _load_migration_query("create_table.sql")
        spark.sql(migration_query.format(target_table=target_table))

    features = build_sku_group_price_index_status(spark, partition_date)
    features.writeTo(target_table).overwritePartitions()


def run(spark: SparkSession, arguments: Arguments):
    save_sku_group_price_index_status(
        spark,
        arguments.partition_start[:10],
        arguments.table_name,
    )
