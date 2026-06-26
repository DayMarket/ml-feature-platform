import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from job.arguments import parse_arguments
from job.backfill_from_feature_store import run


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName(
            "backfill-query-skg-pairwise-features-legacy-from-feature-store"
        )
        .enableHiveSupport()
        .getOrCreate()
    )
    arguments = parse_arguments()

    try:
        run(spark, arguments)
    finally:
        spark.stop()
