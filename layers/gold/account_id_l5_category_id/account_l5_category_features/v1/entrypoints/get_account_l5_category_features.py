"""Spark entrypoint for account-L5-category Gold features."""

import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from job.arguments import parse_arguments
from job.getting_account_category_features import run

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("getting-account-l5-category-features")
        .enableHiveSupport()
        .getOrCreate()
    )
    arguments = parse_arguments()

    try:
        run(spark, arguments)
    finally:
        spark.stop()
