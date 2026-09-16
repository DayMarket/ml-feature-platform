"""Spark entrypoint for L6 category gender Gold features."""

import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from job.arguments import parse_arguments
from job.getting_l6_category_gender_features import run

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("getting-l6-category-gender-features")
        .enableHiveSupport()
        .getOrCreate()
    )
    arguments = parse_arguments()

    try:
        run(spark, arguments)
    finally:
        spark.stop()
