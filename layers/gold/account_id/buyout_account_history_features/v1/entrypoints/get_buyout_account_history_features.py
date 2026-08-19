import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from job.arguments import parse_arguments
from job.getting_buyout_account_history_features import run


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("getting-buyout-account-history-features")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Границы суток режутся FROM_UTC_TIMESTAMP/TO_UTC_TIMESTAMP от Ташкента,
    # поэтому базовый пояс сессии обязан быть UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    arguments = parse_arguments()

    try:
        run(spark, arguments)
    finally:
        spark.stop()
