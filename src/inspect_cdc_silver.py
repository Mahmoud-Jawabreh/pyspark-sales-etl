import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


builder = (
    SparkSession.builder
    .master("local[2]")
    .appName("InspectCDCSilver")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )
)

spark = (
    configure_spark_with_delta_pip(builder)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = (
    spark.read
    .format("delta")
    .load(
        "output/delta/silver_sales_current"
    )
)


print("\n=== CURRENT-STATE SILVER ===")

(
    df
    .orderBy("sale_id")
    .show(
        truncate=False
    )
)


spark.stop()