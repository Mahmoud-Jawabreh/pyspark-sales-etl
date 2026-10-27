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
    .appName("InspectQuarantine")
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


quarantine_df = (
    spark.read
    .format("delta")
    .load("output/delta/quarantine_stream_sales")
)


print("\n=== QUARANTINE DATA ===")
quarantine_df.show(
    truncate=False
)


print("\n=== QUARANTINE COUNT ===")
print(quarantine_df.count())


spark.stop()