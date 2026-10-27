import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("InspectBronze")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = (
    configure_spark_with_delta_pip(builder)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


bronze_df = (
    spark.read
    .format("delta")
    .load("output/delta/bronze_sales")
)


print("\n=== BRONZE SCHEMA ===")
bronze_df.printSchema()


print("\n=== BRONZE DATA ===")

(
    bronze_df
    .orderBy(
        "partition",
        "offset"
    )
    .show(
        truncate=False
    )
)


print("\n=== BRONZE ROW COUNT ===")
print(bronze_df.count())


spark.stop()