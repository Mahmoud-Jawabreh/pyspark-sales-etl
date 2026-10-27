import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("InspectSilver")
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
    configure_spark_with_delta_pip(
        builder
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


silver_df = (
    spark.read
    .format("delta")
    .load(
        "output/delta/silver_sales"
    )
)


print("\n=== SILVER SCHEMA ===")
silver_df.printSchema()


print("\n=== SILVER DATA ===")

(
    silver_df
    .orderBy("sale_id")
    .show(
        truncate=False
    )
)


print("\n=== SILVER ROW COUNT ===")
print(silver_df.count())


spark.stop()