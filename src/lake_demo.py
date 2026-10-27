import os
import sys

os.environ.pop("SPARK_HOME", None)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("ParquetLakeDemo")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------
# CREATE SYNTHETIC SALES DATA
# ---------------------------------

sales_df = (
    spark.range(0, 100_000)
    .withColumn(
        "year",
        (lit(2025) + (col("id") % 2)).cast("int")
    )
    .withColumn(
        "month",
        ((col("id") % 12) + 1).cast("int")
    )
    .withColumn(
        "customer_id",
        (col("id") % 10_000).cast("int")
    )
    .withColumn(
        "product_id",
        (col("id") % 500).cast("int")
    )
    .withColumn(
        "quantity",
        ((col("id") % 5) + 1).cast("int")
    )
    .withColumn(
        "price",
        ((col("id") % 10_000) + 100).cast("double")
    )
    .withColumn(
        "total_amount",
        col("quantity") * col("price")
    )
)

print("\n=== SAMPLE SALES DATA ===")
sales_df.show(20, truncate=False)

print("\n=== SCHEMA ===")
sales_df.printSchema()

print("\n=== ROW COUNT ===")
print(sales_df.count())

print("\n=== YEAR / MONTH DISTRIBUTION ===")
(
    sales_df
    .groupBy("year", "month")
    .count()
    .orderBy("year", "month")
    .show(30)
)

output_path = "output/data_lake/sales"

print("\n=== WRITING PARTITIONED PARQUET ===")

(
    sales_df.write
    .mode("overwrite")
    .partitionBy(
        "year",
        "month"
    )
    .parquet(output_path)
)

print("Write complete.")

lake_df = spark.read.parquet(
    output_path
)

print("\n=== LAKE SCHEMA ===")
lake_df.printSchema()

october_2026_df = (
    lake_df
    .filter(
        (col("year") == 2026) &
        (col("month") == 10) &
        (col("price") > 5000)
    )
    .select(
        "customer_id",
        "product_id",
        "price",
        "total_amount"
    )
)

print("\n=== EXECUTION PLAN ===")

october_2026_df.explain(
    "formatted"
)

input(
    "\nInspect the execution plan, then press ENTER..."
)

print("\n=== RESULT ===")

october_2026_df.show(
    20,
    truncate=False
)

input(
    "\nInspect Spark UI, then press ENTER..."
)

spark.stop()