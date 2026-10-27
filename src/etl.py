import os
import sys

os.environ.pop("SPARK_HOME", None)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Windows Hadoop native utilities required for local file operations.
HADOOP_HOME = r"D:\hadoop-3.5.0-win"

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = (
    os.path.join(HADOOP_HOME, "bin")
    + os.pathsep
    + os.environ["PATH"]
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("PySparkSalesETL")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -------------------------
# 1. EXTRACT
# -------------------------
sales_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/sales.csv")
)

print("\n=== RAW DATA PARTITIONS ===")
print(sales_df.rdd.getNumPartitions())


# -------------------------
# 2. TRANSFORM
# -------------------------

# Remove invalid sales
clean_sales_df = sales_df.filter(
    col("quantity") > 0
)

# Calculate revenue per sale
sales_with_total_df = clean_sales_df.withColumn(
    "total_amount",
    col("quantity") * col("price")
)

# Aggregate sales by product
summary_df = (
    sales_with_total_df
    .groupBy(
        "product_id",
        "product_name"
    )
    .agg(
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("total_amount").alias("total_sales")
    )
)

summary_df.cache()

print("\n=== SPARK EXECUTION PLAN ===")
summary_df.explain("formatted")


print("\n=== TRANSFORMATIONS DEFINED ===")
print("We created:")
print("- filter")
print("- withColumn")
print("- groupBy")
print("- aggregation")

print("\nBut we have not called show(), count(), collect(), or write yet.")

input("\nOpen Spark UI now, then press ENTER to trigger the action...")



# -------------------------
# 3. ACTION
# -------------------------

print("\n=== FINAL SALES SUMMARY ===")

summary_df.show(
    truncate=False
)


input("\nCheck Spark UI again, then press ENTER to continue...")


# -------------------------
# 4. LOAD
# -------------------------

output_path = "output/sales_summary"

summary_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"\nParquet output written to: {output_path}")

print("\n=== VERIFY PARQUET OUTPUT ===")

saved_df = spark.read.parquet(output_path)

saved_df.show(truncate=False)

saved_df.printSchema()

summary_df.unpersist()

spark.stop()