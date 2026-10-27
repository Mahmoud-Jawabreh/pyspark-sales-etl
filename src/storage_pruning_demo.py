import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
)


DATA_PATH = (
    "output/performance/"
    "partitioned_sales_parquet"
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("StoragePruningDemo")
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# CREATE SYNTHETIC SALES
# ==================================================

sales_df = (
    spark.range(
        0,
        2_000_000,
        numPartitions=8
    )

    .withColumn(
        "year",
        (
            lit(2025)
            +
            (col("id") % 2)
        ).cast("int")
    )

    .withColumn(
        "month",
        (
            (col("id") % 12)
            + 1
        ).cast("int")
    )

    .withColumn(
        "customer_id",
        (
            col("id") % 100_000
        ).cast("int")
    )

    .withColumn(
        "product_id",
        (
            col("id") % 500
        ).cast("int")
    )

    .withColumn(
        "quantity",
        (
            (col("id") % 5)
            + 1
        ).cast("int")
    )

    .withColumn(
        "price",
        (
            (col("id") % 5000)
            + 10
        ).cast("double")
    )

    .withColumn(
        "sales_amount",
        col("quantity")
        *
        col("price")
    )
)


# ==================================================
# WRITE PARTITIONED PARQUET
# ==================================================

(
    sales_df.write
    .mode("overwrite")
    .partitionBy(
        "year",
        "month"
    )
    .parquet(
        DATA_PATH
    )
)


print(
    "\nPartitioned Parquet data written."
)


# ==================================================
# READ
# ==================================================

stored_df = (
    spark.read
    .parquet(
        DATA_PATH
    )
)


# ==================================================
# QUERY
#
# Demonstrates:
# - partition pruning
# - predicate pushdown
# - column pruning
# ==================================================

query_df = (
    stored_df

    .filter(
        (col("year") == 2026)
        &
        (col("month") == 10)
        &
        (col("price") > 3000)
    )

    .select(
        "id",
        "product_id",
        "price",
        "sales_amount",
        "year",
        "month",
    )
)


print(
    "\n=============================="
)
print(
    "OPTIMIZED QUERY PLAN"
)
print(
    "=============================="
)


query_df.explain(
    mode="formatted"
)


print(
    "\n=== SAMPLE RESULTS ==="
)

query_df.show(
    20,
    truncate=False
)


print(
    "\n=== MATCHING ROW COUNT ==="
)

print(
    query_df.count()
)


spark.stop()