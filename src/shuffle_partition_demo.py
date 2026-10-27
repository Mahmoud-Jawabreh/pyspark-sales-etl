import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("ShufflePartitionDemo")
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )

    # Disable AQE temporarily.
    #
    # This makes the shuffle partition count
    # easier to see while learning.
    .config(
        "spark.sql.adaptive.enabled",
        "false"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# CREATE SYNTHETIC DATA
# ==================================================

df = (
    spark.range(
        0,
        1_000_000
    )

    .withColumn(
        "product_id",
        (
            col("id") % 100
        ).cast("int")
    )

    .withColumn(
        "customer_id",
        (
            col("id") % 50_000
        ).cast("int")
    )

    .withColumn(
        "quantity",
        (
            (col("id") % 5) + 1
        ).cast("int")
    )

    .withColumn(
        "price",
        (
            (col("id") % 1000) + 10
        ).cast("double")
    )

    .withColumn(
        "sales_amount",
        col("quantity")
        *
        col("price")
    )
)


print("\n=== INITIAL PARTITIONS ===")

print(
    df.rdd.getNumPartitions()
)


# ==================================================
# TEST 1:
# 4 SHUFFLE PARTITIONS
# ==================================================

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "4"
)


summary_4 = (
    df
    .groupBy(
        "product_id"
    )
    .sum(
        "sales_amount"
    )
)


print(
    "\n================================="
)
print(
    "PLAN WITH 4 SHUFFLE PARTITIONS"
)
print(
    "================================="
)

summary_4.explain(
    mode="formatted"
)


print(
    "\nRESULT PARTITIONS:"
)

print(
    summary_4.rdd.getNumPartitions()
)


# Trigger execution
summary_4.count()


# ==================================================
# TEST 2:
# 20 SHUFFLE PARTITIONS
# ==================================================

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "20"
)


summary_20 = (
    df
    .groupBy(
        "product_id"
    )
    .sum(
        "sales_amount"
    )
)


print(
    "\n================================="
)
print(
    "PLAN WITH 20 SHUFFLE PARTITIONS"
)
print(
    "================================="
)

summary_20.explain(
    mode="formatted"
)


print(
    "\nRESULT PARTITIONS:"
)

print(
    summary_20.rdd.getNumPartitions()
)


summary_20.count()


spark.stop()