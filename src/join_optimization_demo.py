import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    broadcast,
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("JoinOptimizationDemo")
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )

    # Disable AQE temporarily so the plans
    # are easier to compare.
    .config(
        "spark.sql.adaptive.enabled",
        "false"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# LARGE FACT TABLE
# ==================================================

fact_df = (
    spark.range(
        0,
        2_000_000,
        numPartitions=8
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
            col("id") % 100_000
        ).cast("int")
    )

    .withColumn(
        "quantity",
        (
            (col("id") % 5) + 1
        ).cast("int")
    )
)


# ==================================================
# SMALL DIMENSION TABLE
# ==================================================

product_df = (
    spark.range(
        0,
        100
    )

    .withColumnRenamed(
        "id",
        "product_id"
    )

    .withColumn(
        "category_id",
        (
            col("product_id") % 10
        ).cast("int")
    )
)


print(
    "\n================================"
)
print(
    "AUTO BROADCAST THRESHOLD"
)
print(
    "================================"
)

print(
    spark.conf.get(
        "spark.sql.autoBroadcastJoinThreshold"
    )
)


# ==================================================
# TEST 1
# FORCE SORT-MERGE STYLE BEHAVIOR
#
# Disable automatic broadcasting.
# ==================================================

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    "-1"
)


sort_merge_df = (
    fact_df.alias("fact")
    .join(
        product_df.alias("product"),

        col("fact.product_id")
        ==
        col("product.product_id"),

        "inner"
    )
)


print(
    "\n================================"
)
print(
    "BROADCAST DISABLED"
)
print(
    "================================"
)

sort_merge_df.explain(
    mode="formatted"
)


print(
    "\nTriggering join..."
)

print(
    "Rows:",
    sort_merge_df.count()
)


# ==================================================
# TEST 2
# EXPLICIT BROADCAST
# ==================================================

broadcast_df = (
    fact_df.alias("fact")
    .join(
        broadcast(
            product_df.alias("product")
        ),

        col("fact.product_id")
        ==
        col("product.product_id"),

        "inner"
    )
)


print(
    "\n================================"
)
print(
    "EXPLICIT BROADCAST"
)
print(
    "================================"
)

broadcast_df.explain(
    mode="formatted"
)


print(
    "\nTriggering broadcast join..."
)

print(
    "Rows:",
    broadcast_df.count()
)


spark.stop()