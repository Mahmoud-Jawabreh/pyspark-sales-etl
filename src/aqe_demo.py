import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("AQEDemo")
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
# CREATE DATA
# ==================================================

df = (
    spark.range(
        0,
        1_000_000,
        numPartitions=8
    )

    .withColumn(
        "product_id",
        (
            col("id") % 100
        ).cast("int")
    )

    .withColumn(
        "amount",
        (
            (col("id") % 1000) + 1
        ).cast("double")
    )
)


# We deliberately choose a high number
# for this small demo.
spark.conf.set(
    "spark.sql.shuffle.partitions",
    "50"
)


# ==================================================
# TEST 1:
# AQE DISABLED
# ==================================================

spark.conf.set(
    "spark.sql.adaptive.enabled",
    "false"
)


without_aqe = (
    df
    .groupBy("product_id")
    .sum("amount")
)


print("\n================================")
print("AQE DISABLED")
print("================================")


# Trigger execution
without_aqe.collect()


print(
    "Result partitions:",
    without_aqe.rdd.getNumPartitions()
)


print("\nPhysical plan:")

without_aqe.explain(
    mode="formatted"
)


# ==================================================
# TEST 2:
# AQE ENABLED
# ==================================================

spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.adaptive.coalescePartitions.enabled",
    "true"
)


with_aqe = (
    df
    .groupBy("product_id")
    .sum("amount")
)


print("\n================================")
print("AQE ENABLED")
print("================================")


# Trigger execution first so AQE has
# runtime information to work with.
with_aqe.collect()


print(
    "Result partitions:",
    with_aqe.rdd.getNumPartitions()
)


print("\nPhysical plan:")

with_aqe.explain(
    mode="formatted"
)


spark.stop()