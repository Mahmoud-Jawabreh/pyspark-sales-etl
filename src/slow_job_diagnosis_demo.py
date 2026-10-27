import os
import sys
import time

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    broadcast,
    sum as spark_sum,
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("SlowJobDiagnosisDemo")
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )

    # Start deliberately inefficient.
    .config(
        "spark.sql.adaptive.enabled",
        "false"
    )
    .config(
        "spark.sql.autoBroadcastJoinThreshold",
        "-1"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "64"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print(
    "\nSpark UI:",
    spark.sparkContext.uiWebUrl
)


# ==================================================
# FACT TABLE
#
# 60% of rows use product_id = 0
# to deliberately create skew.
# ==================================================

fact_df = (
    spark.range(
        0,
        3_000_000,
        numPartitions=8
    )

    .withColumn(
        "product_id",

        when(
            col("id") < 1_800_000,
            lit(0)
        )
        .otherwise(
            (
                (col("id") % 99) + 1
            ).cast("int")
        )
    )

    .withColumn(
        "amount",
        (
            (col("id") % 1000)
            + 1
        ).cast("double")
    )
)


# ==================================================
# SMALL DIMENSION
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


# ==================================================
# BAD VERSION
# ==================================================

slow_join = (
    fact_df

    .join(
        product_df,
        "product_id",
        "inner"
    )
)


slow_result = (
    slow_join

    .groupBy(
        "category_id"
    )

    .agg(
        spark_sum(
            "amount"
        ).alias(
            "revenue"
        )
    )
)


print("\n==============================")
print("SLOW PHYSICAL PLAN")
print("==============================")

slow_result.explain(
    mode="formatted"
)


start = time.perf_counter()

slow_result.collect()

end = time.perf_counter()


print(
    f"\nSlow version runtime: "
    f"{end - start:.3f} seconds"
)


# ==================================================
# OPTIMIZED VERSION
# ==================================================

spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.adaptive.coalescePartitions.enabled",
    "true"
)

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "8"
)


# Reduce 3 million rows BEFORE the join.
#
# Business requirement:
# revenue by category
#
# Therefore we can safely aggregate
# sales by product first.

product_sales = (
    fact_df

    .groupBy(
        "product_id"
    )

    .agg(
        spark_sum(
            "amount"
        ).alias(
            "product_revenue"
        )
    )
)


# Only ~100 product rows remain.
#
# Broadcast the tiny dimension.

optimized_join = (
    product_sales

    .join(
        broadcast(product_df),
        "product_id",
        "inner"
    )
)


optimized_result = (
    optimized_join

    .groupBy(
        "category_id"
    )

    .agg(
        spark_sum(
            "product_revenue"
        ).alias(
            "revenue"
        )
    )
)


print("\n==============================")
print("OPTIMIZED PHYSICAL PLAN")
print("==============================")

optimized_result.explain(
    mode="formatted"
)


start = time.perf_counter()

optimized_result.collect()

end = time.perf_counter()


print(
    f"\nOptimized runtime: "
    f"{end - start:.3f} seconds"
)


print("\n==============================")
print("OPTIMIZED RESULT")
print("==============================")


optimized_result \
    .orderBy("category_id") \
    .show()


# Keep Spark alive so the UI remains available.
input(
    "\nOpen the Spark UI and inspect the jobs/stages. "
    "Press Enter when finished..."
)


spark.stop()