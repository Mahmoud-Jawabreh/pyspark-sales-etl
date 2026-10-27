import os
import sys
import time

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("CachePersistDemo")
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

base_df = (
    spark.range(
        0,
        2_000_000,
        numPartitions=8
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
        col("quantity") * col("price")
    )
)


# ==================================================
# EXPENSIVE REUSABLE RESULT
# ==================================================

summary_df = (
    base_df

    .groupBy(
        "product_id"
    )

    .agg(
        spark_sum(
            "sales_amount"
        ).alias(
            "total_sales"
        )
    )
)


# ==================================================
# WITHOUT CACHE
# ==================================================

print("\n==============================")
print("WITHOUT CACHE")
print("==============================")


start = time.perf_counter()

summary_df.count()

end = time.perf_counter()

print(
    f"First action: "
    f"{end - start:.3f} seconds"
)


start = time.perf_counter()

summary_df.filter(
    col("total_sales") > 1_000_000
).count()

end = time.perf_counter()

print(
    f"Second action: "
    f"{end - start:.3f} seconds"
)


# ==================================================
# CACHE
# ==================================================

print("\n==============================")
print("WITH CACHE")
print("==============================")


cached_df = summary_df.cache()


print(
    "\nStorage level:"
)

print(
    cached_df.storageLevel
)


# --------------------------------------------------
# MATERIALIZE CACHE
# --------------------------------------------------

start = time.perf_counter()

cached_df.count()

end = time.perf_counter()

print(
    f"Cache materialization: "
    f"{end - start:.3f} seconds"
)


# --------------------------------------------------
# REUSE CACHE
# --------------------------------------------------

start = time.perf_counter()

cached_df.filter(
    col("total_sales") > 1_000_000
).count()

end = time.perf_counter()

print(
    f"Cached reuse: "
    f"{end - start:.3f} seconds"
)


# ==================================================
# PHYSICAL PLAN
# ==================================================

print("\n==============================")
print("CACHED PHYSICAL PLAN")
print("==============================")


cached_df.explain(
    mode="formatted"
)


# ==================================================
# FREE MEMORY
# ==================================================

cached_df.unpersist()


print(
    "\nCache released with unpersist()."
)


spark.stop()