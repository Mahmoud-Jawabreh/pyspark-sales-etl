import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    spark_partition_id,
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("RepartitionVsCoalesce")
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )
    .config(
        "spark.sql.adaptive.enabled",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# CREATE DATA WITH 8 PARTITIONS
# ==================================================

df = spark.range(
    start=0,
    end=1_000_000,
    numPartitions=8
)


print("\n=== ORIGINAL ===")

print(
    "Partitions:",
    df.rdd.getNumPartitions()
)


# ==================================================
# HELPER:
# SHOW NUMBER OF ROWS PER PARTITION
# ==================================================

def show_partition_sizes(dataframe, title):

    print(f"\n=== {title} ===")

    print(
        "Number of partitions:",
        dataframe.rdd.getNumPartitions()
    )

    (
        dataframe
        .withColumn(
            "partition_id",
            spark_partition_id()
        )
        .groupBy(
            "partition_id"
        )
        .count()
        .orderBy(
            "partition_id"
        )
        .show()
    )


show_partition_sizes(
    df,
    "ORIGINAL PARTITION DISTRIBUTION"
)


# ==================================================
# COALESCE
#
# Reduce 8 → 2
# No full redistribution requested.
# ==================================================

coalesced_df = (
    df.coalesce(2)
)


show_partition_sizes(
    coalesced_df,
    "AFTER COALESCE(2)"
)


print(
    "\n=== COALESCE PHYSICAL PLAN ==="
)

coalesced_df.explain(
    mode="formatted"
)


# ==================================================
# REPARTITION
#
# Reduce 8 → 2
# Spark redistributes records.
# ==================================================

repartitioned_df = (
    df.repartition(2)
)


show_partition_sizes(
    repartitioned_df,
    "AFTER REPARTITION(2)"
)


print(
    "\n=== REPARTITION PHYSICAL PLAN ==="
)

repartitioned_df.explain(
    mode="formatted"
)


# ==================================================
# REPARTITION CAN ALSO INCREASE
#
# 8 → 16
# ==================================================

more_partitions_df = (
    df.repartition(16)
)


print(
    "\n=== REPARTITION(16) ==="
)

print(
    "Partitions:",
    more_partitions_df
    .rdd
    .getNumPartitions()
)


# ==================================================
# REPARTITION BY KEY
# ==================================================

customer_df = (
    df
    .withColumn(
        "customer_id",
        (
            col("id") % 100
        ).cast("int")
    )
)


customer_partitioned = (
    customer_df
    .repartition(
        4,
        "customer_id"
    )
)


print(
    "\n=== REPARTITION BY CUSTOMER_ID ==="
)

customer_partitioned.explain(
    mode="formatted"
)


spark.stop()