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
    .appName("PartitionSizingDemo")
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


# --------------------------------------------------
# CREATE SYNTHETIC DATA
# --------------------------------------------------

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
)


# --------------------------------------------------
# HELPER FUNCTION
# --------------------------------------------------

def inspect_partitions(df, title):

    print(
        f"\n=============================="
        f"\n{title}"
        f"\n=============================="
    )

    print(
        "Partition count:",
        df.rdd.getNumPartitions()
    )

    partition_counts = (
        df
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
    )

    partition_counts.show(
        100,
        truncate=False
    )


# --------------------------------------------------
# ORIGINAL
# --------------------------------------------------

inspect_partitions(
    base_df,
    "ORIGINAL - 8 PARTITIONS"
)


# --------------------------------------------------
# TOO FEW
# --------------------------------------------------

few_df = (
    base_df
    .repartition(2)
)

inspect_partitions(
    few_df,
    "FEW - 2 PARTITIONS"
)


# --------------------------------------------------
# REASONABLE FOR THIS DEMO
# --------------------------------------------------

medium_df = (
    base_df
    .repartition(8)
)

inspect_partitions(
    medium_df,
    "MEDIUM - 8 PARTITIONS"
)


# --------------------------------------------------
# MANY
# --------------------------------------------------

many_df = (
    base_df
    .repartition(64)
)

inspect_partitions(
    many_df,
    "MANY - 64 PARTITIONS"
)


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

print(
    "\n=== FILE INPUT CONFIGURATION ==="
)

print(
    "spark.sql.files.maxPartitionBytes =",
    spark.conf.get(
        "spark.sql.files.maxPartitionBytes"
    )
)


print(
    "\n=== SHUFFLE CONFIGURATION ==="
)

print(
    "spark.sql.shuffle.partitions =",
    spark.conf.get(
        "spark.sql.shuffle.partitions"
    )
)


spark.stop()