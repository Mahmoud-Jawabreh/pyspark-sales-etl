import os
import sys

os.environ.pop("SPARK_HOME", None)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

HADOOP_HOME = r"D:\hadoop-3.5.0-win"

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = (
    os.path.join(HADOOP_HOME, "bin")
    + os.pathsep
    + os.environ["PATH"]
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    spark_partition_id,
    when,
)


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("DataSkewDemo")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Disable AQE temporarily so Spark does not try
# to optimize our educational example.
spark.conf.set("spark.sql.adaptive.enabled", "false")


# ---------------------------------
# CREATE SYNTHETIC DATA
# ---------------------------------

df = spark.range(0, 100_000)

skewed_df = df.withColumn(
    "customer_id",
    when(
        col("id") < 90_000,
        lit("HOT_CUSTOMER")
    ).otherwise(
        concat(
            lit("C"),
            (col("id") % 1000).cast("string")
        )
    )
)


print("\n=== CUSTOMER DISTRIBUTION ===")

skewed_df.groupBy("customer_id") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)


input("\nPress ENTER to create partitions by customer_id...")


# ---------------------------------
# PARTITION BY CUSTOMER
# ---------------------------------

partitioned_df = skewed_df.repartition(
    4,
    "customer_id"
)


print("\n=== NUMBER OF PARTITIONS ===")
print(
    partitioned_df.rdd.getNumPartitions()
)


# ---------------------------------
# COUNT ROWS PER PARTITION
# ---------------------------------

partition_distribution = (
    partitioned_df
    .withColumn(
        "partition_id",
        spark_partition_id()
    )
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)


print("\n=== ROWS PER PARTITION ===")

partition_distribution.show()


input(
    "\nOpen Spark UI and inspect the tasks. "
    "Press ENTER when finished..."
)


spark.stop()