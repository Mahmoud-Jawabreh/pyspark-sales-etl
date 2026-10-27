import os
import sys
import shutil

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("OutputOptimizationDemo")
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


MANY_FILES_PATH = (
    "output/performance/many_files"
)

OPTIMIZED_PATH = (
    "output/performance/optimized_files"
)


# ==================================================
# CLEAN OLD OUTPUT
# ==================================================

for path in [
    MANY_FILES_PATH,
    OPTIMIZED_PATH
]:
    if os.path.exists(path):
        shutil.rmtree(path)


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
            (col("id") % 1000)
            + 1
        ).cast("double")
    )
)


# ==================================================
# SIMULATE EXCESSIVE PARTITIONING
# ==================================================

many_partitions_df = (
    df.repartition(64)
)

print(
    "\nPartitions before first write:",
    many_partitions_df.rdd.getNumPartitions()
)


(
    many_partitions_df
    .write
    .mode("overwrite")
    .parquet(
        MANY_FILES_PATH
    )
)


# ==================================================
# REDUCE PARTITIONS BEFORE OUTPUT
# ==================================================

optimized_df = (
    many_partitions_df
    .coalesce(4)
)


print(
    "Partitions before optimized write:",
    optimized_df.rdd.getNumPartitions()
)


(
    optimized_df
    .write
    .mode("overwrite")
    .parquet(
        OPTIMIZED_PATH
    )
)


# ==================================================
# COUNT DATA FILES
# ==================================================

def count_parquet_files(path):

    return len(
        [
            file
            for file in os.listdir(path)
            if file.endswith(".parquet")
        ]
    )


print(
    "\n================================"
)

print(
    "Many-files output:",
    count_parquet_files(
        MANY_FILES_PATH
    ),
    "Parquet files"
)

print(
    "Optimized output:",
    count_parquet_files(
        OPTIMIZED_PATH
    ),
    "Parquet files"
)

print(
    "================================"
)


spark.stop()