import os
import sys

os.environ.pop("SPARK_HOME", None)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


spark = (
    SparkSession.builder
    .master("local[4]")
    .appName("DAGDemo")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Disable AQE just for this learning example
# so Spark doesn't modify the plan while it runs.
spark.conf.set("spark.sql.adaptive.enabled", "false")


# ---------------------------------
# CREATE DATA
# ---------------------------------

df = spark.range(
    0,
    1_000_000,
    numPartitions=4
)

print("\n=== INPUT PARTITIONS ===")
print(df.rdd.getNumPartitions())


# ---------------------------------
# NARROW TRANSFORMATIONS
# ---------------------------------

transformed_df = (
    df
    .filter(col("id") % 2 == 0)
    .withColumn(
        "customer_group",
        col("id") % 100
    )
    .withColumn(
        "amount",
        (col("id") % 500).cast("double")
    )
)


# ---------------------------------
# FIRST WIDE TRANSFORMATION
# ---------------------------------

summary_df = (
    transformed_df
    .groupBy("customer_group")
    .agg(
        spark_sum("amount").alias("total_amount")
    )
)


# ---------------------------------
# SECOND WIDE TRANSFORMATION
# ---------------------------------

final_df = summary_df.orderBy(
    col("total_amount").desc()
)


print("\n=== EXECUTION PLAN ===")
final_df.explain("formatted")


input(
    "\nOpen Spark UI BEFORE execution, "
    "then press ENTER to trigger the action..."
)


# ---------------------------------
# ACTION
# ---------------------------------

print("\n=== RESULT ===")

final_df.show(
    20,
    truncate=False
)


input(
    "\nNow inspect Jobs, Stages and DAG Visualization "
    "in Spark UI. Press ENTER when finished..."
)


spark.stop()