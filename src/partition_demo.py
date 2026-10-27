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
from pyspark.sql.functions import spark_partition_id


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("PartitionDemo")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -------------------------
# READ ORIGINAL DATA
# -------------------------

sales_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/sales.csv")
)

print("\n=== ORIGINAL PARTITIONS ===")
print("Partitions:", sales_df.rdd.getNumPartitions())


input("\nOpen Spark UI, then press ENTER...")


# -------------------------
# REPARTITION DATA
# -------------------------

repartitioned_df = sales_df.repartition(4)

print("\n=== AFTER repartition(4) ===")
print("Partitions:", repartitioned_df.rdd.getNumPartitions())


# Add partition number to each row so we can see where rows went.
partitioned_view = repartitioned_df.withColumn(
    "partition_id",
    spark_partition_id()
)

print("\n=== DATA WITH PARTITION IDs ===")
partitioned_view.show(truncate=False)


input("\nCheck Jobs/Stages in Spark UI, then press ENTER...")


spark.stop()