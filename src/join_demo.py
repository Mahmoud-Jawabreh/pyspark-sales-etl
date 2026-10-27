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
from pyspark.sql.functions import broadcast


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("JoinDemo")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    -1
)

# -------------------------
# READ DATA
# -------------------------

sales_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/sales.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/products.csv")
)


print("\n=== SALES ===")
sales_df.show()

print("\n=== PRODUCTS ===")
products_df.show()


# -------------------------
# NORMAL JOIN
# -------------------------

normal_join_df = sales_df.join(
    products_df,
    on="product_id",
    how="inner"
)

print("\n=== NORMAL JOIN EXECUTION PLAN ===")
normal_join_df.explain("formatted")

input("\nInspect the plan, then press ENTER to execute normal join...")


print("\n=== NORMAL JOIN RESULT ===")
normal_join_df.show(truncate=False)


input("\nCheck Spark UI, then press ENTER for broadcast join...")


# -------------------------
# BROADCAST JOIN
# -------------------------

broadcast_join_df = sales_df.join(
    broadcast(products_df),
    on="product_id",
    how="inner"
)

print("\n=== BROADCAST JOIN EXECUTION PLAN ===")
broadcast_join_df.explain("formatted")

input("\nInspect the broadcast plan, then press ENTER to execute...")


print("\n=== BROADCAST JOIN RESULT ===")
broadcast_join_df.show(truncate=False)


input("\nCheck Spark UI again, then press ENTER to stop Spark...")


spark.stop()