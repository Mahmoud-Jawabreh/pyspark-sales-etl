from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("SmallFilesDemo")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = (
    spark.range(0, 20_000)
    .withColumn(
        "customer_id",
        (col("id") % 100).cast("int")
    )
    .withColumn(
        "amount",
        ((col("id") % 1000) + 100).cast("double")
    )
)


print("\n=== NUMBER OF CUSTOMERS ===")

df.select("customer_id") \
    .distinct() \
    .count()


print(
    df.select("customer_id")
    .distinct()
    .count()
)

bad_path = "output/bad_partitioning"

(
    df.write
    .mode("overwrite")
    .partitionBy("customer_id")
    .parquet(bad_path)
)

print("\nBad partitioned dataset written.")

input(
    "\nInspect output/bad_partitioning, "
    "then press ENTER..."
)

spark.stop()