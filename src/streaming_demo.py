import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("StructuredStreamingDemo")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# DEFINE SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
])


# --------------------------------------------------
# STREAMING SOURCE
# --------------------------------------------------

stream_df = (
    spark.readStream
    .schema(schema)
    .option("header", True)
    .csv("data/stream_input")
)


# --------------------------------------------------
# TRANSFORMATION
# --------------------------------------------------

transformed_df = (
    stream_df
    .filter("quantity > 0")
    .withColumn(
        "total_amount",
        stream_df["quantity"] * stream_df["price"]
    )
)


# --------------------------------------------------
# STREAMING SINK
# --------------------------------------------------

query = (
    transformed_df.writeStream
    .format("console")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "output/stream_checkpoint"
    )
    .start()
)


print("\nStreaming query started.")
print("Add CSV files to data/stream_input")
print("Press CTRL+C to stop.\n")


query.awaitTermination()