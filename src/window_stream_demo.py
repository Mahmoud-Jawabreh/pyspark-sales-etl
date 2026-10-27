import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    sum as spark_sum,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("WindowStreamingDemo")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField(
        "sale_id",
        IntegerType(),
        False
    ),
    StructField(
        "event_time",
        TimestampType(),
        False
    ),
    StructField(
        "product",
        StringType(),
        True
    ),
    StructField(
        "quantity",
        IntegerType(),
        True
    ),
    StructField(
        "price",
        DoubleType(),
        True
    ),
])


# --------------------------------------------------
# STREAMING SOURCE
# --------------------------------------------------

stream_df = (
    spark.readStream
    .schema(schema)
    .option("header", True)
    .csv("data/window_stream_input")
)


# --------------------------------------------------
# CALCULATE SALES AMOUNT
# --------------------------------------------------

sales_df = (
    stream_df
    .filter(col("quantity") > 0)
    .withColumn(
        "total_amount",
        col("quantity") * col("price")
    )
)


# --------------------------------------------------
# WATERMARK + WINDOWED AGGREGATION
# --------------------------------------------------

windowed_sales = (
    sales_df
    .withWatermark(
        "event_time",
        "5 minutes"
    )
    .groupBy(
        window(
            col("event_time"),
            "10 minutes"
        ),
        col("product")
    )
    .agg(
        spark_sum(
            "total_amount"
        ).alias("total_sales")
    )
)


# --------------------------------------------------
# OUTPUT
# --------------------------------------------------

query = (
    windowed_sales.writeStream
    .format("console")
    .outputMode("update")
    .option(
        "truncate",
        False
    )
    .option(
        "checkpointLocation",
        "output/window_stream_checkpoint"
    )
    .start()
)


print("\nStreaming query started.")
print(
    "Add CSV files to "
    "data/window_stream_input"
)
print("Press CTRL+C to stop.\n")


query.awaitTermination()