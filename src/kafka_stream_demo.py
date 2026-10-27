import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
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
    .appName("KafkaStructuredStreamingDemo")
    .config(
        "spark.jars.packages",
        "org.apache.spark:"
        "spark-sql-kafka-0-10_2.13:"
        "4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# SALES EVENT SCHEMA
# --------------------------------------------------

sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
])


# --------------------------------------------------
# READ KAFKA STREAM
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "sales-keyed-events"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# --------------------------------------------------
# INSPECT THE RAW KAFKA SCHEMA
# --------------------------------------------------

print("\n=== RAW KAFKA SCHEMA ===")
kafka_df.printSchema()


# --------------------------------------------------
# CONVERT KAFKA VALUE FROM BINARY → STRING
# --------------------------------------------------

json_df = (
    kafka_df
    .select(
        col("key")
        .cast("string")
        .alias("message_key"),

        col("value")
        .cast("string")
        .alias("json"),

        col("partition"),
        col("offset"),

        col("timestamp")
        .alias("kafka_timestamp")
    )
)


# --------------------------------------------------
# PARSE JSON
# --------------------------------------------------

parsed_df = (
    json_df
    .select(
        from_json(
            col("json"),
            sales_schema
        ).alias("data"),

        col("message_key"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp")
    )
    .select(
        "data.*",
        "message_key",
        "partition",
        "offset",
        "kafka_timestamp"
    )
)


# --------------------------------------------------
# SIMPLE TRANSFORMATION
# --------------------------------------------------

sales_df = (
    parsed_df
    .filter(
        col("quantity") > 0
    )
    .withColumn(
        "total_amount",
        col("quantity") * col("price")
    )
)


# --------------------------------------------------
# OUTPUT STREAM
# --------------------------------------------------

query = (
    sales_df.writeStream
    .format("console")
    .outputMode("append")
    .option(
        "truncate",
        False
    )
    .option(
    "checkpointLocation",
    "output/kafka_keyed_stream_checkpoint"
    )
    .start()
)


print("\n====================================")
print("Spark is listening to Kafka...")
print("Topic: sales-keyed-events")
print("====================================\n")


query.awaitTermination()