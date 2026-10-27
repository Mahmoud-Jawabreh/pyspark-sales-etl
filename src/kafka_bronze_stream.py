import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
)


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

KAFKA_TOPIC = "sales-keyed-events"

BRONZE_PATH = "output/delta/bronze_sales"

CHECKPOINT_PATH = (
    "output/checkpoints/kafka_bronze"
)


# --------------------------------------------------
# SPARK SESSION
#
# We need BOTH:
# - Delta Lake
# - Spark Kafka connector
# --------------------------------------------------

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("KafkaBronzeStreaming")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.spark:"
        "spark-sql-kafka-0-10_2.13:"
        "4.2.0"
    ]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# READ FROM KAFKA
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
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# --------------------------------------------------
# CREATE BRONZE DATAFRAME
#
# IMPORTANT:
# We are NOT parsing the JSON yet.
#
# Bronze preserves the raw Kafka event.
# --------------------------------------------------

bronze_df = (
    kafka_df
    .select(
        col("key")
        .cast("string")
        .alias("message_key"),

        col("value")
        .cast("string")
        .alias("raw_json"),

        col("topic"),

        col("partition"),

        col("offset"),

        col("timestamp")
        .alias("kafka_timestamp"),

        current_timestamp()
        .alias("ingested_at")
    )
)


# --------------------------------------------------
# WRITE STREAM TO DELTA
# --------------------------------------------------

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .start(
        BRONZE_PATH
    )
)


print("\n======================================")
print("KAFKA → BRONZE DELTA STREAM RUNNING")
print("======================================")
print(f"Kafka topic: {KAFKA_TOPIC}")
print(f"Bronze table: {BRONZE_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print("\nPress CTRL+C to stop.\n")


query.awaitTermination()