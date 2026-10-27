import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
)


KAFKA_TOPIC = "sales-cdc-events"

BRONZE_PATH = (
    "output/delta/bronze_sales_cdc"
)

CHECKPOINT_PATH = (
    "output/checkpoints/kafka_cdc_bronze"
)


builder = (
    SparkSession.builder
    .master("local[2]")
    .appName("KafkaCDCBronze")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
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


print("\n===================================")
print("CDC KAFKA → BRONZE RUNNING")
print("===================================")
print(f"Topic: {KAFKA_TOPIC}")
print(f"Bronze: {BRONZE_PATH}")
print("\nPress CTRL+C to stop.\n")


query.awaitTermination()