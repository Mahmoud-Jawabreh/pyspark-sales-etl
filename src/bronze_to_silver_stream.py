import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    trim,
    when,
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
# PATHS
# --------------------------------------------------

BRONZE_PATH = "output/delta/bronze_sales"

SILVER_PATH = "output/delta/silver_sales"

QUARANTINE_PATH = "output/delta/quarantine_stream_sales"

SILVER_CHECKPOINT = (
    "output/checkpoints/bronze_to_silver"
)

QUARANTINE_CHECKPOINT = (
    "output/checkpoints/bronze_to_quarantine"
)


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

builder = (
    SparkSession.builder
    .master("local[2]")
    .appName("BronzeToSilverStreaming")
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

spark = (
    configure_spark_with_delta_pip(
        builder
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# EXPECTED BUSINESS SCHEMA
# --------------------------------------------------

sales_schema = StructType([
    StructField(
        "sale_id",
        IntegerType(),
        True
    ),
    StructField(
        "customer_id",
        IntegerType(),
        True
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
    StructField(
        "event_time",
        TimestampType(),
        True
    ),
])


# --------------------------------------------------
# READ BRONZE AS A STREAM
# --------------------------------------------------

bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)


# --------------------------------------------------
# PARSE RAW JSON
# --------------------------------------------------

parsed_df = (
    bronze_df
    .withColumn(
        "parsed",
        from_json(
            col("raw_json"),
            sales_schema
        )
    )
)


# --------------------------------------------------
# EXPAND BUSINESS COLUMNS
# --------------------------------------------------

expanded_df = (
    parsed_df
    .select(
        col("parsed.sale_id").alias("sale_id"),
        col("parsed.customer_id").alias("customer_id"),
        col("parsed.product").alias("product"),
        col("parsed.quantity").alias("quantity"),
        col("parsed.price").alias("price"),
        col("parsed.event_time").alias("event_time"),

        # Keep useful source metadata
        col("message_key"),
        col("topic").alias("source_topic"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        col("kafka_timestamp"),
        col("ingested_at"),

        # Keep original raw event for quarantine/debugging
        col("raw_json")
    )
)


# --------------------------------------------------
# DATA QUALITY CLASSIFICATION
#
# For this demo we store the first error found.
# --------------------------------------------------

classified_df = (
    expanded_df
    .withColumn(
        "error_reason",

        when(
            col("sale_id").isNull(),
            lit("INVALID_OR_MISSING_SALE_ID")
        )

        .when(
            col("customer_id").isNull(),
            lit("INVALID_OR_MISSING_CUSTOMER_ID")
        )

        .when(
            col("product").isNull()
            | (trim(col("product")) == ""),
            lit("INVALID_PRODUCT")
        )

        .when(
            col("quantity").isNull(),
            lit("MISSING_QUANTITY")
        )

        .when(
            col("quantity") <= 0,
            lit("INVALID_QUANTITY")
        )

        .when(
            col("price").isNull(),
            lit("MISSING_PRICE")
        )

        .when(
            col("price") < 0,
            lit("INVALID_PRICE")
        )

        .when(
            col("event_time").isNull(),
            lit("INVALID_OR_MISSING_EVENT_TIME")
        )
    )
)


# --------------------------------------------------
# INVALID RECORDS → QUARANTINE
# --------------------------------------------------

quarantine_df = (
    classified_df
    .filter(
        col("error_reason").isNotNull()
    )
)


# --------------------------------------------------
# VALID RECORDS
# --------------------------------------------------

valid_df = (
    classified_df
    .filter(
        col("error_reason").isNull()
    )
    .drop("error_reason")
    .withColumn(
        "total_amount",
        col("quantity") * col("price")
    )
)


# --------------------------------------------------
# STREAMING DEDUPLICATION
#
# sale_id represents a unique immutable sale/event.
#
# Keep state for duplicates arriving within
# the watermark period.
# --------------------------------------------------

silver_df = (
    valid_df
    .withWatermark(
        "event_time",
        "7 days"
    )
    .dropDuplicatesWithinWatermark(
        ["sale_id"]
    )
    .drop("raw_json")
)


# --------------------------------------------------
# SILVER STREAM
# --------------------------------------------------

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        SILVER_CHECKPOINT
    )
    .start(
        SILVER_PATH
    )
)


# --------------------------------------------------
# QUARANTINE STREAM
# --------------------------------------------------

quarantine_query = (
    quarantine_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        QUARANTINE_CHECKPOINT
    )
    .start(
        QUARANTINE_PATH
    )
)


print("\n==========================================")
print("BRONZE → SILVER PIPELINE RUNNING")
print("==========================================")

print(f"Bronze:     {BRONZE_PATH}")
print(f"Silver:     {SILVER_PATH}")
print(f"Quarantine: {QUARANTINE_PATH}")

print("\nPress CTRL+C to stop.\n")


spark.streams.awaitAnyTermination()