import os
import sys
from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)


# --------------------------------------------------
# CREATE DELTA-ENABLED SPARK SESSION
# --------------------------------------------------

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("IncrementalDemo")
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
    builder
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# DEFINE SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("product", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("updated_at", TimestampType(), False),
])


# --------------------------------------------------
# INITIAL TARGET DATA
# --------------------------------------------------

initial_data = [
    (
        1,
        "Laptop",
        2,
        3500.0,
        datetime(2026, 10, 12, 8, 0)
    ),
    (
        2,
        "Mouse",
        5,
        120.0,
        datetime(2026, 10, 12, 9, 0)
    ),
    (
        3,
        "Keyboard",
        3,
        250.0,
        datetime(2026, 10, 12, 10, 0)
    ),
]

initial_df = spark.createDataFrame(
    initial_data,
    schema
)

target_path = "output/delta/incremental_sales"


print("\n=== INITIAL TARGET DATA ===")
initial_df.show(truncate=False)


(
    initial_df.write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)


# --------------------------------------------------
# SIMULATE NEXT SOURCE BATCH
# --------------------------------------------------

source_data = [
    # Old record — already processed
    (
        1,
        "Laptop",
        2,
        3500.0,
        datetime(2026, 10, 12, 8, 0)
    ),

    # Existing sale has been updated
    (
        2,
        "Mouse",
        8,
        120.0,
        datetime(2026, 10, 13, 8, 30)
    ),

    # Brand new sale
    (
        4,
        "Monitor",
        2,
        900.0,
        datetime(2026, 10, 13, 9, 15)
    ),
]

source_df = spark.createDataFrame(
    source_data,
    schema
)


print("\n=== SOURCE BATCH ===")
source_df.show(truncate=False)


# --------------------------------------------------
# PREVIOUS SUCCESSFUL WATERMARK
# --------------------------------------------------

last_watermark = datetime(
    2026,
    10,
    12,
    23,
    59
)


# --------------------------------------------------
# EXTRACT ONLY NEW / CHANGED RECORDS
# --------------------------------------------------

incremental_df = source_df.filter(
    col("updated_at") > last_watermark
)


print("\n=== RECORDS TO PROCESS ===")
incremental_df.show(truncate=False)


# --------------------------------------------------
# MERGE INTO DELTA TARGET
# --------------------------------------------------

target = DeltaTable.forPath(
    spark,
    target_path
)

(
    target.alias("target")
    .merge(
        incremental_df.alias("source"),
        "target.sale_id = source.sale_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)


# --------------------------------------------------
# INSPECT FINAL TABLE
# --------------------------------------------------

final_df = (
    spark.read
    .format("delta")
    .load(target_path)
)

print("\n=== FINAL DELTA TABLE ===")

final_df \
    .orderBy("sale_id") \
    .show(truncate=False)


# --------------------------------------------------
# INSPECT DELTA HISTORY
# --------------------------------------------------

print("\n=== DELTA TABLE HISTORY ===")

target.history().select(
    "version",
    "timestamp",
    "operation"
).show(
    truncate=False
)


spark.stop()