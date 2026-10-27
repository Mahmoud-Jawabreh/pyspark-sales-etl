import os
import sys

from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    max as spark_max,
    row_number,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
    TimestampType,
    BooleanType,
)
from pyspark.sql.window import Window


# --------------------------------------------------
# PATH
# --------------------------------------------------

DIM_PATH = "output/delta/dim_customer_scd2"


# --------------------------------------------------
# SPARK SESSION
# --------------------------------------------------

builder = (
    SparkSession.builder
    .master("local[2]")
    .appName("SCDType2CustomerDemo")
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
    configure_spark_with_delta_pip(builder)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# DIMENSION SCHEMA
# --------------------------------------------------

dimension_schema = StructType([
    StructField("customer_sk", LongType(), False),
    StructField("customer_id", IntegerType(), False),

    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True),

    StructField("valid_from", TimestampType(), False),
    StructField("valid_to", TimestampType(), True),

    StructField("is_current", BooleanType(), False),
])


# --------------------------------------------------
# INITIAL CUSTOMER DIMENSION
#
# For this learning demo we overwrite the table
# every time so rerunning the script is predictable.
# --------------------------------------------------

initial_data = [
    (
        1,
        1001,
        "Ahmad",
        "Riyadh",
        "Retail",
        datetime(2026, 1, 1),
        None,
        True,
    ),
    (
        2,
        1002,
        "Sara",
        "Jeddah",
        "Premium",
        datetime(2026, 1, 1),
        None,
        True,
    ),
    (
        3,
        1003,
        "Omar",
        "Dammam",
        "Retail",
        datetime(2026, 1, 1),
        None,
        True,
    ),
]


initial_df = spark.createDataFrame(
    initial_data,
    dimension_schema
)


(
    initial_df.write
    .format("delta")
    .mode("overwrite")
    .save(DIM_PATH)
)


print("\n=== INITIAL DIM_CUSTOMER ===")

initial_df.orderBy(
    "customer_id"
).show(
    truncate=False
)


# --------------------------------------------------
# NEW CUSTOMER DATA
# --------------------------------------------------

incoming_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("effective_time", TimestampType(), False),
])


incoming_data = [

    # Existing customer:
    # Riyadh → Jeddah
    (
        1001,
        "Ahmad",
        "Jeddah",
        "Retail",
        datetime(2026, 10, 19, 15, 30),
    ),

    # Existing customer:
    # nothing changed
    (
        1002,
        "Sara",
        "Jeddah",
        "Premium",
        datetime(2026, 10, 19, 15, 30),
    ),

    # New customer
    (
        1004,
        "Lina",
        "Riyadh",
        "Premium",
        datetime(2026, 10, 19, 15, 30),
    ),
]


incoming_df = spark.createDataFrame(
    incoming_data,
    incoming_schema
)


print("\n=== INCOMING DATA ===")

incoming_df.show(
    truncate=False
)


# --------------------------------------------------
# READ CURRENT DIMENSION ROWS
# --------------------------------------------------

current_df = (
    spark.read
    .format("delta")
    .load(DIM_PATH)
    .filter(
        col("is_current") == True
    )
)


# --------------------------------------------------
# COMPARE INCOMING DATA WITH CURRENT VERSION
# --------------------------------------------------

joined_df = (
    incoming_df.alias("incoming")
    .join(
        current_df.alias("current"),
        col("incoming.customer_id")
        == col("current.customer_id"),
        "left"
    )
)


# Null-safe comparisons:
#
# A change exists if any tracked attribute differs.
#
# We are tracking:
# name
# city
# segment

same_attributes = (
    col("incoming.name").eqNullSafe(
        col("current.name")
    )
    &
    col("incoming.city").eqNullSafe(
        col("current.city")
    )
    &
    col("incoming.segment").eqNullSafe(
        col("current.segment")
    )
)


classified_df = (
    joined_df
    .select(
        col("incoming.customer_id")
        .alias("customer_id"),

        col("incoming.name")
        .alias("name"),

        col("incoming.city")
        .alias("city"),

        col("incoming.segment")
        .alias("segment"),

        col("incoming.effective_time")
        .alias("effective_time"),

        col("current.customer_id")
        .alias("existing_customer_id"),

        same_attributes.alias(
            "same_attributes"
        ),
    )
    .withColumn(
        "change_type",

        # No existing current record
        # means new customer.
        (
            lit("NEW")
            if False
            else
            lit(None)
        )
    )
)


# Build change_type clearly
from pyspark.sql.functions import when


classified_df = (
    classified_df
    .withColumn(
        "change_type",

        when(
            col("existing_customer_id").isNull(),
            lit("NEW")
        )

        .when(
            col("same_attributes") == False,
            lit("CHANGED")
        )

        .otherwise(
            lit("UNCHANGED")
        )
    )
    .drop(
        "same_attributes"
    )
)


# Materialize classification BEFORE changing
# the Delta target.
#
# This is important because Spark is lazy.
classified_df.cache()

classified_df.count()


print("\n=== CHANGE CLASSIFICATION ===")

classified_df.select(
    "customer_id",
    "name",
    "city",
    "segment",
    "change_type"
).orderBy(
    "customer_id"
).show(
    truncate=False
)


# --------------------------------------------------
# STEP 1:
# EXPIRE OLD CURRENT VERSIONS
# --------------------------------------------------

changed_df = (
    classified_df
    .filter(
        col("change_type") == "CHANGED"
    )
    .select(
        "customer_id",
        "effective_time"
    )
)


target = DeltaTable.forPath(
    spark,
    DIM_PATH
)


(
    target.alias("target")

    .merge(
        changed_df.alias("source"),

        """
        target.customer_id = source.customer_id
        AND target.is_current = true
        """
    )

    .whenMatchedUpdate(
        set={
            "is_current":
                "false",

            "valid_to":
                "source.effective_time",
        }
    )

    .execute()
)


# --------------------------------------------------
# STEP 2:
# CREATE NEW VERSIONS
#
# Includes:
# - changed customers
# - completely new customers
# --------------------------------------------------

new_versions_source = (
    classified_df
    .filter(
        col("change_type").isin(
            "NEW",
            "CHANGED"
        )
    )
    .select(
        "customer_id",
        "name",
        "city",
        "segment",
        "effective_time"
    )
)


# --------------------------------------------------
# GENERATE NEW SURROGATE KEYS
# --------------------------------------------------

max_sk = (
    spark.read
    .format("delta")
    .load(DIM_PATH)
    .agg(
        spark_max("customer_sk")
    )
    .first()[0]
)

if max_sk is None:
    max_sk = 0


sk_window = (
    Window
    .orderBy("customer_id")
)


new_versions_df = (
    new_versions_source

    .withColumn(
        "customer_sk",
        (
            lit(max_sk)
            + row_number().over(
                sk_window
            )
        ).cast("long")
    )

    .withColumn(
        "valid_from",
        col("effective_time")
    )

    .withColumn(
        "valid_to",
        lit(None).cast(
            "timestamp"
        )
    )

    .withColumn(
        "is_current",
        lit(True)
    )

    .select(
        "customer_sk",
        "customer_id",
        "name",
        "city",
        "segment",
        "valid_from",
        "valid_to",
        "is_current",
    )
)


# --------------------------------------------------
# APPEND NEW VERSIONS
# --------------------------------------------------

(
    new_versions_df.write
    .format("delta")
    .mode("append")
    .save(DIM_PATH)
)


# --------------------------------------------------
# FINAL RESULT
# --------------------------------------------------

final_df = (
    spark.read
    .format("delta")
    .load(DIM_PATH)
)


print("\n=== FINAL SCD TYPE 2 DIMENSION ===")

(
    final_df
    .orderBy(
        "customer_id",
        "valid_from"
    )
    .show(
        truncate=False
    )
)


classified_df.unpersist()

spark.stop()