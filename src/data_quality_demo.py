import os
import sys
import shutil

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit
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

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("DataQualityAndSchemaEvolutionDemo")
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
# OUTPUT PATHS
# --------------------------------------------------

quarantine_path = "output/delta/quarantine_sales"
clean_path = "output/delta/clean_sales"


# --------------------------------------------------
# RESET DEMO OUTPUT
#
# This is only for our learning experiment.
# It ensures every run starts with a fresh Delta table.
# --------------------------------------------------

for path in [quarantine_path, clean_path]:
    if os.path.exists(path):
        shutil.rmtree(path)


# --------------------------------------------------
# ORIGINAL SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
])


# --------------------------------------------------
# RAW / BRONZE-LIKE DATA
# --------------------------------------------------

data = [
    (1, "Laptop", 2, 3500.0),
    (2, "Mouse", 5, 120.0),

    # Invalid quantity
    (3, "Keyboard", -2, 250.0),

    # Null sale_id
    (None, "Monitor", 1, 900.0),

    # Negative price
    (5, "Headset", 2, -300.0),

    # Empty product
    (6, "", 1, 500.0),

    # Duplicate sale_id
    (2, "Mouse", 5, 120.0),
]

df = spark.createDataFrame(
    data,
    schema
)


print("\n==========================================")
print("=== RAW / BRONZE-LIKE DATA ===")
print("==========================================")

df.show(truncate=False)

print("\n=== RAW SCHEMA ===")
df.printSchema()


# --------------------------------------------------
# ROW-LEVEL VALIDATION
# --------------------------------------------------

valid_condition = (
    col("sale_id").isNotNull()
    & col("product").isNotNull()
    & (trim(col("product")) != "")
    & col("quantity").isNotNull()
    & (col("quantity") > 0)
    & col("price").isNotNull()
    & (col("price") >= 0)
)


valid_df = df.filter(
    valid_condition
)

invalid_df = df.filter(
    ~valid_condition
)


print("\n=== VALID ROW-LEVEL RECORDS ===")
valid_df.show(truncate=False)


print("\n=== INVALID ROW-LEVEL RECORDS ===")
invalid_df.show(truncate=False)


# --------------------------------------------------
# DUPLICATE DETECTION
# --------------------------------------------------

duplicate_ids = (
    valid_df
    .groupBy("sale_id")
    .count()
    .filter(col("count") > 1)
)


print("\n=== DUPLICATE SALE IDS ===")
duplicate_ids.show(truncate=False)


duplicate_records = (
    valid_df
    .join(
        duplicate_ids.select("sale_id"),
        on="sale_id",
        how="inner"
    )
)


print("\n=== DUPLICATE RECORDS ===")
duplicate_records.show(truncate=False)


# --------------------------------------------------
# REMOVE ALL DUPLICATED SALE IDS
#
# For this lesson, we quarantine both duplicate
# records instead of arbitrarily keeping one.
# --------------------------------------------------

clean_df = (
    valid_df
    .join(
        duplicate_ids.select("sale_id"),
        on="sale_id",
        how="left_anti"
    )
)


print("\n=== CLEAN / SILVER-READY DATA ===")
clean_df.show(truncate=False)


# --------------------------------------------------
# CLASSIFY ROW-LEVEL ERRORS
# --------------------------------------------------

classified_df = (
    df
    .withColumn(
        "error_reason",

        when(
            col("sale_id").isNull(),
            lit("NULL_SALE_ID")
        )

        .when(
            col("quantity").isNull(),
            lit("NULL_QUANTITY")
        )

        .when(
            col("quantity") <= 0,
            lit("INVALID_QUANTITY")
        )

        .when(
            col("price").isNull(),
            lit("NULL_PRICE")
        )

        .when(
            col("price") < 0,
            lit("NEGATIVE_PRICE")
        )

        .when(
            col("product").isNull()
            | (trim(col("product")) == ""),
            lit("INVALID_PRODUCT")
        )
    )
)


print("\n=== CLASSIFIED DATA ===")
classified_df.show(truncate=False)


# --------------------------------------------------
# CREATE QUARANTINE DATA
# --------------------------------------------------

invalid_with_reason = (
    classified_df
    .filter(
        col("error_reason").isNotNull()
    )
)


duplicate_with_reason = (
    duplicate_records
    .withColumn(
        "error_reason",
        lit("DUPLICATE_SALE_ID")
    )
)


quarantine_df = (
    invalid_with_reason
    .unionByName(
        duplicate_with_reason
    )
)


print("\n=== QUARANTINE DATA ===")
quarantine_df.show(truncate=False)


# --------------------------------------------------
# WRITE QUARANTINE TABLE
# --------------------------------------------------

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_path)
)


print(
    f"\nQuarantine table written to: "
    f"{quarantine_path}"
)


# --------------------------------------------------
# WRITE CLEAN SILVER TABLE
# --------------------------------------------------

(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .save(clean_path)
)


print(
    f"Clean table written to: "
    f"{clean_path}"
)


# ==================================================
# SCHEMA EVOLUTION EXPERIMENT
# ==================================================

print("\n\n")
print("==========================================")
print("=== SCHEMA EVOLUTION EXPERIMENT ===")
print("==========================================")


# --------------------------------------------------
# READ ORIGINAL DELTA TABLE
# --------------------------------------------------

original_df = (
    spark.read
    .format("delta")
    .load(clean_path)
)


print("\n=== ORIGINAL DELTA TABLE ===")
original_df.show(truncate=False)


print("\n=== ORIGINAL DELTA TABLE SCHEMA ===")
original_df.printSchema()


# Expected schema:
#
# sale_id
# product
# quantity
# price


# --------------------------------------------------
# CREATE NEW DATA WITH AN EXTRA COLUMN
# --------------------------------------------------

new_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),

    # NEW COLUMN
    StructField("discount", DoubleType(), True),
])


new_data = [
    (
        7,
        "Webcam",
        1,
        450.0,
        50.0
    ),
    (
        8,
        "USB Hub",
        2,
        100.0,
        10.0
    ),
]


new_df = spark.createDataFrame(
    new_data,
    new_schema
)


print("\n=== NEW INCOMING DATA ===")
new_df.show(truncate=False)


print("\n=== NEW INCOMING SCHEMA ===")
new_df.printSchema()


# At this point:
#
# Existing Delta table:
#
# sale_id
# product
# quantity
# price
#
#
# New DataFrame:
#
# sale_id
# product
# quantity
# price
# discount


# --------------------------------------------------
# TEST 1:
# APPEND WITHOUT SCHEMA EVOLUTION
# --------------------------------------------------

print("\n")
print("==========================================")
print("=== TEST 1: SCHEMA ENFORCEMENT ===")
print("==========================================")


print(
    "\nAttempting to append the new data "
    "WITHOUT mergeSchema..."
)


try:

    (
        new_df.write
        .format("delta")
        .mode("append")
        .save(clean_path)
    )

    print(
        "\nUnexpected: the write succeeded."
    )


except Exception as error:

    print(
        "\nWRITE REJECTED AS EXPECTED."
    )

    print(
        "\nDelta detected that the incoming "
        "schema does not match the table schema."
    )

    print("\nError type:")
    print(type(error).__name__)

    print("\nBeginning of Delta error message:")
    print(str(error)[:1500])


# --------------------------------------------------
# VERIFY THAT THE FAILED WRITE DID NOT MODIFY
# THE TABLE
# --------------------------------------------------

after_failed_write_df = (
    spark.read
    .format("delta")
    .load(clean_path)
)


print(
    "\n=== TABLE AFTER REJECTED WRITE ==="
)

after_failed_write_df \
    .orderBy("sale_id") \
    .show(truncate=False)


print(
    "\n=== SCHEMA AFTER REJECTED WRITE ==="
)

after_failed_write_df.printSchema()


# discount should NOT exist yet.


# --------------------------------------------------
# TEST 2:
# ENABLE SCHEMA EVOLUTION
# --------------------------------------------------

print("\n")
print("==========================================")
print("=== TEST 2: SCHEMA EVOLUTION ===")
print("==========================================")


print(
    "\nAppending again, this time with "
    "mergeSchema=true..."
)


(
    new_df.write
    .format("delta")
    .mode("append")
    .option(
        "mergeSchema",
        "true"
    )
    .save(clean_path)
)


print(
    "\nWrite succeeded with "
    "schema evolution enabled."
)


# --------------------------------------------------
# READ EVOLVED TABLE
# --------------------------------------------------

evolved_df = (
    spark.read
    .format("delta")
    .load(clean_path)
)


print("\n=== EVOLVED DELTA TABLE ===")

evolved_df \
    .orderBy("sale_id") \
    .show(truncate=False)


print("\n=== EVOLVED DELTA TABLE SCHEMA ===")

evolved_df.printSchema()


# Expected schema:
#
# sale_id
# product
# quantity
# price
# discount


# --------------------------------------------------
# INSPECT OLD VS NEW RECORDS
# --------------------------------------------------

print("\n=== OLD RECORDS ===")

evolved_df \
    .filter(
        col("sale_id") < 7
    ) \
    .show(truncate=False)


print("\n=== NEW RECORDS ===")

evolved_df \
    .filter(
        col("sale_id") >= 7
    ) \
    .show(truncate=False)


# Old rows never contained a discount column,
# so Spark should show:
#
# discount = NULL
#
# for those records.


# --------------------------------------------------
# INSPECT DELTA HISTORY
# --------------------------------------------------

delta_table = DeltaTable.forPath(
    spark,
    clean_path
)


print("\n=== CLEAN SALES DELTA HISTORY ===")

(
    delta_table
    .history()
    .select(
        "version",
        "timestamp",
        "operation"
    )
    .orderBy(
        col("version").asc()
    )
    .show(
        truncate=False
    )
)


# --------------------------------------------------
# FINAL EXPLANATION
# --------------------------------------------------

print("\n")
print("==========================================")
print("=== EXPERIMENT COMPLETE ===")
print("==========================================")

print(
    """
What happened:

1. We created a Delta table with:

   sale_id
   product
   quantity
   price

2. New data arrived with:

   sale_id
   product
   quantity
   price
   discount

3. Delta rejected the first append because
   the schemas did not match.

   → Schema enforcement

4. We explicitly enabled:

   mergeSchema = true

5. Delta accepted the new column.

   → Schema evolution

6. Old rows now have:

   discount = NULL

   because their original Parquet files
   never contained the discount column.

7. New rows contain actual discount values.

The physical data is still stored in Parquet,
while Delta's transaction log manages the
logical table schema and table versions.
"""
)


spark.stop()