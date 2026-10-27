import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("DeltaLakeDemo")
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


# ---------------------------------
# CREATE INITIAL SALES DATA
# ---------------------------------

sales_data = [
    (1, 101, "Laptop", 2, 3500.0),
    (2, 102, "Mouse", 5, 120.0),
    (3, 103, "Keyboard", 3, 250.0),
    (4, 104, "Monitor", 2, 900.0),
    (5, 105, "Headset", 4, 300.0),
]

columns = [
    "sale_id",
    "product_id",
    "product_name",
    "quantity",
    "price",
]

sales_df = spark.createDataFrame(
    sales_data,
    columns
)

sales_df = sales_df.withColumn(
    "total_amount",
    sales_df["quantity"] * sales_df["price"]
)


print("\n=== INITIAL SALES DATA ===")
sales_df.show(truncate=False)


# ---------------------------------
# WRITE AS DELTA
# ---------------------------------

delta_path = "output/delta/sales"

(
    sales_df.write
    .format("delta")
    .mode("overwrite")
    .save(delta_path)
)

print(
    f"\nDelta table written to: {delta_path}"
)


# ---------------------------------
# READ DELTA TABLE
# ---------------------------------

delta_df = (
    spark.read
    .format("delta")
    .load(delta_path)
)

print("\n=== DATA READ FROM DELTA ===")
delta_df.show(truncate=False)

print("\n=== DELTA TABLE SCHEMA ===")
delta_df.printSchema()


input(
    "\nInspect output/delta/sales, "
    "then press ENTER to stop Spark..."
)

new_sales_data = [
    (6, 101, "Laptop", 1, 3500.0),
    (7, 104, "Monitor", 3, 900.0),
]

new_sales_df = spark.createDataFrame(
    new_sales_data,
    columns
)

new_sales_df = new_sales_df.withColumn(
    "total_amount",
    new_sales_df["quantity"] * new_sales_df["price"]
)

(
    new_sales_df.write
    .format("delta")
    .mode("append")
    .save(delta_path)
)

updates_data = [
    # Existing sale_id → corrected quantity
    (3, 103, "Keyboard", 5, 250.0),

    # Brand new sale
    (8, 102, "Mouse", 4, 120.0),
]

updates_df = spark.createDataFrame(
    updates_data,
    columns
)

updates_df = updates_df.withColumn(
    "total_amount",
    updates_df["quantity"] * updates_df["price"]
)

delta_table = DeltaTable.forPath(
    spark,
    delta_path
)

(
    delta_table.alias("target")
    .merge(
        updates_df.alias("source"),
        "target.sale_id = source.sale_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("\n=== CURRENT DELTA TABLE ===")

(
    spark.read
    .format("delta")
    .load(delta_path)
    .orderBy("sale_id")
    .show(truncate=False)
)

print("\n=== DELTA TABLE HISTORY ===")

delta_table.history().show(
    truncate=False
)

version_0_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(delta_path)
)

print("\n=== VERSION 0 ===")
version_0_df.orderBy("sale_id").show()

version_1_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .load(delta_path)
)

print("\n=== VERSION 1 ===")
version_1_df.orderBy("sale_id").show()

spark.stop()