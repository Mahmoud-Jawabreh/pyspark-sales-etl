import os
import sys

from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    datediff,
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("FactTableTypesDemo")
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# 1. TRANSACTION FACT
#
# Grain:
# One row per sale.
# ==================================================

sales_data = [
    (601, 1, 1, 1, 3500.0),
    (602, 2, 2, 2, 240.0),
    (603, 3, 4, 1, 900.0),
]

sales_columns = [
    "sale_id",
    "customer_sk",
    "product_sk",
    "quantity",
    "sales_amount",
]

transaction_fact = spark.createDataFrame(
    sales_data,
    sales_columns
)


print("\n=== TRANSACTION FACT ===")
print("Grain: one row per sale")

transaction_fact.show()


# ==================================================
# 2. PERIODIC SNAPSHOT
#
# Grain:
# One row per product per day.
# ==================================================

inventory_data = [
    (
        datetime(2026, 10, 18),
        1,
        50
    ),
    (
        datetime(2026, 10, 18),
        2,
        120
    ),
    (
        datetime(2026, 10, 19),
        1,
        45
    ),
    (
        datetime(2026, 10, 19),
        2,
        115
    ),
    (
        datetime(2026, 10, 20),
        1,
        42
    ),
    (
        datetime(2026, 10, 20),
        2,
        110
    ),
]

inventory_columns = [
    "snapshot_date",
    "product_sk",
    "stock_quantity",
]

periodic_snapshot = spark.createDataFrame(
    inventory_data,
    inventory_columns
)


print("\n=== PERIODIC SNAPSHOT ===")
print("Grain: one row per product per day")

periodic_snapshot.show()


# ==================================================
# Demonstrate semi-additive behavior
# ==================================================

print(
    "\n=== INVENTORY ON OCTOBER 20 ==="
)

(
    periodic_snapshot
    .filter(
        col("snapshot_date")
        == datetime(2026, 10, 20)
    )
    .groupBy()
    .sum("stock_quantity")
    .show()
)


print(
    "\nWARNING:"
    "\nSumming stock across all dates would "
    "not represent an actual inventory level."
)


# ==================================================
# 3. ACCUMULATING SNAPSHOT
#
# Grain:
# One row per order lifecycle.
# ==================================================

orders_data = [
    (
        9001,
        datetime(2026, 10, 1),
        datetime(2026, 10, 1),
        datetime(2026, 10, 2),
        datetime(2026, 10, 4),
    ),

    (
        9002,
        datetime(2026, 10, 5),
        datetime(2026, 10, 6),
        datetime(2026, 10, 8),
        None,
    ),

    (
        9003,
        datetime(2026, 10, 10),
        None,
        None,
        None,
    ),
]

orders_columns = [
    "order_id",
    "order_date",
    "payment_date",
    "ship_date",
    "delivery_date",
]

accumulating_snapshot = (
    spark.createDataFrame(
        orders_data,
        orders_columns
    )
)


# ==================================================
# Calculate lifecycle measures
# ==================================================

accumulating_snapshot = (
    accumulating_snapshot

    .withColumn(
        "days_to_payment",
        datediff(
            col("payment_date"),
            col("order_date")
        )
    )

    .withColumn(
        "days_to_ship",
        datediff(
            col("ship_date"),
            col("order_date")
        )
    )

    .withColumn(
        "days_to_delivery",
        datediff(
            col("delivery_date"),
            col("order_date")
        )
    )
)


print("\n=== ACCUMULATING SNAPSHOT ===")
print("Grain: one row per order lifecycle")

accumulating_snapshot.show(
    truncate=False
)


spark.stop()