import os
import sys
from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
    DoubleType,
    TimestampType,
)


CUSTOMER_DIM_PATH = (
    "output/delta/dim_customer_scd2"
)

PRODUCT_DIM_PATH = (
    "output/delta/dim_product"
)

FACT_PATH = (
    "output/delta/fact_sales_gold"
)


# --------------------------------------------------
# SPARK
# --------------------------------------------------

builder = (
    SparkSession.builder
    .master("local[2]")
    .appName("GoldStarSchemaDemo")
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
# DIM_PRODUCT
# --------------------------------------------------

product_schema = StructType([
    StructField("product_sk", LongType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), True),
])


product_data = [
    (1, 101, "Laptop", "Computers"),
    (2, 102, "Mouse", "Accessories"),
    (3, 103, "Keyboard", "Accessories"),
    (4, 104, "Monitor", "Displays"),
]


product_df = spark.createDataFrame(
    product_data,
    product_schema
)


(
    product_df.write
    .format("delta")
    .mode("overwrite")
    .save(PRODUCT_DIM_PATH)
)


print("\n=== DIM_PRODUCT ===")

product_df.show(
    truncate=False
)


# --------------------------------------------------
# SOURCE SALES
#
# GRAIN:
# ONE ROW PER PRODUCT SOLD IN A TRANSACTION
# --------------------------------------------------

sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("sale_time", TimestampType(), False),
])


sales_data = [

    # Ahmad while in Riyadh
    (
        601,
        1001,
        101,
        1,
        3500.0,
        datetime(2026, 3, 10, 10, 0),
    ),

    # Sara
    (
        602,
        1002,
        102,
        2,
        120.0,
        datetime(2026, 5, 1, 12, 0),
    ),

    # Omar
    (
        603,
        1003,
        104,
        1,
        900.0,
        datetime(2026, 8, 20, 15, 0),
    ),

    # Ahmad after moving to Jeddah
    (
        604,
        1001,
        103,
        2,
        250.0,
        datetime(2026, 11, 10, 11, 0),
    ),

    # Lina
    (
        605,
        1004,
        101,
        1,
        3500.0,
        datetime(2026, 11, 15, 18, 0),
    ),
]


sales_df = spark.createDataFrame(
    sales_data,
    sales_schema
)


# --------------------------------------------------
# LOAD DIMENSIONS
# --------------------------------------------------

customer_df = (
    spark.read
    .format("delta")
    .load(CUSTOMER_DIM_PATH)
)


products_df = (
    spark.read
    .format("delta")
    .load(PRODUCT_DIM_PATH)
)


# --------------------------------------------------
# HISTORICAL CUSTOMER LOOKUP
# --------------------------------------------------

sales_customer_df = (
    sales_df.alias("sale")

    .join(
        customer_df.alias("customer"),

        (
            (
                col("sale.customer_id")
                ==
                col("customer.customer_id")
            )
            &
            (
                col("sale.sale_time")
                >=
                col("customer.valid_from")
            )
            &
            (
                col("customer.valid_to").isNull()
                |
                (
                    col("sale.sale_time")
                    <
                    col("customer.valid_to")
                )
            )
        ),

        "left"
    )
)


# --------------------------------------------------
# PRODUCT LOOKUP
# --------------------------------------------------

fact_df = (
    sales_customer_df

    .join(
        products_df.alias("product"),

        col("sale.product_id")
        ==
        col("product.product_id"),

        "left"
    )

    .select(
        col("sale.sale_id"),

        col("customer.customer_sk"),

        col("product.product_sk"),

        col("sale.quantity"),

        col("sale.unit_price"),

        (
            col("sale.quantity")
            *
            col("sale.unit_price")
        ).alias(
            "sales_amount"
        ),

        col("sale.sale_time"),
    )
)


# --------------------------------------------------
# SIMPLE DATE KEY
#
# Example:
# 2026-03-10 -> 20260310
# --------------------------------------------------

fact_df = (
    fact_df

    .withColumn(
        "date_sk",

        date_format(
            col("sale_time"),
            "yyyyMMdd"
        ).cast("int")
    )
)


# --------------------------------------------------
# FINAL FACT
# --------------------------------------------------

fact_df = (
    fact_df
    .select(
        "sale_id",
        "customer_sk",
        "product_sk",
        "date_sk",
        "quantity",
        "unit_price",
        "sales_amount",
        "sale_time",
    )
)


print("\n=== FACT_SALES GOLD ===")

fact_df.orderBy(
    "sale_id"
).show(
    truncate=False
)


(
    fact_df.write
    .format("delta")
    .mode("overwrite")
    .save(FACT_PATH)
)


# --------------------------------------------------
# BUSINESS QUERY
# Revenue by city and product category
# --------------------------------------------------

gold_query_df = (
    fact_df.alias("fact")

    .join(
        customer_df.alias("customer"),

        col("fact.customer_sk")
        ==
        col("customer.customer_sk"),
    )

    .join(
        products_df.alias("product"),

        col("fact.product_sk")
        ==
        col("product.product_sk"),
    )
)


print(
    "\n=== REVENUE BY CITY AND CATEGORY ==="
)

(
    gold_query_df

    .groupBy(
        col("customer.city"),
        col("product.category"),
    )

    .sum(
        "sales_amount"
    )

    .withColumnRenamed(
        "sum(sales_amount)",
        "total_revenue"
    )

    .orderBy(
        "customer.city",
        "product.category",
    )

    .show(
        truncate=False
    )
)


spark.stop()