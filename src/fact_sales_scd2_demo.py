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
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
)


# --------------------------------------------------
# PATHS
# --------------------------------------------------

CUSTOMER_DIM_PATH = (
    "output/delta/dim_customer_scd2"
)

FACT_PATH = (
    "output/delta/fact_sales"
)


# --------------------------------------------------
# SPARK
# --------------------------------------------------

builder = (
    SparkSession.builder
    .master("local[2]")
    .appName("FactSalesSCD2Demo")
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
# SOURCE SALES
# --------------------------------------------------

sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("sale_time", TimestampType(), False),
])


sales_data = [

    # Ahmad still lived in Riyadh
    (
        501,
        1001,
        1000.0,
        datetime(2026, 3, 10, 10, 0),
    ),

    # Ahmad now lives in Jeddah
    (
        502,
        1001,
        1500.0,
        datetime(2026, 11, 10, 10, 0),
    ),

    # Sara
    (
        503,
        1002,
        800.0,
        datetime(2026, 5, 1, 12, 0),
    ),

    # Omar
    (
        504,
        1003,
        1200.0,
        datetime(2026, 8, 20, 15, 0),
    ),

    # Lina joined after the October dimension change
    (
        505,
        1004,
        2000.0,
        datetime(2026, 11, 15, 18, 0),
    ),
]


sales_df = spark.createDataFrame(
    sales_data,
    sales_schema
)


print("\n=== SOURCE SALES ===")

sales_df.orderBy(
    "sale_id"
).show(
    truncate=False
)


# --------------------------------------------------
# LOAD SCD2 CUSTOMER DIMENSION
# --------------------------------------------------

customer_dim_df = (
    spark.read
    .format("delta")
    .load(CUSTOMER_DIM_PATH)
)


# --------------------------------------------------
# HISTORICAL DIMENSION LOOKUP
#
# Match each sale to the dimension version that
# was valid when that sale occurred.
# --------------------------------------------------

fact_df = (
    sales_df.alias("sale")

    .join(
        customer_dim_df.alias("customer"),

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

    .select(
        col("sale.sale_id"),

        col("customer.customer_sk"),

        col("sale.customer_id")
        .alias("customer_id_source"),

        col("sale.amount"),

        col("sale.sale_time"),
    )
)


print("\n=== FACT AFTER SCD2 LOOKUP ===")

fact_df.orderBy(
    "sale_id"
).show(
    truncate=False
)


# --------------------------------------------------
# CHECK FOR UNMATCHED DIMENSION ROWS
# --------------------------------------------------

unmatched_df = (
    fact_df
    .filter(
        col("customer_sk").isNull()
    )
)


unmatched_count = unmatched_df.count()


if unmatched_count > 0:

    print(
        "\nWARNING: "
        f"{unmatched_count} sales "
        "could not find a customer dimension version."
    )

    unmatched_df.show(
        truncate=False
    )

else:

    print(
        "\nAll sales matched a customer dimension version."
    )


# --------------------------------------------------
# WRITE FACT TABLE
# --------------------------------------------------

(
    fact_df.write
    .format("delta")
    .mode("overwrite")
    .save(FACT_PATH)
)


# --------------------------------------------------
# HISTORICAL ANALYTICS
#
# Join fact to dimension USING customer_sk.
# --------------------------------------------------

fact_saved_df = (
    spark.read
    .format("delta")
    .load(FACT_PATH)
)


historical_sales_df = (
    fact_saved_df.alias("fact")

    .join(
        customer_dim_df.alias("customer"),

        col("fact.customer_sk")
        ==
        col("customer.customer_sk"),

        "left"
    )

    .select(
        col("fact.sale_id"),
        col("fact.sale_time"),
        col("fact.amount"),

        col("customer.customer_id"),
        col("customer.name"),
        col("customer.city"),

        col("fact.customer_sk"),
    )
)


print("\n=== HISTORICALLY CORRECT SALES ===")

historical_sales_df.orderBy(
    "sale_id"
).show(
    truncate=False
)


# --------------------------------------------------
# REVENUE BY HISTORICAL CITY
# --------------------------------------------------

print("\n=== REVENUE BY HISTORICAL CITY ===")

(
    historical_sales_df

    .groupBy(
        "city"
    )

    .sum(
        "amount"
    )

    .withColumnRenamed(
        "sum(amount)",
        "total_revenue"
    )

    .orderBy(
        col("total_revenue").desc()
    )

    .show(
        truncate=False
    )
)


spark.stop()