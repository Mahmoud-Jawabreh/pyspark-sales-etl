import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    spark_partition_id,
    concat,
)


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("SkewMitigationDemo")
    .config(
        "spark.driver.host",
        "127.0.0.1"
    )
    .config(
        "spark.driver.bindAddress",
        "127.0.0.1"
    )

    # Disable AQE for this experiment
    # so Spark does not hide the skew from us.
    .config(
        "spark.sql.adaptive.enabled",
        "false"
    )

    # Disable broadcast so we're forced
    # to observe a shuffle join.
    .config(
        "spark.sql.autoBroadcastJoinThreshold",
        "-1"
    )

    .config(
        "spark.sql.shuffle.partitions",
        "8"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==================================================
# CREATE A SKEWED FACT TABLE
#
# 70% of rows:
# product_id = 0
#
# Remaining 30%:
# product_id = 1..99
# ==================================================

fact_df = (
    spark.range(
        0,
        2_000_000,
        numPartitions=8
    )

    .withColumn(
        "product_id",

        when(
            col("id") < 1_400_000,
            lit(0)
        )

        .otherwise(
            (
                (col("id") % 99) + 1
            ).cast("int")
        )
    )
)


# ==================================================
# SMALL PRODUCT DIMENSION
# ==================================================

product_df = (
    spark.range(
        0,
        100
    )

    .withColumnRenamed(
        "id",
        "product_id"
    )

    .withColumn(
        "category",
        concat(
            lit("category_"),
            (
                col("product_id") % 10
            )
        )
    )
)


# ==================================================
# SHOW HOT KEYS
# ==================================================

print("\n=== TOP PRODUCT KEYS ===")

(
    fact_df
    .groupBy("product_id")
    .count()
    .orderBy(
        col("count").desc()
    )
    .show(
        10,
        truncate=False
    )
)


# ==================================================
# SEE WHAT HASH PARTITIONING DOES
# ==================================================

skewed_partitioned_df = (
    fact_df
    .repartition(
        8,
        "product_id"
    )
)


print(
    "\n=== PARTITION DISTRIBUTION "
    "BEFORE SALTING ==="
)

(
    skewed_partitioned_df

    .withColumn(
        "partition_id",
        spark_partition_id()
    )

    .groupBy(
        "partition_id"
    )

    .count()

    .orderBy(
        "partition_id"
    )

    .show()
)


# ==================================================
# NORMAL SKEWED JOIN
# ==================================================

normal_join = (
    fact_df.alias("fact")

    .join(
        product_df.alias("product"),

        col("fact.product_id")
        ==
        col("product.product_id"),

        "inner"
    )
)


print(
    "\n=== NORMAL JOIN PLAN ==="
)

normal_join.explain(
    mode="formatted"
)


# ==================================================
# SALTING
# ==================================================

NUM_SALTS = 8


# For the hot key only:
#
# product 0 becomes:
#
# (0,0)
# (0,1)
# ...
# (0,7)
#
# Other product IDs simply get salt 0.

salted_fact_df = (
    fact_df

    .withColumn(
        "salt",

        when(
            col("product_id") == 0,

            (
                col("id") % NUM_SALTS
            ).cast("int")
        )

        .otherwise(
            lit(0)
        )
    )
)


# ==================================================
# REPLICATE THE HOT DIMENSION ROW
#
# Since fact rows for product 0 now have
# salt 0..7, the product dimension also needs:
#
# product 0, salt 0
# product 0, salt 1
# ...
# product 0, salt 7
# ==================================================

salt_values_df = (
    spark.range(
        0,
        NUM_SALTS
    )
    .select(
        col("id")
        .cast("int")
        .alias("salt")
    )
)


hot_product_df = (
    product_df

    .filter(
        col("product_id") == 0
    )

    .crossJoin(
        salt_values_df
    )
)


normal_products_df = (
    product_df

    .filter(
        col("product_id") != 0
    )

    .withColumn(
        "salt",
        lit(0)
    )
)


salted_product_df = (
    normal_products_df
    .unionByName(
        hot_product_df
    )
)


# ==================================================
# INSPECT SALTED PARTITION DISTRIBUTION
# ==================================================

salted_partitioned_df = (
    salted_fact_df

    .repartition(
        8,
        "product_id",
        "salt"
    )
)


print(
    "\n=== PARTITION DISTRIBUTION "
    "AFTER SALTING ==="
)

(
    salted_partitioned_df

    .withColumn(
        "partition_id",
        spark_partition_id()
    )

    .groupBy(
        "partition_id"
    )

    .count()

    .orderBy(
        "partition_id"
    )

    .show()
)


# ==================================================
# SALTED JOIN
# ==================================================

salted_join = (
    salted_fact_df.alias("fact")

    .join(
        salted_product_df.alias("product"),

        (
            col("fact.product_id")
            ==
            col("product.product_id")
        )
        &
        (
            col("fact.salt")
            ==
            col("product.salt")
        ),

        "inner"
    )
)


print(
    "\n=== SALTED JOIN PLAN ==="
)

salted_join.explain(
    mode="formatted"
)


print(
    "\n=== SALTED JOIN ROW COUNT ==="
)

print(
    salted_join.count()
)


spark.stop()