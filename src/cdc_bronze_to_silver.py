import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    row_number,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.window import Window


# --------------------------------------------------
# PATHS
# --------------------------------------------------

BRONZE_PATH = "output/delta/bronze_sales_cdc"

SILVER_PATH = "output/delta/silver_sales_current"

CHECKPOINT_PATH = (
    "output/checkpoints/cdc_bronze_to_silver"
)


# --------------------------------------------------
# SPARK
# --------------------------------------------------

builder = (
    SparkSession.builder
    .master("local[2]")
    .appName("CDCToSilver")
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
# CDC EVENT SCHEMA
# --------------------------------------------------

cdc_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("sequence_number", IntegerType(), True),
    StructField("operation", StringType(), True),

    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),

    StructField("event_time", TimestampType(), True),
])


# --------------------------------------------------
# SILVER TARGET SCHEMA
# --------------------------------------------------

silver_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),

    StructField("last_event_id", StringType(), True),
    StructField(
        "last_sequence_number",
        IntegerType(),
        True
    ),
    StructField(
        "last_event_time",
        TimestampType(),
        True
    ),

    StructField(
        "source_partition",
        IntegerType(),
        True
    ),
    StructField(
        "source_offset",
        IntegerType(),
        True
    ),

    StructField(
        "updated_at",
        TimestampType(),
        True
    ),
])


# --------------------------------------------------
# CREATE EMPTY SILVER TABLE IF NEEDED
# --------------------------------------------------

if not DeltaTable.isDeltaTable(
    spark,
    SILVER_PATH
):
    (
        spark.createDataFrame(
            [],
            silver_schema
        )
        .write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

    print(
        "\nCreated empty Silver current-state table."
    )


# --------------------------------------------------
# READ BRONZE AS STREAM
# --------------------------------------------------

bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)


# --------------------------------------------------
# PARSE CDC JSON
# --------------------------------------------------

cdc_df = (
    bronze_df
    .withColumn(
        "data",
        from_json(
            col("raw_json"),
            cdc_schema
        )
    )
    .select(
        col("data.event_id"),
        col("data.sequence_number"),
        col("data.operation"),

        col("data.sale_id"),
        col("data.customer_id"),
        col("data.product"),
        col("data.quantity"),
        col("data.price"),
        col("data.event_time"),

        col("partition")
        .alias("source_partition"),

        col("offset")
        .alias("source_offset")
    )
)


# --------------------------------------------------
# BASIC CDC VALIDATION
# --------------------------------------------------

valid_cdc_df = (
    cdc_df
    .filter(
        col("event_id").isNotNull()
        & col("sale_id").isNotNull()
        & col("sequence_number").isNotNull()
        & col("operation").isin(
            "I",
            "U",
            "D"
        )
    )
)


# --------------------------------------------------
# FOREACHBATCH FUNCTION
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(
        f"\n===== MICRO-BATCH {batch_id} ====="
    )

    # ----------------------------------------------
    # IMPORTANT:
    # One micro-batch could contain:
    #
    # sale 301 sequence 1 INSERT
    # sale 301 sequence 2 UPDATE
    #
    # We only need the newest state for this
    # current-state table.
    # ----------------------------------------------

    window_spec = (
        Window
        .partitionBy("sale_id")
        .orderBy(
            col("sequence_number").desc(),
            col("source_offset").desc()
        )
    )

    latest_df = (
        batch_df
        .withColumn(
            "row_num",
            row_number().over(
                window_spec
            )
        )
        .filter(
            col("row_num") == 1
        )
        .drop("row_num")
    )


    print("\nLatest event per sale:")

    latest_df.select(
        "event_id",
        "sequence_number",
        "operation",
        "sale_id",
        "quantity",
        "source_partition",
        "source_offset"
    ).orderBy(
        "sale_id"
    ).show(
        truncate=False
    )


    # ----------------------------------------------
    # DELTA TARGET
    # ----------------------------------------------

    target = DeltaTable.forPath(
        spark,
        SILVER_PATH
    )


    # ----------------------------------------------
    # MERGE CDC INTO CURRENT STATE
    # ----------------------------------------------

    (
        target.alias("target")

        .merge(
            latest_df.alias("source"),

            """
            target.sale_id =
            source.sale_id
            """
        )


        # ------------------------------------------
        # DELETE
        # ------------------------------------------

        .whenMatchedDelete(
            condition="""
                source.operation = 'D'
                AND
                source.sequence_number >
                target.last_sequence_number
            """
        )


        # ------------------------------------------
        # UPDATE
        # ------------------------------------------

        .whenMatchedUpdate(
            condition="""
                source.operation IN ('I', 'U')
                AND
                source.sequence_number >
                target.last_sequence_number
            """,

            set={
                "customer_id":
                    "source.customer_id",

                "product":
                    "source.product",

                "quantity":
                    "source.quantity",

                "price":
                    "source.price",

                "total_amount":
                    "source.quantity * source.price",

                "last_event_id":
                    "source.event_id",

                "last_sequence_number":
                    "source.sequence_number",

                "last_event_time":
                    "source.event_time",

                "source_partition":
                    "source.source_partition",

                "source_offset":
                    "source.source_offset",

                "updated_at":
                    "current_timestamp()",
            }
        )


        # ------------------------------------------
        # INSERT
        #
        # We allow I or U here.
        #
        # Why allow U?
        # Because the first micro-batch may already
        # contain INSERT + UPDATE for an entity.
        # We keep only the newest state.
        # ------------------------------------------

        .whenNotMatchedInsert(
            condition="""
                source.operation IN ('I', 'U')
            """,

            values={
                "sale_id":
                    "source.sale_id",

                "customer_id":
                    "source.customer_id",

                "product":
                    "source.product",

                "quantity":
                    "source.quantity",

                "price":
                    "source.price",

                "total_amount":
                    "source.quantity * source.price",

                "last_event_id":
                    "source.event_id",

                "last_sequence_number":
                    "source.sequence_number",

                "last_event_time":
                    "source.event_time",

                "source_partition":
                    "source.source_partition",

                "source_offset":
                    "source.source_offset",

                "updated_at":
                    "current_timestamp()",
            }
        )

        .execute()
    )


    print(
        f"Micro-batch {batch_id} merged successfully."
    )


# --------------------------------------------------
# START STREAM
# --------------------------------------------------

query = (
    valid_cdc_df.writeStream

    .foreachBatch(
        process_batch
    )

    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    .start()
)


print("\n====================================")
print("CDC BRONZE → SILVER MERGE RUNNING")
print("====================================")

print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")

print("\nPress CTRL+C to stop.\n")


query.awaitTermination()