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
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType,
)
from pyspark.sql.window import Window


BRONZE_PATH = "output/delta/bronze_sales_cdc"

SILVER_PATH = (
    "output/delta/silver_sales_cdc_safe"
)

CHECKPOINT_PATH = (
    "output/checkpoints/cdc_safe_silver"
)


# --------------------------------------------------
# SPARK
# --------------------------------------------------

builder = (
    SparkSession.builder
    .master("local[2]")
    .appName("SafeCDCToSilver")
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
# CDC SCHEMA
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
# SILVER SCHEMA
# --------------------------------------------------

silver_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),

    StructField("is_deleted", BooleanType(), False),

    StructField("last_operation", StringType(), True),
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
# CREATE TARGET
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

    print("Created safe CDC Silver table.")


# --------------------------------------------------
# READ BRONZE
# --------------------------------------------------

bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)


# --------------------------------------------------
# PARSE CDC
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


valid_df = (
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
# FOREACH BATCH
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(
        f"\n===== MICRO-BATCH {batch_id} ====="
    )


    # Latest event for each sale in this batch
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


    latest_df.select(
        "event_id",
        "sequence_number",
        "operation",
        "sale_id",
        "quantity"
    ).show(
        truncate=False
    )


    target = DeltaTable.forPath(
        spark,
        SILVER_PATH
    )


    (
        target.alias("target")

        .merge(
            latest_df.alias("source"),
            "target.sale_id = source.sale_id"
        )


        # ------------------------------------------
        # NEWER DELETE
        #
        # Keep the row as a tombstone.
        # ------------------------------------------

        .whenMatchedUpdate(
            condition="""
                source.operation = 'D'
                AND
                source.sequence_number >
                target.last_sequence_number
            """,

            set={
                "is_deleted":
                    "true",

                "last_operation":
                    "source.operation",

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
        # NEWER INSERT / UPDATE
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

                "is_deleted":
                    "false",

                "last_operation":
                    "source.operation",

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
        # NEW ENTITY: INSERT / UPDATE
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

                "is_deleted":
                    "false",

                "last_operation":
                    "source.operation",

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
        # DELETE ARRIVES BEFORE ENTITY EXISTS
        #
        # Create a tombstone anyway.
        # ------------------------------------------

        .whenNotMatchedInsert(
            condition="""
                source.operation = 'D'
            """,

            values={
                "sale_id":
                    "source.sale_id",

                "is_deleted":
                    "true",

                "last_operation":
                    "source.operation",

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
        f"Micro-batch {batch_id} completed."
    )


query = (
    valid_df.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .start()
)


print("\n====================================")
print("SAFE CDC SILVER PIPELINE RUNNING")
print("====================================")

query.awaitTermination()