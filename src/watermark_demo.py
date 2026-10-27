import os
import sys
from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

target_path = "output/delta/incremental_sales"
control_path = "output/delta/pipeline_control"

pipeline_name = "sales_incremental"

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

control_schema = StructType([
    StructField(
        "pipeline_name",
        StringType(),
        False
    ),
    StructField(
        "last_watermark",
        TimestampType(),
        False
    ),
])

initial_control_data = [
    (
        "sales_incremental",
        datetime(2026, 10, 12, 23, 59)
    )
]

control_df = spark.createDataFrame(
    initial_control_data,
    control_schema
)

(
    control_df.write
    .format("delta")
    .mode("overwrite")
    .save(control_path)
)

control_df = (
    spark.read
    .format("delta")
    .load(control_path)
)

pipeline_control = (
    control_df
    .filter(
        col("pipeline_name") == pipeline_name
    )
    .first()
)

last_watermark = (
    pipeline_control["last_watermark"]
)

print(
    "\nPrevious watermark:",
    last_watermark
)

current_watermark = datetime(
    2026,
    10,
    13,
    10,
    0
)

incremental_df = source_df.filter(
    (col("updated_at") > last_watermark)
    &
    (col("updated_at") <= current_watermark)
)

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

control_table = DeltaTable.forPath(
    spark,
    control_path
)

new_control_df = spark.createDataFrame(
    [
        (
            pipeline_name,
            current_watermark
        )
    ],
    control_schema
)

(
    control_table.alias("target")
    .merge(
        new_control_df.alias("source"),
        """
        target.pipeline_name =
        source.pipeline_name
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)