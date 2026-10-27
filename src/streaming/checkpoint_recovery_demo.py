import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

SRC_DIR = (
    PROJECT_ROOT
    / "src"
)

sys.path.insert(
    0,
    str(SRC_DIR),
)


from config.settings import load_config
from config.spark import build_spark_session


INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "streaming"
    / "checkpoint_demo"
    / "inbox"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "output"
    / "streaming"
    / "checkpoint_demo"
    / "data"
)

CHECKPOINT_PATH = (
    PROJECT_ROOT
    / "output"
    / "checkpoints"
    / "streaming_checkpoint_demo"
)


SALES_SCHEMA = StructType([
    StructField(
        "sale_id",
        IntegerType(),
        False,
    ),
    StructField(
        "customer_id",
        IntegerType(),
        True,
    ),
    StructField(
        "product_id",
        IntegerType(),
        True,
    ),
    StructField(
        "quantity",
        IntegerType(),
        True,
    ),
    StructField(
        "price",
        DoubleType(),
        True,
    ),
    StructField(
        "event_time",
        StringType(),
        True,
    ),
])


def main():

    config = load_config(
        "dev"
    )

    spark = (
        build_spark_session(
            config,
            app_name=(
                "CheckpointRecoveryDemo"
            ),
        )
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )


    sales_stream = (
        spark.readStream
        .schema(SALES_SCHEMA)
        .json(
            str(INPUT_PATH)
        )
    )


    processed_stream = (
        sales_stream
        .withColumn(
            "total_amount",
            F.col("quantity")
            * F.col("price"),
        )
        .withColumn(
            "processed_at",
            F.current_timestamp(),
        )
    )


    query = (
        processed_stream
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            str(OUTPUT_PATH),
        )
        .option(
            "checkpointLocation",
            str(CHECKPOINT_PATH),
        )
        .trigger(
            availableNow=True
        )
        .start()
    )


    print(
        f"Query ID: {query.id}"
    )

    print(
        f"Run ID: {query.runId}"
    )


    query.awaitTermination()


    result_df = (
        spark.read
        .parquet(
            str(OUTPUT_PATH)
        )
    )


    print(
        "Total output rows:",
        result_df.count(),
    )

    result_df.orderBy(
        "sale_id"
    ).show(
        truncate=False
    )


    spark.stop()


if __name__ == "__main__":
    main()