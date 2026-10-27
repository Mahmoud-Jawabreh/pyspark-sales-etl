import os
import sys

from pyspark.sql import SparkSession


def build_spark_session(
    config: dict,
    app_name: str | None = None,
) -> SparkSession:

    os.environ[
        "PYSPARK_PYTHON"
    ] = sys.executable

    os.environ[
        "PYSPARK_DRIVER_PYTHON"
    ] = sys.executable

    os.environ[
        "SPARK_LOCAL_IP"
    ] = "127.0.0.1"

    spark_config = config[
        "spark"
    ]

    application_name = (
        app_name
        or config["app"]["name"]
    )

    spark = (
        SparkSession.builder
        .master(
            spark_config[
                "master"
            ]
        )
        .appName(
            application_name
        )
        .config(
            "spark.driver.host",
            "127.0.0.1",
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(
                spark_config[
                    "shuffle_partitions"
                ]
            ),
        )
        .config(
            "spark.sql.adaptive.enabled",
            str(
                spark_config[
                    "adaptive_enabled"
                ]
            ).lower(),
        )
        .getOrCreate()
    )

    return spark