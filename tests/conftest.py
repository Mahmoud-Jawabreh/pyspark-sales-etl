import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


ROOT_DIR = Path(__file__).resolve().parents[1]

sys.path.insert(
    0,
    str(ROOT_DIR / "src")
)


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


@pytest.fixture(scope="session")
def spark():

    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySparkTests")
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

    spark_session.sparkContext.setLogLevel(
        "ERROR"
    )

    yield spark_session

    spark_session.stop()