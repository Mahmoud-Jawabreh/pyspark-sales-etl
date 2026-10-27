from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)


SALES_SCHEMA = StructType([
    StructField(
        "sale_id",
        IntegerType(),
        True,
    ),
    StructField(
        "customer_id",
        IntegerType(),
        True,
    ),
    StructField(
        "product",
        StringType(),
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
        "product_id",
        IntegerType(),
        True,
    ),
])


def read_sales_csv(
    spark: SparkSession,
    path: str,
) -> DataFrame:
    """
    Read raw sales CSV using an explicit schema.
    """

    return (
        spark.read
        .option(
            "header",
            True,
        )
        .option(
            "mode",
            "FAILFAST",
        )
        .schema(
            SALES_SCHEMA
        )
        .csv(
            path
        )
    )