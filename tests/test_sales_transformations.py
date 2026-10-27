from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

from pyspark.testing.utils import (
    assertDataFrameEqual,
)

from pyspark.sql.functions import count, col

from transformations.sales import clean_sales


INPUT_SCHEMA = StructType([
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
])


EXPECTED_SCHEMA = StructType([
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
        "total_amount",
        DoubleType(),
        True,
    ),
])


def test_clean_sales_filters_invalid_rows(
    spark,
):

    # ARRANGE
    input_data = [
        (
            1,
            1001,
            " Laptop ",
            2,
            3500.0,
        ),
        (
            2,
            None,
            "Mouse",
            1,
            120.0,
        ),
        (
            3,
            1003,
            "",
            1,
            250.0,
        ),
        (
            4,
            1004,
            "Monitor",
            0,
            900.0,
        ),
        (
            5,
            1005,
            "Headset",
            2,
            -10.0,
        ),
    ]

    input_df = spark.createDataFrame(
        input_data,
        INPUT_SCHEMA,
    )


    expected_data = [
        (
            1,
            1001,
            "Laptop",
            2,
            3500.0,
            7000.0,
        )
    ]

    expected_df = spark.createDataFrame(
        expected_data,
        EXPECTED_SCHEMA,
    )


    # ACT
    actual_df = clean_sales(
        input_df
    )


    # ASSERT
    assertDataFrameEqual(
        actual_df,
        expected_df,
        checkRowOrder=False,
    )


def test_clean_sales_calculates_total_amount(
    spark,
):

    # ARRANGE
    input_data = [
        (
            10,
            2001,
            "Keyboard",
            3,
            250.0,
        ),
        (
            11,
            2002,
            "Mouse",
            2,
            120.0,
        ),
    ]

    input_df = spark.createDataFrame(
        input_data,
        INPUT_SCHEMA,
    )


    expected_data = [
        (
            10,
            2001,
            "Keyboard",
            3,
            250.0,
            750.0,
        ),
        (
            11,
            2002,
            "Mouse",
            2,
            120.0,
            240.0,
        ),
    ]

    expected_df = spark.createDataFrame(
        expected_data,
        EXPECTED_SCHEMA,
    )


    # ACT
    actual_df = clean_sales(
        input_df
    )


    # ASSERT
    assertDataFrameEqual(
        actual_df,
        expected_df,
        checkRowOrder=False,
    )

def test_clean_sales_schema(
    spark,
):

    input_data = [
        (
            1,
            1001,
            "Laptop",
            2,
            3500.0,
        )
    ]

    input_df = spark.createDataFrame(
        input_data,
        INPUT_SCHEMA,
    )

    actual_df = clean_sales(
        input_df
    )

    assert (
        actual_df.schema
        ==
        EXPECTED_SCHEMA
    )


def test_clean_sales_has_no_invalid_values(
    spark,
):

    input_data = [
        (
            1,
            1001,
            "Laptop",
            2,
            3500.0,
        ),
        (
            2,
            1002,
            "Mouse",
            3,
            120.0,
        ),
    ]

    input_df = spark.createDataFrame(
        input_data,
        INPUT_SCHEMA,
    )

    actual_df = clean_sales(
        input_df
    )

    assert (
        actual_df
        .filter(
            col("sale_id").isNull()
        )
        .count()
        == 0
    )

    assert (
        actual_df
        .filter(
            col("customer_id").isNull()
        )
        .count()
        == 0
    )

    assert (
        actual_df
        .filter(
            col("quantity") <= 0
        )
        .count()
        == 0
    )

    assert (
        actual_df
        .filter(
            col("price") < 0
        )
        .count()
        == 0
    )



def test_sale_id_is_unique(
    spark,
):

    input_data = [
        (
            1,
            1001,
            "Laptop",
            2,
            3500.0,
        ),
        (
            2,
            1002,
            "Mouse",
            1,
            120.0,
        ),
    ]

    input_df = spark.createDataFrame(
        input_data,
        INPUT_SCHEMA,
    )

    actual_df = clean_sales(
        input_df
    )

    duplicate_count = (
        actual_df
        .groupBy("sale_id")
        .agg(
            count("*").alias("row_count")
        )
        .filter(
            col("row_count") > 1
        )
        .count()
    )

    assert duplicate_count == 0

