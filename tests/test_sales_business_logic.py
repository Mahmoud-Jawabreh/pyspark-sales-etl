from pyspark.sql.functions import count, col

from pyspark.testing.utils import (
    assertDataFrameEqual,
)

from transformations.sales import (
    enrich_sales_with_products,
    calculate_product_revenue
)


def test_product_join_preserves_fact_grain(
    spark,
):

    sales_data = [
        (
            1,
            101,
            100.0,
        ),
        (
            2,
            102,
            200.0,
        ),
    ]

    sales_df = spark.createDataFrame(
        sales_data,
        [
            "sale_id",
            "product_id",
            "total_amount",
        ],
    )


    product_data = [
      (
          101,
          "Computers",
      ),
      (
          102,
          "Accessories",
      ),
    ]

    products_df = spark.createDataFrame(
        product_data,
        [
            "product_id",
            "category",
        ],
    )


    actual_df = (
        enrich_sales_with_products(
            sales_df,
            products_df,
        )
    )


    assert (
        actual_df.count()
        ==
        sales_df.count()
    )


def test_product_dimension_key_is_unique(
    spark,
):

    product_data = [
        (
            101,
            "Computers",
        ),
        (
            102,
            "Accessories",
        ),
    ]

    products_df = spark.createDataFrame(
        product_data,
        [
            "product_id",
            "category",
        ],
    )

    duplicate_keys = (
        products_df
        .groupBy("product_id")
        .agg(
            count("*").alias("row_count")
        )
        .filter(
            col("row_count") > 1
        )
        .count()
    )

    assert duplicate_keys == 0

def test_product_revenue_aggregation(
    spark,
):

    sales_data = [
        (
            1,
            101,
            100.0,
        ),
        (
            2,
            101,
            200.0,
        ),
        (
            3,
            102,
            50.0,
        ),
    ]

    sales_df = spark.createDataFrame(
        sales_data,
        [
            "sale_id",
            "product_id",
            "total_amount",
        ],
    )


    expected_data = [
        (
            101,
            300.0,
        ),
        (
            102,
            50.0,
        ),
    ]

    expected_df = spark.createDataFrame(
        expected_data,
        [
            "product_id",
            "revenue",
        ],
    )


    actual_df = (
        calculate_product_revenue(
            sales_df
        )
    )


    assertDataFrameEqual(
        actual_df,
        expected_df,
        checkRowOrder=False,
    )


def test_missing_product_is_detectable(
    spark,
):

    sales_df = spark.createDataFrame(
        [
            (
                1,
                999,
                100.0,
            )
        ],
        [
            "sale_id",
            "product_id",
            "total_amount",
        ],
    )

    products_df = spark.createDataFrame(
        [
            (
                101,
                "Computers",
            )
        ],
        [
            "product_id",
            "category",
        ],
    )


    actual_df = (
        enrich_sales_with_products(
            sales_df,
            products_df,
        )
    )


    missing_dimensions = (
        actual_df
        .filter(
            col("category").isNull()
        )
        .count()
    )


    assert missing_dimensions == 1