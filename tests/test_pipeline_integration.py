from pyspark.testing.utils import (
    assertDataFrameEqual,
)

from transformations.sales import (
    clean_sales,
    enrich_sales_with_products,
    calculate_product_revenue,
)


def test_sales_pipeline_end_to_end(
    spark,
):

    # ---------------------------------------------
    # RAW SALES
    # ---------------------------------------------

    raw_sales_df = spark.createDataFrame(
        [
            (
                1,
                1001,
                "Laptop",
                1,
                3500.0,
                101,
            ),
            (
                2,
                1002,
                "Mouse",
                2,
                120.0,
                102,
            ),
            (
                3,
                None,
                "Keyboard",
                1,
                250.0,
                103,
            ),
        ],
        [
            "sale_id",
            "customer_id",
            "product",
            "quantity",
            "price",
            "product_id",
        ],
    )


    # ---------------------------------------------
    # PRODUCT DIMENSION
    # ---------------------------------------------

    products_df = spark.createDataFrame(
        [
            (
                101,
                "Computers",
            ),
            (
                102,
                "Accessories",
            ),
            (
                103,
                "Accessories",
            ),
        ],
        [
            "product_id",
            "category",
        ],
    )


    # ---------------------------------------------
    # PIPELINE
    # ---------------------------------------------

    clean_df = clean_sales(
        raw_sales_df
    )

    enriched_df = (
        enrich_sales_with_products(
            clean_df,
            products_df,
        )
    )

    revenue_df = (
        calculate_product_revenue(
            enriched_df
        )
    )


    # ---------------------------------------------
    # EXPECTED RESULT
    # ---------------------------------------------

    expected_df = spark.createDataFrame(
        [
            (
                101,
                3500.0,
            ),
            (
                102,
                240.0,
            ),
        ],
        [
            "product_id",
            "revenue",
        ],
    )


    assertDataFrameEqual(
        revenue_df,
        expected_df,
        checkRowOrder=False,
    )