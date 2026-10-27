from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim
from pyspark.sql.functions import (
    sum as spark_sum,
)


def clean_sales(df: DataFrame) -> DataFrame:
    """
    Clean sales records and calculate total_amount.

    Valid rows require:
    - sale_id
    - customer_id
    - non-empty product
    - quantity > 0
    - price >= 0
    """

    return (
        df
        .withColumn(
            "product",
            trim(col("product"))
        )
        .filter(
            col("sale_id").isNotNull()
        )
        .filter(
            col("customer_id").isNotNull()
        )
        .filter(
            col("product").isNotNull()
            & (col("product") != "")
        )
        .filter(
            col("quantity") > 0
        )
        .filter(
            col("price") >= 0
        )
        .withColumn(
            "total_amount",
            col("quantity") * col("price")
        )
    )

def enrich_sales_with_products(
    sales_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:

    return (
        sales_df
        .join(
            products_df,
            on="product_id",
            how="left",
        )
    )



def calculate_product_revenue(
    sales_df: DataFrame,
) -> DataFrame:

    return (
        sales_df
        .groupBy(
            "product_id"
        )
        .agg(
            spark_sum(
                "total_amount"
            ).alias(
                "revenue"
            )
        )
    )