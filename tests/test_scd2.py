from pyspark.sql.functions import (
    col,
    count,
)

from pyspark.testing.utils import (
    assertDataFrameEqual,
)

from dimensions.scd2 import (
    classify_customer_changes,
)
def test_scd2_classifies_new_changed_and_unchanged(
    spark,
):

    current_df = spark.createDataFrame(
        [
            (
                1,
                1001,
                "Mahmoud",
                "Riyadh",
                "Retail",
                True,
            ),
            (
                2,
                1002,
                "Sara",
                "Jeddah",
                "Premium",
                True,
            ),
        ],
        [
            "customer_sk",
            "customer_id",
            "name",
            "city",
            "segment",
            "is_current",
        ],
    )

    incoming_df = spark.createDataFrame(
        [
            (
                1001,
                "Mahmoud",
                "Jeddah",
                "Retail",
            ),
            (
                1002,
                "Sara",
                "Jeddah",
                "Premium",
            ),
            (
                1003,
                "Omar",
                "Dammam",
                "Retail",
            ),
        ],
        [
            "customer_id",
            "name",
            "city",
            "segment",
        ],
    )

    actual_df = (
        classify_customer_changes(
            current_df,
            incoming_df,
        )
    )

    expected_df = spark.createDataFrame(
        [
            (
                1001,
                "Mahmoud",
                "Jeddah",
                "Retail",
                "CHANGED",
            ),
            (
                1002,
                "Sara",
                "Jeddah",
                "Premium",
                "UNCHANGED",
            ),
            (
                1003,
                "Omar",
                "Dammam",
                "Retail",
                "NEW",
            ),
        ],
        [
            "customer_id",
            "name",
            "city",
            "segment",
            "change_type",
        ],
    )

    assertDataFrameEqual(
        actual_df,
        expected_df,
        checkRowOrder=False,
    )



def test_scd2_has_only_one_current_row_per_customer(
    spark,
):

    dimension_df = spark.createDataFrame(
        [
            (
                1,
                1001,
                False,
            ),
            (
                4,
                1001,
                True,
            ),
            (
                2,
                1002,
                True,
            ),
        ],
        [
            "customer_sk",
            "customer_id",
            "is_current",
        ],
    )

    invalid_customers = (
        dimension_df
        .filter(
            col("is_current") == True
        )
        .groupBy(
            "customer_id"
        )
        .agg(
            count("*").alias(
                "current_count"
            )
        )
        .filter(
            col("current_count") > 1
        )
        .count()
    )

    assert invalid_customers == 0