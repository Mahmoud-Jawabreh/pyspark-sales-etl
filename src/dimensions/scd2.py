from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
)


def classify_customer_changes(
    current_df: DataFrame,
    incoming_df: DataFrame,
) -> DataFrame:
    """
    Classify incoming customer rows for SCD Type 2.

    NEW:
        customer does not exist

    CHANGED:
        customer exists but tracked attributes changed

    UNCHANGED:
        customer exists and tracked attributes are identical
    """

    current = (
        current_df
        .filter(
            col("is_current") == True
        )
        .select(
            "customer_id",
            col("name").alias("current_name"),
            col("city").alias("current_city"),
            col("segment").alias(
                "current_segment"
            ),
        )
    )

    joined = (
        incoming_df.alias("incoming")
        .join(
            current.alias("current"),
            on="customer_id",
            how="left",
        )
    )

    changed = (
        ~col("incoming.name")
        .eqNullSafe(
            col("current.current_name")
        )
        |
        ~col("incoming.city")
        .eqNullSafe(
            col("current.current_city")
        )
        |
        ~col("incoming.segment")
        .eqNullSafe(
            col("current.current_segment")
        )
    )

    return (
        joined
        .withColumn(
            "change_type",
            when(
                col("current_name").isNull(),
                lit("NEW"),
            )
            .when(
                changed,
                lit("CHANGED"),
            )
            .otherwise(
                lit("UNCHANGED")
            ),
        )
        .select(
            "customer_id",
            col("incoming.name").alias("name"),
            col("incoming.city").alias("city"),
            col("incoming.segment").alias(
                "segment"
            ),
            "change_type",
        )
    )