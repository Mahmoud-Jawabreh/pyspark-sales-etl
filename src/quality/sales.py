from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_sales_metrics(
    raw_df: DataFrame,
    clean_df: DataFrame,
) -> dict:

    raw_metrics = (
        raw_df
        .agg(
            F.count("*")
            .alias("raw_rows"),

            F.sum(
                F.when(
                    F.col("customer_id").isNull(),
                    1,
                ).otherwise(0)
            )
            .alias("missing_customer_rows"),

            F.sum(
                F.when(
                    F.col("quantity") <= 0,
                    1,
                ).otherwise(0)
            )
            .alias("invalid_quantity_rows"),
        )
        .first()
        .asDict()
    )


    clean_metrics = (
        clean_df
        .agg(
            F.count("*")
            .alias("clean_rows"),

            F.coalesce(
                F.sum("total_amount"),
                F.lit(0.0),
            )
            .alias("total_revenue"),
        )
        .first()
        .asDict()
    )


    raw_rows = int(
        raw_metrics["raw_rows"]
    )

    clean_rows = int(
        clean_metrics["clean_rows"]
    )

    rejected_rows = (
        raw_rows
        - clean_rows
    )

    rejected_ratio = (
        rejected_rows / raw_rows
        if raw_rows > 0
        else 0.0
    )


    return {
        "raw_rows": raw_rows,

        "clean_rows": clean_rows,

        "rejected_rows": rejected_rows,

        "rejected_ratio": rejected_ratio,

        "missing_customer_rows": int(
            raw_metrics[
                "missing_customer_rows"
            ] or 0
        ),

        "invalid_quantity_rows": int(
            raw_metrics[
                "invalid_quantity_rows"
            ] or 0
        ),

        "total_revenue": float(
            clean_metrics[
                "total_revenue"
            ] or 0.0
        ),
    }