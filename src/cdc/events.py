from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    row_number,
)


def select_latest_cdc_events(
    cdc_df: DataFrame,
) -> DataFrame:
    """
    Keep only the newest CDC event for each sale_id.

    sequence_number is the main ordering field.
    source_offset is used as a tie-breaker.
    """

    window = (
        Window
        .partitionBy("sale_id")
        .orderBy(
            col("sequence_number").desc(),
            col("source_offset").desc(),
        )
    )

    return (
        cdc_df
        .withColumn(
            "_row_number",
            row_number().over(window),
        )
        .filter(
            col("_row_number") == 1
        )
        .drop("_row_number")
    )


def filter_newer_cdc_events(
    events_df: DataFrame,
    current_df: DataFrame,
) -> DataFrame:
    """
    Accept only CDC events that are newer than
    the current target state.

    Equal sequence number:
        duplicate -> ignore

    Lower sequence number:
        stale -> ignore

    Higher sequence number:
        accept

    Missing target:
        accept
    """

    current_sequences = (
        current_df
        .select(
            "sale_id",
            col("last_sequence_number")
            .alias("current_sequence"),
        )
    )

    return (
        events_df.alias("event")
        .join(
            current_sequences.alias("current"),
            on="sale_id",
            how="left",
        )
        .filter(
            col("current_sequence").isNull()
            |
            (
                col("event.sequence_number")
                >
                col("current_sequence")
            )
        )
        .drop("current_sequence")
    )