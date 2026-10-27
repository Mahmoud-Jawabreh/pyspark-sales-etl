from pyspark.testing.utils import (
    assertDataFrameEqual,
)

from cdc.events import (
    select_latest_cdc_events,
    filter_newer_cdc_events,
)


def test_latest_cdc_event_per_sale_is_selected(
    spark,
):

    input_df = spark.createDataFrame(
        [
            (301, "I", 1, 10),
            (301, "U", 2, 11),
            (302, "I", 1, 12),
        ],
        [
            "sale_id",
            "operation",
            "sequence_number",
            "source_offset",
        ],
    )

    actual_df = (
        select_latest_cdc_events(
            input_df
        )
    )

    expected_df = spark.createDataFrame(
        [
            (301, "U", 2, 11),
            (302, "I", 1, 12),
        ],
        [
            "sale_id",
            "operation",
            "sequence_number",
            "source_offset",
        ],
    )

    assertDataFrameEqual(
        actual_df,
        expected_df,
        checkRowOrder=False,
    )


def test_duplicate_and_stale_cdc_events_are_ignored(
    spark,
):

    current_df = spark.createDataFrame(
        [
            (301, 5),
        ],
        [
            "sale_id",
            "last_sequence_number",
        ],
    )

    events_df = spark.createDataFrame(
        [
            (301, "U", 4, 20),  # stale
            (301, "U", 5, 21),  # duplicate
            (301, "U", 6, 22),  # newer
        ],
        [
            "sale_id",
            "operation",
            "sequence_number",
            "source_offset",
        ],
    )

    actual_df = (
        filter_newer_cdc_events(
            events_df,
            current_df,
        )
    )

    expected_df = spark.createDataFrame(
        [
            (301, "U", 6, 22),
        ],
        [
            "sale_id",
            "operation",
            "sequence_number",
            "source_offset",
        ],
    )

    assertDataFrameEqual(
        actual_df,
        expected_df,
        checkRowOrder=False,
    )


def test_new_entity_cdc_event_is_accepted(
    spark,
):

    current_df = spark.createDataFrame(
        [
            (301, 5),
        ],
        [
            "sale_id",
            "last_sequence_number",
        ],
    )

    events_df = spark.createDataFrame(
        [
            (999, "I", 1, 30),
        ],
        [
            "sale_id",
            "operation",
            "sequence_number",
            "source_offset",
        ],
    )

    actual_df = (
        filter_newer_cdc_events(
            events_df,
            current_df,
        )
    )

    assert actual_df.count() == 1


def test_delete_event_is_not_lost(
    spark,
):

    current_df = spark.createDataFrame(
        [
            (401, 2),
        ],
        [
            "sale_id",
            "last_sequence_number",
        ],
    )

    events_df = spark.createDataFrame(
        [
            (401, "D", 3, 40),
        ],
        [
            "sale_id",
            "operation",
            "sequence_number",
            "source_offset",
        ],
    )

    actual_df = (
        filter_newer_cdc_events(
            events_df,
            current_df,
        )
    )

    row = actual_df.collect()[0]

    assert row.operation == "D"
    assert row.sequence_number == 3