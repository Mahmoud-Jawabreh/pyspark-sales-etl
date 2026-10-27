def print_stream_health(
    query,
) -> None:

    progress = (
        query.lastProgress
    )

    if not progress:

        print(
            "No completed "
            "micro-batch yet."
        )

        return


    print(
        "\n--- STREAM HEALTH ---"
    )

    print(
        "Batch ID:",
        progress.get(
            "batchId"
        ),
    )

    print(
        "Input rows:",
        progress.get(
            "numInputRows"
        ),
    )

    print(
        "Input rows/sec:",
        round(
            progress.get(
                "inputRowsPerSecond",
                0.0,
            ),
            2,
        ),
    )

    print(
        "Processed rows/sec:",
        round(
            progress.get(
                "processedRowsPerSecond",
                0.0,
            ),
            2,
        ),
    )


    durations = (
        progress.get(
            "durationMs",
            {}
        )
    )

    print(
        "Trigger duration ms:",
        durations.get(
            "triggerExecution"
        ),
    )


    sources = (
        progress.get(
            "sources",
            []
        )
    )


    for source in sources:

        print(
            "\nSource:",
            source.get(
                "description"
            ),
        )

        print(
            "Start offset:",
            source.get(
                "startOffset"
            ),
        )

        print(
            "End offset:",
            source.get(
                "endOffset"
            ),
        )

        print(
            "Latest offset:",
            source.get(
                "latestOffset"
            ),
        )


        metrics = (
            source.get(
                "metrics",
                {}
            )
        )


        if metrics:

            print(
                "Min offsets behind:",
                metrics.get(
                    "minOffsetsBehindLatest"
                ),
            )

            print(
                "Avg offsets behind:",
                metrics.get(
                    "avgOffsetsBehindLatest"
                ),
            )

            print(
                "Max offsets behind:",
                metrics.get(
                    "maxOffsetsBehindLatest"
                ),
            )


    print(
        "---------------------\n"
    )