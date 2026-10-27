import time

from datetime import (
    datetime,
    timezone,
)

from pathlib import Path

from pyspark.sql import SparkSession

from ingestion.sales import (
    read_sales_csv,
)

from transformations.sales import (
    clean_sales,
)

from quality.sales import (
    calculate_sales_metrics,
)

from utils.storage import (
    write_parquet,
)

from utils.logger import (
    get_logger,
)


logger = get_logger(
    __name__
)


def run_sales_batch(
    spark: SparkSession,
    config: dict,
) -> None:

    pipeline_name = (
        "sales_batch"
    )

    input_path = (
        config["paths"][
            "sales_input"
        ]
    )

    output_path = (
        config["paths"][
            "batch_sales_output"
        ]
    )

    started_at = (
        time.perf_counter()
    )


    logger.info(
        "Sales batch pipeline started",
        extra={
            "event": "pipeline_started",
            "pipeline": pipeline_name,
            "input_path": input_path,
        },
    )


    #
    # Source freshness
    #

    source_file = Path(
        input_path
    )

    source_modified_at = (
        datetime.fromtimestamp(
            source_file
            .stat()
            .st_mtime,
            tz=timezone.utc,
        )
    )

    current_time = (
        datetime.now(
            timezone.utc
        )
    )

    source_age_seconds = (
        current_time
        - source_modified_at
    ).total_seconds()


    logger.info(
        "Source freshness measured",
        extra={
            "event": "metric",
            "pipeline": pipeline_name,
            "metric_name": (
                "source_age_seconds"
            ),
            "metric_value": (
                source_age_seconds
            ),
            "source_age_seconds": (
                source_age_seconds
            ),
        },
    )


    #
    # Read
    #

    logger.info(
        "Reading sales data",
        extra={
            "event": "read_started",
            "pipeline": pipeline_name,
            "input_path": input_path,
        },
    )

    raw_sales_df = (
        read_sales_csv(
            spark,
            input_path,
        )
    )


    #
    # Transform
    #

    logger.info(
        "Applying sales transformations",
        extra={
            "event": (
                "transformation_started"
            ),
            "pipeline": pipeline_name,
        },
    )

    clean_sales_df = (
        clean_sales(
            raw_sales_df
        )
    )

    clean_sales_df.cache()


    try:

        #
        # Metrics
        #

        metrics = (
            calculate_sales_metrics(
                raw_sales_df,
                clean_sales_df,
            )
        )


        for (
            metric_name,
            metric_value,
        ) in metrics.items():

            logger.info(
                "Pipeline metric recorded",
                extra={
                    "event": "metric",
                    "pipeline": (
                        pipeline_name
                    ),
                    "metric_name": (
                        metric_name
                    ),
                    "metric_value": (
                        metric_value
                    ),
                },
            )


        #
        # Data quality threshold
        #

        max_rejected_ratio = (
            config["quality"][
                "max_rejected_ratio"
            ]
        )


        if (
            metrics[
                "rejected_ratio"
            ]
            >
            max_rejected_ratio
        ):

            raise ValueError(
                "Data quality check failed: "
                f"rejected_ratio="
                f"{metrics['rejected_ratio']:.2%} "
                f"exceeds maximum="
                f"{max_rejected_ratio:.2%}"
            )


        #
        # Write
        #

        logger.info(
            "Writing clean sales",
            extra={
                "event": "write_started",
                "pipeline": (
                    pipeline_name
                ),
                "output_path": (
                    output_path
                ),
            },
        )


        write_parquet(
            clean_sales_df,
            output_path,
            mode="overwrite",
        )


    finally:

        clean_sales_df.unpersist()


    #
    # Duration
    #

    duration_seconds = (
        time.perf_counter()
        - started_at
    )


    logger.info(
        "Sales batch pipeline completed",
        extra={
            "event": (
                "pipeline_completed"
            ),
            "pipeline": (
                pipeline_name
            ),
            "duration_seconds": (
                duration_seconds
            ),
            "output_path": (
                output_path
            ),
        },
    )