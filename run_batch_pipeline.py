import sys
from pathlib import Path


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parent
)

SRC_DIR = (
    PROJECT_ROOT
    / "src"
)

sys.path.insert(
    0,
    str(SRC_DIR),
)


from config.settings import (
    load_config,
)

from config.spark import (
    build_spark_session,
)

from pipelines.sales_batch import (
    run_sales_batch,
)

from utils.logger import (
    configure_logging,
    get_logger,
)


def main() -> None:

    configure_logging()

    logger = get_logger(
        "run_batch_pipeline"
    )

    spark = None

    try:

        logger.info(
            "Loading configuration"
        )

        config = load_config()


        logger.info(
            "Creating SparkSession"
        )

        spark = (
            build_spark_session(
                config,
                app_name=(
                    "SalesBatchPipeline"
                ),
            )
        )


        run_sales_batch(
            spark,
            config,
        )


    except Exception:

        logger.exception(
            "Sales batch pipeline failed"
        )

        raise


    finally:

        if spark is not None:

            logger.info(
                "Stopping SparkSession"
            )

            spark.stop()


if __name__ == "__main__":
    main()