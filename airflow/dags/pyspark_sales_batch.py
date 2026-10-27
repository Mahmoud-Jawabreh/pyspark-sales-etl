from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task

from airflow.providers.standard.sensors.filesystem import (
    FileSensor,
)


INPUT_PATH = Path(
    "/opt/project/data/sales_batch_input.csv"
)

OUTPUT_PATH = Path(
    "/opt/spark-output/batch/sales_clean"
)


@dag(
    dag_id="pyspark_sales_batch",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        10,
        24,
        tz="Asia/Riyadh",
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "pyspark",
        "sales",
        "data-engineering",
    ],
)
def pyspark_sales_batch():

    wait_for_sales_file = FileSensor(
        task_id="wait_for_sales_file",

        fs_conn_id="sales_input_fs",

        filepath="sales_batch_input.csv",

        poke_interval=10,

        timeout=300,

        deferrable=True,
    )


    @task
    def validate_input():

        file_size = (
            INPUT_PATH.stat().st_size
        )

        if file_size == 0:
            raise ValueError(
                "Sales input file is empty."
            )

        print(
            "Input validation passed."
        )

        print(
            f"Input size: "
            f"{file_size} bytes"
        )

        return {
            "path": str(INPUT_PATH),
            "size_bytes": file_size,
        }


    @task.bash(
        retries=1,
        retry_delay=timedelta(
            seconds=15
        ),
        execution_timeout=timedelta(
        minutes=5
        ),
        cwd="/opt/project",
    )
    def run_pyspark_pipeline():

        return (
            "python "
            "run_batch_pipeline.py"
        )


    @task
    def verify_output():

        if not OUTPUT_PATH.exists():
            raise FileNotFoundError(
                f"Pipeline output not found: "
                f"{OUTPUT_PATH}"
            )

        parquet_files = list(
            OUTPUT_PATH.glob(
                "part-*.parquet"
            )
        )

        if not parquet_files:
            raise ValueError(
                "No Parquet files "
                "were created."
            )

        success_file = (
            OUTPUT_PATH
            / "_SUCCESS"
        )

        if not success_file.exists():
            raise ValueError(
                "_SUCCESS marker "
                "was not created."
            )

        print(
            "Output verification passed."
        )

        print(
            f"Parquet files created: "
            f"{len(parquet_files)}"
        )


    validation = validate_input()

    pipeline = (
        run_pyspark_pipeline()
    )

    output_check = (
        verify_output()
    )


    (
        wait_for_sales_file
        >>
        validation
        >>
        pipeline
        >>
        output_check
    )


pyspark_sales_batch()