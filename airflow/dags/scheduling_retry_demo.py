from datetime import timedelta

import pendulum

from airflow.sdk import (
    dag,
    task,
    get_current_context,
)


@dag(
    dag_id="scheduling_retry_demo",

    # Run every 5 minutes
    schedule="*/5 * * * *",

    start_date=pendulum.datetime(
        2026,
        10,
        24,
        tz="Asia/Riyadh",
    ),

    # Do not automatically create old missed runs
    catchup=False,

    # Only one DAG run at a time
    max_active_runs=1,

    tags=[
        "learning",
        "scheduling",
        "retries",
    ],
)
def scheduling_retry_demo():

    @task
    def extract():

        print(
            "Extracting sales data..."
        )


    @task(
        retries=2,
        retry_delay=timedelta(
            seconds=10
        ),
    )
    def transform():

        context = (
            get_current_context()
        )

        task_instance = (
            context["ti"]
        )

        try_number = (
            task_instance.try_number
        )

        print(
            f"Transform attempt: "
            f"{try_number}"
        )

        # Deliberately fail only
        # the first attempt
        if try_number == 1:

            raise RuntimeError(
                "Simulated transient failure"
            )

        print(
            "Transform succeeded."
        )


    @task
    def load():

        print(
            "Loading sales data..."
        )


    (
        extract()
        >>
        transform()
        >>
        load()
    )


scheduling_retry_demo()