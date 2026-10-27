from airflow.sdk import dag, task

import pendulum


@dag(
    dag_id="hello_airflow",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=[
        "learning",
        "data-engineering",
    ],
)
def hello_airflow():

    @task
    def extract():

        print(
            "Extracting sales data..."
        )


    @task
    def transform():

        print(
            "Transforming sales data..."
        )


    @task
    def load():

        print(
            "Loading sales data..."
        )


    extract_task = extract()

    transform_task = transform()

    load_task = load()


    (
        extract_task
        >>
        transform_task
        >>
        load_task
    )


hello_airflow()