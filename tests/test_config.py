from config.settings import (
    load_config,
)


def test_dev_config_loads():

    config = load_config(
        "dev"
    )

    assert (
        config["spark"]["master"]
        ==
        "local[2]"
    )

    assert (
        config[
            "spark"
        ][
            "shuffle_partitions"
        ]
        ==
        8
    )

    assert (
        config[
            "kafka"
        ][
            "bootstrap_servers"
        ]
        ==
        "localhost:9092"
    )

    assert (
        "bronze_sales"
        in config["paths"]
    )