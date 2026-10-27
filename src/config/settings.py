import os
from pathlib import Path

import yaml


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)


def load_config(
    environment: str | None = None,
) -> dict:
    """
    Load application configuration.

    If environment is not provided,
    APP_ENV is checked.

    Default environment:
    dev
    """

    environment = (
        environment
        or os.getenv(
            "APP_ENV",
            "dev",
        )
    )

    config_path = (
        PROJECT_ROOT
        / "configs"
        / f"{environment}.yaml"
    )

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: "
            f"{config_path}"
        )

    with config_path.open(
        "r",
        encoding="utf-8",
    ) as file:

        config = yaml.safe_load(
            file
        )

    return config