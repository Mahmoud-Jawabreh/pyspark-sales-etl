from pyspark.sql import DataFrame


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
) -> None:
    """
    Write a DataFrame as Parquet.
    """

    (
        df.write
        .mode(mode)
        .parquet(path)
    )