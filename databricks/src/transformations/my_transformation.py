from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = "workspace"

SOURCE_PATH = (
    spark.conf.get(
    "sales.source_path"
    )
)

SOURCE_SCHEMA = """
    sale_id BIGINT,
    product STRING,
    quantity BIGINT,
    price DECIMAL(12, 2),
    _rescued_data STRING
"""

QUALITY_RULES = {
    "sale_id_present": "sale_id IS NOT NULL",
    "product_present": (
        "product IS NOT NULL "
        "AND length(trim(product)) > 0"
    ),
    "positive_quantity": (
        "quantity IS NOT NULL AND quantity > 0"
    ),
    "nonnegative_price": (
        "price IS NOT NULL AND price >= 0"
    ),
    "schema_matches_contract": "_rescued_data IS NULL",
}

VALID_RECORD = " AND ".join(
    f"({condition})"
    for condition in QUALITY_RULES.values()
)

@dp.table(
    name="lf_bronze_sales",
    comment="Source sales records with ingestion metadata.",
)
@dp.expect_all(QUALITY_RULES)
def bronze_sales():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SOURCE_SCHEMA)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file"),
        )
        .withColumn(
            "ingested_at",
            F.current_timestamp(),
        )
    )

@dp.table(
    name="lf_silver_sales",
    comment="Validated sales with calculated total amounts.",
)
@dp.expect_all_or_drop(QUALITY_RULES)
def silver_sales():
    return (
        spark.readStream
        .table("lf_bronze_sales")
        .withColumn(
            "product",
            F.trim(F.col("product")),
        )
        .withColumn(
            "total_amount",
            F.col("quantity") * F.col("price"),
        )
    )

@dp.table(
    name="lf_quarantine_sales",
    comment="Sales that fail schema or business validation.",
)
def quarantine_sales():
    is_valid = F.coalesce(
        F.expr(VALID_RECORD),
        F.lit(False),
    )

    return (
        spark.readStream
        .table("lf_bronze_sales")
        .filter(~is_valid)
    )

@dp.materialized_view(
    name="lf_gold_product_sales",
    comment="Product revenue from accepted sales records.",
)
def gold_product_sales():
    return (
        spark.read
        .table("lf_silver_sales")
        .groupBy("product")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("revenue"),
        )
    )