from deltalake import DeltaTable


SILVER_PATH = "output/delta/silver_sales_current"


# Read the current Delta snapshot
delta_table = DeltaTable(SILVER_PATH)

df = delta_table.to_pandas()


print("\n=== CURRENT-STATE SILVER ===")

if df.empty:
    print("Silver table is empty.")
else:
    df = df.sort_values("sale_id")

    columns = [
        "sale_id",
        "customer_id",
        "product",
        "quantity",
        "price",
        "total_amount",
        "last_event_id",
        "last_sequence_number",
        "last_event_time",
        "source_partition",
        "source_offset",
        "updated_at",
    ]

    print(
        df[columns]
        .to_string(index=False)
    )


print("\n=== ROW COUNT ===")
print(len(df))


print("\n=== DELTA VERSION ===")
print(delta_table.version())