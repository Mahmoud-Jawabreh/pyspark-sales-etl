from deltalake import DeltaTable


PATH = "output/delta/silver_sales_cdc_safe"


table = DeltaTable(PATH)

df = table.to_pandas()


print("\n=== ALL CDC STATE INCLUDING TOMBSTONES ===")

print(
    df.sort_values("sale_id")
    .to_string(index=False)
)


print("\n=== ACTIVE CURRENT STATE ===")

active_df = df[
    df["is_deleted"] == False
]

print(
    active_df
    .sort_values("sale_id")
    .to_string(index=False)
)


print("\n=== TOMBSTONES ===")

deleted_df = df[
    df["is_deleted"] == True
]

print(
    deleted_df[
        [
            "sale_id",
            "is_deleted",
            "last_operation",
            "last_event_id",
            "last_sequence_number",
        ]
    ]
    .sort_values("sale_id")
    .to_string(index=False)
)


print(
    "\nDelta version:",
    table.version()
)