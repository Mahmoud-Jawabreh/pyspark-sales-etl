from deltalake import DeltaTable


PATH = "output/delta/dim_customer_scd2"


table = DeltaTable(PATH)

df = table.to_pandas()


print("\n=== COMPLETE CUSTOMER HISTORY ===")

print(
    df.sort_values(
        [
            "customer_id",
            "valid_from"
        ]
    ).to_string(
        index=False
    )
)


print("\n=== CURRENT CUSTOMERS ONLY ===")

current = df[
    df["is_current"] == True
]

print(
    current.sort_values(
        "customer_id"
    ).to_string(
        index=False
    )
)


print("\n=== CUSTOMER 1001 HISTORY ===")

history = df[
    df["customer_id"] == 1001
]

print(
    history.sort_values(
        "valid_from"
    ).to_string(
        index=False
    )
)


print(
    "\nDelta version:",
    table.version()
)