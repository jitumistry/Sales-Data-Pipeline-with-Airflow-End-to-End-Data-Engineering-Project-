import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from utils.db import get_source_engine, get_warehouse_engine


SOURCE_DB = "postgresql://admin:admin@source_postgres:5432/sales_source"
WH_DB = "postgresql://admin:admin@warehouse_postgres:5432/sales_wh"


def load_customers_scd2():
    source_engine = get_source_engine()
    wh_engine = get_warehouse_engine()

    source_df = pd.read_sql("SELECT * FROM customers", source_engine)
    dim_df = pd.read_sql("SELECT * FROM dim_customers", wh_engine)

    now = datetime.now()

    # correct data types
    source_df["customer_id"] = source_df["customer_id"].astype(int)

    if not dim_df.empty:
        dim_df["customer_id"] = dim_df["customer_id"].astype(int)


    # first load
    if dim_df.empty:
        source_df["start_date"] = now
        source_df["end_date"] = None
        source_df["is_current"] = 'Y'

        source_df = source_df[
            ["customer_id", "name", "email", "city", "start_date", "end_date", "is_current"]
        ]

        source_df.to_sql("dim_customers", wh_engine, if_exists="append", index=False)
        print("Initial load completed for dim_customers")
        return

    # incremental load
    for _, row in source_df.iterrows():

        existing = dim_df[
            (dim_df["customer_id"] == row["customer_id"]) &
            (dim_df["is_current"] == 'Y')
        ]

        if existing.empty:
            # Insert new
            new_row = {
                "customer_id": row["customer_id"],
                "name": row["name"],
                "email": row["email"],
                "city": row["city"],
                "start_date": now,
                "end_date": None,
                "is_current": 'Y'
            }

            pd.DataFrame([new_row]).to_sql(
                "dim_customers", wh_engine, if_exists="append", index=False
            )

        else:
            existing_row = existing.iloc[0]

            if (
                existing_row["name"] != row["name"] or
                existing_row["email"] != row["email"] or
                existing_row["city"] != row["city"]
            ):
                # Expire old
                with wh_engine.begin() as conn:
                    conn.execute(text(f"""
                        UPDATE dim_customers
                        SET end_date = :now, is_current = 'N'
                        WHERE surrogate_key = :key
                    """), {"now": now, "key": int(existing_row["surrogate_key"])})

                # Insert new
                new_row = {
                    "customer_id": row["customer_id"],
                    "name": row["name"],
                    "email": row["email"],
                    "city": row["city"],
                    "start_date": now,
                    "end_date": None,
                    "is_current": 'Y'
                }

                pd.DataFrame([new_row]).to_sql(
                    "dim_customers", wh_engine, if_exists="append", index=False
                )


def load_fact_orders():
    source_engine = get_source_engine()
    wh_engine = get_warehouse_engine()

    # 1️⃣ Get last run timestamp
    last_run_df = pd.read_sql(
        "SELECT last_run FROM etl_metadata WHERE table_name = 'fact_orders'",
        wh_engine
    )

    if last_run_df.empty:
        last_run = "1900-01-01"
    else:
        last_run = last_run_df.iloc[0]["last_run"]

    # 2️⃣ Incremental extract
    query = f"""
        SELECT * FROM orders
        WHERE updated_at > '{last_run}'
    """

    df = pd.read_sql(query, source_engine)

    if df.empty:
        print("No new records to load")
        return

    # 3️⃣ Keep only needed columns
    df = df[
        ["order_id", "customer_id", "product_id", "quantity", "total_amount", "order_date"]
    ]

    # 4️⃣ Load
    df.to_sql("fact_orders", wh_engine, if_exists="append", index=False)

    # 5️⃣ Update metadata
    new_last_run = pd.Timestamp.now()

    with wh_engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_metadata
            SET last_run = :ts
            WHERE table_name = 'fact_orders'
        """), {"ts": new_last_run})

    print(f"Loaded {len(df)} records into fact_orders")
