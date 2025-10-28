import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

def append_to_sqlite(cleaned_file_path: str, db_path: str = "../ShippingCompanies_history.db", table_name: str = "ShippingCompanies_data"):
    """Read cleaned Excel file and append it to SQLite database with a timestamp,
       skipping any duplicate AWB values already stored."""
    cleaned_path = Path(cleaned_file_path)
    if not cleaned_path.exists():
        print(f"Cleaned file not found: {cleaned_file_path}")
        return

    df_new = pd.read_excel(cleaned_path, engine="openpyxl")
    if df_new.empty:
        print("Cleaned file is empty, skipping SQL save.")
        return

    df_new["run_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    db_path = Path(__file__).resolve().parent.parent / "ShippingCompanies_history.db"
    conn = sqlite3.connect(db_path)

    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            AWB TEXT,
            OrderID TEXT,
            Pickup_Date TEXT,
            Status TEXT,
            Status_Date TEXT,
            Shipping_Company TEXT,
            Description TEXT,
            Payment_Type TEXT,
            COD_Value REAL,
            Number_Of_attempts REAL,
            City TEXT,
            run_date TEXT
        )
    """)
    conn.commit()

    existing_awbs = pd.read_sql_query(f"SELECT DISTINCT AWB FROM {table_name}", conn)
    existing_awbs_set = set(existing_awbs["AWB"].dropna().astype(str).tolist())

    df_new["AWB"] = df_new["AWB"].astype(str)
    df_to_add = df_new[~df_new["AWB"].isin(existing_awbs_set)].copy()

    if df_to_add.empty:
        print("No new AWB records found. Database is already up to date.")
    else:
        df_to_add.to_sql(table_name, conn, if_exists="append", index=False)
        print(f"Added {len(df_to_add)} new rows (unique AWBs) to {db_path}:{table_name}")

    conn.close()
