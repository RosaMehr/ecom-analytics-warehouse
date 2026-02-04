import os
import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5433/analytics"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

FILES = [
    ("olist_geolocation_dataset.csv", "geolocation"),
    ("olist_customers_dataset.csv", "customers"),
    ("olist_sellers_dataset.csv", "sellers"),
    ("olist_products_dataset.csv", "products"),
    ("product_category_name_translation.csv", "product_category_translation"),
    ("olist_orders_dataset.csv", "orders"),
    ("olist_order_items_dataset.csv", "order_items"),
    ("olist_order_payments_dataset.csv", "payments"),
    ("olist_order_reviews_dataset.csv", "reviews"),
]

def load_csv(engine, csv_path, table_name):
    df = pd.read_csv(csv_path)

    # Normalize column names: lower-case, safe for SQL
    df.columns = [c.strip().lower() for c in df.columns]

    df.to_sql(
        table_name,
        engine,
        schema="raw",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000
    )
    print(f"Loaded raw.{table_name}: {len(df):,} rows from {os.path.basename(csv_path)}")

def main():
    missing = []
    for filename, _ in FILES:
        p = os.path.join(DATA_DIR, filename)
        if not os.path.exists(p):
            missing.append(p)

    if missing:
        print("ERROR: Missing required files:")
        for m in missing:
            print(" -", m)
        raise SystemExit(1)

    engine = create_engine(DB_URL)

    for filename, table in FILES:
        path = os.path.join(DATA_DIR, filename)
        load_csv(engine, path, table)

if __name__ == "__main__":
    main()
