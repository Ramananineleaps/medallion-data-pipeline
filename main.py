import pandas as pd
from sqlalchemy import create_engine
import hashlib

# ---------------------------
# PostgreSQL connection
# ---------------------------
DB_URI = "postgresql+psycopg2://postgres:nineleaps@localhost:5432/capstone_db"
engine = create_engine(DB_URI)

# ---------------------------
# Function to calculate checksum
# ---------------------------
def checksum(file_path):
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)
    return file_hash.hexdigest()

# ---------------------------
# Function to load CSV into PostgreSQL
# ---------------------------
def load_csv_to_postgres(file_path, table_name, schema="bronze", date_cols=None):
    df = pd.read_csv(file_path)
    
    # Convert date columns to YYYY-MM-DD
    if date_cols:
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    
    row_count = len(df)
    df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
    print(f"✅ Loaded {row_count} rows into {schema}.{table_name}")
    print(f"Checksum: {checksum(file_path)}\n")

# ---------------------------
# Main loader
# ---------------------------
if __name__ == "__main__":
    load_csv_to_postgres(
        "/home/nineleaps/Medallion Data Pipeline/Bronze Layer/Uber rides - customers.csv",
        "customers",
        date_cols=["signup_date"]
    )

    load_csv_to_postgres(
        "/home/nineleaps/Medallion Data Pipeline/Bronze Layer/Uber rides - drivers.csv",
        "drivers"
    )

    load_csv_to_postgres(
        "/home/nineleaps/Medallion Data Pipeline/Bronze Layer/Uber rides - trips.csv",
        "trips"
    )

    load_csv_to_postgres(
        "/home/nineleaps/Medallion Data Pipeline/Bronze Layer/Uber rides - payments.csv",
        "payments"
    )
