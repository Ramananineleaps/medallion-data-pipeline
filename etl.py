import os
import argparse
import hashlib
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------
# Config
# ----------------------------
DB_URI = "postgresql+psycopg2://postgres:nineleaps@localhost:5432/capstone_db"
engine = create_engine(DB_URI)

BRONZE_DIR = "Bronze Layer"
BRONZE_FILES = {
    "customers": "Uber rides - customers.csv",
    "drivers":   "Uber rides - drivers.csv",
    "trips":     "Uber rides - trips.csv",
    "payments":  "Uber rides - payments.csv",
}

VALID_VEHICLE_TYPES = ["Sedan", "SUV", "Hatchback", "Mini"]

# ----------------------------
# Helpers
# ----------------------------
def ensure_schemas():
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit;"))

def md5_checksum(path: str) -> str:
    with open(path, "rb") as f:
        h = hashlib.md5()
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def log_rows_and_checksum(df: pd.DataFrame, layer: str, table: str):
    os.makedirs(f"log/{layer}", exist_ok=True)
    out_path = f"log/{layer}/{table}.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ {layer}.{table}: rows={len(df)}  checksum={md5_checksum(out_path)}")

def log_dq(table: str, missing: int, invalid: int):
    os.makedirs("log/silver", exist_ok=True)
    row = {
        "table_name": table,
        "missing_values": int(missing),
        "invalid_values": int(invalid),
        "processed_at": datetime.now()
    }
    df = pd.DataFrame([row])
    df.to_csv(f"log/silver/{table}_dq.csv", index=False)

    # Also persist to DB
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit.rejected_rows (
                table_name TEXT,
                missing_values BIGINT,
                invalid_values BIGINT,
                processed_at TIMESTAMP
            );
        """))
    df.to_sql("rejected_rows", engine, schema="audit", if_exists="append", index=False)

def log_reconciliation(results: dict):
    os.makedirs("log/gold", exist_ok=True)
    df = pd.DataFrame([{**results, "checked_at": datetime.now()}])
    # Append to CSV (create if missing)
    out_csv = "log/gold/reconciliation.csv"
    if os.path.exists(out_csv):
        existing = pd.read_csv(out_csv)
        df = pd.concat([existing, df], ignore_index=True)
    df.to_csv(out_csv, index=False)

    # Also persist to DB
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit.reconciliation_log (
                silver_trips_sum NUMERIC,
                silver_payments_sum NUMERIC,
                gold_fact_sum NUMERIC,
                trips_count_silver BIGINT,
                payments_count_silver BIGINT,
                fact_trips_count_gold BIGINT,
                fare_diff_silver NUMERIC,
                silver_vs_gold_sum_diff NUMERIC,
                trips_count_diff BIGINT,
                checked_at TIMESTAMP
            );
        """))
    df.to_sql("reconciliation_log", engine, schema="audit", if_exists="append", index=False)

# ----------------------------
# Bronze
# ----------------------------
def load_bronze():
    ensure_schemas()
    for table, filename in BRONZE_FILES.items():
        path = os.path.join(BRONZE_DIR, filename)
        if not os.path.exists(path):
            print(f"⚠️ Missing CSV for {table}: {path} — skipping.")
            continue
        df = pd.read_csv(path)
        df.to_sql(table, engine, schema="bronze", if_exists="replace", index=False)
        log_rows_and_checksum(df, "bronze", table)
    print("✅ Bronze load complete.")

# ----------------------------
# Silver
# ----------------------------
def build_silver():
    ensure_schemas()

    # Read bronze
    df_customers = pd.read_sql("SELECT * FROM bronze.customers", engine)
    df_drivers   = pd.read_sql("SELECT * FROM bronze.drivers", engine)
    df_trips     = pd.read_sql("SELECT * FROM bronze.trips", engine)
    df_payments  = pd.read_sql("SELECT * FROM bronze.payments", engine)

    # Dedupe by PKs
    df_customers = df_customers.drop_duplicates(subset="customer_id")
    df_drivers   = df_drivers.drop_duplicates(subset="driver_id")
    df_trips     = df_trips.drop_duplicates(subset="trip_id")
    df_payments  = df_payments.drop_duplicates(subset="payment_id")

    # Types
    df_customers["signup_date"] = pd.to_datetime(df_customers["signup_date"], errors="coerce")
    df_trips["trip_fare"] = pd.to_numeric(df_trips["trip_fare"], errors="coerce")
    df_payments["trip_fare"] = pd.to_numeric(df_payments["trip_fare"], errors="coerce")

    # Enum normalization
    if "vehicle_type" in df_drivers.columns:
        df_drivers = df_drivers[df_drivers["vehicle_type"].isin(VALID_VEHICLE_TYPES)]

    # FK checks
    df_trips = df_trips[df_trips["customer_id"].isin(df_customers["customer_id"])]
    df_trips = df_trips[df_trips["driver_id"].isin(df_drivers["driver_id"])]
    df_payments = df_payments[df_payments["trip_id"].isin(df_trips["trip_id"])]

    # DQ checks
    missing_customers = df_customers.isna().sum().sum()
    missing_drivers   = df_drivers.isna().sum().sum()
    invalid_trip_fare = df_trips["trip_fare"].isna().sum()
    invalid_pay_fare  = df_payments["trip_fare"].isna().sum()

    print(f"⚠️ Customers missing: {missing_customers}")
    print(f"⚠️ Drivers missing:   {missing_drivers}")
    print(f"⚠️ Trips invalid fare: {invalid_trip_fare}")
    print(f"⚠️ Pays invalid fare:  {invalid_pay_fare}")

    log_dq("customers", missing_customers, 0)
    log_dq("drivers",   missing_drivers,   0)
    log_dq("trips",     0,                 invalid_trip_fare)
    log_dq("payments",  0,                 invalid_pay_fare)

    # Save Silver
    df_customers.to_sql("customers", engine, schema="silver", if_exists="replace", index=False)
    df_drivers.to_sql("drivers", engine, schema="silver", if_exists="replace", index=False)
    df_trips.to_sql("trips", engine, schema="silver", if_exists="replace", index=False)
    df_payments.to_sql("payments", engine, schema="silver", if_exists="replace", index=False)

    # Log silver snapshots + checksums
    log_rows_and_checksum(df_customers, "silver", "customers")
    log_rows_and_checksum(df_drivers,   "silver", "drivers")
    log_rows_and_checksum(df_trips,     "silver", "trips")
    log_rows_and_checksum(df_payments,  "silver", "payments")

    print("✅ Silver build complete.")

# ----------------------------
# Gold
# ----------------------------
def build_gold():
    ensure_schemas()
    with engine.begin() as conn:
        # driver_performance
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.driver_performance (
                driver_id TEXT,
                driver_name TEXT,
                vehicle_type TEXT,
                trips_count BIGINT,
                total_fare NUMERIC,
                avg_fare NUMERIC
            );
            TRUNCATE TABLE gold.driver_performance;
            INSERT INTO gold.driver_performance
            SELECT
                d.driver_id,
                d.driver_name,
                d.vehicle_type,
                COUNT(*) AS trips_count,
                SUM(t.trip_fare) AS total_fare,
                AVG(t.trip_fare) AS avg_fare
            FROM silver.trips t
            JOIN silver.drivers d ON d.driver_id = t.driver_id
            GROUP BY d.driver_id, d.driver_name, d.vehicle_type;
        """))

        # route_performance
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.route_performance (
                pickup_location TEXT,
                drop_location TEXT,
                trips_count BIGINT,
                total_fare NUMERIC,
                avg_fare NUMERIC
            );
            TRUNCATE TABLE gold.route_performance;
            INSERT INTO gold.route_performance
            SELECT
                t.pickup_location,
                t.drop_location,
                COUNT(*) AS trips_count,
                SUM(t.trip_fare) AS total_fare,
                AVG(t.trip_fare) AS avg_fare
            FROM silver.trips t
            GROUP BY t.pickup_location, t.drop_location;
        """))

        # fact_trips_dashboard (wide table)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS gold.fact_trips_dashboard AS
            SELECT
                t.trip_id,
                t.customer_id,
                c.customer_name,
                c.signup_date,
                DATE_TRUNC('month', c.signup_date)::date AS signup_month,
                t.driver_id,
                d.driver_name,
                d.vehicle_type,
                t.pickup_location,
                t.drop_location,
                t.trip_fare AS trip_fare,
                p.trip_fare AS paid_fare,
                COALESCE(p.mode_of_payment, 'Unknown') AS mode_of_payment,
                (p.trip_fare = t.trip_fare) AS fare_matches
            FROM silver.trips t
            LEFT JOIN silver.customers c ON c.customer_id = t.customer_id
            LEFT JOIN silver.drivers d   ON d.driver_id   = t.driver_id
            LEFT JOIN silver.payments p  ON p.trip_id     = t.trip_id
            WHERE 1=0; -- create empty table with schema
        """))
        conn.execute(text("TRUNCATE TABLE gold.fact_trips_dashboard;"))
        conn.execute(text("""
            INSERT INTO gold.fact_trips_dashboard
            SELECT
                t.trip_id,
                t.customer_id,
                c.customer_name,
                c.signup_date,
                DATE_TRUNC('month', c.signup_date)::date AS signup_month,
                t.driver_id,
                d.driver_name,
                d.vehicle_type,
                t.pickup_location,
                t.drop_location,
                t.trip_fare AS trip_fare,
                p.trip_fare AS paid_fare,
                COALESCE(p.mode_of_payment, 'Unknown') AS mode_of_payment,
                (p.trip_fare = t.trip_fare) AS fare_matches
            FROM silver.trips t
            LEFT JOIN silver.customers c ON c.customer_id = t.customer_id
            LEFT JOIN silver.drivers d   ON d.driver_id   = t.driver_id
            LEFT JOIN silver.payments p  ON p.trip_id     = t.trip_id;
        """))

    print("✅ Gold layer built: driver_performance, route_performance, fact_trips_dashboard")

# ----------------------------
# Reconciliation
# ----------------------------
def reconcile():
    # Pull aggregates
    silver_trips_sum = pd.read_sql("SELECT COALESCE(SUM(trip_fare),0) AS s FROM silver.trips", engine)["s"][0]
    silver_pay_sum   = pd.read_sql("SELECT COALESCE(SUM(trip_fare),0) AS s FROM silver.payments", engine)["s"][0]
    gold_fact_sum    = pd.read_sql("SELECT COALESCE(SUM(trip_fare),0) AS s FROM gold.fact_trips_dashboard", engine)["s"][0]

    trips_count_s    = pd.read_sql("SELECT COUNT(*) AS c FROM silver.trips", engine)["c"][0]
    pays_count_s     = pd.read_sql("SELECT COUNT(*) AS c FROM silver.payments", engine)["c"][0]
    fact_count_g     = pd.read_sql("SELECT COUNT(*) AS c FROM gold.fact_trips_dashboard", engine)["c"][0]

    results = {
        "silver_trips_sum": float(silver_trips_sum),
        "silver_payments_sum": float(silver_pay_sum),
        "gold_fact_sum": float(gold_fact_sum),
        "trips_count_silver": int(trips_count_s),
        "payments_count_silver": int(pays_count_s),
        "fact_trips_count_gold": int(fact_count_g),
        "fare_diff_silver": float(silver_trips_sum - silver_pay_sum),
        "silver_vs_gold_sum_diff": float(silver_trips_sum - gold_fact_sum),
        "trips_count_diff": int(trips_count_s - fact_count_g),
    }
    print("🔎 Reconciliation:", results)
    log_reconciliation(results)
    print("✅ Reconciliation logged (CSV + DB).")

# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medallion ETL")
    parser.add_argument(
        "step",
        choices=["extract", "load_bronze", "build_silver", "build_gold", "all"],
        nargs="?",
        default="all",
        help="Which step to run (default: all)"
    )
    args = parser.parse_args()

    if args.step in ["extract", "load_bronze", "all"]:
        load_bronze()
    if args.step in ["build_silver", "all"]:
        build_silver()
    if args.step in ["build_gold", "all"]:
        build_gold()
        reconcile()
