import pandas as pd
from sqlalchemy import create_engine

# 1️⃣ Database connection
DB_URI = "postgresql+psycopg2://postgres:nineleaps@localhost:5432/capstone_db"
engine = create_engine(DB_URI)

# 2️⃣ Read Bronze tables
df_customers = pd.read_sql("SELECT * FROM bronze.customers", engine)
df_drivers   = pd.read_sql("SELECT * FROM bronze.drivers", engine)
df_trips     = pd.read_sql("SELECT * FROM bronze.trips", engine)
df_payments  = pd.read_sql("SELECT * FROM bronze.payments", engine)

# 3️⃣ Remove duplicates
df_customers = df_customers.drop_duplicates(subset="customer_id")
df_drivers   = df_drivers.drop_duplicates(subset="driver_id")
df_trips     = df_trips.drop_duplicates(subset="trip_id")
df_payments  = df_payments.drop_duplicates(subset="payment_id")

# 4️⃣ Type enforcement
df_customers["signup_date"] = pd.to_datetime(df_customers["signup_date"], errors="coerce")
df_trips["trip_fare"] = pd.to_numeric(df_trips["trip_fare"], errors="coerce")
df_payments["trip_fare"] = pd.to_numeric(df_payments["trip_fare"], errors="coerce")

# 5️⃣ Enum normalization
# Example: vehicle_type standardization
valid_vehicle_types = ["Sedan", "SUV", "Hatchback", "Mini"]
df_drivers = df_drivers[df_drivers["vehicle_type"].isin(valid_vehicle_types)]

# 6️⃣ Foreign Key (FK) checks
df_trips = df_trips[df_trips["customer_id"].isin(df_customers["customer_id"])]
df_trips = df_trips[df_trips["driver_id"].isin(df_drivers["driver_id"])]

df_payments = df_payments[df_payments["trip_id"].isin(df_trips["trip_id"])]

# 7️⃣ Data Quality (DQ) checks
missing_customers = df_customers.isna().sum().sum()
missing_drivers   = df_drivers.isna().sum().sum()
invalid_trip_fare = df_trips[df_trips["trip_fare"].isna()].shape[0]
invalid_payment_fare = df_payments[df_payments["trip_fare"].isna()].shape[0]

print(f"⚠️ Customers with missing values: {missing_customers}")
print(f"⚠️ Drivers with missing values: {missing_drivers}")
print(f"⚠️ Trips with invalid fare: {invalid_trip_fare}")
print(f"⚠️ Payments with invalid fare: {invalid_payment_fare}")

# 8️⃣ Save to Silver schema
# Use 'replace' to refresh clean data
df_customers.to_sql("customers", engine, schema="silver", if_exists="replace", index=False)
df_drivers.to_sql("drivers", engine, schema="silver", if_exists="replace", index=False)
df_trips.to_sql("trips", engine, schema="silver", if_exists="replace", index=False)
df_payments.to_sql("payments", engine, schema="silver", if_exists="replace", index=False)

print("✅ Silver tables created and loaded successfully!")
