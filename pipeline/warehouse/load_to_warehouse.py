import pandas as pd
import os
from sqlalchemy import create_engine

# Read entire partitioned folder
local_parquet_path = "warehouse_export"

df = pd.read_parquet(local_parquet_path)

print("Loaded rows:", len(df))

# Example Postgres connection
engine = create_engine(
    "postgresql://admin:admin@localhost:5432/cost_analytics"
)

df.to_sql("daily_account_cost", engine, if_exists="replace", index=False)

print("Loaded into Postgres successfully.")

