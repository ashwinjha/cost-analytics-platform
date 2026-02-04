from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

spark = (
    SparkSession.builder
    .appName("stage2_aggregate_daily_cost")
    .master("local[*]")
    .getOrCreate()
)

# ------------------------------------------------
# Read Stage 1 normalized events (Parquet)
# ------------------------------------------------
input_path = "/home/ashwin/spark_outputs/stage1_normalized"

events_df = spark.read.parquet(input_path)

print("=== Stage 1 Normalized Events ===")
events_df.show(truncate=False)
events_df.printSchema()

# ------------------------------------------------
# Aggregate to account-service-day grain
# ------------------------------------------------
daily_cost_df = (
    events_df
    .groupBy(
        "account_id",
        "service_name",
        "usage_date"
    )
    .agg(
        _sum("cost_usd").alias("total_cost_usd")
    )
)

print("=== Stage 2 Daily Cost Fact ===")
daily_cost_df.show(truncate=False)

# ------------------------------------------------
# Write curated fact (Parquet)
# ------------------------------------------------
output_path = "/home/ashwin/spark_outputs/stage2_account_service_day_cost"

(
    daily_cost_df
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Stage 2 complete. Daily cost fact written to {output_path}")
