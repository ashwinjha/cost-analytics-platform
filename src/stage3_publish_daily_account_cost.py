from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, lit, current_timestamp

spark = (
    SparkSession.builder
    .appName("stage3_publish_daily_account_cost")
    .master("local[*]")
    .getOrCreate()
)

# ------------------------------------------------
# Read Stage 2 fact
# ------------------------------------------------
input_path = "/home/ashwin/spark_outputs/stage2_account_service_day_cost"

service_day_df = spark.read.parquet(input_path)

print("=== Stage 2 Input ===")
service_day_df.show(truncate=False)

# ------------------------------------------------
# Aggregate to account-day (canonical grain)
# ------------------------------------------------
account_day_df = (
    service_day_df
    .groupBy(
        "account_id",
        "usage_date"
    )
    .agg(
        _sum("total_cost_usd").alias("total_cost_usd")
    )
)

# ------------------------------------------------
# Add ownership & publishing semantics
# ------------------------------------------------
published_df = (
    account_day_df
    .withColumn("data_complete", lit(True))
    .withColumn("published_at", current_timestamp())
)

print("=== Stage 3 Published Dataset ===")
published_df.show(truncate=False)

# ------------------------------------------------
# Write canonical dataset
# ------------------------------------------------
output_path = "/home/ashwin/spark_outputs/stage3_daily_account_cost"

(
    published_df
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Stage 3 complete. Canonical dataset written to {output_path}")
