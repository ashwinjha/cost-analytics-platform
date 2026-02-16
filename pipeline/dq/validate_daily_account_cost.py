import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("dq_validate_daily_account_cost") \
    .getOrCreate()

BASE_OUTPUT_PATH = os.getenv(
    "BASE_OUTPUT_PATH",
    "s3a://cost-analytics-ashwin-0310"
)

published_path = f"{BASE_OUTPUT_PATH}/published/daily_account_cost"

df = spark.read.parquet(published_path)

# ----------------------------
# Primary key checks
# ----------------------------
assert df.filter(col("account_id").isNull()).count() == 0
assert df.filter(col("usage_date").isNull()).count() == 0

# ----------------------------
# Metric sanity checks
# ----------------------------
assert df.filter(col("total_cost_usd") < 0).count() == 0

# ----------------------------
# Completeness check
# ----------------------------
assert df.count() > 0

print("Daily account cost dataset passed validation checks.")

