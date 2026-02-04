from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("dq_validate_daily_account_cost").getOrCreate()

df = spark.read.parquet("outputs/published/")

# Primary key checks
assert df.filter(col("account_id").isNull()).count() == 0
assert df.filter(col("usage_date").isNull()).count() == 0

# Metric sanity checks
assert df.filter(col("total_cost_usd") < 0).count() == 0

# Completeness check
assert df.count() > 0

print("Daily account cost dataset passed validation checks.")
