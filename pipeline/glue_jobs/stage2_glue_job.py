import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as spark_sum
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ----------------------------------------
# Glue Job Arguments
# ----------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BASE_OUTPUT_PATH"])

BASE_OUTPUT_PATH = args["BASE_OUTPUT_PATH"]

# ----------------------------------------
# Initialize Glue
# ----------------------------------------

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------------------
# Paths
# ----------------------------------------

input_path = f"{BASE_OUTPUT_PATH}/normalized"
output_path = f"{BASE_OUTPUT_PATH}/facts/daily_account_cost"

# ----------------------------------------
# Read Normalized Events
# ----------------------------------------

events_df = spark.read.parquet(input_path)

# ----------------------------------------
# Aggregate to account-service-day
# ----------------------------------------

fact_df = (
    events_df
    .groupBy("account_id", "service_name", "usage_date")
    .agg(spark_sum("cost_usd").alias("total_cost_usd"))
)

# ----------------------------------------
# Write Partitioned Output
# ----------------------------------------

fact_df.write.mode("overwrite") \
    .partitionBy("usage_date") \
    .parquet(output_path)

job.commit()

