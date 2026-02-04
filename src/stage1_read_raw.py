from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("stage1_read_raw")
    .master("local[*]")
    .getOrCreate()
)

# Read raw CSV
raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/raw_billing_events.csv")
)

print("=== Raw Billing Events ===")
raw_df.show(truncate=False)
raw_df.printSchema()

# Dedup logic: latest ingestion wins
window_spec = Window.partitionBy("event_id").orderBy(col("ingestion_date").desc())

normalized_df = (
    raw_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Write Parquet to Linux filesystem
output_path = "/home/ashwin/spark_outputs/stage1_normalized"

(
    normalized_df
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Stage 1 complete. Normalized parquet written to {output_path}")
