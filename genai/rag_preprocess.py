import boto3
import pandas as pd
import json

ATHENA_DATABASE = "cost_analytics"
ATHENA_OUTPUT = "s3://cost-analytics-ashwin-0310/athena-results/"

QUERY = """
SELECT account_id,
       service_name,
       usage_date,
       total_cost_usd
FROM cost_analytics.daily_account_cost
ORDER BY usage_date DESC
LIMIT 20;
"""

athena = boto3.client("athena", region_name="us-east-1")

response = athena.start_query_execution(
    QueryString=QUERY,
    QueryExecutionContext={"Database": ATHENA_DATABASE},
    ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
)

query_execution_id = response["QueryExecutionId"]

# Wait for query to complete
import time
while True:
    result = athena.get_query_execution(QueryExecutionId=query_execution_id)
    state = result["QueryExecution"]["Status"]["State"]
    if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
        break
    time.sleep(2)

if state != "SUCCEEDED":
    raise Exception(f"Athena query failed with state {state}")

results = athena.get_query_results(QueryExecutionId=query_execution_id)

rows = results["ResultSet"]["Rows"]

chunks = []

for row in rows[1:]:
    account = row["Data"][0]["VarCharValue"]
    service = row["Data"][1]["VarCharValue"]
    date = row["Data"][2]["VarCharValue"]
    cost = row["Data"][3]["VarCharValue"]

    text_chunk = f"""
On {date}, account {account} spent ${cost} on {service}.
    """.strip()

    chunks.append({
        "account_id": account,
        "service_name": service,
        "usage_date": date,
        "total_cost_usd": cost,
        "text": text_chunk
    })

with open("genai/anomaly_chunks.json", "w") as f:
    json.dump(chunks, f, indent=2)

print("RAG preprocessing complete. anomaly_chunks.json created.")

