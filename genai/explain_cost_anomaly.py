import boto3
import pandas as pd

# pulled from Athena query results
data = [
    {"usage_date": "2025-01-01", "total_cost_usd": 100},
    {"usage_date": "2025-01-02", "total_cost_usd": 98},
    {"usage_date": "2025-01-03", "total_cost_usd": 102},
    {"usage_date": "2025-01-04", "total_cost_usd": 180},  # anomaly
]

df = pd.DataFrame(data)

avg_cost = df["total_cost_usd"].mean()
latest = df.iloc[-1]

if latest["total_cost_usd"] > 1.5 * avg_cost:
    anomaly_detected = True
else:
    anomaly_detected = False

context = f"""
Daily cost anomaly detected.

Historical average daily cost: ${avg_cost:.2f}
Latest daily cost: ${latest['total_cost_usd']:.2f}
Increase over baseline: {((latest['total_cost_usd'] / avg_cost) - 1) * 100:.1f}%

The dataset represents aggregated cloud service spend.
"""
