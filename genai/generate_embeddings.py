import json
import boto3

# Bedrock runtime client
client = boto3.client("bedrock-runtime", region_name="us-east-1")

# Load anomaly chunks
with open("genai/anomaly_chunks.json", "r") as f:
    chunks = json.load(f)

embedded_chunks = []

for chunk in chunks:
    response = client.invoke_model(
        modelId="amazon.titan-embed-text-v1",
        body=json.dumps({
            "inputText": chunk["text"]
        }),
        contentType="application/json",
        accept="application/json"
    )

    result = json.loads(response["body"].read())
    embedding = result["embedding"]

    chunk["embedding"] = embedding
    embedded_chunks.append(chunk)

# Save locally
with open("genai/anomaly_embeddings.json", "w") as f:
    json.dump(embedded_chunks, f)

print("Embedding generation complete.")

