import json
import numpy as np
import faiss
import boto3

# --- Load FAISS index ---
index = faiss.read_index("genai/faiss_index.bin")

with open("genai/faiss_texts.json", "r") as f:
    texts = json.load(f)

# --- Bedrock client for embeddings ---
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

query = "Why did EC2 costs increase significantly?"

# --- Embed query ---
response = bedrock_runtime.invoke_model(
    modelId="amazon.titan-embed-text-v1",
    body=json.dumps({"inputText": query}),
    contentType="application/json",
    accept="application/json"
)

embedding = json.loads(response["body"].read())["embedding"]
query_vector = np.array([embedding]).astype("float32")

# Normalize
faiss.normalize_L2(query_vector)

# Search top 1
distances, indices = index.search(query_vector, k=1)

top_index = indices[0][0]
top_text = texts[top_index]

print("\nQuery:", query)
print("\nTop Retrieved Context:\n")
print(top_text)
print("\nSimilarity Score:", distances[0][0])

