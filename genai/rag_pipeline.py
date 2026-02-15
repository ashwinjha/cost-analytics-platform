import time
from observability import log_event, track_latency

import json
import numpy as np
import faiss
import boto3
from llm_client import LLMClient

# --------------------------
# Load FAISS index + texts
# --------------------------
index = faiss.read_index("genai/faiss_index.bin")

with open("genai/faiss_texts.json", "r") as f:
    texts = json.load(f)

# --------------------------
# Bedrock client (embeddings only)
# --------------------------
bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1"
)

# --------------------------
# User Query
# --------------------------
query = "Why did EC2 costs increase significantly?"
start_time = time.time()

# --------------------------
# Generate Query Embedding
# --------------------------
response = bedrock_runtime.invoke_model(
    modelId="amazon.titan-embed-text-v1",
    body=json.dumps({"inputText": query}),
    contentType="application/json",
    accept="application/json"
)

embedding = json.loads(response["body"].read())["embedding"]
query_vector = np.array([embedding]).astype("float32")

# Normalize for cosine similarity
faiss.normalize_L2(query_vector)

# --------------------------
# Retrieve Top Context
# --------------------------
distances, indices = index.search(query_vector, k=1)

top_index = indices[0][0]
top_context = texts[top_index]
similarity_score = distances[0][0]
log_event("retrieval", {
    "query": query,
    "similarity_score": float(similarity_score)
})
# --------------------------
# LLM Orchestration Layer
# --------------------------
llm = LLMClient()

final_prompt = f"""
You are a cloud cost analyst.

User question:
{query}

Relevant historical anomaly:
{top_context}

Based on this context, clearly explain:
1) Why the cost likely increased
2) What technical drivers may have caused it
3) What a finance team should investigate
"""

answer = llm.generate(final_prompt)
latency = track_latency(start_time)

log_event("generation", {
    "query": query,
    "latency_seconds": latency
})
# --------------------------
# Output
# --------------------------
print("\n--- RAG PIPELINE EXECUTION ---\n")
print("Query:", query)
print("\nRetrieved Context:\n", top_context)
print("\nSimilarity Score:", similarity_score)
print("\nLLM Explanation:\n")
print(answer)

