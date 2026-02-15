import json
import boto3
import math
from llm_client import LLMClient

# --- Cosine similarity ---
def cosine_similarity(vec1, vec2):
    dot = sum(a*b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a*a for a in vec1))
    norm2 = math.sqrt(sum(b*b for b in vec2))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)

# --- Bedrock client (embeddings only) ---
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

# --- Load stored embeddings ---
with open("genai/anomaly_embeddings.json", "r") as f:
    stored_chunks = json.load(f)

# --- User query ---
query = "Why did EC2 costs increase significantly?"

# --- Embed query ---
embed_response = bedrock_runtime.invoke_model(
    modelId="amazon.titan-embed-text-v1",
    body=json.dumps({"inputText": query}),
    contentType="application/json",
    accept="application/json"
)

query_embedding = json.loads(embed_response["body"].read())["embedding"]

# --- Retrieve top chunk ---
scores = []
for chunk in stored_chunks:
    score = cosine_similarity(query_embedding, chunk["embedding"])
    scores.append((score, chunk["text"]))

scores.sort(reverse=True)
top_context = scores[0][1]

# --- LLM Orchestration Layer ---
llm = LLMClient()

final_prompt = f"""
You are a cloud cost analyst.

User question:
{query}

Relevant anomaly:
{top_context}

Explain clearly why the cost increased and what might have caused it.
"""

answer = llm.generate(final_prompt)

print("\nRetrieved Context:\n")
print(top_context)

print("\nLLM Explanation:\n")
print(answer)
