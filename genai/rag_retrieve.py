import json
import boto3
import math

# --- Cosine similarity ---
def cosine_similarity(vec1, vec2):
    dot = sum(a*b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a*a for a in vec1))
    norm2 = math.sqrt(sum(b*b for b in vec2))
    return dot / (norm1 * norm2)

# --- Bedrock runtime ---
client = boto3.client("bedrock-runtime", region_name="us-east-1")

# --- Load stored embeddings ---
with open("genai/anomaly_embeddings.json", "r") as f:
    stored_chunks = json.load(f)

# --- User query ---
query = "Why did EC2 costs increase significantly?"

# --- Embed the query ---
response = client.invoke_model(
    modelId="amazon.titan-embed-text-v1",
    body=json.dumps({
        "inputText": query
    }),
    contentType="application/json",
    accept="application/json"
)

result = json.loads(response["body"].read())
query_embedding = result["embedding"]

# --- Compute similarity ---
scores = []

for chunk in stored_chunks:
    score = cosine_similarity(query_embedding, chunk["embedding"])
    scores.append((score, chunk["text"]))

scores.sort(reverse=True)

# --- Top match ---
top_match = scores[0]

print("Query:", query)
print("\nMost relevant anomaly:\n")
print(top_match[1])
print("\nSimilarity score:", top_match[0])

