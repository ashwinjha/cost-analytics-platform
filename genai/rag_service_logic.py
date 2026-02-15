import json
import time
import faiss
import numpy as np
import boto3
from genai.observability import log_event, track_latency
from genai.llm_client import LLMClient

# Load FAISS index
index = faiss.read_index("genai/faiss_index.bin")

with open("genai/anomaly_chunks.json", "r") as f:
    stored_chunks = json.load(f)

bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

def generate_explanation(query: str):
    start_time = time.time()

    # Embed query
    embed_response = bedrock_runtime.invoke_model(
        modelId="amazon.titan-embed-text-v1",
        body=json.dumps({"inputText": query}),
        contentType="application/json",
        accept="application/json"
    )

    query_embedding = json.loads(embed_response["body"].read())["embedding"]
    query_vector = np.array([query_embedding]).astype("float32")

    # Retrieve
    distances, indices = index.search(query_vector, k=1)
    top_chunk = stored_chunks[indices[0][0]]["text"]
    similarity_score = float(distances[0][0])

    log_event("retrieval", {
        "query": query,
        "similarity_score": similarity_score
    })

    # LLM
    llm = LLMClient()

    final_prompt = f"""
    You are a cloud cost analyst.

    User question:
    {query}

    Relevant anomaly:
    {top_chunk}

    Explain clearly why the cost increased and what might have caused it.
    """

    answer = llm.generate(final_prompt)

    latency = track_latency(start_time)

    log_event("generation", {
        "query": query,
        "latency_seconds": latency
    })

    return {
        "query": query,
        "retrieved_context": top_chunk,
        "similarity_score": similarity_score,
        "latency_seconds": latency,
        "answer": answer
    }

