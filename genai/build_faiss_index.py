import json
import numpy as np
import faiss

# Load stored embeddings
with open("genai/anomaly_embeddings.json", "r") as f:
    data = json.load(f)

texts = []
vectors = []

for item in data:
    texts.append(item["text"])
    vectors.append(item["embedding"])

# Convert to NumPy array
vectors = np.array(vectors).astype("float32")

# Normalize vectors (important for cosine similarity)
faiss.normalize_L2(vectors)

# Create FAISS index
dimension = vectors.shape[1]
index = faiss.IndexFlatIP(dimension)  # Inner product = cosine after normalization
index.add(vectors)

# Save index
faiss.write_index(index, "genai/faiss_index.bin")

# Save text mapping
with open("genai/faiss_texts.json", "w") as f:
    json.dump(texts, f)

print("FAISS index built successfully.")

