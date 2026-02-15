from fastapi import FastAPI
from pydantic import BaseModel
from genai.rag_service_logic import generate_explanation

app = FastAPI(title="Cost Analytics RAG Service")

class QueryRequest(BaseModel):
    query: str

@app.post("/explain")
def explain_cost(request: QueryRequest):
    return generate_explanation(request.query)

@app.get("/health")
def health_check():
    return {"status": "healthy"}

