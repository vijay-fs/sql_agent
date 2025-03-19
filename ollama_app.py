from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import uvicorn

from database.db_setup import create_database, get_tables_description, execute_query
from agent.ollama_agent import OllamaAgent

# Create the database
engine = create_database()
tables_description = get_tables_description(engine)

# Create FastAPI app
app = FastAPI(
    title="SQL Agent with Ollama",
    description="A text-to-SQL agent using locally running Ollama models",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create Ollama agent
ollama_agent = OllamaAgent(engine, tables_description)

class QueryRequest(BaseModel):
    query: str
    model_name: Optional[str] = "llama3"  # Default to llama3

class SQLRequest(BaseModel):
    sql_query: str

@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    """
    Convert natural language to SQL and execute it using Ollama
    """
    if not request.query:
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    
    # Update the model if specified in the request
    if request.model_name:
        ollama_agent.model_name = request.model_name
    
    # Run the agent with the natural language query
    response = ollama_agent.run(request.query)
    
    return response

@app.post("/api/direct-sql")
async def execute_direct_sql(request: SQLRequest):
    """
    Execute SQL queries directly
    """
    if not request.sql_query:
        raise HTTPException(status_code=400, detail="SQL query cannot be empty")
    
    # Execute the SQL query directly
    result = execute_query(engine, request.sql_query)
    
    return {
        "sql_query": request.sql_query,
        "result": result
    }

@app.get("/api/models")
async def list_models():
    """
    List available Ollama models
    """
    try:
        import requests
        response = requests.get("http://localhost:11434/api/tags")
        if response.status_code == 200:
            models = response.json().get("models", [])
            return {"models": [model.get("name") for model in models]}
        else:
            return {"error": "Could not fetch models from Ollama", "details": response.text}
    except Exception as e:
        return {"error": str(e)}

@app.get("/")
async def root():
    return {
        "message": "Welcome to the SQL Agent with Ollama",
        "docs": "/docs",
        "endpoints": [
            "/api/ask - Ask questions in natural language",
            "/api/direct-sql - Execute SQL queries directly",
            "/api/models - List available Ollama models"
        ]
    }

# Run the application
if __name__ == "__main__":
    uvicorn.run("ollama_app:app", host="127.0.0.1", port=8000, reload=True)