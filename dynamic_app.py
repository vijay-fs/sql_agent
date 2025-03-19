from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import uvicorn

from database.db_manager import DatabaseManager
from agent.dynamic_agent import DynamicOllamaAgent

# Create FastAPI app
app = FastAPI(
    title="Dynamic SQL Agent with Ollama",
    description="A text-to-SQL agent using Ollama models with dynamic database connections",
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

# Create database manager
db_manager = DatabaseManager()

class DatabaseConfig(BaseModel):
    databasetype: str = "mysql"
    envirment: str = "localhost"
    port: str = "3306"
    database: str
    username: str
    password: str
    ssl: str = "false"

class QueryRequest(BaseModel):
    db_config: DatabaseConfig
    query: str
    model_name: Optional[str] = "llama3"

class DirectSQLRequest(BaseModel):
    db_config: DatabaseConfig
    sql_query: str

@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    """
    Convert natural language to SQL and execute it on the specified database
    """
    try:
        # Create agent for this database connection
        agent = DynamicOllamaAgent(
            db_manager=db_manager,
            db_config=request.db_config.dict(),
            model_name=request.model_name
        )
        
        # Run the query
        response = agent.run(request.query)
        
        return response
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/direct-sql")
async def execute_direct_sql(request: DirectSQLRequest):
    """
    Execute SQL queries directly on the specified database
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(request.db_config.dict())
        
        # Execute the query
        result = db_manager.execute_query(engine, request.sql_query)
        
        return {
            "db_config": {
                "databasetype": request.db_config.databasetype,
                "envirment": request.db_config.envirment,
                "port": request.db_config.port,
                "database": request.db_config.database,
                "username": request.db_config.username,
                # Don't include password in response
                "ssl": request.db_config.ssl
            },
            "sql_query": request.sql_query,
            "result": result
        }
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/schema")
async def get_database_schema(db_config: DatabaseConfig):
    """
    Get the schema of the specified database
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(db_config.dict())
        
        # Get schema information
        schema = db_manager.get_tables_schema(engine)
        
        return {
            "db_config": {
                "databasetype": db_config.databasetype,
                "envirment": db_config.envirment,
                "port": db_config.port,
                "database": db_config.database,
                "username": db_config.username,
                # Don't include password in response
                "ssl": db_config.ssl
            },
            "schema": schema
        }
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
        "message": "Welcome to the Dynamic SQL Agent with Ollama",
        "docs": "/docs",
        "endpoints": [
            "/api/ask - Ask questions in natural language",
            "/api/direct-sql - Execute SQL queries directly",
            "/api/schema - Get database schema",
            "/api/models - List available Ollama models"
        ]
    }

# Run the application
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)