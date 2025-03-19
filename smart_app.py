from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import uvicorn

from database.db_manager import DatabaseManager
from agent.smart_join_agent import SmartJoinAgent

# Create FastAPI app
app = FastAPI(
    title="Smart SQL Agent with Table Verification",
    description="A text-to-SQL agent using LLMs with intelligent schema inspection and table verification",
    version="2.1.0"
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
    enable_smart_joins: Optional[bool] = True
    raw_data: Optional[bool] = True

class DirectSQLRequest(BaseModel):
    db_config: DatabaseConfig
    sql_query: str

class APIResponse(BaseModel):
    user_query: str
    sql_query: str
    result: str
    data: Optional[List[Dict[str, Any]]] = None
    original_query: Optional[str] = None
    note: Optional[str] = None

@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    """
    Convert natural language to SQL and execute it with smart schema analysis
    """
    try:
        # Create agent for this database connection
        agent = SmartJoinAgent(
            db_manager=db_manager,
            db_config=request.db_config.dict(),
            model_name=request.model_name
        )
        
        # Run the query
        response = agent.run(request.query, return_raw_data=request.raw_data)
        
        # If there was an error, try a direct approach
        if "error" in response and "doesn't exist" in response.get("result", ""):
            # Extract table name from error message
            import re
            error_msg = response.get("result", "")
            match = re.search(r"Table '[^']*\.([^']*)' doesn't exist", error_msg)
            
            # Check what tables are available
            available_tables = agent.join_detector.inspector.get_table_names()
            
            # If no tables found, return the original error
            if not available_tables:
                return response
            
            # Try using the first available table as a fallback
            fallback_query = f"SELECT * FROM {available_tables[0]};"
            fallback_result = agent.sql_engine(fallback_query, return_raw_data=request.raw_data)
            
            return {
                "user_query": request.query,
                "sql_query": fallback_query,
                "result": fallback_result.get("result", ""),
                "data": fallback_result.get("data", []),
                "original_query": response.get("sql_query", ""),
                "note": f"Original query had an error. Showing data from {available_tables[0]} table instead."
            }
        
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
        result = db_manager.execute_query(engine, request.sql_query, return_raw_data=True)
        
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
            "result": result.get("result", ""),
            "data": result.get("data", [])
        }
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/list-tables")
async def list_tables(db_config: DatabaseConfig):
    """
    List all tables in the database
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(db_config.dict())
        
        # Import and initialize the SmartJoinDetector
        from database.smart_join_detector import SmartJoinDetector
        detector = SmartJoinDetector(engine)
        
        # Get list of tables
        tables = detector.inspector.get_table_names()
        
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
            "tables": tables
        }
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/table-data")
async def get_table_data(db_config: DatabaseConfig, table_name: str, limit: int = 100):
    """
    Get data from a specific table
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(db_config.dict())
        
        # Execute a simple query to get the data
        query = f"SELECT * FROM {table_name} LIMIT {limit};"
        result = db_manager.execute_query(engine, query, return_raw_data=True)
        
        return {
            "table": table_name,
            "query": query,
            "result": result.get("result", ""),
            "data": result.get("data", [])
        }
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/analyze-schema")
async def analyze_database_schema(db_config: DatabaseConfig):
    """
    Analyze database schema and discover potential relationships
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(db_config.dict())
        
        # Import and initialize the SmartJoinDetector
        from database.smart_join_detector import SmartJoinDetector
        detector = SmartJoinDetector(engine)
        
        # Get discovered relationships
        tables = detector.inspector.get_table_names()
        relationships = {}
        
        for table in tables:
            relationships[table] = detector.get_relationships_for_table(table)
        
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
            "tables": tables,
            "discovered_relationships": relationships
        }
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/suggest-join-query")
async def suggest_join_query(db_config: DatabaseConfig, table_name: str):
    """
    Suggest a smart JOIN query for a specific table
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(db_config.dict())
        
        # Import and initialize the SmartJoinDetector
        from database.smart_join_detector import SmartJoinDetector
        detector = SmartJoinDetector(engine)
        
        # Generate a suggested join query
        suggested_query = detector.generate_join_query(table_name)
        
        return {
            "table": table_name,
            "suggested_query": suggested_query
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
        "message": "Welcome to the Smart SQL Agent with Table Verification",
        "docs": "/docs",
        "endpoints": [
            "/api/ask - Ask questions in natural language with smart schema analysis",
            "/api/direct-sql - Execute SQL queries directly",
            "/api/list-tables - List all tables in the database",
            "/api/table-data - Get data from a specific table",
            "/api/analyze-schema - Analyze database schema and discover relationships",
            "/api/suggest-join-query - Get a suggested smart JOIN query for a table",
            "/api/models - List available Ollama models"
        ]
    }

# Run the application
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)