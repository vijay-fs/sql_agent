from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
import uvicorn

from database.enhanced_db_manager import EnhancedDatabaseManager
from agent.enhanced_dynamic_agent import EnhancedDynamicAgent

# Create FastAPI app
app = FastAPI(
    title="Dynamic SQL Agent with Auto Join Detection",
    description="A text-to-SQL agent using Ollama models with dynamic database connections and automatic foreign key detection",
    version="1.2.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create enhanced database manager
db_manager = EnhancedDatabaseManager()

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
    auto_join: Optional[bool] = True  # Default to include foreign key data

class DirectSQLRequest(BaseModel):
    db_config: DatabaseConfig
    sql_query: str

@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    """
    Convert natural language to SQL and execute it on the specified database
    with automatic foreign key detection and JOIN enhancement
    """
    try:
        # Create agent for this database connection
        agent = EnhancedDynamicAgent(
            db_manager=db_manager,
            db_config=request.db_config.dict(),
            model_name=request.model_name
        )
        
        # Run the query with normalization
        response = agent.run(request.query, normalize_results=request.auto_join)
        
        # If no normalized data was provided by the agent but we have data,
        # try to manually normalize it
        if "data" in response and response["data"] and not response.get("normalized_data"):
            # Try to extract the main table from the query
            main_table = None
            if "sql_query" in response:
                # Use a simple regex to extract the table name after FROM
                import re
                from_match = re.search(r'FROM\s+(\w+)', response["sql_query"], re.IGNORECASE)
                if from_match:
                    main_table = from_match.group(1)
            
            if main_table:
                # Get normalized data for this table
                normalized_result = db_manager.get_normalized_data(
                    agent.engine, main_table, limit=100
                )
                if normalized_result and "normalized_data" in normalized_result:
                    response["normalized_data"] = normalized_result["normalized_data"]
        
        return response
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/direct-sql")
async def execute_direct_sql(request: DirectSQLRequest):
    """
    Execute SQL queries directly on the specified database
    with schema validation and error recovery
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(request.db_config.dict())
        
        # Get schema information - useful for debugging in response
        schema_info = db_manager.get_tables_schema(engine)
        available_tables = list(schema_info.keys())
        table_columns = {}
        for table_name, table_info in schema_info.items():
            table_columns[table_name] = [col['name'] for col in table_info['columns']]
        
        # Execute the query with the enhanced error handling - return full result
        result_obj = db_manager.execute_query(engine, request.sql_query, return_full_result=True)
        
        # Prepare response with enhanced data
        response_data = {
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
            "result": result_obj["result"],
            "data": result_obj.get("data", [])
        }
        
        # Add normalized data if available
        if "normalized_data" in result_obj:
            response_data["normalized_data"] = result_obj["normalized_data"]
        
        # If no normalized data was provided but we have data, try to manually normalize it
        if "data" in result_obj and result_obj["data"] and "normalized_data" not in result_obj:
            try:
                # Try to extract the main table from the query
                import re
                from_match = re.search(r'FROM\s+(\w+)', request.sql_query, re.IGNORECASE)
                if from_match:
                    main_table = from_match.group(1)
                    
                    # Check if this is a valid table
                    if main_table in available_tables:
                        # Get or create schema validator for this engine
                        engine_id = str(id(engine))
                        if engine_id not in db_manager.schema_validators:
                            from database.schema_validator import SchemaValidator
                            db_manager.schema_validators[engine_id] = SchemaValidator(engine)
                        
                        schema_validator = db_manager.schema_validators[engine_id]
                        
                        # Resolve foreign keys for the data
                        normalized_data = schema_validator.resolve_foreign_keys(result_obj["data"], main_table)
                        response_data["normalized_data"] = normalized_data
            except Exception:
                # If normalization fails, just continue without it
                pass
        
        # Add schema information to help with debugging
        # Only add this if there was an error in the result
        if "Error executing query:" in result_obj["result"]:
            response_data["available_tables"] = available_tables
            response_data["table_columns"] = table_columns
            
            # Extract referenced tables from the query to help debugging
            import re
            tables_in_query = []
            # Try to extract table names from the FROM clause
            from_matches = re.findall(r'\bFROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', request.sql_query, re.IGNORECASE)
            if from_matches:
                for match in from_matches:
                    tables_in_query.append(match[0])
            # Try to extract table names from JOIN clauses
            join_matches = re.findall(r'\bJOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', request.sql_query, re.IGNORECASE)
            if join_matches:
                for match in join_matches:
                    tables_in_query.append(match[0])
            
            # Add referenced tables to the response
            response_data["tables_in_query"] = tables_in_query
            
            # Compare with available tables to identify missing tables
            missing_tables = []
            for table in tables_in_query:
                if table not in available_tables:
                    missing_tables.append(table)
            if missing_tables:
                response_data["missing_tables"] = missing_tables
                
                # Suggest closest matches for missing tables
                suggestions = {}
                for missing in missing_tables:
                    closest = None
                    min_distance = float('inf')
                    for available in available_tables:
                        # Simple Levenshtein distance calculation
                        distance = sum(1 for a, b in zip(missing.lower(), available.lower()) if a != b) + abs(len(missing) - len(available))
                        if distance < min_distance:
                            min_distance = distance
                            closest = available
                    if closest and min_distance <= 3:  # Only suggest if reasonably close
                        suggestions[missing] = closest
                if suggestions:
                    response_data["table_suggestions"] = suggestions
        
        return response_data
        
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/schema")
async def get_database_schema(db_config: DatabaseConfig):
    """
    Get the schema of the specified database including foreign key relationships
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(db_config.dict())
        
        # Get schema information
        schema = db_manager.get_tables_schema(engine)
        foreign_keys = db_manager.get_foreign_keys(engine)
        
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
            "schema": schema,
            "foreign_keys": foreign_keys
        }
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/suggest-join")
async def suggest_join_query(db_config: DatabaseConfig, table_name: str):
    """
    Suggest a JOIN query for the specified table based on foreign key relationships
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(db_config.dict())
        
        # Generate suggested join query
        suggested_query = db_manager.suggest_join_query(engine, table_name)
        
        return {
            "table_name": table_name,
            "suggested_query": suggested_query
        }
    except ConnectionError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/normalized-data")
async def get_normalized_table_data(db_config: DatabaseConfig, table_name: str, limit: int = 100):
    """
    Get fully normalized data for a table with all foreign key references resolved
    """
    try:
        # Get connection to the database
        engine = db_manager.get_connection(db_config.dict())
        
        # Get normalized data
        result = db_manager.get_normalized_data(engine, table_name, limit)
        
        return result
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
        "message": "Welcome to the Dynamic SQL Agent with Enhanced Foreign Key Resolution",
        "docs": "/docs",
        "endpoints": [
            "/api/ask - Ask questions in natural language with automatic JOIN detection",
            "/api/direct-sql - Execute SQL queries directly",
            "/api/schema - Get database schema with foreign key relationships", 
            "/api/suggest-join - Get a suggested JOIN query for a specific table",
            "/api/normalized-data - Get fully normalized data for a table with all foreign key references resolved",
            "/api/models - List available Ollama models"
        ]
    }

# Run the application
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)