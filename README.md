# SQL Agent Project

A sophisticated text-to-SQL conversion system that leverages local language models (Ollama) to transform natural language queries into executable SQL with advanced database schema understanding and auto-join detection.

## Project Overview

The SQL Agent project is an advanced system for connecting to various database types (MySQL, PostgreSQL), extracting schema information, and translating natural language questions into optimized SQL queries. It features:

- **Multi-database support**: Connects seamlessly to MySQL and PostgreSQL databases
- **Automatic foreign key detection**: Identifies relationships between tables automatically
- **Smart JOIN suggestions**: Analyzes database schema to suggest optimal table joins
- **Natural language to SQL conversion**: Uses Ollama's local LLM capabilities
- **Advanced error handling**: Provides detailed error analysis and suggestions
- **Schema validation**: Validates queries against the database schema before execution

## Development Setup

### Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- MySQL or PostgreSQL database for testing (optional)
- Ollama (for local LLM execution)

### Installation Steps

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd sql_agent_project
   ```

2. **Install required packages**

   ```bash
   pip install -r requirements.txt
   ```

   The project depends on:
   - fastapi==0.103.1
   - uvicorn==0.23.2
   - sqlalchemy==2.0.20
   - pydantic==2.3.0
   - requests==2.31.0
   - pymysql==1.1.0

3. **Install Ollama**

   #### macOS
   ```bash
   brew install ollama
   ```
   Or download from: https://ollama.com/download/mac

   #### Linux
   ```bash
   curl -fsSL https://ollama.com/install.sh | sh
   ```

   #### Windows
   Download from: https://ollama.com/download/windows

4. **Start Ollama service**

   ```bash
   ollama serve
   ```

5. **Pull the Llama model**

   ```bash
   ollama pull llama3
   ```

6. **Start the application**

   ```bash
   uvicorn app:app --reload
   ```

   The API will be available at http://127.0.0.1:8000

## Project Architecture

```
sql_agent_project/
├── agent/                      # Contains LLM agent implementations
│   ├── __init__.py
│   └── enhanced_dynamic_agent.py # Main agent for text-to-SQL with auto-join
├── database/                   # Database interaction modules
│   ├── __init__.py
│   ├── enhanced_db_manager.py  # Advanced database connections and schema extraction
│   └── schema_validator.py     # SQL validation against schema
├── tests/                      # Test modules
│   ├── test_schema_validator.py
│   ├── test_project_fk_resolver.py
│   └── test_foreign_key_display.py
├── utils/                      # Utility functions
│   └── inspect_schema.py       # Schema inspection utilities
├── app.py                      # FastAPI application entry point
├── requirements.txt            # Project dependencies
└── README.md                   # Project documentation
```

## Core Components

### EnhancedDatabaseManager

The `EnhancedDatabaseManager` class located in `database/enhanced_db_manager.py` handles:

- Dynamic connections to multiple database types
- Schema extraction and caching
- Foreign key detection and relationship mapping
- SQL query execution with enhanced error handling
- Smart JOIN suggestions based on schema analysis

### EnhancedDynamicAgent

The `EnhancedDynamicAgent` class in `agent/enhanced_dynamic_agent.py` provides:

- Integration with Ollama's local LLM capabilities
- Context-aware prompting with schema information
- Natural language to SQL conversion
- Error analysis and suggestions
- Query optimization through automatic join detection

## API Endpoints

The application provides several REST API endpoints:

- **POST /api/ask**  
  Convert natural language to SQL and execute it on the specified database with automatic foreign key detection and JOIN enhancement.

- **POST /api/direct-sql**  
  Execute SQL queries directly on the specified database with schema validation and error recovery.

- **POST /api/schema**  
  Get the schema of the specified database including foreign key relationships.

- **POST /api/suggest-join**  
  Suggest a JOIN query for the specified table based on foreign key relationships.

- **GET /api/models**  
  List available Ollama models.

## Database Normalization Features

This project includes a comprehensive database normalization service with the following capabilities:

1. Support for multiple database types (PostgreSQL, MySQL)
2. Automatic foreign key resolution and relationship mapping
3. Semantic understanding of database schema
4. Natural language querying of normalized data

Key components of the normalization service:
- Dynamic schema extraction
- Foreign key reference resolution and expansion
- Smart JOIN detection based on schema analysis
- Error detection and recovery mechanisms

## Running Tests

To run the test suite:

```bash
python -m pytest tests/
```

## Development Workflow

1. Start the development server with auto-reload:
   ```bash
   uvicorn app:app --reload
   ```

2. Make API requests to the endpoints using a tool like curl, Postman, or the built-in Swagger UI at http://127.0.0.1:8000/docs

3. For database testing, you can use the included test fixtures or connect to your own database

## Troubleshooting

- **Connection Issues**: Ensure database credentials are correct and the database server is accessible
- **Ollama Errors**: Verify that Ollama is running with `ollama serve` and that the model has been downloaded
- **SQL Errors**: Check the schema validator output for detailed information about SQL syntax issues

## Future Enhancements

- Add support for additional database types (MS SQL Server, Oracle)
- Implement vector embeddings for semantic search
- Add incremental normalization capabilities
- Create migration scripts for existing databases
- Improve error handling and logging

class OllamaAgent:
    def __init__(self, engine, tables_description, model_name="llama3"):
        """
        Initialize an agent that uses Ollama for language model inference.
        
        Args:
            engine: SQLAlchemy engine for executing SQL queries
            tables_description: Description of available database tables
            model_name: Name of the Ollama model to use (default: llama3)
        """
        self.engine = engine
        self.tables_description = tables_description
        self.model_name = model_name
        self.ollama_url = "http://localhost:11434/api/generate"
    
    def sql_engine(self, query):
        """
        Execute a SQL query and return the results
        """
        output = ""
        try:
            with self.engine.connect() as con:
                rows = con.execute(text(query))
                for row in rows:
                    output += "\n" + str(row)
            return output.strip() if output else "Query executed successfully. No rows returned."
        except Exception as e:
            return f"Error executing query: {str(e)}"
    
    def generate_prompt(self, user_query):
        """
        Generate a prompt for the language model
        """
        return f"""You are a helpful SQL assistant. 
        
Database information:
{self.tables_description}

User's question: {user_query}

First, think through what SQL query would answer this question. 
Then, provide the exact SQL query. Format your final SQL query between triple backticks like:
```sql
SELECT * FROM example_table;
```

Think carefully about the query. Do not include any explanations or additional content in your response, ONLY output the SQL query between triple backticks."""
    
    def extract_sql_query(self, response):
        """Extract SQL query from model response"""
        try:
            # Look for SQL query between ```sql and ``` markers
            if "```sql" in response and "```" in response.split("```sql")[1]:
                sql_part = response.split("```sql")[1].split("```")[0].strip()
                return sql_part
            
            # Alternative pattern: just between ``` markers
            elif "```" in response and "```" in response.split("```")[1]:
                sql_part = response.split("```")[1].split("```")[0].strip()
                return sql_part
                
            # Fallback: just return the whole response
            return response.strip()
        except Exception:
            return response.strip()
    
    def run(self, query):
        """
        Run the agent with a natural language query
        
        Args:
            query: Natural language query from the user
            
        Returns:
            Dictionary with the original query, generated SQL, and results
        """
        try:
            # 1. Generate prompt for Ollama
            prompt = self.generate_prompt(query)
            
            # 2. Call Ollama API
            response = requests.post(
                self.ollama_url,
                json={
                    "model": self.model_name,
                    "prompt": prompt,
                    "stream": False
                }
            )
            
            if response.status_code != 200:
                return f"Error calling Ollama API: {response.text}"
            
            # 3. Extract the generated text
            generated_text = response.json().get("response", "")
            
            # 4. Extract SQL query from the response
            sql_query = self.extract_sql_query(generated_text)
            
            # 5. Execute the SQL query
            result = self.sql_engine(sql_query)
            
            # 6. Format and return the response
            return {
                "user_query": query,
                "sql_query": sql_query,
                "result": result
            }
            
        except Exception as e:
            return f"Error running Ollama agent: {str(e)}"
```

## Step 6: Create the Main Application

Create a file `ollama_app.py` at the root of the project with the following content:

```python
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
```

## Step 7: Run the Application

1. Ensure Ollama is running:

```bash
ollama serve
```

2. In a separate terminal, run the application:

```bash
python ollama_app.py
```

3. The API will be available at http://127.0.0.1:8000

## Step 8: Using the API

### API Endpoints:

1. **Ask Questions in Natural Language**
   - URL: http://127.0.0.1:8000/api/ask
   - Method: POST
   - Body:
     ```json
     {
       "query": "Which waiter got more total money from tips?",
       "model_name": "llama3"  // Optional, defaults to llama3
     }
     ```

2. **Execute SQL Directly**
   - URL: http://127.0.0.1:8000/api/direct-sql
   - Method: POST
   - Body:
     ```json
     {
       "sql_query": "SELECT * FROM receipts;"
     }
     ```

3. **List Available Models**
   - URL: http://127.0.0.1:8000/api/models
   - Method: GET

## Example Curl Commands

```bash
# Ask a question in natural language
curl -X POST "http://127.0.0.1:8000/api/ask" \
  -H "Content-Type: application/json" \
  -d '{"query": "Who are all the waiters in the system?", "model_name": "llama3"}'

# Execute SQL directly
curl -X POST "http://127.0.0.1:8000/api/direct-sql" \
  -H "Content-Type: application/json" \
  -d '{"sql_query": "SELECT * FROM waiters;"}'

# List available models
curl -X GET "http://127.0.0.1:8000/api/models"
```

## Troubleshooting

1. **Ollama not running**
   - Error: "Connection refused"
   - Solution: Ensure Ollama is running with `ollama serve`

2. **Model not found**
   - Error: "Model '[model_name]' not found"
   - Solution: Pull the model with `ollama pull [model_name]`

3. **FastAPI application not starting**
   - Check that you've installed all required packages
   - Ensure there are no syntax errors in your Python files

4. **SQL execution errors**
   - Check the SQL query syntax
   - Verify that the tables and columns exist

## Next Steps

1. **Try different models**: Ollama supports various models like llama3, mistral, gemma, etc.
   ```bash
   ollama pull mistral
   ```

2. **Customize the prompt**: Modify the `generate_prompt` method in the OllamaAgent class to improve SQL generation.

3. **Add persistent storage**: Modify the database setup to use a file-based SQLite database or another database system:
   ```python
   engine = create_engine("sqlite:///sql_agent.db")  # File-based SQLite
   ```

4. **Add authentication**: Implement user authentication to secure the API.

5. **Create a frontend**: Build a web interface to interact with the API.