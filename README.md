# Complete Guide to SQL Agent with Ollama

This guide will walk you through setting up and running the SQL Agent project that uses Ollama for text-to-SQL conversion.

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)
- Ollama (for local LLM execution)

## Step 1: Project Setup

1. Create the project directory structure:

```bash
mkdir -p sql_agent_project/database
mkdir -p sql_agent_project/agent
touch sql_agent_project/database/__init__.py
touch sql_agent_project/agent/__init__.py
```

2. Navigate to the project directory:

```bash
cd sql_agent_project
```

## Step 2: Install Required Packages

1. Create a `requirements.txt` file with the following content:

```
fastapi==0.103.1
uvicorn==0.23.2
sqlalchemy==2.0.20
pydantic==2.3.0
requests==2.31.0
```

2. Install the required packages:

```bash
pip install -r requirements.txt

# or 

pip install fastapi uvicorn sqlalchemy pydantic requests
```

## Step 3: Install and Set Up Ollama

1. Install Ollama based on your operating system:

### macOS

```bash
brew install ollama
```

Or download from: https://ollama.com/download/mac

### Linux

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

### Windows

Download from: https://ollama.com/download/windows

2. Start the Ollama service:

```bash
ollama serve
```

3. Pull the Llama model:

```bash
ollama pull llama3
```

## Step 4: Create the Database Setup Module

Create a file `database/db_setup.py` with the following content:

```python
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    insert,
    inspect,
    text,
)

def create_database():
    """Create and populate the database with sample data"""
    engine = create_engine("sqlite:///:memory:")
    metadata_obj = MetaData()

    # Create receipts table
    receipts = Table(
        "receipts",
        metadata_obj,
        Column("receipt_id", Integer, primary_key=True),
        Column("customer_name", String(16), primary_key=True),
        Column("price", Float),
        Column("tip", Float),
    )

    # Create waiters table
    waiters = Table(
        "waiters",
        metadata_obj,
        Column("receipt_id", Integer, primary_key=True),
        Column("waiter_name", String(16), primary_key=True),
    )

    # Create all tables in the database
    metadata_obj.create_all(engine)

    # Insert sample data into receipts table
    receipt_rows = [
        {"receipt_id": 1, "customer_name": "Alan Payne", "price": 12.06, "tip": 1.20},
        {"receipt_id": 2, "customer_name": "Alex Mason", "price": 23.86, "tip": 0.24},
        {"receipt_id": 3, "customer_name": "Woodrow Wilson", "price": 53.43, "tip": 5.43},
        {"receipt_id": 4, "customer_name": "Margaret James", "price": 21.11, "tip": 1.00},
    ]
    
    for row in receipt_rows:
        stmt = insert(receipts).values(**row)
        with engine.begin() as connection:
            connection.execute(stmt)

    # Insert sample data into waiters table
    waiter_rows = [
        {"receipt_id": 1, "waiter_name": "Corey Johnson"},
        {"receipt_id": 2, "waiter_name": "Michael Watts"},
        {"receipt_id": 3, "waiter_name": "Michael Watts"},
        {"receipt_id": 4, "waiter_name": "Margaret James"},
    ]
    
    for row in waiter_rows:
        stmt = insert(waiters).values(**row)
        with engine.begin() as connection:
            connection.execute(stmt)
    
    return engine

def get_tables_description(engine):
    """Generate a description of all tables in the database"""
    description = "Allows you to perform SQL queries on the tables. Returns a string representation of the result.\nIt can use the following tables:"

    inspector = inspect(engine)
    for table in ["receipts", "waiters"]:
        columns_info = [(col["name"], col["type"]) for col in inspector.get_columns(table)]
        
        table_description = f"\n\nTable '{table}':\nColumns:"
        for name, col_type in columns_info:
            table_description += f"\n  - {name}: {col_type}"
        
        description += table_description
    
    return description

def execute_query(engine, query):
    """Execute a SQL query and return the results as a string"""
    output = ""
    try:
        with engine.connect() as con:
            rows = con.execute(text(query))
            for row in rows:
                output += "\n" + str(row)
        return output.strip() if output else "Query executed successfully. No rows returned."
    except Exception as e:
        return f"Error executing query: {str(e)}"
```

## Step 5: Create the Ollama Agent Module

Create a file `agent/ollama_agent.py` with the following content:

```python
import json
import requests
from sqlalchemy import text

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