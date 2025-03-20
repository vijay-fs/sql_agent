## Performance Considerations

When deploying the SQL Agent in production environments, several performance considerations should be taken into account.

### Database Connection Pooling

SQLAlchemy's connection pooling is used to minimize the overhead of establishing new database connections:

```python
def get_connection(self, db_config):
    """
    Get or create a database connection based on configuration
    """
    # Generate a unique key for this connection
    conn_key = json.dumps(db_config, sort_keys=True)
    
    # Return existing connection if available
    if conn_key in self.connections:
        return self.connections[conn_key]
    
    # Create new connection with pooling settings
    try:
        conn_string = self.get_connection_string(db_config)
        
        # Configure connection pooling
        engine = create_engine(
            conn_string,
            pool_size=10,  # Maximum number of connections to keep
            max_overflow=20,  # Maximum overflow connections when pool is full
            pool_timeout=30,  # Timeout for getting a connection from pool
            pool_recycle=1800  # Recycle connections after 30 minutes
        )
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Store connection
        self.connections[conn_key] = engine
        return engine
    except Exception as e:
        raise ConnectionError(f"Failed to connect to database: {str(e)}")
```

### Schema Caching

Schema information is cached to avoid repeated introspection of the database:

```python
def get_tables_schema(self, engine, refresh=False):
    """
    Get schema information for all tables in the database
    """
    # Use cached data if available and refresh is not requested
    engine_id = str(id(engine))
    if engine_id in self.schema_cache and not refresh:
        return self.schema_cache[engine_id]
    
    # Extract schema information
    # ...
    
    # Cache the results
    self.schema_cache[engine_id] = schema_info
    return schema_info
```

### Query Result Limitations

To prevent memory issues with large result sets, implement limits:

```python
def execute_query(self, engine, query, return_full_result=False, max_rows=1000):
    """
    Execute a SQL query with row limitations
    """
    # Add LIMIT clause if not present for SELECT queries
    if query.strip().upper().startswith("SELECT") and "LIMIT" not in query.upper():
        query = query.rstrip(";")
        query += f" LIMIT {max_rows};"
    
    # Execute query and return results
    # ...
```

### Ollama Model Optimization

When using Ollama, consider model size and performance tradeoffs:

1. Use smaller models for faster responses (like llama3-8b instead of llama3-70b)
2. Configure model serving parameters in Ollama:

```bash
# Configure Ollama to use less memory
ollama run llama3 --gpu 50%  # Use only 50% of GPU for better sharing
```

### Query Complexity Management

Implement logic to detect and handle overly complex queries:

```python
def is_complex_query(query):
    """Detect potentially expensive queries"""
    # Check for multiple JOINs
    join_count = query.upper().count("JOIN")
    
    # Check for aggregate functions without LIMIT
    has_aggregates = any(func in query.upper() for func in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("])
    has_limit = "LIMIT" in query.upper()
    
    # Check for DISTINCT operations
    has_distinct = "DISTINCT" in query.upper()
    
    # Consider complex if it has multiple criteria
    complexity_score = join_count + (1 if has_aggregates and not has_limit else 0) + (1 if has_distinct else 0)
    
    return complexity_score >= 3
```

### Asynchronous Processing

For long-running queries, implement asynchronous processing:

```python
from fastapi import BackgroundTasks

@app.post("/api/ask-async")
async def ask_question_async(
    request: QueryRequest,
    background_tasks: BackgroundTasks
):
    """
    Asynchronous version of ask_question that processes in the background
    """
    # Generate a unique ID for this request
    query_id = str(uuid.uuid4())
    
    # Store initial status
    query_results[query_id] = {"status": "processing", "timestamp": time.time()}
    
    # Process in background
    background_tasks.add_task(process_query, query_id, request)
    
    # Return the ID immediately
    return {"query_id": query_id, "status": "processing"}

@app.get("/api/results/{query_id}")
async def get_results(query_id: str):
    """
    Get results of an asynchronous query
    """
    if query_id not in query_results:
        raise HTTPException(status_code=404, detail="Query ID not found")
    
    return query_results[query_id]

async def process_query(query_id, request):
    """
    Background task to process a query
    """
    try:
        # Create agent and process
        agent = DynamicAgent(
            db_manager=db_manager,
            db_config=request.db_config.dict(),
            model_name=request.model_name
        )
        
        # Run the query
        result = agent.run(request.query, normalize_results=request.auto_join)
        
        # Store the result
        query_results[query_id] = {
            "status": "completed",
            "timestamp": time.time(),
            "result": result
        }
    except Exception as e:
        # Store the error
        query_results[query_id] = {
            "status": "error",
            "timestamp": time.time(),
            "error": str(e)
        }
```

### Response Compression

To reduce network overhead for large responses, enable response compression:

```python
from fastapi.middleware.gzip import GZipMiddleware

# Add compression middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)
```

### Caching Common Queries

Implement a cache for common queries:

```python
from functools import lru_cache
import hashlib

# Create a hash of a query and its parameters
def query_hash(db_config, query):
    """Generate a unique hash for a query and its context"""
    hash_input = f"{json.dumps(db_config, sort_keys=True)}:{query}"
    return hashlib.md5(hash_input.encode()).hexdigest()

# Cache query results
@lru_cache(maxsize=100)
def cached_query_result(query_hash):
    """Cache to store query results by hash"""
    pass

@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    """
    Convert natural language to SQL and execute it
    """
    # Generate hash for this request
    hash_key = query_hash(request.db_config.dict(), request.query)
    
    # Check cache first
    cached_result = cached_query_result(hash_key)
    if cached_result is not None:
        return cached_result
    
    # Process as usual if not cached
    # ...
    
    # Cache the result before returning
    cached_query_result.cache_set(hash_key, response)
    
    return response
```

### Monitoring and Profiling

Implement monitoring to identify performance bottlenecks:

```python
import time
from contextlib import contextmanager

# Context manager for timing operations
@contextmanager
def timing(operation_name):
    start_time = time.time()
    yield
    end_time = time.time()
    duration = end_time - start_time
    print(f"Operation '{operation_name}' took {duration:.4f} seconds")

# Use in agent methods
def run(self, query, normalize_results=True):
    try:
        with timing("generate_prompt"):
            prompt = self.generate_prompt(query)
        
        with timing("ollama_request"):
            response = requests.post(self.ollama_url, json={"model": self.model_name, "prompt": prompt})
        
        with timing("extract_query"):
            sql_query = self.extract_sql_query(response.json().get("response", ""))
        
        with timing("enhance_query"):
            enhanced_query = self.enhance_query_with_joins(sql_query)
        
        with timing("execute_query"):
            result_obj = self.sql_engine(enhanced_query, return_full_result=True)
        
        # Rest of the method
        # ...
```

## Final Thoughts

The SQL Agent system provides a powerful bridge between natural language and databases, making data access more intuitive and accessible. The architecture prioritizes:

1. **Modularity**: Separating database, schema validation, and language model components
2. **Extensibility**: Providing hooks for customization and extension
3. **Error resilience**: Implementing robust fallback strategies
4. **Performance**: Optimizing for efficient database access and query processing

By leveraging local language models through Ollama, the system maintains privacy while providing sophisticated natural language understanding.

For additional information and updates, refer to the [official repository](https://github.com/yourusername/sql_agent_project) and the [README.md](README.md) file.
## Performance Considerations

When deploying the SQL Agent in production environments, several performance considerations should be taken into account.

### Database Connection Pooling

SQLAlchemy's connection pooling is used to minimize the overhead of establishing new database connections:

```python
def get_connection(self, db_config):
    """
    Get or create a database connection based on configuration
    """
    # Generate a unique key for this connection
    conn_key = json.dumps(db_config, sort_keys=True)
    
    # Return existing connection if available
    if conn_key in self.connections:
        return self.connections[conn_key]
    
    # Create new connection with pooling settings
    try:
        conn_string = self.get_connection_string(db_config)
        
        # Configure connection pooling
        engine = create_engine(
            conn_string,
            pool_size=10,  # Maximum number of connections to keep
            max_overflow=20,  # Maximum overflow connections when pool is full
            pool_timeout=30,  # Timeout for getting a connection from pool
            pool_recycle=1800  # Recycle connections after 30 minutes
        )
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Store connection
        self.connections[conn_key] = engine
        return engine
    except Exception as e:
        raise ConnectionError(f"Failed to connect to database: {str(e)}")
```

### Schema Caching

Schema information is cached to avoid repeated introspection of the database:

```python
def get_tables_schema(self, engine, refresh=False):
    """
    Get schema information for all tables in the database
    """
    # Use cached data if available and refresh is not requested
    engine_id = str(id(engine))
    if engine_id in self.schema_cache and not refresh:
        return self.schema_cache[engine_id]
    
    # Extract schema information
    # ...
    
    # Cache the results
    self.schema_cache[engine_id] = schema_info
    return schema_info
```

### Query Result Limitations

To prevent memory issues with large result sets, implement limits:

```python
def execute_query(self, engine, query, return_full_result=False, max_rows=1000):
    """
    Execute a SQL query with row limitations
    """
    # Add LIMIT clause if not present for SELECT queries
    if query.strip().upper().startswith("SELECT") and "LIMIT" not in query.upper():
        query = query.rstrip(";")
        query += f" LIMIT {max_rows};"
    
    # Execute query and return results
    # ...
```

### Ollama Model Optimization

When using Ollama, consider model size and performance tradeoffs:

1. Use smaller models for faster responses (like llama3-8b instead of llama3-70b)
2. Configure model serving parameters in Ollama:

```bash
# Configure Ollama to use less memory
ollama run llama3 --gpu 50%  # Use only 50% of GPU for better sharing
```

### Query Complexity Management

Implement logic to detect and handle overly complex queries:

```python
def is_complex_query(query):
    """Detect potentially expensive queries"""
    # Check for multiple JOINs
    join_count = query.upper().count("JOIN")
    
    # Check for aggregate functions without LIMIT
    has_aggregates = any(func in query.upper() for func in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN("])
    has_limit = "LIMIT" in query.upper()
    
    # Check for DISTINCT operations
    has_distinct = "DISTINCT" in query.upper()
    
    # Consider complex if it has multiple criteria
    complexity_score = join_count + (1 if has_aggregates and not has_limit else 0) + (1 if has_distinct else 0)
    
    return complexity_score >= 3
```

### Asynchronous Processing

For long-running queries, implement asynchronous processing:

```python
from fastapi import BackgroundTasks

@app.post("/api/ask-async")
async def ask_question_async(
    request: QueryRequest,
    background_tasks: BackgroundTasks
):
    """
    Asynchronous version of ask_question that processes in the background
    """
    # Generate a unique ID for this request
    query_id = str(uuid.uuid4())
    
    # Store initial status
    query_results[query_id] = {"status": "processing", "timestamp": time.time()}
    
    # Process in background
    background_tasks.add_task(process_query, query_id, request)
    
    # Return the ID immediately
    return {"query_id": query_id, "status": "processing"}

@app.get("/api/results/{query_id}")
async def get_results(query_id: str):
    """
    Get results of an asynchronous query
    """
    if query_id not in query_results:
        raise HTTPException(status_code=404, detail="Query ID not found")
    
    return query_results[query_id]

async def process_query(query_id, request):
    """
    Background task to process a query
    """
    try:
        # Create agent and process
        agent = DynamicAgent(
            db_manager=db_manager,
            db_config=request.db_config.dict(),
            model_name=request.model_name
        )
        
        # Run the query
        result = agent.run(request.query, normalize_results=request.auto_join)
        
        # Store the result
        query_results[query_id] = {
            "status": "completed",
            "timestamp": time.time(),
            "result": result
        }
    except Exception as e:
        # Store the error
        query_results[query_id] = {
            "status": "error",
            "timestamp": time.time(),
            "error": str(e)
        }
```

### Response Compression

To reduce network overhead for large responses, enable response compression:

```python
from fastapi.middleware.gzip import GZipMiddleware

# Add compression middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)
```

### Caching Common Queries

Implement a cache for common queries:

```python
from functools import lru_cache
import hashlib

# Create a hash of a query and its parameters
def query_hash(db_config, query):
    """Generate a unique hash for a query and its context"""
    hash_input = f"{json.dumps(db_config, sort_keys=True)}:{query}"
    return hashlib.md5(hash_input.encode()).hexdigest()

# Cache query results
@lru_cache(maxsize=100)
def cached_query_result(query_hash):
    """Cache to store query results by hash"""
    pass

@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    """
    Convert natural language to SQL and execute it
    """
    # Generate hash for this request
    hash_key = query_hash(request.db_config.dict(), request.query)
    
    # Check cache first
    cached_result = cached_query_result(hash_key)
    if cached_result is not None:
        return cached_result
    
    # Process as usual if not cached
    # ...
    
    # Cache the result before returning
    cached_query_result.cache_set(hash_key, response)
    
    return response# SQL Agent Technical Documentation

*Comprehensive technical documentation for the SQL Agent project that enables natural language to SQL conversion with advanced database schema understanding and auto-join capabilities.*

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Core Components](#core-components)
    - [Database Manager](#database-manager)
    - [Schema Validator](#schema-validator)
    - [Dynamic Agent](#dynamic-agent)
3. [Key Features](#key-features)
    - [Multi-Database Support](#multi-database-support)
    - [Schema Understanding and Validation](#schema-understanding-and-validation)
    - [Automatic Foreign Key Resolution](#automatic-foreign-key-resolution)
    - [Smart JOIN Detection](#smart-join-detection)
    - [Error Handling and Recovery](#error-handling-and-recovery)
4. [API Endpoints](#api-endpoints)
5. [Configuration Options](#configuration-options)
6. [Installation and Setup](#installation-and-setup)
7. [Usage Examples](#usage-examples)
8. [Troubleshooting](#troubleshooting)
9. [Advanced Customization](#advanced-customization)
10. [Performance Considerations](#performance-considerations)

## Architecture Overview

The SQL Agent is built with a modular architecture that separates database connectivity, schema analysis, and the language model integration. This separation of concerns ensures maintainability and makes the system extensible for future enhancements.

**Key Architectural Principles:**

1. **Modular Design**: Each component (database manager, schema validator, and LLM agent) has a dedicated responsibility.
2. **Intelligent Caching**: Database schema and foreign key information are cached to minimize database round-trips.
3. **Graceful Degradation**: The system attempts multiple fallback strategies when faced with errors.
4. **Dynamic Connection Management**: Database connections are initialized and pooled based on runtime configurations.

**System Flow Diagram:**

```
User Query → FastAPI Controller → Dynamic Agent → Schema & DB Validation → SQL Execution → Result Normalization → API Response
```

## Core Components

### Database Manager

The `DatabaseManager` class (located in `database/db_manager.py`) serves as the central hub for all database interactions, handling connection management, schema extraction, and query execution with intelligent error handling.

#### Connection Management

```python
def get_connection(self, db_config):
    """
    Get or create a database connection based on configuration
    """
    # Generate a unique key for this connection
    conn_key = json.dumps(db_config, sort_keys=True)
    
    # Return existing connection if available
    if conn_key in self.connections:
        return self.connections[conn_key]
    
    # Create new connection
    try:
        conn_string = self.get_connection_string(db_config)
        engine = create_engine(conn_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        # Store connection
        self.connections[conn_key] = engine
        return engine
    except Exception as e:
        raise ConnectionError(f"Failed to connect to database: {str(e)}")
```

This method implements an efficient connection pooling pattern that:
- Creates a unique key for each database configuration
- Reuses existing connections when possible
- Performs a lightweight connection test before returning the engine
- Properly handles connection errors with meaningful error messages

#### Foreign Key Detection

```python
def get_foreign_keys(self, engine, refresh=False):
    """
    Get all foreign key relationships in the database
    """
    # Use cached data if available and refresh is not requested
    engine_id = str(id(engine))
    if engine_id in self.foreign_keys_cache and not refresh:
        return self.foreign_keys_cache[engine_id]
    
    foreign_keys = {}
    inspector = inspect(engine)
    
    # Get list of tables
    tables = inspector.get_table_names()
    
    for table in tables:
        # Get foreign key constraints for this table
        fkeys = inspector.get_foreign_keys(table)
        
        if fkeys:
            foreign_keys[table] = []
            
            for fkey in fkeys:
                foreign_keys[table].append({
                    'constrained_columns': fkey['constrained_columns'],
                    'referred_table': fkey['referred_table'],
                    'referred_columns': fkey['referred_columns']
                })
    
    # Cache the results
    self.foreign_keys_cache[engine_id] = foreign_keys
    return foreign_keys
```

This method intelligently extracts and caches foreign key relationships:
- Uses SQLAlchemy's inspection API to gather structural database information
- Implements a caching mechanism to improve performance on repeated calls
- Organizes foreign key data in a structured format optimized for later use
- Provides a refresh option to force reloading when schema might have changed

#### Schema Extraction and Description

The `get_tables_schema` and `get_tables_description` methods extract comprehensive metadata about database tables and present them in user-friendly formats:

```python
def get_tables_description(self, engine):
    """Generate a description of all tables in the database for the agent"""
    description = "Allows you to perform SQL queries on the tables. Returns a string representation of the result.\nIt can use the following tables:"

    # Get schema info with foreign keys
    schema_info = self.get_tables_schema(engine)
    
    for table_name, table_info in schema_info.items():
        columns_info = [(col["name"], col["type"]) for col in table_info["columns"]]
        
        table_description = f"\n\nTable '{table_name}':\nColumns:"
        for name, col_type in columns_info:
            # Mark primary keys
            is_pk = name in table_info["primary_keys"]
            pk_indicator = " (Primary Key)" if is_pk else ""
            
            # Check if it's a foreign key
            is_fk = False
            fk_reference = ""
            for fk in table_info.get("foreign_keys", []):
                if name in fk["constrained_columns"]:
                    is_fk = True
                    ref_table = fk["referred_table"]
                    ref_col = fk["referred_columns"][fk["constrained_columns"].index(name)]
                    fk_reference = f" (Foreign Key to {ref_table}.{ref_col})"
                    break
            
            table_description += f"\n  - {name}: {col_type}{pk_indicator}{fk_reference}"
        
        description += table_description
    
    # Add relationships section
    description += "\n\nRelationships between tables:"
    relationship_found = False
    
    for table_name, table_info in schema_info.items():
        for fk in table_info.get("foreign_keys", []):
            relationship_found = True
            constrained_col = fk["constrained_columns"][0]  # Simplify for first column
            referred_table = fk["referred_table"]
            referred_col = fk["referred_columns"][0]  # Simplify for first column
            
            description += f"\n  - {table_name}.{constrained_col} references {referred_table}.{referred_col}"
    
    if not relationship_found:
        description += "\n  No foreign key relationships detected."
    
    return description
```

This provides a human-readable description of database tables including:
- Detailed column information with types
- Clear identification of primary and foreign keys
- Explicit documentation of relationships between tables
- A structured format optimized for LLM consumption

#### Smart JOIN Generation

The system can automatically generate JOIN queries based on foreign key relationships:

```python
def suggest_join_query(self, engine, main_table):
    """Generate a suggested JOIN query for a table with its related tables"""
    schema_info = self.get_tables_schema(engine)
    foreign_keys = self.get_foreign_keys(engine)
    
    if main_table not in schema_info:
        return f"Table '{main_table}' not found in the schema."
        
    if main_table not in foreign_keys and not self._is_referenced_by_others(main_table, foreign_keys):
        return f"No foreign key relationships found for table '{main_table}'."
        
    # Generate a query that joins the main table with all related tables
    query = f"SELECT {main_table}.*"
    joins = []
    
    # Add columns from related tables
    table_alias_counter = {}
    
    # Handle foreign keys from this table to others
    if main_table in foreign_keys:
        for fk in foreign_keys[main_table]:
            referred_table = fk["referred_table"]
            
            # Generate a unique alias for the referred table
            if referred_table not in table_alias_counter:
                table_alias_counter[referred_table] = 1
                alias = referred_table[0]  # First letter
            else:
                table_alias_counter[referred_table] += 1
                alias = f"{referred_table[0]}{table_alias_counter[referred_table]}"
            
            # Add columns from the referred table
            if referred_table in schema_info:
                for col in schema_info[referred_table]["columns"]:
                    col_name = col["name"]
                    # Skip joining table's primary key to avoid duplicate column names
                    if col_name != fk["referred_columns"][0]:
                        query += f", {alias}.{col_name} AS {referred_table}_{col_name}"
            
            # Create the JOIN clause
            constrained_col = fk["constrained_columns"][0]
            referred_col = fk["referred_columns"][0]
            joins.append(f"LEFT JOIN {referred_table} AS {alias} ON {main_table}.{constrained_col} = {alias}.{referred_col}")
    
    # Handle foreign keys from other tables to this one
    for other_table, fks in foreign_keys.items():
        if other_table == main_table:
            continue
            
        for fk in fks:
            if fk["referred_table"] == main_table:
                # Generate a unique alias for the other table
                if other_table not in table_alias_counter:
                    table_alias_counter[other_table] = 1
                    alias = other_table[0]  # First letter
                else:
                    table_alias_counter[other_table] += 1
                    alias = f"{other_table[0]}{table_alias_counter[other_table]}"
                
                # Add columns from the other table
                if other_table in schema_info:
                    for col in schema_info[other_table]["columns"]:
                        col_name = col["name"]
                        # Skip the foreign key to avoid redundancy
                        if col_name != fk["constrained_columns"][0]:
                            query += f", {alias}.{col_name} AS {other_table}_{col_name}"
                
                # Create the JOIN clause
                constrained_col = fk["constrained_columns"][0]
                referred_col = fk["referred_columns"][0]
                joins.append(f"LEFT JOIN {other_table} AS {alias} ON {alias}.{constrained_col} = {main_table}.{referred_col}")
    
    # Complete the query
    query += f"\nFROM {main_table}"
    for join in joins:
        query += f"\n{join}"
    
    return query + ";"
```

This algorithm builds sophisticated JOIN queries with several key features:
- Bidirectional relationship mapping (both outbound and inbound foreign keys)
- Intelligent table aliasing to prevent naming conflicts
- Column selection with appropriate prefixing to avoid ambiguity
- LEFT JOIN preference to ensure all primary table records are included

### Schema Validator

The `SchemaValidator` class (in `database/schema_validator.py`) provides robust query validation, error detection, and data normalization:

#### Query Validation and Adaptation

```python
def validate_and_adapt_query(self, query: str) -> Tuple[str, List[str]]:
    """
    Validate and adapt a SQL query to match the actual database schema
    
    Args:
        query: The SQL query to validate and adapt
        
    Returns:
        Tuple of (adapted_query, warning_messages)
    """
    warnings = []
    
    # Clean the query
    clean_query = query.replace("```sql", "").replace("```", "").strip()
    
    # Handle different query types
    query_type = self._detect_query_type(clean_query)
    
    if query_type == "SELECT":
        return self._adapt_select_query(clean_query, warnings)
    elif query_type in ["INSERT", "UPDATE", "DELETE"]:
        return self._adapt_data_modification_query(clean_query, query_type, warnings)
    else:
        # For other query types, just return as is
        return clean_query, warnings
```

This method forms the foundation of the query validation system:
- Automatically identifies the query type (SELECT, INSERT, UPDATE, DELETE)
- Routes to specialized handling based on query type
- Returns both the adapted query and warnings that might help users
- Handles markdown code formatting that might come from LLM outputs

The specialized handlers provide type-specific validation:

```python
def _adapt_select_query(self, query: str, warnings: List[str]) -> Tuple[str, List[str]]:
    """
    Adapt a SELECT query to match the actual database schema
    """
    # Extract table references from FROM clause
    table_matches = re.findall(r'FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
    
    # Also handle JOIN clauses
    join_matches = re.findall(r'JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
    
    # Process table references
    for match in table_matches + join_matches:
        table_name = match[0]
        alias = match[1] if len(match) > 1 and match[1] else table_name
        
        actual_table = self.get_actual_table_name(table_name)
        
        if actual_table and actual_table != table_name:
            # Replace table name in the query
            # ... (replacement logic)
            
    # Handle column references in SELECT, WHERE, etc.
    # ... (column validation logic)
    
    return query, warnings
```

#### Levenshtein Distance for Fuzzy Matching

To accommodate slight spelling errors in table or column names, the system implements a Levenshtein distance algorithm:

```python
def _levenshtein_distance(self, s1: str, s2: str) -> int:
    """
    Calculate the Levenshtein distance between two strings
    
    Args:
        s1: First string
        s2: Second string
        
    Returns:
        The Levenshtein distance between the two strings
    """
    if len(s1) < len(s2):
        return self._levenshtein_distance(s2, s1)
        
    if len(s2) == 0:
        return len(s1)
        
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
        
    return previous_row[-1]
```

This implementation:
- Calculates edit distance between strings for fuzzy matching
- Optimizes the algorithm with row reuse for better performance
- Is used to suggest corrections for misspelled table or column names
- Applies a threshold to ensure only reasonably close matches are suggested

#### Foreign Key Resolution

One of the most powerful features is the ability to automatically resolve foreign key references:

```python
def resolve_foreign_keys(self, data_rows: List[Dict], main_table: str) -> List[Dict]:
    """
    Resolve foreign key references in data rows
    
    Args:
        data_rows: List of data dictionaries
        main_table: The main table these rows are from
        
    Returns:
        List of enriched data dictionaries with resolved foreign key references
    """
    if not data_rows or main_table not in self.foreign_key_map:
        return data_rows
        
    enriched_rows = []
    
    # Get foreign key relationships for this table
    fk_relationships = self.foreign_key_map.get(main_table, {})
    
    # Get foreign keys that reference this table
    reverse_references = self._get_reverse_references(main_table)
    
    # For each data row, resolve the foreign keys
    for row in data_rows:
        enriched_row = row.copy()
        
        # Process each foreign key
        for fk_column, fk_info in fk_relationships.items():
            if fk_column in row and row[fk_column] is not None:
                # Get the foreign key value
                fk_value = row[fk_column]
                
                # Get the referenced table and column
                referred_table = fk_info['referred_table']
                referred_column = fk_info['referred_column']
                
                # Fetch the referenced row
                referenced_row = self.fetch_referenced_row(referred_table, referred_column, fk_value)
                
                if referenced_row:
                    # Get descriptive field for display
                    display_field = self.get_display_field(referred_table, referenced_row)
                    display_value = referenced_row.get(display_field, str(fk_value))
                    
                    # Add resolved reference to the enriched row
                    enriched_row[f"{fk_column}_display"] = display_value
                    
                    # Create a new field with the related table name
                    relation_key = referred_table if "_id" in fk_column else fk_column.replace("_id", "")
                    enriched_row[relation_key] = {
                        "id": fk_value,
                        "table": referred_table,
                        "display": display_value,
                        **{k: v for k, v in referenced_row.items() if k != referred_column}
                    }
                    
                    # Format the original FK field for display
                    enriched_row[fk_column] = f"{fk_value} ({display_value})"
        
        # Add reverse references (other tables that reference this row)
        if reverse_references:
            related_collections = {}
            
            # For each table that references this one
            for ref_table, ref_info in reverse_references.items():
                local_column = ref_info['referred_column']  # Column in this table
                foreign_column = ref_info['local_column']   # Column in the referencing table
                
                if local_column in row and row[local_column] is not None:
                    # Fetch rows that reference this one
                    related_rows = self.fetch_related_rows(ref_table, foreign_column, row[local_column])
                    
                    if related_rows:
                        # Process into a simpler format
                        simple_related = []
                        for related_row in related_rows:
                            display_field = self.get_display_field(ref_table, related_row)
                            display_value = related_row.get(display_field, f"ID: {related_row.get('id', 'Unknown')}")
                            simple_related.append({
                                "id": related_row.get('id'),
                                "display": display_value,
                                "data": related_row
                            })
                        
                        # Add to the collections
                        if simple_related:
                            related_collections[ref_table] = simple_related
            
            # Only add if we found any related items
            if related_collections:
                enriched_row['related_collections'] = related_collections
        
        enriched_rows.append(enriched_row)
    
    return enriched_rows
```

This sophisticated foreign key resolution system:
- Enriches data by fetching related records from referenced tables
- Includes both direct references (outbound foreign keys) and reverse references (inbound foreign keys)
- Intelligently selects display fields for human-readable outputs
- Creates a consistent, hierarchical structure for related data
- Optimizes database access with targeted queries

#### Fallback Query Generation

When a query fails, the system attempts to generate a fallback query:

```python
def _execute_fallback_query(self, original_query: str, error_message: str) -> Optional[Dict[str, Any]]:
    """
    Generate and execute a fallback query when the original query fails
    
    Args:
        original_query: The original failed query
        error_message: The error message from the failed query
        
    Returns:
        Result dictionary if successful, None otherwise
    """
    try:
        # Extract main table and problematic entities from the query and error
        tables_in_query = self._extract_tables_from_query(original_query)
        
        # Check for unknown column errors
        unknown_column_match = re.search(r"Unknown column '([^']+)'", error_message)
        problematic_column = None
        problematic_table_alias = None
        
        if unknown_column_match:
            problematic_column = unknown_column_match.group(1)
            # If it's a qualified column (table.column), extract the table alias and column name
            if '.' in problematic_column:
                parts = problematic_column.split('.')
                problematic_table_alias = parts[0]
                problematic_column = parts[1]
        
        # Check for unknown table errors
        unknown_table_match = re.search(r"Table '([^']+)' doesn't exist", error_message)
        problematic_table = None
        if unknown_table_match:
            problematic_table = unknown_table_match.group(1)
        
        # If no tables were found in the query, or if the problematic table was identified
        # and not in the tables list, add it
        if problematic_table and problematic_table not in tables_in_query:
            tables_in_query.append(problematic_table)
        
        # No tables to work with, can't generate fallback
        if not tables_in_query:
            return None
        
        # Try to generate a valid fallback query
        valid_tables = []
        for table in tables_in_query:
            actual_table = self.get_actual_table_name(table)
            if actual_table:
                valid_tables.append(actual_table)
        
        # If no valid tables found, can't generate fallback
        if not valid_tables:
            return None
        
        # Generate a simple query for the first valid table
        main_table = valid_tables[0]
        
        # For Unknown column errors, try to provide feedback on all available columns
        if unknown_column_match:
            available_columns = self._get_available_columns(main_table)
            column_info = f"\n\nAvailable columns in {main_table}:\n" + "\n".join([f"- {col}" for col in available_columns])
            
            if problematic_table_alias and problematic_column:
                error_detail = f"The column '{problematic_column}' doesn't exist in table '{main_table}' (aliased as '{problematic_table_alias}').{column_info}"
            else:
                error_detail = f"Unknown column error. {column_info}"
        else:
            error_detail = ""
        
        # Build a query with all columns from this table
        fallback_query = f"SELECT * FROM {main_table} LIMIT 5;"
        
        # Execute the fallback query
        with self.engine.connect() as conn:
            result = conn.execute(text(fallback_query))
            rows = result.fetchall()
            
            # Format results
            if not rows:
                return {"result": f"Fallback query executed successfully, but no rows returned.\n\nFallback query: {fallback_query}{error_detail}", "data": []}
            
            # Get column names
            column_names = result.keys()
            
            # Format results as a table
            header = " | ".join(column_names)
            separator = "-" * len(header)
            output = f"Fallback query executed instead of the original query.\n\nFallback query: {fallback_query}{error_detail}\n\n{header}\n{separator}"
            
            # Convert rows to list of dicts for the data field
            data = []
            for row in rows:
                # Basic row values for output
                row_values = [str(value) for value in row]
                output += f"\n{' | '.join(row_values)}"
                
                # Build data row
                data_row = {}
                for i, col in enumerate(column_names):
                    data_row[col] = row[i]
                data.append(data_row)
            
            # Resolve foreign key references for the data
            normalized_data = self.resolve_foreign_keys(data, main_table)
            
            return {
                "result": output, 
                "data": data,
                "normalized_data": normalized_data,
                "fallback_query": fallback_query
            }
            
    except Exception as e:
        # If the fallback query also fails, just return None
        return None
```

This error recovery mechanism:
- Analyzes error messages to identify problematic tables or columns
- Uses fuzzy matching to find valid alternatives for misspelled entities
- Generates a simplified query that is likely to succeed
- Provides educational feedback about available schema options
- Ensures users get some useful data even when their original query fails

### Dynamic Agent

The `DynamicAgent` class (in `agent/dynamic_agent.py`) serves as the bridge between natural language and database operations:

#### Dynamic Prompt Generation

```python
def generate_prompt(self, user_query):
    """
    Generate a prompt for the language model with dynamic join hints
    and detailed schema validation instructions
    """
    # Build a compact representation of available tables and columns
    table_column_info = ""
    for table_name, table_info in self.schema.items():
        columns = [col['name'] for col in table_info['columns']]
        table_column_info += f"\n- Table '{table_name}' has columns: {', '.join(columns)}"
    
    return f"""You are a helpful SQL assistant. Your job is to convert a natural language question into a valid SQL query.

Database information:
{self.tables_description}

Available tables and columns:{table_column_info}

Foreign key relationships:
{self.join_hints}

User's question: {user_query}

IMPORTANT INSTRUCTIONS:
1. Think step by step about what SQL query would best answer this question.
2. ONLY use tables and columns that actually exist in the database schema provided above.
3. Double-check all table and column names to ensure they match exactly what's in the schema.
4. When the query involves multiple tables, use JOIN clauses based on the foreign key relationships provided.
5. Always use table aliases when joining tables (e.g., 'projects AS p').
6. Always qualify column names with their table aliases (e.g., 'p.id', not just 'id').
7. If the request is for data from a table that has foreign keys, automatically join with the related tables.
8. Use LEFT JOIN instead of INNER JOIN by default to ensure all primary table records are included.
9. When joining multiple tables, create meaningful aliases like 'p' for 'projects', 'c' for 'categories', etc.
10. Include a semicolon at the end of your query.
11. ONLY provide the SQL query itself without any markdown formatting, explanations, or additional text.
12. Do NOT use triple backticks or markdown formatting.

Now, provide ONLY the SQL query for the user's question above."""
```

This LLM prompt:
- Includes comprehensive database schema information
- Provides specific instructions for generating valid SQL
- Emphasizes best practices like table aliasing and column qualification
- Instructs the model to focus solely on the SQL query itself
- Is dynamically generated with the current database schema

#### Query Extraction and Enhancement

```python
def enhance_query_with_joins(self, query):
    """
    Enhance a query with appropriate JOINs if it doesn't already have them
    and the main table has foreign key relationships.
    """
    # If no foreign key relationships exist, return the original query
    if not self.foreign_keys:
        return query
    
    # Clean the query
    cleaned_query = query.replace("```sql", "").replace("```", "").strip()
    
    # Detect query type
    query_type = self._detect_query_type(cleaned_query)
    
    # Only enhance SELECT queries that don't already have JOINs
    if query_type != "SELECT" or self._has_join(cleaned_query):
        return cleaned_query
    
    # Extract the main table
    main_table = self._extract_main_table(cleaned_query)
    if not main_table or main_table not in self.schema:
        return cleaned_query
    
    # Check if the table has foreign keys or is referenced by foreign keys
    has_fk_relations = main_table in self.foreign_keys or self._is_referenced_by_others(main_table)
    if not has_fk_relations:
        return cleaned_query
    
    # Generate a suggested join query
    suggested_query = self.db_manager.suggest_join_query(self.engine, main_table)
    
    # If we couldn't generate a suggested query, return the original
    if not suggested_query or suggested_query.startswith("Table") or suggested_query.startswith("No foreign key"):
        return cleaned_query
    
    # Try to preserve WHERE, ORDER BY, LIMIT clauses from the original query
    # Extract SELECT part from the original query
    original_select = None
    if "SELECT " in cleaned_query.upper() and " FROM " in cleaned_query.upper():
        original_select = cleaned_query.upper().split("SELECT ")[1].split(" FROM ")[0].strip()
    
    # Extract clauses after the FROM and table name
    after_from = None
    if " FROM " in cleaned_query.upper() and main_table.upper() in cleaned_query.upper():
        from_parts = cleaned_query.upper().split(" FROM ")[1]
        after_table_parts = from_parts.split(main_table.upper(), 1)
        if len(after_table_parts) > 1:
            after_from = after_table_parts[1].strip()
    
    # If the original query has a custom SELECT part, try to preserve it
    if original_select and original_select != "*" and "SELECT " in suggested_query.upper():
        suggested_query = suggested_query.replace(suggested_query.split("SELECT ")[1].split(" FROM ")[0].strip(), original_select)
    
    # If the original query has clauses after FROM, try to preserve them
    if after_from and " WHERE " not in suggested_query.upper():
        # Remove semicolon from suggested query if it exists
        if suggested_query.endswith(";"):
            suggested_query = suggested_query[:-1]
        
        # Add the original clauses
        if after_from.startswith("WHERE") or after_from.startswith("ORDER") or after_from.startswith("LIMIT"):
            suggested_query += " " + after_from
        else:
            # If it doesn't start with a clause keyword, assume it might be a WHERE clause without the keyword
            suggested_query += " WHERE " + after_from
        
        # Ensure the query ends with a semicolon
        if not suggested_query.endswith(";"):
            suggested_query += ";"
    
    return suggested_query
```

This query enhancement system:
- Detects when a query could benefit from automatic JOIN inclusion
- Preserves the original query's conditions (WHERE, ORDER BY, etc.)
- Intelligently merges the original query with the enhanced JOIN structure
- Only applies enhancements when they would improve the query
- Maintains the original intent of the query while adding relationship information

#### Main Execution Logic

```python
def run(self, query, normalize_results=True):
    """
    Run the agent with a natural language query
    
    Args:
        query: Natural language query from the user
        normalize_results: Whether to normalize results by including related table data
            
    Returns:
        Dictionary with the original query, generated SQL, and results including normalized data
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
            return {
                "user_query": query,
                "error": f"Error calling Ollama API: {response.text}",
                "status_code": response.status_code
            }
        
        # 3. Extract the generated text
        generated_text = response.json().get("response", "")
        
        # 4. Extract SQL query from the response
        sql_query = self.extract_sql_query(generated_text)
        
        # 5. Enhance the query with JOINs if needed and applicable
        enhanced_query = self.enhance_query_with_joins(sql_query)
        
                 # 6. Execute the query with full result data
        try:
            # Get the full result object with enhanced data
            result_obj = self.sql_engine(enhanced_query, return_full_result=True)
            
            # Extract the text result for backward compatibility
            result = result_obj["result"]
            
            # Extract data and normalized data if available
            data = result_obj.get("data", [])
            normalized_data = result_obj.get("normalized_data", [])
            
            # If normalization is requested but no normalized data available yet,
            # extract the main table and get normalized data directly
            if normalize_results and not normalized_data and data:
                main_table = self._extract_main_table(enhanced_query)
                if main_table and main_table in self.schema:
                    # Get the schema validator for this engine
                    engine_id = str(id(self.engine))
                    if engine_id in self.db_manager.schema_validators:
                        schema_validator = self.db_manager.schema_validators[engine_id]
                        normalized_data = schema_validator.resolve_foreign_keys(data, main_table)
                    else:
                        # Try to get normalized data from the db_manager directly
                        normalized_result = self.db_manager.get_normalized_data(
                            self.engine, main_table, limit=100
                        )
                        if normalized_result and "normalized_data" in normalized_result:
                            normalized_data = normalized_result["normalized_data"]
            
            # 7. Format and return the response with all available data
            response_data = {
                "user_query": query,
                "sql_query": enhanced_query,
                # "result": result,
                "data": data
            }
            
            # Add normalized data if available
            if normalized_data:
                response_data["normalized_data"] = normalized_data
            
            # Add debug info if query was enhanced
            if enhanced_query != sql_query:
                response_data["original_query"] = sql_query
                response_data["note"] = "The original query was enhanced with JOINs based on foreign key relationships."
            
            return response_data
                
        except Exception as e:
            # Extract the main table from the query, if possible
            main_table = self._extract_main_table(enhanced_query)
            
            if main_table and main_table in self.schema:
                # Get normalized data for this table
                if normalize_results:
                    normalized_result = self.db_manager.get_normalized_data(
                        self.engine, main_table, limit=100
                    )
                    
                    # Log the fallback
                    fallback_note = f"The original query failed: {str(e)}. Using a normalized query instead."
                    
                    # Return the normalized data
                    return {
                        "user_query": query,
                        "sql_query": normalized_result.get("sql_query", ""),
                        "original_query": enhanced_query,
                        "result": normalized_result.get("result", ""),
                        "data": normalized_result.get("data", []),
                        "normalized_data": normalized_result.get("normalized_data", []),
                        "note": fallback_note
                    }
                else:
                    # Just use a simple query
                    simple_query = f"SELECT * FROM {main_table};"
                    
                    # Log the fallback
                    fallback_note = f"The original query failed: {str(e)}. Using a simplified query."
                    
                    # Execute the query
                    result_obj = self.sql_engine(simple_query, return_full_result=True)
                    
                    # Return the result
                    return {
                        "user_query": query,
                        "sql_query": simple_query,
                        "original_query": enhanced_query,
                        "result": result_obj.get("result", ""),
                        "data": result_obj.get("data", []),
                        "normalized_data": result_obj.get("normalized_data", []),
                        "note": fallback_note
                    }
            else:
                # If we can't extract a main table, raise the original error
                raise
                
        except Exception as e:
            return {
                "user_query": query,
                "error": f"Error running agent: {str(e)}",
                "fallback_query": "SHOW TABLES;",
                "fallback_result": self.sql_engine("SHOW TABLES;")
            }