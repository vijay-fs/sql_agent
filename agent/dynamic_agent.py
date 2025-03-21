import json
import requests
import re
from sqlalchemy import text, exc
from typing import Dict, List, Any

class DynamicAgent:
    def __init__(self, db_manager, db_config, model_name="llama3"):
        """
        Initialize an agent that uses Ollama for language model inference
        with dynamic database connections and automatic foreign key detection.
        
        Args:
            db_manager: EnhancedDatabaseManager instance for handling connections
            db_config: Database configuration dictionary
            model_name: Name of the Ollama model to use (default: llama3)
        """
        self.db_manager = db_manager
        self.db_config = db_config
        self.model_name = model_name
        self.ollama_url = "http://localhost:11434/api/generate"
        
        # Get engine and schema description
        self.engine = self.db_manager.get_connection(db_config)
        self.tables_description = self.db_manager.get_tables_description(self.engine)
        self.schema = self.db_manager.get_tables_schema(self.engine)
        self.foreign_keys = self.db_manager.get_foreign_keys(self.engine)
        self.join_hints = self.db_manager.generate_join_hints(self.engine)
        
        # Map of table names to their possible column names
        self.table_columns = self._build_table_columns_map()
    
    def _build_table_columns_map(self):
        """Build a map of table names to their columns"""
        table_columns = {}
        for table_name, table_info in self.schema.items():
            columns = [col['name'] for col in table_info['columns']]
            table_columns[table_name] = columns
        return table_columns
    
    def sql_engine(self, query, return_full_result=False):
        """
        Execute a SQL query and return the results
        
        Args:
            query: SQL query string
            return_full_result: Whether to return the full result dictionary with enhanced data
            
        Returns:
            String result or full result dictionary based on return_full_result
        """
        return self.db_manager.execute_query(self.engine, query, return_full_result)
    
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
                
            # Handle case where response ends with partial ``` markers
            elif "```sql" in response:
                sql_part = response.split("```sql")[1].strip()
                return sql_part
                
            # Fallback: just return the whole response with any markdown removed
            response = response.replace("```sql", "").replace("```", "").strip()
            return response
        except Exception:
            # Remove any markdown formatters as a last resort
            return response.replace("```sql", "").replace("```", "").strip()
    
    def _detect_query_type(self, query):
        """Detect the type of query (SELECT, INSERT, UPDATE, DELETE, etc.)"""
        query = query.strip().upper()
        if query.startswith("SELECT"):
            return "SELECT"
        elif query.startswith("INSERT"):
            return "INSERT"
        elif query.startswith("UPDATE"):
            return "UPDATE"
        elif query.startswith("DELETE"):
            return "DELETE"
        elif query.startswith("SHOW") or query.startswith("DESCRIBE"):
            return "SHOW"
        else:
            return "OTHER"
    
    def _extract_main_table(self, query):
        """Extract the main table from a SQL query"""
        # For SELECT queries
        if "FROM " in query.upper():
            after_from = query.upper().split("FROM ")[1].strip()
            # Extract table name (handles cases with WHERE, JOIN, etc.)
            table_parts = re.split(r'\s+', after_from, 1)
            table_name = table_parts[0].strip()
            # Remove any trailing comma or semicolon
            return table_name.rstrip(',;')
        
        # For UPDATE queries
        elif query.upper().startswith("UPDATE "):
            after_update = query.upper().split("UPDATE ")[1].strip()
            # Extract table name (handles cases with SET, WHERE, etc.)
            table_parts = re.split(r'\s+', after_update, 1)
            table_name = table_parts[0].strip()
            # Remove any trailing comma or semicolon
            return table_name.rstrip(',;')
        
        # For INSERT queries
        elif "INTO " in query.upper():
            after_into = query.upper().split("INTO ")[1].strip()
            # Extract table name (handles cases with VALUES, etc.)
            table_parts = re.split(r'\s+', after_into, 1)
            table_name = table_parts[0].strip()
            # Remove any trailing comma, semicolon, or parenthesis
            return table_name.rstrip(',;(')
        
        # For DELETE queries
        elif "FROM " in query.upper() and query.upper().startswith("DELETE"):
            after_from = query.upper().split("FROM ")[1].strip()
            # Extract table name (handles cases with WHERE, etc.)
            table_parts = re.split(r'\s+', after_from, 1)
            table_name = table_parts[0].strip()
            # Remove any trailing comma or semicolon
            return table_name.rstrip(',;')
        
        return None
    
    def _has_join(self, query):
        """Check if a query already has JOIN clauses"""
        return " JOIN " in query.upper()
    
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
    
    def _is_referenced_by_others(self, table):
        """Check if a table is referenced by other tables' foreign keys"""
        for other_table, fks in self.foreign_keys.items():
            for fk in fks:
                if fk["referred_table"] == table:
                    return True
        return False
    
    def build_normalized_query(self, table_name, include_related=True):
        """
        Build a query that returns normalized data for a table by automatically 
        including related tables through foreign key relationships
        
        Args:
            table_name: The main table to query
            include_related: Whether to include related tables
            
        Returns:
            SQL query string with appropriate JOINs
        """
        if not include_related or table_name not in self.schema:
            return f"SELECT * FROM {table_name};"
            
        # Use the db_manager's suggest_join_query to create a comprehensive JOIN query
        join_query = self.db_manager.suggest_join_query(self.engine, table_name)
        
        # If the suggested query generation fails, fall back to a simple query
        if not join_query or join_query.startswith("Table") or join_query.startswith("No foreign key"):
            return f"SELECT * FROM {table_name};"
            
        return join_query
    
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