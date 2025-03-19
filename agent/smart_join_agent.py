import json
import requests
import re
from sqlalchemy import text, exc
from typing import Dict, List, Any

class SmartJoinAgent:
    def __init__(self, db_manager, db_config, model_name="llama3"):
        """
        Initialize an agent that uses Ollama for language model inference
        with smart join detection based on actual table structure.
        
        Args:
            db_manager: Database manager instance
            db_config: Database configuration dictionary
            model_name: Name of the Ollama model to use (default: llama3)
        """
        self.db_manager = db_manager
        self.db_config = db_config
        self.model_name = model_name
        self.ollama_url = "http://localhost:11434/api/generate"
        
        # Get engine and schema description
        self.engine = self.db_manager.get_connection(db_config)
        
        # Initialize smart join detector
        from database.smart_join_detector import SmartJoinDetector
        self.join_detector = SmartJoinDetector(self.engine)
        
        # Get schema info and join hints
        self.tables_description = self._generate_tables_description()
        self.schema = self.db_manager.get_tables_schema(self.engine)
        self.table_columns = self._build_table_columns_map()
    
    def _build_table_columns_map(self):
        """Build a map of table names to their columns"""
        table_columns = {}
        for table_name, table_info in self.schema.items():
            columns = [col['name'] for col in table_info['columns']]
            table_columns[table_name] = columns
        return table_columns
    
    def _generate_tables_description(self):
        """Generate a description of tables including discovered relationships"""
        inspector = self.join_detector.inspector
        description = "Database structure with tables and their relationships:\n"
        
        # Add table descriptions
        for table_name in inspector.get_table_names():
            columns_info = [(col["name"], col["type"]) for col in inspector.get_columns(table_name)]
            
            table_description = f"\n\nTable '{table_name}':\nColumns:"
            for name, col_type in columns_info:
                table_description += f"\n  - {name}: {col_type}"
            
            description += table_description
        
        # Add discovered relationships
        description += "\n\nDiscovered relationships between tables:"
        
        for table_name in inspector.get_table_names():
            relationships = self.join_detector.get_relationships_for_table(table_name)
            
            if relationships:
                description += f"\n\nRelationships for '{table_name}':"
                
                for rel in relationships:
                    confidence = rel.get("confidence", "unknown")
                    source_col = rel["source_column"]
                    target_table = rel["target_table"]
                    target_col = rel["target_column"]
                    
                    description += f"\n  - {table_name}.{source_col} → {target_table}.{target_col} ({confidence} confidence)"
        
        return description
    
    def sql_engine(self, query, return_raw_data=False):
        """
        Execute a SQL query and return the results
        """
        return self.db_manager.execute_query(self.engine, query, return_raw_data=return_raw_data)
    
    def generate_prompt(self, user_query):
        """
        Generate a prompt for the language model with smart join hints
        """
        return f"""You are a helpful SQL assistant. Your job is to convert a natural language question into a valid SQL query.

Database information:
{self.tables_description}

User's question: {user_query}

IMPORTANT INSTRUCTIONS:
1. Think step by step about what SQL query would best answer this question.
2. Always use only the tables that actually exist in the database.
3. If the user asks about "employees" or "users", check which one of these tables exists in the database.
4. When the query involves multiple tables, use appropriate JOIN clauses based on the relationships described.
5. Always use table aliases when joining tables (e.g., 'projects AS p').
6. Always qualify column names with their table aliases (e.g., 'p.id', not just 'id').
7. Use LEFT JOIN instead of INNER JOIN by default to ensure all primary table records are included.
8. Identify the main table the user is asking about and make that the base of your query.
9. Include a semicolon at the end of your query.
10. ONLY provide the SQL query itself without any markdown formatting, explanations, or additional text.
11. Do NOT use triple backticks or markdown formatting.

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
            table_parts = re.split(r'[\s,;()]', after_from, 1)
            table_name = table_parts[0].strip()
            # Remove any trailing comma or semicolon
            return table_name.rstrip(',;')
        
        # For UPDATE queries
        elif query.upper().startswith("UPDATE "):
            after_update = query.upper().split("UPDATE ")[1].strip()
            # Extract table name (handles cases with SET, WHERE, etc.)
            table_parts = re.split(r'[\s,;()]', after_update, 1)
            table_name = table_parts[0].strip()
            # Remove any trailing comma or semicolon
            return table_name.rstrip(',;')
        
        # For INSERT queries
        elif "INTO " in query.upper():
            after_into = query.upper().split("INTO ")[1].strip()
            # Extract table name (handles cases with VALUES, etc.)
            table_parts = re.split(r'[\s,;()]', after_into, 1)
            table_name = table_parts[0].strip()
            # Remove any trailing comma, semicolon, or parenthesis
            return table_name.rstrip(',;(')
        
        # For DELETE queries
        elif "FROM " in query.upper() and query.upper().startswith("DELETE"):
            after_from = query.upper().split("FROM ")[1].strip()
            # Extract table name (handles cases with WHERE, etc.)
            table_parts = re.split(r'[\s,;()]', after_from, 1)
            table_name = table_parts[0].strip()
            # Remove any trailing comma or semicolon
            return table_name.rstrip(',;')
        
        return None
    
    def _has_join(self, query):
        """Check if a query already has JOIN clauses"""
        return " JOIN " in query.upper()
    
    def smartify_query(self, query):
        """
        Make a query smarter by validating or enhancing it based on actual schema
        """
        # Clean the query
        cleaned_query = query.replace("```sql", "").replace("```", "").strip()
        
        # Detect query type
        query_type = self._detect_query_type(cleaned_query)
        
        # Only enhance SELECT queries
        if query_type != "SELECT":
            return cleaned_query
        
        # Check for JOIN errors - Table doesn't exist
        if " JOIN " in cleaned_query.upper():
            # Extract table names from joins
            join_pattern = r'JOIN\s+(\w+)(?:\s+AS\s+(\w+))?'
            matches = re.findall(join_pattern, cleaned_query, re.IGNORECASE)
            
            # Get list of existing tables
            existing_tables = set(self.join_detector.inspector.get_table_names())
            
            # Check if all referenced tables exist
            missing_tables = []
            for match in matches:
                table_name = match[0].lower()
                if table_name not in existing_tables:
                    missing_tables.append(table_name)
            
            # If tables are missing, correct the query to use only existing tables
            if missing_tables:
                # Extract the main table
                main_table = self._extract_main_table(cleaned_query)
                if main_table and main_table.lower() in existing_tables:
                    return f"SELECT * FROM {main_table};"
                else:
                    # If main table doesn't exist either, use the first available table
                    if existing_tables:
                        first_table = list(existing_tables)[0]
                        return f"SELECT * FROM {first_table};"
                    else:
                        return cleaned_query
        
        # Option 1: If query has JOINs and all tables exist, validate them against actual schema
        if " JOIN " in cleaned_query.upper():
            validated_query = self.join_detector.validate_join_conditions(cleaned_query)
            return validated_query
        
        # Option 2: If query has no JOINs, consider adding them if appropriate
        else:
            # Extract the main table
            main_table = self._extract_main_table(cleaned_query)
            if not main_table:
                return cleaned_query
            
            # Make sure table exists
            existing_tables = set(self.join_detector.inspector.get_table_names())
            if main_table.lower() not in existing_tables:
                # Try to find a suitable replacement table
                replacement_found = False
                
                # If asking for employee table but it doesn't exist, try users table
                if main_table.lower() in ['employee', 'employees'] and 'users' in existing_tables:
                    main_table = 'users'
                    replacement_found = True
                # If asking for users table but it doesn't exist, try employees table
                elif main_table.lower() in ['user', 'users'] and any(t in existing_tables for t in ['employee', 'employees']):
                    if 'employees' in existing_tables:
                        main_table = 'employees'
                    else:
                        main_table = 'employee'
                    replacement_found = True
                
                if replacement_found:
                    # Replace the table name in the query
                    if re.match(r'^\s*SELECT\s+\*\s+FROM\s+\w+\s*;?\s*$', cleaned_query, re.IGNORECASE):
                        return f"SELECT * FROM {main_table};"
                    else:
                        # For more complex queries, try a simple replacement
                        original_main_table = self._extract_main_table(cleaned_query)
                        if original_main_table:
                            return cleaned_query.replace(original_main_table, main_table)
                else:
                    # If no replacement found and table doesn't exist, use the first available table
                    if existing_tables:
                        first_table = list(existing_tables)[0]
                        return f"SELECT * FROM {first_table};"
                    return cleaned_query
            
            # Check if main table exists and has relationships
            relationships = self.join_detector.get_relationships_for_table(main_table)
            if not relationships:
                return cleaned_query
            
            # If it's a simple "SELECT * FROM table", enhance it with smart joins
            if re.match(r'^\s*SELECT\s+\*\s+FROM\s+\w+\s*;?\s*$', cleaned_query, re.IGNORECASE):
                return self.join_detector.generate_join_query(main_table)
            
            # For more complex queries, try to preserve WHERE clauses and other parts
            simple_pattern = r'^\s*SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+AS\s+(\w+))?\s*(WHERE.+)?$'
            match = re.match(simple_pattern, cleaned_query, re.IGNORECASE|re.DOTALL)
            
            if match:
                select_part = match.group(1).strip()
                table_name = match.group(2).strip()
                alias = match.group(3).strip() if match.group(3) else table_name[0]
                where_part = match.group(4) if match.group(4) else ""
                
                # Get suggested join query from detector
                base_join_query = self.join_detector.generate_join_query(table_name, include_columns=False)
                
                # Remove trailing semicolon for appending WHERE clause
                if base_join_query.endswith(";"):
                    base_join_query = base_join_query[:-1]
                
                # If only requesting certain columns, append WHERE clause
                if where_part:
                    if where_part.endswith(";"):
                        where_part = where_part[:-1]
                    return base_join_query + " " + where_part + ";"
                else:
                    return base_join_query
            
            # If we couldn't enhance it, return the original query
            return cleaned_query
    
    def run(self, query, return_raw_data=False):
        """
        Run the agent with a natural language query
        
        Args:
            query: Natural language query from the user
            return_raw_data: Whether to include raw data in the response
            
        Returns:
            Dictionary with the original query, generated SQL, results and optionally raw data
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
            
            # 5. Make the query smarter by validating or enhancing it
            smart_query = self.smartify_query(sql_query)
            
            # 6. Execute the enhanced query
            result = self.sql_engine(smart_query, return_raw_data=return_raw_data)
            
            # 7. Format and return the response
            response_data = {
                "user_query": query,
                "sql_query": smart_query,
                "result": result.get("result", "")
            }
            
            # Add data if requested
            if return_raw_data:
                response_data["data"] = result.get("data", [])
            
            # Add debug info if query was enhanced
            if smart_query != sql_query:
                response_data["original_query"] = sql_query
                response_data["note"] = "The original query was enhanced based on actual database schema."
            
            return response_data
            
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            error_response = {
                "user_query": query,
                "error": f"Error running agent: {str(e)}",
                "traceback": trace
            }
            
            # Try to fall back to a simple query
            try:
                # Get list of tables
                tables = self.join_detector.inspector.get_table_names()
                if tables:
                    # Use the first table as a fallback
                    fallback_query = f"SELECT * FROM {tables[0]};"
                    fallback_result = self.sql_engine(fallback_query, return_raw_data=return_raw_data)
                    
                    error_response["fallback_query"] = fallback_query
                    error_response["result"] = fallback_result.get("result", "")
                    
                    if return_raw_data:
                        error_response["data"] = fallback_result.get("data", [])
                    
                    error_response["note"] = f"Original query failed, showing data from {tables[0]} instead."
            except:
                # If fallback fails too, just return the error
                pass
            
            return error_response