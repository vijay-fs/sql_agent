import json
import requests
import re
from sqlalchemy import text, exc
from typing import Dict, List, Any

class DynamicOllamaAgent:
    def __init__(self, db_manager, db_config, model_name="llama3"):
        """
        Initialize an agent that uses Ollama for language model inference
        with dynamic database connections.
        
        Args:
            db_manager: DatabaseManager instance for handling connections
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
        
        # Map of table names to their possible column names
        self.table_columns = self._build_table_columns_map()
    
    def _build_table_columns_map(self):
        """Build a map of table names to their columns"""
        table_columns = {}
        for table_name, table_info in self.schema.items():
            columns = [col['name'] for col in table_info['columns']]
            table_columns[table_name] = columns
        return table_columns
    
    def sql_engine(self, query):
        """
        Execute a SQL query and return the results
        """
        return self.db_manager.execute_query(self.engine, query)
    
    def generate_prompt(self, user_query):
        """
        Generate a prompt for the language model
        """
        return f"""You are a helpful SQL assistant. Your job is to convert a natural language question into a valid SQL query.

Database information:
{self.tables_description}

User's question: {user_query}

IMPORTANT INSTRUCTIONS:
1. Think step by step about what SQL query would best answer this question.
2. ONLY provide the SQL query itself without any markdown formatting, explanations, or additional text.
3. Do NOT use triple backticks, markdown, or any other formatting.
4. Make sure to use standard SQL syntax that's compatible with MySQL.
5. Include a semicolon at the end of your query.
6. Always use table aliases when joining tables to avoid ambiguity.
7. ALWAYS qualify ALL column names with their table aliases (e.g., use 'users.id' or 'u.id', not just 'id').
8. When joining tables, make sure to properly alias all tables and qualify all column references.

For example, for "Show all users", you should output EXACTLY:
SELECT u.id, u.name, u.email FROM users u;

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
    
    def _extract_query_parts(self, query):
        """Extract different parts of a SQL query"""
        # Initialize with default values
        parts = {
            "select": "",
            "from": "",
            "where": "",
            "group_by": "",
            "having": "",
            "order_by": "",
            "limit": ""
        }
        
        # Clean the query
        query = query.replace("```sql", "").replace("```", "").strip()
        
        # Extract SELECT part
        if "SELECT " in query.upper():
            parts["select"] = query.upper().split("SELECT ")[1].split(" FROM ")[0].strip()
        
        # Extract FROM part
        if " FROM " in query.upper():
            after_from = query.upper().split(" FROM ")[1]
            
            # Extract WHERE if exists
            if " WHERE " in after_from:
                parts["from"] = after_from.split(" WHERE ")[0].strip()
                after_where = after_from.split(" WHERE ")[1]
                
                # Extract GROUP BY, HAVING, ORDER BY, LIMIT if they exist
                if " GROUP BY " in after_where:
                    parts["where"] = after_where.split(" GROUP BY ")[0].strip()
                    after_group_by = after_where.split(" GROUP BY ")[1]
                    
                    if " HAVING " in after_group_by:
                        parts["group_by"] = after_group_by.split(" HAVING ")[0].strip()
                        after_having = after_group_by.split(" HAVING ")[1]
                        
                        if " ORDER BY " in after_having:
                            parts["having"] = after_having.split(" ORDER BY ")[0].strip()
                            after_order_by = after_having.split(" ORDER BY ")[1]
                            
                            if " LIMIT " in after_order_by:
                                parts["order_by"] = after_order_by.split(" LIMIT ")[0].strip()
                                parts["limit"] = after_order_by.split(" LIMIT ")[1].strip().replace(";", "")
                            else:
                                parts["order_by"] = after_order_by.strip().replace(";", "")
                        else:
                            parts["having"] = after_having.strip().replace(";", "")
                    elif " ORDER BY " in after_group_by:
                        parts["group_by"] = after_group_by.split(" ORDER BY ")[0].strip()
                        after_order_by = after_group_by.split(" ORDER BY ")[1]
                        
                        if " LIMIT " in after_order_by:
                            parts["order_by"] = after_order_by.split(" LIMIT ")[0].strip()
                            parts["limit"] = after_order_by.split(" LIMIT ")[1].strip().replace(";", "")
                        else:
                            parts["order_by"] = after_order_by.strip().replace(";", "")
                    else:
                        parts["group_by"] = after_group_by.strip().replace(";", "")
                elif " ORDER BY " in after_where:
                    parts["where"] = after_where.split(" ORDER BY ")[0].strip()
                    after_order_by = after_where.split(" ORDER BY ")[1]
                    
                    if " LIMIT " in after_order_by:
                        parts["order_by"] = after_order_by.split(" LIMIT ")[0].strip()
                        parts["limit"] = after_order_by.split(" LIMIT ")[1].strip().replace(";", "")
                    else:
                        parts["order_by"] = after_order_by.strip().replace(";", "")
                elif " LIMIT " in after_where:
                    parts["where"] = after_where.split(" LIMIT ")[0].strip()
                    parts["limit"] = after_where.split(" LIMIT ")[1].strip().replace(";", "")
                else:
                    parts["where"] = after_where.strip().replace(";", "")
            elif " GROUP BY " in after_from:
                parts["from"] = after_from.split(" GROUP BY ")[0].strip()
                after_group_by = after_from.split(" GROUP BY ")[1]
                
                # Continue with similar logic as above...
                if " HAVING " in after_group_by:
                    parts["group_by"] = after_group_by.split(" HAVING ")[0].strip()
                    # ... and so on
            elif " ORDER BY " in after_from:
                parts["from"] = after_from.split(" ORDER BY ")[0].strip()
                # ... and so on
            elif " LIMIT " in after_from:
                parts["from"] = after_from.split(" LIMIT ")[0].strip()
                parts["limit"] = after_from.split(" LIMIT ")[1].strip().replace(";", "")
            else:
                parts["from"] = after_from.strip().replace(";", "")
                
        return parts
    
    def _extract_table_aliases(self, from_clause):
        """Extract table aliases from the FROM clause"""
        aliases = {}
        tables = re.findall(r'(\w+)(?:\s+(?:as\s+)?(\w+))?', from_clause, re.IGNORECASE)
        
        for match in tables:
            if len(match) >= 2 and match[1]:  # If there's an alias
                aliases[match[1].lower()] = match[0].lower()
            else:
                aliases[match[0].lower()] = match[0].lower()  # Use table name as its own alias
                
        # Also extract aliases from JOIN clauses
        join_pattern = r'(?:JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN)\s+(\w+)(?:\s+(?:as\s+)?(\w+))?'
        joins = re.findall(join_pattern, from_clause, re.IGNORECASE)
        
        for match in joins:
            if len(match) >= 2 and match[1]:  # If there's an alias
                aliases[match[1].lower()] = match[0].lower()
            else:
                aliases[match[0].lower()] = match[0].lower()  # Use table name as its own alias
                
        return aliases
    
    def _fix_ambiguous_columns(self, query):
        """Fix ambiguous column references in a query"""
        # Extract query parts
        parts = self._extract_query_parts(query)
        select_clause = parts["select"]
        from_clause = parts["from"]
        
        # Extract table aliases
        aliases = self._extract_table_aliases(from_clause)
        
        # No need to continue if there's no select clause or from clause
        if not select_clause or not from_clause:
            return query
            
        # Find column references in select clause
        column_refs = re.findall(r'([a-zA-Z0-9_]+)(?:\s+(?:as\s+)?[a-zA-Z0-9_]+)?', select_clause, re.IGNORECASE)
        
        # Find columns that might be ambiguous (not prefixed with an alias)
        ambiguous_columns = []
        for col in column_refs:
            if '.' not in col and col.lower() not in ['distinct', 'as', 'count', 'sum', 'avg', 'min', 'max']:
                ambiguous_columns.append(col)
        
        if not ambiguous_columns:
            return query
            
        # For each ambiguous column, find which table it belongs to
        for col in ambiguous_columns:
            # Find which tables have this column
            tables_with_col = []
            for alias, table in aliases.items():
                if table in self.table_columns and col in self.table_columns[table]:
                    tables_with_col.append(alias)
            
            # If only one table has this column, prefix it
            if len(tables_with_col) == 1:
                alias = tables_with_col[0]
                new_select = select_clause.replace(col, f"{alias}.{col}")
                query = query.replace(select_clause, new_select)
            # If multiple tables have this column, it's truly ambiguous
            elif len(tables_with_col) > 1:
                # Try to infer the correct table from context (table name matches column prefix)
                # For example, if column is "name" and we have tables "users" and "products",
                # we might prefer "users.name" if the query is about users
                
                # Simple heuristic: use the first table in the FROM clause
                first_alias = list(aliases.keys())[0] if aliases else None
                if first_alias:
                    new_select = select_clause.replace(col, f"{first_alias}.{col}")
                    query = query.replace(select_clause, new_select)
        
        return query
    
    def _fix_table_references(self, query):
        """Fix table references in a query based on available tables"""
        # Get available tables
        available_tables = list(self.schema.keys())
        
        # Extract FROM clause
        parts = self._extract_query_parts(query)
        from_clause = parts["from"]
        
        # Extract table references
        table_refs = re.findall(r'(\w+)(?:\s+(?:as\s+)?(\w+))?', from_clause, re.IGNORECASE)
        
        # Check if any tables don't exist
        for match in table_refs:
            table_name = match[0].lower()
            if table_name not in available_tables and table_name not in ['as', 'join', 'inner', 'left', 'right', 'full', 'on']:
                # Try to find a similar table
                similar_table = self._find_similar_table(table_name, available_tables)
                if similar_table:
                    query = query.replace(match[0], similar_table)
                    
        # Also check JOIN clauses
        join_pattern = r'(?:JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL JOIN)\s+(\w+)(?:\s+(?:as\s+)?(\w+))?'
        joins = re.findall(join_pattern, from_clause, re.IGNORECASE)
        
        for match in joins:
            table_name = match[0].lower()
            if table_name not in available_tables:
                # Try to find a similar table
                similar_table = self._find_similar_table(table_name, available_tables)
                if similar_table:
                    query = query.replace(match[0], similar_table)
        
        return query
    
    def _find_similar_table(self, table_name, available_tables):
        """Find a similar table name from available tables"""
        # Simple case: plural/singular forms
        if table_name.endswith('s') and table_name[:-1] in available_tables:
            return table_name[:-1]
        if table_name + 's' in available_tables:
            return table_name + 's'
            
        # Alternative approach: find table with most character overlap
        best_match = None
        best_score = 0
        for table in available_tables:
            # Calculate character overlap
            common_chars = set(table_name).intersection(set(table))
            score = len(common_chars) / max(len(table_name), len(table))
            
            if score > best_score:
                best_score = score
                best_match = table
                
        # Only return if it's a reasonably good match
        if best_score > 0.6:
            return best_match
            
        return None
    
    def sanitize_query(self, query, max_retries=3):
        """
        Attempt to sanitize and fix common SQL query issues
        """
        # First, handle basic syntax errors
        clean_query = query.replace("```sql", "").replace("```", "").strip()
        
        # Apply multiple fixes
        try:
            # Fix ambiguous column references
            clean_query = self._fix_ambiguous_columns(clean_query)
            
            # Fix table references
            clean_query = self._fix_table_references(clean_query)
            
            # Try the query
            try:
                with self.engine.connect() as con:
                    con.execute(text("EXPLAIN " + clean_query))
                return clean_query  # Query is valid, return it
            except Exception as e:
                error_msg = str(e)
                
                # Handle specific error types
                if "ambiguous" in error_msg.lower() and "column" in error_msg.lower():
                    # Try a more aggressive fix for ambiguous columns
                    # Extract the column name from the error message
                    column_match = re.search(r"Column '([^']+)' in .+ is ambiguous", error_msg)
                    if column_match and max_retries > 0:
                        ambiguous_column = column_match.group(1)
                        
                        # Try to find which tables have this column
                        tables_with_col = []
                        for table, columns in self.table_columns.items():
                            if ambiguous_column in columns:
                                tables_with_col.append(table)
                        
                        # If we found tables with this column, try to fix the query
                        if tables_with_col:
                            # Extract table aliases
                            from_clause = self._extract_query_parts(clean_query)["from"]
                            aliases = self._extract_table_aliases(from_clause)
                            
                            # Find the first table alias that contains this column
                            for alias, table in aliases.items():
                                if table in tables_with_col:
                                    # Replace the ambiguous column with a qualified reference
                                    pattern = r'\b' + re.escape(ambiguous_column) + r'\b(?!\s*\.\s*\w+)'
                                    clean_query = re.sub(pattern, f"{alias}.{ambiguous_column}", clean_query)
                                    break
                            
                            # Try the modified query
                            return self.sanitize_query(clean_query, max_retries-1)
                
                # If we've exhausted retries or can't fix specific errors, try a more general approach
                if max_retries > 0:
                    # Simplify the query if it's complex
                    if " JOIN " in clean_query:
                        # Extract the main table
                        from_parts = self._extract_query_parts(clean_query)["from"].split(" ")[0]
                        main_table = from_parts.strip()
                        
                        # Create a simple query
                        if main_table:
                            simple_query = f"SELECT * FROM {main_table};"
                            return simple_query
                
                # If all else fails and retries are exhausted, return a safe query
                return "SHOW TABLES;"
        except Exception:
            # If there's an error in our fixing logic, return a safe query
            if max_retries > 0:
                # Try a simpler approach
                try:
                    # Simply return a SELECT * query for the first table mentioned
                    match = re.search(r'FROM\s+(\w+)', clean_query, re.IGNORECASE)
                    if match:
                        table = match.group(1)
                        return f"SELECT * FROM {table};"
                except:
                    pass
            
            # Last resort
            return "SHOW TABLES;"
    
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
            
            # 5. Attempt to sanitize and fix the query if needed
            sanitized_query = self.sanitize_query(sql_query)
            
            # 6. Execute the sanitized query
            result = self.sql_engine(sanitized_query)
            
            # 7. Format and return the response
            response_data = {
                "user_query": query,
                "sql_query": sanitized_query,
                "result": result
            }
            
            # Add debug info if query was sanitized
            if sanitized_query != sql_query:
                response_data["original_query"] = sql_query
                response_data["note"] = "The original query was modified to work with your database schema."
            
            return response_data
            
        except Exception as e:
            return {
                "user_query": query,
                "error": f"Error running Ollama agent: {str(e)}",
                "fallback_query": "SHOW TABLES;",
                "fallback_result": self.sql_engine("SHOW TABLES;")
            }