from sqlalchemy import inspect, text
import re
from typing import Dict, List, Any, Tuple, Optional

class SchemaValidator:
    """
    A utility class to validate and adapt SQL queries to match the actual database schema
    """
    def __init__(self, engine):
        """
        Initialize the schema validator with a database engine
        
        Args:
            engine: SQLAlchemy engine connected to the database
        """
        self.engine = engine
        self.inspector = inspect(engine)
        self.tables_info = self._get_tables_info()
        
    def _get_tables_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed information about all tables in the database
        
        Returns:
            Dict mapping table names to their column information
        """
        tables_info = {}
        
        for table_name in self.inspector.get_table_names():
            columns = {}
            for column in self.inspector.get_columns(table_name):
                columns[column['name']] = {
                    'type': str(column['type']),
                    'nullable': column.get('nullable', True)
                }
                
            tables_info[table_name] = {
                'columns': columns,
                'pk_columns': self.inspector.get_pk_constraint(table_name).get('constrained_columns', []),
                'foreign_keys': self.inspector.get_foreign_keys(table_name)
            }
            
        return tables_info
    
    def get_actual_table_name(self, table_name: str) -> Optional[str]:
        """
        Find the actual table name that most closely matches the provided name
        
        Args:
            table_name: The table name to find a match for
            
        Returns:
            The actual table name if found, None otherwise
        """
        # Direct match
        if table_name in self.tables_info:
            return table_name
            
        # Case-insensitive match
        for actual_table in self.tables_info.keys():
            if actual_table.lower() == table_name.lower():
                return actual_table
                
        # Check for plural/singular forms
        if table_name.endswith('s'):
            singular = table_name[:-1]
            if singular in self.tables_info:
                return singular
        else:
            plural = f"{table_name}s"
            if plural in self.tables_info:
                return plural
                
        # Use Levenshtein distance for approximate matching
        min_distance = float('inf')
        closest_match = None
        
        for actual_table in self.tables_info.keys():
            distance = self._levenshtein_distance(table_name.lower(), actual_table.lower())
            if distance < min_distance:
                min_distance = distance
                closest_match = actual_table
                
        # Only return if it's a reasonably good match
        if min_distance <= 3 or (min_distance / len(table_name)) < 0.3:
            return closest_match
            
        return None
    
    def get_actual_column_name(self, table_name: str, column_name: str) -> Optional[str]:
        """
        Find the actual column name that most closely matches the provided name
        
        Args:
            table_name: The table name the column belongs to
            column_name: The column name to find a match for
            
        Returns:
            The actual column name if found, None otherwise
        """
        if table_name not in self.tables_info:
            return None
            
        # Direct match
        if column_name in self.tables_info[table_name]['columns']:
            return column_name
            
        # Case-insensitive match
        for actual_column in self.tables_info[table_name]['columns'].keys():
            if actual_column.lower() == column_name.lower():
                return actual_column
                
        # Use Levenshtein distance for approximate matching
        min_distance = float('inf')
        closest_match = None
        
        for actual_column in self.tables_info[table_name]['columns'].keys():
            distance = self._levenshtein_distance(column_name.lower(), actual_column.lower())
            if distance < min_distance:
                min_distance = distance
                closest_match = actual_column
                
        # Only return if it's a reasonably good match
        if min_distance <= 3 or (min_distance / len(column_name)) < 0.3:
            return closest_match
            
        return None
    
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
    
    def _detect_query_type(self, query: str) -> str:
        """
        Detect the type of SQL query
        
        Args:
            query: The SQL query
            
        Returns:
            The query type (SELECT, INSERT, UPDATE, DELETE, or OTHER)
        """
        query_upper = query.strip().upper()
        if query_upper.startswith("SELECT"):
            return "SELECT"
        elif query_upper.startswith("INSERT"):
            return "INSERT"
        elif query_upper.startswith("UPDATE"):
            return "UPDATE"
        elif query_upper.startswith("DELETE"):
            return "DELETE"
        else:
            return "OTHER"
    
    def _adapt_select_query(self, query: str, warnings: List[str]) -> Tuple[str, List[str]]:
        """
        Adapt a SELECT query to match the actual database schema
        
        Args:
            query: The SELECT query to adapt
            warnings: List to append warning messages to
            
        Returns:
            Tuple of (adapted_query, warning_messages)
        """
        # Extract table references from FROM clause
        table_matches = re.findall(r'FROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
        
        # Also handle JOIN clauses
        join_matches = re.findall(r'JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
        
        table_matches.extend(join_matches)
        
        # Dictionary to map table aliases to their actual names
        table_alias_map = {}
        
        # Process table references
        for match in table_matches:
            table_name = match[0]
            alias = match[1] if len(match) > 1 and match[1] else table_name
            
            actual_table = self.get_actual_table_name(table_name)
            
            if actual_table and actual_table != table_name:
                query = re.sub(r'\b' + re.escape(table_name) + r'\b(?!\s*\.\s*\w+)', actual_table, query)
                warnings.append(f"Table '{table_name}' was replaced with '{actual_table}'")
            elif not actual_table:
                warnings.append(f"Warning: Table '{table_name}' not found in database")
            
            # Store the table alias mapping (using actual table name if found)
            table_alias_map[alias] = actual_table if actual_table else table_name
        
        # Extract column references from SELECT clause
        if "SELECT " in query.upper() and " FROM " in query.upper():
            select_clause = query.upper().split("SELECT ")[1].split(" FROM ")[0].strip()
            
            if select_clause != "*":
                # Handle column references
                column_matches = re.findall(r'(\w+)\.(\w+)', select_clause, re.IGNORECASE)
                
                for match in column_matches:
                    alias = match[0]
                    column_name = match[1]
                    
                    if alias in table_alias_map:
                        table_name = table_alias_map[alias]
                        
                        if table_name in self.tables_info:
                            actual_column = self.get_actual_column_name(table_name, column_name)
                            
                            if actual_column and actual_column != column_name:
                                pattern = r'\b' + re.escape(alias) + r'\.' + re.escape(column_name) + r'\b'
                                replacement = f"{alias}.{actual_column}"
                                query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
                                warnings.append(f"Column '{alias}.{column_name}' was replaced with '{alias}.{actual_column}'")
                            elif not actual_column:
                                warnings.append(f"Warning: Column '{column_name}' not found in table '{table_name}'")
                
                # Also handle unqualified column references
                unqualified_columns = re.findall(r'SELECT\s+(?:.*,\s*)?(\w+)(?:\s*,|\s|$)', query, re.IGNORECASE)
                
                for column_name in unqualified_columns:
                    if column_name.lower() not in ['distinct', 'as', 'count', 'sum', 'avg', 'min', 'max']:
                        # Try to find this column in any of the tables
                        found = False
                        for table, alias in table_alias_map.items():
                            if alias in self.tables_info and self.get_actual_column_name(alias, column_name):
                                found = True
                                break
                        
                        if not found:
                            warnings.append(f"Warning: Unqualified column '{column_name}' not found in any referenced table")
        
        # Handle WHERE clauses for column references
        if " WHERE " in query.upper():
            where_clause = query.upper().split(" WHERE ")[1].split(" GROUP BY ")[0].split(" ORDER BY ")[0].split(" LIMIT ")[0].strip()
            
            # Extract column references from WHERE clause
            column_matches = re.findall(r'(\w+)\.(\w+)', where_clause, re.IGNORECASE)
            
            for match in column_matches:
                alias = match[0]
                column_name = match[1]
                
                if alias in table_alias_map:
                    table_name = table_alias_map[alias]
                    
                    if table_name in self.tables_info:
                        actual_column = self.get_actual_column_name(table_name, column_name)
                        
                        if actual_column and actual_column != column_name:
                            pattern = r'\b' + re.escape(alias) + r'\.' + re.escape(column_name) + r'\b'
                            replacement = f"{alias}.{actual_column}"
                            query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
                            warnings.append(f"Column '{alias}.{column_name}' in WHERE clause was replaced with '{alias}.{actual_column}'")
                        elif not actual_column:
                            warnings.append(f"Warning: Column '{column_name}' in WHERE clause not found in table '{table_name}'")
        
        return query, warnings
    
    def _adapt_data_modification_query(self, query: str, query_type: str, warnings: List[str]) -> Tuple[str, List[str]]:
        """
        Adapt an INSERT, UPDATE, or DELETE query to match the actual database schema
        
        Args:
            query: The query to adapt
            query_type: The type of query (INSERT, UPDATE, or DELETE)
            warnings: List to append warning messages to
            
        Returns:
            Tuple of (adapted_query, warning_messages)
        """
        if query_type == "INSERT":
            # Extract table name from INSERT INTO clause
            table_matches = re.findall(r'INSERT\s+INTO\s+(\w+)', query, re.IGNORECASE)
        elif query_type == "UPDATE":
            # Extract table name from UPDATE clause
            table_matches = re.findall(r'UPDATE\s+(\w+)', query, re.IGNORECASE)
        elif query_type == "DELETE":
            # Extract table name from DELETE FROM clause
            table_matches = re.findall(r'DELETE\s+FROM\s+(\w+)', query, re.IGNORECASE)
        else:
            return query, warnings
        
        if table_matches:
            table_name = table_matches[0]
            actual_table = self.get_actual_table_name(table_name)
            
            if actual_table and actual_table != table_name:
                # Replace table name in the query
                if query_type == "INSERT":
                    query = re.sub(r'INSERT\s+INTO\s+' + re.escape(table_name), f"INSERT INTO {actual_table}", query, flags=re.IGNORECASE)
                elif query_type == "UPDATE":
                    query = re.sub(r'UPDATE\s+' + re.escape(table_name), f"UPDATE {actual_table}", query, flags=re.IGNORECASE)
                elif query_type == "DELETE":
                    query = re.sub(r'DELETE\s+FROM\s+' + re.escape(table_name), f"DELETE FROM {actual_table}", query, flags=re.IGNORECASE)
                
                warnings.append(f"Table '{table_name}' was replaced with '{actual_table}'")
            elif not actual_table:
                warnings.append(f"Warning: Table '{table_name}' not found in database")
        
        # Handle column references in INSERT queries
        if query_type == "INSERT" and "(" in query and ")" in query and "VALUES" in query.upper():
            # Extract column names
            columns_part = query.split("(")[1].split(")")[0].strip()
            column_names = [col.strip() for col in columns_part.split(",")]
            
            # Get the actual table name
            table_name = table_matches[0] if table_matches else None
            actual_table = self.get_actual_table_name(table_name) if table_name else None
            
            if actual_table:
                # Check and replace each column name
                new_columns = []
                for column_name in column_names:
                    actual_column = self.get_actual_column_name(actual_table, column_name)
                    
                    if actual_column and actual_column != column_name:
                        new_columns.append(actual_column)
                        warnings.append(f"Column '{column_name}' was replaced with '{actual_column}'")
                    elif not actual_column:
                        warnings.append(f"Warning: Column '{column_name}' not found in table '{actual_table}'")
                        new_columns.append(column_name)  # Keep original
                    else:
                        new_columns.append(column_name)  # Keep original
                
                # Replace columns in the query
                new_columns_str = ", ".join(new_columns)
                query = query.replace(columns_part, new_columns_str)
        
        # Handle column references in UPDATE queries
        if query_type == "UPDATE" and "SET" in query.upper():
            # Extract SET clause
            set_clause = query.upper().split("SET")[1].split("WHERE")[0].strip() if "WHERE" in query.upper() else query.upper().split("SET")[1].strip()
            
            # Get the actual table name
            table_name = table_matches[0] if table_matches else None
            actual_table = self.get_actual_table_name(table_name) if table_name else None
            
            if actual_table:
                # Extract column assignments
                assignments = set_clause.split(",")
                
                for assignment in assignments:
                    if "=" in assignment:
                        column_name = assignment.split("=")[0].strip()
                        
                        actual_column = self.get_actual_column_name(actual_table, column_name)
                        
                        if actual_column and actual_column != column_name:
                            # Replace column name in the query
                            pattern = r'SET\s+(?:.*,\s*)?(' + re.escape(column_name) + r'\s*=)'
                            replacement = f"SET {actual_column} ="
                            query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
                            warnings.append(f"Column '{column_name}' was replaced with '{actual_column}'")
                        elif not actual_column:
                            warnings.append(f"Warning: Column '{column_name}' not found in table '{actual_table}'")
        
        return query, warnings
        
    def execute_query_safely(self, query: str) -> Tuple[Dict[str, Any], List[str]]:
        """
        Validate, adapt, and execute a SQL query safely
        
        Args:
            query: The SQL query to execute
            
        Returns:
            Tuple of (result_dict, warning_messages)
        """
        original_query = query
        
        # Validate and adapt the query
        adapted_query, warnings = self.validate_and_adapt_query(query)
        
        # If the query was adapted, make note of it
        if adapted_query != original_query:
            warnings.append(f"Query was automatically adapted to: {adapted_query}")
        
        try:
            # Execute the adapted query
            with self.engine.connect() as conn:
                result = conn.execute(text(adapted_query))
                rows = result.fetchall()
                
                # Format results
                if not rows:
                    return {"result": "Query executed successfully. No rows returned.", "data": []}, warnings
                
                # Get column names
                column_names = result.keys()
                
                # Format results as a table
                header = " | ".join(column_names)
                separator = "-" * len(header)
                output = f"{header}\n{separator}"
                
                # Convert rows to list of dicts for the data field
                data = []
                for row in rows:
                    row_values = [str(value) for value in row]
                    output += f"\n{' | '.join(row_values)}"
                    
                    data_row = {}
                    for i, col in enumerate(column_names):
                        data_row[col] = row[i]
                    data.append(data_row)
                
                return {"result": output, "data": data}, warnings
                
        except Exception as e:
            error_msg = f"Error executing query: {str(e)}"
            warnings.append(error_msg)
            tables_in_query = self._extract_tables_from_query(query)
            
            # First try to generate fallback query and execute it
            fallback_result = self._execute_fallback_query(query, str(e))
            if fallback_result:
                warnings.append("Automatically executed a fallback query to retrieve similar data")
                return fallback_result, warnings
            
            # Detect unknown column errors
            if "unknown column" in str(e).lower() or "no such column" in str(e).lower() or "column not found" in str(e).lower():
                column_match = re.search(r"unknown column '([^']+)'", str(e).lower()) or \
                              re.search(r"no such column[:]? ([^\s]+)", str(e).lower()) or \
                              re.search(r"column '([^']+)' not found", str(e).lower())
                              
                if column_match:
                    problematic_column = column_match.group(1)
                    table_alias = None
                    
                    # If the column name includes a table qualifier (e.g., t.name), extract both parts
                    if '.' in problematic_column:
                        parts = problematic_column.split('.')
                        table_alias = parts[0]
                        column_name = parts[1]
                        
                        # Try to find the actual table this alias refers to
                        for table_name in self.tables_info.keys():
                            # Look for any hints in the query like 'FROM table AS alias'
                            alias_match = re.search(rf"from\s+{table_name}\s+(?:as\s+)?{table_alias}\b", query, re.IGNORECASE)
                            if alias_match:
                                # We found the table this alias refers to
                                warnings.append(f"Table alias '{table_alias}' refers to table '{table_name}'")
                                
                                # List all available columns in this table
                                column_list = list(self.tables_info[table_name]['columns'].keys())
                                warnings.append(f"Available columns in table '{table_name}': {', '.join(column_list)}")
                                
                                # Check for similar column names
                                actual_column = self.get_actual_column_name(table_name, column_name)
                                if actual_column:
                                    warnings.append(f"Suggestion: Column '{column_name}' might be '{actual_column}' in table '{table_name}'")
                                break
                    else:
                        column_name = problematic_column
                        # Try to find this column in any of the tables used in the query
                        for table in tables_in_query:
                            # Get the actual table name (in case of aliases or typos)
                            actual_table = self.get_actual_table_name(table)
                            if actual_table and actual_table in self.tables_info:
                                # List available columns
                                column_list = list(self.tables_info[actual_table]['columns'].keys())
                                warnings.append(f"Available columns in table '{actual_table}': {', '.join(column_list)}")
                                
                                # Check for similar column names
                                actual_column = self.get_actual_column_name(actual_table, column_name)
                                if actual_column:
                                    warnings.append(f"Suggestion: Column '{column_name}' might be '{actual_column}' in table '{actual_table}'")
            
            # Detect unknown table errors
            elif "table" in str(e).lower() and ("not found" in str(e).lower() or "doesn't exist" in str(e).lower()):
                table_match = re.search(r"table ['\"]?([^'\"\s]+)['\"]?(?:\S+)? (?:not found|doesn't exist)", str(e).lower())
                if table_match:
                    problematic_table = table_match.group(1)
                    
                    # List all available tables
                    available_tables = list(self.tables_info.keys())
                    warnings.append(f"Available tables: {', '.join(available_tables)}")
                    
                    # Check for similar table names
                    actual_table = self.get_actual_table_name(problematic_table)
                    if actual_table:
                        warnings.append(f"Suggestion: Table '{problematic_table}' might be '{actual_table}'")
            
            return {"result": error_msg, "data": []}, warnings
            
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
            
            # Build a query with all valid columns from this table
            valid_columns = []
            for col_name in self.tables_info[main_table]['columns'].keys():
                valid_columns.append(f"{main_table}.{col_name}")
            
            # If no valid columns, use * as fallback
            if not valid_columns:
                columns_clause = f"{main_table}.*"
            else:
                columns_clause = ", ".join(valid_columns)
            
            fallback_query = f"SELECT {columns_clause} FROM {main_table} LIMIT 10;"
            
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
                    row_values = [str(value) for value in row]
                    output += f"\n{' | '.join(row_values)}"
                    
                    data_row = {}
                    for i, col in enumerate(column_names):
                        data_row[col] = row[i]
                    data.append(data_row)
                
                return {"result": output, "data": data, "fallback_query": fallback_query}
                
        except Exception as e:
            # If the fallback query also fails, just return None
            return None
            
    def _get_available_columns(self, table_name: str) -> List[str]:
        """
        Get all available columns for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names
        """
        if table_name not in self.tables_info:
            return []
            
        return list(self.tables_info[table_name]['columns'].keys())
            
    def _extract_tables_from_query(self, query: str) -> List[str]:
        """
        Extract table names from a query string
        
        Args:
            query: SQL query string
            
        Returns:
            List of table names found in the query
        """
        tables = []
        
        # Extract table names from FROM clauses
        from_matches = re.findall(r'\bFROM\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
        for match in from_matches:
            table_name = match[0]
            tables.append(table_name)
        
        # Extract table names from JOIN clauses
        join_matches = re.findall(r'\bJOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?', query, re.IGNORECASE)
        for match in join_matches:
            table_name = match[0]
            tables.append(table_name)
        
        return tables
