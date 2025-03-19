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
                
                # Process the enhanced data to ensure foreign key relationships are properly displayed
                if 'enhanced_data' in fallback_result:
                    # Make a copy of the original data for reference
                    fallback_result['original_data'] = fallback_result.get('data', [])
                    
                    # Use our specialized formatter to handle foreign key relationships
                    processed_data = self.format_related_data(fallback_result['enhanced_data'])
                    
                    # Replace the data with the processed enhanced data
                    fallback_result['data'] = processed_data
                    
                    # Keep the enhanced_data for reference but renamed to make it clearer
                    fallback_result['detailed_data'] = fallback_result['enhanced_data']
                    fallback_result.pop('enhanced_data', None)
                
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
            
            # Check if we should include foreign key joins
            include_joins = self._detect_query_type(original_query) == "SELECT" and len(valid_tables) == 1
            related_tables_info = self._get_related_tables(main_table) if include_joins else []
            
            # Build a query with all valid columns from this table and joined tables
            valid_columns = []
            join_clauses = []
            
            # Add columns from the main table
            for col_name in self.tables_info[main_table]['columns'].keys():
                valid_columns.append(f"{main_table}.{col_name}")
            
            # Add joins and columns from related tables, focusing on descriptive fields
            related_columns_map = {}  # To keep track of what relates to what
            for relation in related_tables_info:
                related_table = relation['referred_table']
                relationship_type = relation.get('relationship_type', 'outgoing')  # Default to outgoing
                
                # Handle different types of relationships
                if relationship_type == 'outgoing':
                    # This table has a foreign key to another table
                    local_column = relation['constrained_columns'][0]  # Assuming single-column FK for simplicity
                    foreign_column = relation['referred_columns'][0]
                    alias = f"fk_out_{related_table}"
                    join_clause = f"LEFT JOIN {related_table} AS {alias} ON {main_table}.{local_column} = {alias}.{foreign_column}"
                else:
                    # Another table has a foreign key to this table
                    local_column = relation['constrained_columns'][0]  # Column in this table being referenced
                    foreign_column = relation['referred_columns'][0]  # FK column in the other table
                    alias = f"fk_in_{related_table}"
                    join_clause = f"LEFT JOIN {related_table} AS {alias} ON {main_table}.{local_column} = {alias}.{foreign_column}"
                
                # Add join clause
                join_clauses.append(join_clause)
                
                # Add columns from the joined table, focusing on descriptive ones
                if related_table in self.tables_info:
                    # Get descriptive columns for this related table
                    descriptive_columns = relation.get('descriptive_columns', []) or self._identify_descriptive_columns(related_table)
                    
                    # Store the relationship for result formatting
                    relation_key = f"{relationship_type}_{local_column}"
                    related_columns_map[relation_key] = {
                        'table': related_table,
                        'alias': alias,
                        'local_column': local_column,
                        'foreign_column': foreign_column,
                        'relationship_type': relationship_type,
                        'descriptive_columns': descriptive_columns
                    }
                    
                    # Always include the primary key column from related table
                    pk_columns = self.tables_info[related_table]['pk_columns']
                    if pk_columns:
                        pk_col = pk_columns[0]
                        valid_columns.append(f"{alias}.{pk_col} AS {alias}_{pk_col}")
                    
                    # Add descriptive columns
                    for col_name in descriptive_columns:
                        if col_name in self.tables_info[related_table]['columns']:
                            valid_columns.append(f"{alias}.{col_name} AS {alias}_{col_name}")
                            
                    # If no descriptive columns found, add at least a few important columns
                    if not descriptive_columns:
                        # Try to add some commonly useful columns if they exist
                        candidate_columns = ['name', 'title', 'description', 'label', 'code', 'key', 'value', 'display_name']
                        added = False
                        for col_name in candidate_columns:
                            if col_name in self.tables_info[related_table]['columns']:
                                valid_columns.append(f"{alias}.{col_name} AS {alias}_{col_name}")
                                added = True
                        
                        # If still no useful columns found, add the first few columns
                        if not added:
                            for i, col_name in enumerate(self.tables_info[related_table]['columns'].keys()):
                                if i < 3:  # Just add first 3 columns
                                    valid_columns.append(f"{alias}.{col_name} AS {alias}_{col_name}")
            
            # If no valid columns, use * as fallback
            if not valid_columns:
                columns_clause = f"{main_table}.*"
            else:
                columns_clause = ", ".join(valid_columns)
            
            # Build the complete query - intentionally excluding any WHERE clauses from original query
            # because we want to show some data even if the original query had restrictive conditions
            from_clause = f"FROM {main_table}"
            if join_clauses:
                from_clause += " " + " ".join(join_clauses)
                
            fallback_query = f"SELECT {columns_clause} {from_clause} LIMIT 5;"  # Limit to just 5 rows
            
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
                
                # Convert rows to list of dicts for the data field, reorganizing to highlight relationships
                data = []
                enhanced_data = []  # To store the enhanced data with relationships
                
                # Process each row
                for row in rows:
                    # Basic row values for output
                    row_values = [str(value) for value in row]
                    output += f"\n{' | '.join(row_values)}"
                    
                    # Build enhanced data structure with relationships
                    data_row = {}
                    enhanced_row = {}
                    relations = {}
                    
                    # First add main table columns
                    for i, col in enumerate(column_names):
                        value = row[i]
                        data_row[col] = value
                        
                        # Check if this is a foreign key column needing lookup
                        col_parts = col.split('.') if '.' in col else [col]
                        col_base_name = col_parts[-1]  # Get the last part after any dots
                        
                        # Store main table values directly - not from a related table
                        if not col.startswith('fk_'):
                            enhanced_row[col_base_name] = value
                        else:
                            # Group related table values by their relationship type and table
                            # Format is now fk_[in/out]_[table]_[column]
                            parts = col.split('_', 3) if '_' in col else [col]
                            
                            if len(parts) >= 4:
                                rel_direction = parts[1]  # 'in' or 'out'
                                related_table = parts[2]
                                related_col = parts[3]
                                
                                relation_key = f"{rel_direction}_{related_table}"
                                if relation_key not in relations:
                                    relations[relation_key] = {}
                                    
                                relations[relation_key][related_col] = value
                    
                    # Add relations with descriptive labels
                    for relation_key, relation_info in related_columns_map.items():
                        rel_type = relation_info.get('relationship_type', 'outgoing')
                        table = relation_info['table']
                        alias = relation_info['alias']
                        local_col = relation_info['local_column']
                        foreign_col = relation_info['foreign_column']
                        
                        # Parse the alias to get the direction and table
                        alias_parts = alias.split('_', 2) if '_' in alias else [alias]
                        if len(alias_parts) >= 3:
                            rel_direction = alias_parts[1]  # 'in' or 'out'
                            related_table_from_alias = alias_parts[2]
                            
                            # The key to lookup in our relations dict
                            rel_lookup_key = f"{rel_direction}_{related_table_from_alias}"
                            
                            # Only include if we have data for this relation
                            if rel_lookup_key in relations:
                                rel_data = relations[rel_lookup_key]
                                
                                # Build a descriptive label using available data
                                descriptive_parts = []
                                for desc_col in relation_info['descriptive_columns']:
                                    if desc_col in rel_data and rel_data[desc_col]:
                                        descriptive_parts.append(str(rel_data[desc_col]))
                                
                                # Create a more structured representation of the relation
                                if rel_type == 'outgoing':
                                    # This is a foreign key from our table to another
                                    fk_value = enhanced_row.get(local_col)
                                    
                                    if descriptive_parts:
                                        # Create a normalized structure for the related entity
                                        related_entity = {
                                            'id': fk_value,
                                            'table': table,
                                            'display': ' - '.join(descriptive_parts),
                                            'data': rel_data
                                        }
                                        
                                        # Add to enhanced row in multiple formats
                                        enhanced_row[f"{local_col}_raw"] = fk_value
                                        enhanced_row[f"{local_col}_related"] = ' - '.join(descriptive_parts)
                                        
                                        # Add the full related entity information
                                        enhanced_row[f"{table}"] = related_entity
                                        
                                        # For category_id, client_id, etc. - retain the original ID but also provide the descriptive name
                                        # in the enhanced data format for better displays
                                        if local_col in enhanced_row:
                                            # Keep the original numeric ID value so joins will still work
                                            # The enhanced display will be processed in execute_query_safely
                                            pass
                                else:
                                    # This is a reference from another table to ours (incoming relationship)
                                    # Add as a related collection since there could be multiple
                                    related_collection_key = f"{table}_collection"
                                    if related_collection_key not in enhanced_row:
                                        enhanced_row[related_collection_key] = []
                                        
                                    # Only add if we have meaningful data
                                    if descriptive_parts:
                                        related_item = {
                                            'table': table,
                                            'foreign_key': foreign_col,
                                            'local_key': local_col,
                                            'display': ' - '.join(descriptive_parts),
                                            'data': rel_data
                                        }
                                        enhanced_row[related_collection_key].append(related_item)
                    
                    data.append(data_row)  # Original format
                    enhanced_data.append(enhanced_row)  # Enhanced format with relationships
                
                # Add more user-friendly explanation
                output += "\n\nForeign Key Relationships:\n"
                for relation_key, relation_info in related_columns_map.items():
                    rel_type = relation_info.get('relationship_type', 'outgoing')
                    local_col = relation_info['local_column']
                    foreign_col = relation_info['foreign_column']
                    related_table = relation_info['table']
                    
                    desc_cols = ', '.join(relation_info['descriptive_columns']) if relation_info['descriptive_columns'] else 'No descriptive columns found'
                    
                    if rel_type == 'outgoing':
                        # This table references another
                        output += f"- {main_table}.{local_col} → {related_table}.{foreign_col} (Using descriptive fields: {desc_cols})\n"
                    else:
                        # Another table references this one
                        output += f"- {related_table}.{foreign_col} → {main_table}.{local_col} (Using descriptive fields: {desc_cols})\n"
                
                return {
                    "result": output, 
                    "data": data,  # Original data format
                    "enhanced_data": enhanced_data,  # New data format with resolved references
                    "fallback_query": fallback_query,
                    "relationships": [{k: v} for k, v in related_columns_map.items()]  # Include relationship metadata
                }
                
        except Exception as e:
            # If the fallback query also fails, just return None
            return None
    
    def _get_related_tables(self, table_name: str, related_depth: int = 1) -> List[Dict[str, Any]]:
        """
        Get tables related to the specified table via foreign keys
        
        Args:
            table_name: Name of the table to find relations for
            related_depth: How many levels of related tables to include (defaults to 1)
            
        Returns:
            List of related table information with relationship type
        """
        if table_name not in self.tables_info:
            return []
            
        # Get direct foreign keys (outgoing relationships)
        direct_relations = self.tables_info[table_name]['foreign_keys']
        for relation in direct_relations:
            relation['relationship_type'] = 'outgoing'  # This table has a foreign key to another table
        
        # Also look for incoming relationships (other tables referencing this one)
        incoming_relations = []
        for other_table, info in self.tables_info.items():
            if other_table == table_name:
                continue
                
            # Check if any foreign keys in this table point to our table
            for fk in info['foreign_keys']:
                if fk['referred_table'] == table_name:
                    # Create a reverse relationship record
                    incoming_relation = {
                        'name': f"incoming_{other_table}_to_{table_name}",
                        'constrained_columns': fk['referred_columns'],  # Columns in our table
                        'referred_table': other_table,                   # The other table name
                        'referred_columns': fk['constrained_columns'],   # Columns in the other table
                        'relationship_type': 'incoming'                  # Other table has FK to this table
                    }
                    incoming_relations.append(incoming_relation)
        
        # Combine both directions of relationships
        all_relations = direct_relations + incoming_relations
        
        # For each relation, detect the most likely descriptive field in the related table
        for relation in all_relations:
            related_table = relation['referred_table']
            if related_table in self.tables_info:
                relation['descriptive_columns'] = self._identify_descriptive_columns(related_table)
        
        return all_relations
        
    def _identify_descriptive_columns(self, table_name: str) -> List[str]:
        """
        Identify columns that are likely to be descriptive (not just IDs)
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names that are likely descriptive
        """
        if table_name not in self.tables_info:
            return []
            
        descriptive_columns = []
        name_pattern = re.compile(r'(name|title|label|description|summary|text|content)', re.IGNORECASE)
        
        # Check each column
        for col_name in self.tables_info[table_name]['columns'].keys():
            # Skip ID columns and other common non-descriptive columns
            if col_name.lower() == 'id' or col_name.endswith('_id') or col_name.endswith('_at'):
                continue
                
            # Include columns with descriptive names
            if name_pattern.search(col_name):
                descriptive_columns.append(col_name)
                
        # If no descriptive columns found, include everything except obvious IDs and timestamps
        if not descriptive_columns:
            for col_name in self.tables_info[table_name]['columns'].keys():
                if not (col_name.lower() == 'id' or col_name.endswith('_id') or 
                        col_name.endswith('_at') or col_name.startswith('is_')):
                    descriptive_columns.append(col_name)
                    
        # If still no columns, include the first non-ID column
        if not descriptive_columns:
            for col_name in self.tables_info[table_name]['columns'].keys():
                if col_name.lower() != 'id':
                    descriptive_columns.append(col_name)
                    break
                    
        return descriptive_columns
            
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
    
    def format_related_data(self, data_rows):
        """
        Format data with foreign key relationships for better display
        
        Args:
            data_rows: List of data rows to format
            
        Returns:
            List of formatted data rows with human-readable foreign key information
        """
        formatted_rows = []
        
        for row in data_rows:
            formatted_row = {}
            
            # Process each field in the row
            for key, value in row.items():
                # Skip special internal fields
                if key.endswith('_raw') or key.endswith('_collection'):
                    continue
                    
                # Format foreign key fields with descriptive information
                if key.endswith('_id') and f"{key}_related" in row:
                    # Format as "ID (Description)" for better readability
                    formatted_row[key] = f"{value} ({row[f'{key}_related']})"
                # Handle fields that contain related table information
                elif isinstance(value, dict) and 'table' in value and 'display' in value:
                    # Format as "Table: Display" for better readability
                    formatted_row[key] = f"{value['table']}: {value['display']}"
                # Handle normal fields
                elif not key.endswith('_related'):
                    formatted_row[key] = value
            
            # Add any collection relationships in a structured format
            collections = {}
            for key, value in row.items():
                if key.endswith('_collection') and isinstance(value, list) and value:
                    related_table = key[:-11]  # Remove '_collection' suffix
                    collections[related_table] = []
                    
                    for item in value:
                        if 'display' in item:
                            collections[related_table].append(item['display'])
            
            if collections:
                formatted_row['related_collections'] = collections
            
            formatted_rows.append(formatted_row)
            
        return formatted_rows
            
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
        
    def _detect_query_type(self, query: str) -> str:
        """
        Detect the type of SQL query (SELECT, INSERT, UPDATE, DELETE, etc.)
        
        Args:
            query: The SQL query
            
        Returns:
            String indicating the query type
        """
        query = query.strip().upper()
        
        if query.startswith('SELECT'):
            return 'SELECT'
        elif query.startswith('INSERT'):
            return 'INSERT'
        elif query.startswith('UPDATE'):
            return 'UPDATE'
        elif query.startswith('DELETE'):
            return 'DELETE'
        elif query.startswith('CREATE'):
            return 'CREATE'
        elif query.startswith('ALTER'):
            return 'ALTER'
        elif query.startswith('DROP'):
            return 'DROP'
        else:
            return 'UNKNOWN'
