from sqlalchemy import create_engine, inspect, text, MetaData, Table
from urllib.parse import quote_plus
import json
from database.schema_validator import SchemaValidator

class EnhancedDatabaseManager:
    def __init__(self):
        self.connections = {}
        self.foreign_keys_cache = {}
        self.schema_validators = {}
    
    def get_connection_string(self, db_config):
        """
        Create a SQLAlchemy connection string based on database configuration
        """
        db_type = db_config.get("databasetype", "mysql")
        
        if db_type == "mysql":
            username = db_config.get("username", "")
            password = db_config.get("password", "")
            host = db_config.get("envirment", "localhost")
            port = db_config.get("port", "3306")
            database = db_config.get("database", "")
            use_ssl = db_config.get("ssl", "false").lower() == "true"
            
            # URL encode the password to handle special characters
            encoded_password = quote_plus(password)
            
            # Construct connection string
            conn_str = f"mysql+pymysql://{username}:{encoded_password}@{host}:{port}/{database}"
            
            # Add SSL parameters if required
            if use_ssl:
                conn_str += "?ssl=true"
                
            return conn_str
        elif db_type == "postgresql":
            username = db_config.get("username", "")
            password = db_config.get("password", "")
            host = db_config.get("envirment", "localhost")
            port = db_config.get("port", "5432")
            database = db_config.get("database", "")
            
            # URL encode the password to handle special characters
            encoded_password = quote_plus(password)
            
            # Construct connection string
            conn_str = f"postgresql://{username}:{encoded_password}@{host}:{port}/{database}"
            return conn_str
        elif db_type == "sqlite":
            database = db_config.get("database", ":memory:")
            conn_str = f"sqlite:///{database}"
            return conn_str
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
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
    
    def get_tables_schema(self, engine):
        """
        Get schema information for all tables in the database
        """
        schema_info = {}
        inspector = inspect(engine)
        
        # Get foreign key relationships
        foreign_keys = self.get_foreign_keys(engine)
        
        # Get list of tables
        tables = inspector.get_table_names()
        
        for table in tables:
            columns = []
            for column in inspector.get_columns(table):
                columns.append({
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": column.get("nullable", True)
                })
            
            # Get primary key information
            pk_columns = inspector.get_pk_constraint(table).get("constrained_columns", [])
            
            # Get foreign key information
            fks = foreign_keys.get(table, [])
            
            # Store table schema
            schema_info[table] = {
                "columns": columns,
                "primary_keys": pk_columns,
                "foreign_keys": fks
            }
        
        return schema_info
    
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
    
    def generate_join_hints(self, engine):
        """Generate hints for join queries based on detected foreign keys"""
        foreign_keys = self.get_foreign_keys(engine)
        
        if not foreign_keys:
            return "No foreign key relationships detected in the schema."
            
        hints = "When creating JOIN queries, consider these relationships:"
        
        for table, fks in foreign_keys.items():
            for fk in fks:
                constrained_col = fk["constrained_columns"][0]  # Simplify for first column
                referred_table = fk["referred_table"]
                referred_col = fk["referred_columns"][0]  # Simplify for first column
                
                hints += f"\n- JOIN {referred_table} ON {table}.{constrained_col} = {referred_table}.{referred_col}"
        
        return hints
    
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
    
    def _is_referenced_by_others(self, table, foreign_keys):
        """Check if a table is referenced by other tables' foreign keys"""
        for other_table, fks in foreign_keys.items():
            for fk in fks:
                if fk["referred_table"] == table:
                    return True
        return False
    
    def execute_query(self, engine, query, return_full_result=False):
        """
        Execute a SQL query and return the results.
        Uses SchemaValidator to validate and adapt the query to the actual database schema.
        
        Args:
            engine: SQLAlchemy engine
            query: SQL query string
            return_full_result: If True, returns the full result dictionary with enhanced data
                               If False, returns only the text output (default behavior)
        
        Returns:
            String result or full result dictionary based on return_full_result
        """
        # Get or create schema validator for this engine
        engine_id = str(id(engine))
        if engine_id not in self.schema_validators:
            self.schema_validators[engine_id] = SchemaValidator(engine)
        
        schema_validator = self.schema_validators[engine_id]
        
        # Validate, adapt, and execute the query
        result_dict, warnings = schema_validator.execute_query_safely(query)
        
        # Include warnings in the output
        if warnings:
            warning_output = "\n\nWarnings/Suggestions:"
            for warning in warnings:
                warning_output += f"\n- {warning}"
            result_dict["result"] += warning_output
        
        # Return the full result dictionary if requested
        if return_full_result:
            result_dict["warnings"] = warnings
            return result_dict
        else:
            # Return just the text output for backward compatibility
            return result_dict["result"]
            
    def get_normalized_data(self, engine, table_name, limit=100):
        """
        Get fully normalized data for a table with all foreign key references resolved
        
        Args:
            engine: SQLAlchemy engine
            table_name: Name of the table to query
            limit: Maximum number of rows to return
            
        Returns:
            Dictionary with the original query, normalized data, and text results
        """
        # Get or create schema validator for this engine
        engine_id = str(id(engine))
        if engine_id not in self.schema_validators:
            self.schema_validators[engine_id] = SchemaValidator(engine)
        
        schema_validator = self.schema_validators[engine_id]
        
        # Create a join query if the table has foreign keys
        join_query = self.suggest_join_query(engine, table_name)
        
        # If no join query could be generated, use a simple query
        if join_query.startswith("Table") or join_query.startswith("No foreign key"):
            query = f"SELECT * FROM {table_name} LIMIT {limit};"
        else:
            # Add LIMIT to the join query
            if ";" in join_query:
                join_query = join_query[:-1]  # Remove trailing semicolon
            query = f"{join_query} LIMIT {limit};"
        
        # Execute the query
        try:
            result_dict, warnings = schema_validator.execute_query_safely(query)
            
            # Include warnings in the output
            if warnings:
                warning_output = "\n\nWarnings/Suggestions:"
                for warning in warnings:
                    warning_output += f"\n- {warning}"
                result_dict["result"] += warning_output
            
            return {
                "sql_query": query,
                "result": result_dict["result"],
                "data": result_dict.get("data", []),
                "normalized_data": result_dict.get("normalized_data", [])
            }
            
        except Exception as e:
            # If the join query fails, try a simple query
            simple_query = f"SELECT * FROM {table_name} LIMIT {limit};"
            result_dict, warnings = schema_validator.execute_query_safely(simple_query)
            
            if warnings:
                warning_output = "\n\nWarnings/Suggestions:"
                for warning in warnings:
                    warning_output += f"\n- {warning}"
                result_dict["result"] += warning_output
            
            return {
                "sql_query": simple_query,
                "result": result_dict["result"],
                "data": result_dict.get("data", []),
                "normalized_data": result_dict.get("normalized_data", []),
                "error": str(e),
                "original_query": query
            }