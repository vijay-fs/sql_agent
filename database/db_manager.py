from sqlalchemy import create_engine, inspect, text, MetaData, Table
from urllib.parse import quote_plus
import json
from database.schema_validator import SchemaValidator

class DatabaseManager:
    def __init__(self):
        self.connections = {}
    
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
    
    def get_tables_schema(self, engine):
        """
        Get schema information for all tables in the database
        """
        schema_info = {}
        inspector = inspect(engine)
        
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
            fks = inspector.get_foreign_keys(table)
            
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

        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        for table in tables:
            columns_info = [(col["name"], col["type"]) for col in inspector.get_columns(table)]
            
            table_description = f"\n\nTable '{table}':\nColumns:"
            for name, col_type in columns_info:
                table_description += f"\n  - {name}: {col_type}"
            
            description += table_description
        
        return description
    
    def execute_query(self, engine, query, return_raw_data=False, auto_join=True):
        """
        Execute a SQL query and return the results as a string and/or raw data
        
        Args:
            engine: SQLAlchemy engine
            query: SQL query to execute
            return_raw_data: Whether to include raw data in the response
            auto_join: Whether to automatically join related tables via foreign keys
            
        Returns:
            dict: Contains 'result' string and optionally 'data' list of dicts
        """
        output = ""
        data = []
        warnings = []
        
        try:
            # Clean up the query (remove any lingering markdown or formatting)
            cleaned_query = query.replace("```sql", "").replace("```", "").strip()
            
            # If auto_join is enabled, use SchemaValidator to enhance the query with foreign key resolution
            if auto_join:
                # Create a schema validator for this connection
                validator = SchemaValidator(engine)
                
                # Execute the query safely with foreign key resolution
                result_dict, query_warnings = validator.execute_query_safely(cleaned_query)
                
                # Process the results
                if result_dict:
                    # Add any warnings from the validator
                    warnings.extend(query_warnings)
                    
                    # Extract the output string and data
                    output = result_dict.get("result", "")
                    data = result_dict.get("data", [])
                    
                    # Return the enhanced results
                    if return_raw_data:
                        return {
                            "result": output,
                            "data": data,
                            "warnings": warnings if warnings else None
                        }
                    else:
                        return {
                            "result": output,
                            "warnings": warnings if warnings else None
                        }
            
            # Standard execution without SchemaValidator
            with engine.connect() as con:
                rows = con.execute(text(cleaned_query))
                
                # Process results
                rows_data = rows.fetchall()
                if not rows_data:
                    if return_raw_data:
                        return {
                            "result": "Query executed successfully. No rows returned.",
                            "data": []
                        }
                    else:
                        return {
                            "result": "Query executed successfully. No rows returned."
                        }
                
                # Get column names for better output
                column_names = rows.keys()
                
                # Format results as a table
                header = " | ".join(column_names)
                separator = "-" * len(header)
                output = f"{header}\n{separator}"
                
                # Add data rows
                for row in rows_data:
                    row_values = [str(value) for value in row]
                    output += f"\n{' | '.join(row_values)}"
                    
                    # Add to raw data if requested
                    if return_raw_data:
                        data_row = {}
                        for i, col in enumerate(column_names):
                            data_row[col] = row[i]
                        data.append(data_row)
            
            if return_raw_data:
                return {
                    "result": output,
                    "data": data
                }
            else:
                return {
                    "result": output
                }
                
        except Exception as e:
            error_msg = f"Error executing query: {str(e)}\n[SQL: {cleaned_query}]\n(Background on this error at: https://sqlalche.me/e/20/e3q8)"
            warnings.append(error_msg)
            
            # If we have an error and auto_join is enabled, try to get fallback results
            if auto_join:
                try:
                    validator = SchemaValidator(engine)
                    result_dict, query_warnings = validator.execute_query_safely(cleaned_query)
                    
                    if result_dict:
                        # Add any additional warnings
                        warnings.extend(query_warnings)
                        
                        # Return the fallback results
                        return {
                            "result": result_dict.get("result", "Fallback query executed instead of the original query."),
                            "data": result_dict.get("data", []),
                            "warnings": warnings
                        }
                except Exception as inner_e:
                    # If the fallback also fails, add that to warnings
                    warnings.append(f"Fallback query also failed: {str(inner_e)}")
            
            # If we reach here, both the original query and fallback failed or no fallback was attempted
            if return_raw_data:
                return {
                    "result": error_msg,
                    "data": []
                }
            else:
                return {
                    "result": error_msg
                }