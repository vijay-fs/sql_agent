from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus
import json

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
            
            # Store table schema
            schema_info[table] = {
                "columns": columns,
                "primary_keys": pk_columns
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
    
    def execute_query(self, engine, query):
        """Execute a SQL query and return the results as a string"""
        output = ""
        try:
            # Clean up the query (remove any lingering markdown or formatting)
            cleaned_query = query.replace("```sql", "").replace("```", "").strip()
            
            # Execute the query
            with engine.connect() as con:
                rows = con.execute(text(cleaned_query))
                
                # Process results
                rows_data = rows.fetchall()
                if not rows_data:
                    return "Query executed successfully. No rows returned."
                
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
                
            return output
        except Exception as e:
            return f"Error executing query: {str(e)}"