#!/usr/bin/env python
"""
Utility script to inspect the database schema.
Helps identify the actual column names in tables for debugging purposes.
"""

from database.enhanced_db_manager import EnhancedDatabaseManager
from sqlalchemy import inspect, text

def inspect_schema(db_config):
    """Print detailed schema information for all tables in the database."""
    print("Inspecting database schema...")
    
    # Create DB manager and connection
    db_manager = EnhancedDatabaseManager()
    engine = db_manager.get_connection(db_config)
    
    # Get the inspector
    inspector = inspect(engine)
    
    # Get all table names
    tables = inspector.get_table_names()
    print(f"Found {len(tables)} tables: {', '.join(tables)}")
    print("\n" + "="*80 + "\n")
    
    # For each table, print its structure
    for table in tables:
        print(f"TABLE: {table}")
        print("-" * 40)
        
        # Get columns
        columns = inspector.get_columns(table)
        print("COLUMNS:")
        for col in columns:
            print(f"  - {col['name']} ({col['type']})")
        
        # Get primary keys
        pk = inspector.get_pk_constraint(table)
        if pk['constrained_columns']:
            print("\nPRIMARY KEY:")
            print(f"  {', '.join(pk['constrained_columns'])}")
        
        # Get foreign keys
        fks = inspector.get_foreign_keys(table)
        if fks:
            print("\nFOREIGN KEYS:")
            for fk in fks:
                print(f"  {', '.join(fk['constrained_columns'])} -> {fk['referred_table']}.{', '.join(fk['referred_columns'])}")
        
        # Sample data (first 3 rows)
        print("\nSAMPLE DATA (first 3 rows):")
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT * FROM {table} LIMIT 3"))
                rows = result.fetchall()
                if rows:
                    # Print column headers
                    print("  | " + " | ".join(result.keys()) + " |")
                    print("  | " + " | ".join(["-"*len(col) for col in result.keys()]) + " |")
                    
                    # Print data rows
                    for row in rows:
                        print("  | " + " | ".join([str(val) for val in row]) + " |")
                else:
                    print("  (No data)")
        except Exception as e:
            print(f"  Error fetching sample data: {str(e)}")
            
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    # Sample config for MySQL database
    db_config = {
        "databasetype": "mysql",
        "envirment": "localhost", 
        "port": 3306,
        "database": "testdb",
        "username": "root",
        "password": "",
        "ssl": False
    }
    
    # Modify this with the actual database configuration being used
    inspect_schema(db_config)
