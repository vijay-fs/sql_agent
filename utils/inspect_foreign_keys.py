#!/usr/bin/env python
"""
Utility script to inspect foreign key structure in SQLite database.
"""

from sqlalchemy import create_engine, inspect, text
import json

def setup_test_database():
    """Create a test database with foreign keys for inspection."""
    print("Setting up test database...")
    
    # Create in-memory SQLite database
    engine = create_engine('sqlite:///:memory:')
    
    # Create test tables
    with engine.connect() as conn:
        # Enable foreign keys in SQLite
        conn.execute(text('PRAGMA foreign_keys = ON'))
        
        # Create departments table first (referenced table)
        conn.execute(text('''
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            dept_name TEXT NOT NULL,
            location TEXT
        )
        '''))
        
        # Create employees table with foreign key reference
        conn.execute(text('''
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            job_title TEXT,
            salary REAL,
            department_id INTEGER,
            FOREIGN KEY (department_id) REFERENCES departments(id)
        )
        '''))
        
        # Insert sample data
        conn.execute(text('''
        INSERT INTO departments (id, dept_name, location)
        VALUES  (1, 'Engineering', 'Building A'),
                (2, 'Design', 'Building B'),
                (3, 'Data Science', 'Building C')
        '''))
        
        conn.execute(text('''
        INSERT INTO employees (id, name, job_title, salary, department_id)
        VALUES  (1, 'John Smith', 'Software Engineer', 85000, 1),
                (2, 'Mary Johnson', 'Project Manager', 95000, 1),
                (3, 'James Brown', 'UI Designer', 75000, 2),
                (4, 'Patricia Davis', 'Data Scientist', 90000, 3)
        '''))
        
    return engine

def inspect_foreign_keys(engine):
    """Inspect and print foreign key structure."""
    inspector = inspect(engine)
    
    # Get list of tables
    tables = inspector.get_table_names()
    print(f"Tables in database: {tables}")
    
    # Inspect foreign keys in each table
    for table_name in tables:
        print(f"\nForeign keys for table '{table_name}':")
        foreign_keys = inspector.get_foreign_keys(table_name)
        
        if not foreign_keys:
            print("  No foreign keys")
        else:
            for i, fk in enumerate(foreign_keys, 1):
                print(f"  FK {i}: {json.dumps(fk, indent=2)}")
                
                # Print key components in a more readable format
                constrained_cols = fk.get('constrained_columns', [])
                referred_table = fk.get('referred_table', '')
                referred_cols = fk.get('referred_columns', [])
                
                print(f"  Readable format: {', '.join(constrained_cols)} -> {referred_table}.{', '.join(referred_cols)}")

def main():
    """Main function."""
    engine = setup_test_database()
    inspect_foreign_keys(engine)

if __name__ == "__main__":
    main()
