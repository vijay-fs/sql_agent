#!/usr/bin/env python3
"""
Test script for the improved foreign key display functionality in SchemaValidator
"""

from sqlalchemy import create_engine, text
from database.schema_validator import SchemaValidator
import json

def setup_test_db():
    """Set up a test database with related tables"""
    # Use a file-based SQLite database to ensure persistence
    import os
    test_db_path = 'test_database.db'
    
    # Remove the database file if it exists to start fresh
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        
    engine = create_engine(f'sqlite:///{test_db_path}', echo=False)
    
    with engine.connect() as conn:
        # Create departments table
        conn.execute(text('''
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            dept_name TEXT NOT NULL,
            location TEXT
        )
        '''))
        
        # Create employees table with foreign key to departments
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
        departments = [
            (1, "Engineering", "Building A"),
            (2, "Marketing", "Building B"),
            (3, "HR", "Building C"),
            (4, "Sales", "Building D")
        ]
        
        employees = [
            (1, "John Smith", "Software Engineer", 85000, 1),
            (2, "Mary Johnson", "Marketing Manager", 75000, 2),
            (3, "Bob Williams", "HR Specialist", 65000, 3),
            (4, "Alice Brown", "Sales Representative", 60000, 4),
            (5, "David Lee", "Senior Developer", 95000, 1)
        ]
        
        for dept in departments:
            conn.execute(text(f"INSERT INTO departments VALUES {dept}"))
            
        for emp in employees:
            conn.execute(text(f"INSERT INTO employees VALUES {emp}"))
        
        # Verify data was inserted correctly
        result = conn.execute(text("SELECT * FROM employees"))
        print("Inserted employee data:")
        for row in result.fetchall():
            print(row)
            
        result = conn.execute(text("SELECT * FROM departments"))
        print("Inserted department data:")
        for row in result.fetchall():
            print(row)
    
    return engine

def test_foreign_key_display():
    """Test that foreign keys are resolved and displayed correctly"""
    engine = setup_test_db()
    validator = SchemaValidator(engine)
    
    print("Testing foreign key resolution...\n")
    
    # Query with foreign key reference
    query = "SELECT employees.id, employees.name, employees.department_id FROM employees"
    
    print(f"Original query: \n{query}\n")
    result, warnings = validator.execute_query_safely(query)
    
    print("Result:")
    print(json.dumps(result, indent=2, default=str))
    
    # Query with invalid column that should trigger fallback
    query = "SELECT employees.id, employees.name, employees.title, employees.department_id FROM employees"
    
    print("\n\nTesting fallback with foreign key resolution...\n")
    print(f"Invalid query: \n{query}\n")
    result, warnings = validator.execute_query_safely(query)
    
    print("Result from fallback query:")
    print(json.dumps(result, indent=2, default=str))
    
    print("\nWarnings:")
    for warning in warnings:
        print(f"- {warning}")

if __name__ == "__main__":
    test_foreign_key_display()
