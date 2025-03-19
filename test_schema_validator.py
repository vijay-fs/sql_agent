#!/usr/bin/env python
"""
Test script for the SchemaValidator improvements.
This script tests the schema validator with tables that exist but have incorrect column names.
"""

from database.schema_validator import SchemaValidator
from database.enhanced_db_manager import EnhancedDatabaseManager
from sqlalchemy import create_engine, text

def setup_test_database():
    """Create a test database with sample tables."""
    print("Setting up test database...")
    
    # Create in-memory SQLite database
    engine = create_engine('sqlite:///:memory:')
    
    # Create test tables
    with engine.connect() as conn:
        conn.execute(text('''
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            job_title TEXT,
            salary REAL,
            department_id INTEGER
        )
        '''))
        
        conn.execute(text('''
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            dept_name TEXT NOT NULL,
            location TEXT
        )
        '''))
        
        # Insert sample data
        conn.execute(text('''
        INSERT INTO employees (id, name, job_title, salary, department_id)
        VALUES  (1, 'John Smith', 'Software Engineer', 85000, 1),
                (2, 'Mary Johnson', 'Project Manager', 95000, 1),
                (3, 'James Brown', 'UI Designer', 75000, 2),
                (4, 'Patricia Davis', 'Data Scientist', 90000, 3)
        '''))
        
        conn.execute(text('''
        INSERT INTO departments (id, dept_name, location)
        VALUES  (1, 'Engineering', 'Building A'),
                (2, 'Design', 'Building B'),
                (3, 'Data Science', 'Building C')
        '''))
        
    return engine

def test_query_with_valid_tables_invalid_columns(validator):
    """Test a query with valid tables but invalid column names."""
    print("\n--- Testing query with valid tables but invalid column names ---")
    
    # Query with incorrect column names
    test_query = """
    SELECT employees.id, employees.employee_name, employees.position, departments.dept_name
    FROM employees 
    JOIN departments ON employees.department_id = departments.id
    WHERE employees.salary > 80000
    """
    
    print(f"Original query: {test_query}")
    
    # Execute the query with the validator
    result, warnings = validator.execute_query_safely(test_query)
    
    # Print the result and warnings
    print("\nResult:")
    print(result['result'])
    
    print("\nWarnings:")
    for warning in warnings:
        print(f"- {warning}")
    
    # Check if there's a fallback query
    if 'fallback_query' in result:
        print(f"\nFallback query: {result['fallback_query']}")

def test_query_with_correct_structure(validator):
    """Test a query with correct table and column names."""
    print("\n--- Testing query with correct structure ---")
    
    # Correct query
    test_query = """
    SELECT employees.id, employees.name, employees.job_title, departments.dept_name
    FROM employees 
    JOIN departments ON employees.department_id = departments.id
    WHERE employees.salary > 80000
    """
    
    print(f"Original query: {test_query}")
    
    # Execute the query with the validator
    result, warnings = validator.execute_query_safely(test_query)
    
    # Print the result and warnings
    print("\nResult:")
    print(result['result'])
    
    print("\nWarnings:")
    for warning in warnings:
        print(f"- {warning}")

def main():
    """Main test function."""
    # Setup test database
    engine = setup_test_database()
    
    # Create schema validator
    validator = SchemaValidator(engine)
    
    # Print the tables in the database
    print("\nAvailable tables and columns:")
    for table, info in validator.tables_info.items():
        print(f"Table: {table}")
        for column in info['columns']:
            print(f"  - {column}")
    
    # Run tests
    test_query_with_valid_tables_invalid_columns(validator)
    test_query_with_correct_structure(validator)

if __name__ == "__main__":
    main()
