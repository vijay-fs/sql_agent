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
    
    # Create in-memory SQLite database with foreign key support
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
        
        # Insert sample data - first into departments (referenced table)
        conn.execute(text('''
        INSERT INTO departments (id, dept_name, location)
        VALUES  (1, 'Engineering', 'Building A'),
                (2, 'Design', 'Building B'),
                (3, 'Data Science', 'Building C')
        '''))
        
        # Then insert into employees (referencing table)
        conn.execute(text('''
        INSERT INTO employees (id, name, job_title, salary, department_id)
        VALUES  (1, 'John Smith', 'Software Engineer', 85000, 1),
                (2, 'Mary Johnson', 'Project Manager', 95000, 1),
                (3, 'James Brown', 'UI Designer', 75000, 2),
                (4, 'Patricia Davis', 'Data Scientist', 90000, 3)
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

def test_foreign_key_inclusion(validator):
    """Test that foreign keys are automatically included in fallback queries."""
    print("\n--- Testing foreign key inclusion in fallback queries ---")
    
    # Query that will trigger the fallback with foreign keys
    test_query = """
    SELECT employees.id, employees.name, employees.nonexistent_field, departments.dept_name
    FROM employees
    WHERE employees.salary > 70000
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
        # Check if the fallback query includes a JOIN
        if "JOIN" in result['fallback_query']:
            print("✅ SUCCESS: Foreign key relations were automatically included in the fallback query!")
            
            # Display the data retrieved
            print("\nRetrieved data with foreign key relations:")
            if 'data' in result and result['data']:
                # Print the first few records to show the joined data
                for i, row in enumerate(result['data'][:3]):  # Show only first 3 rows
                    print(f"\nRecord {i+1}:")
                    for key, value in row.items():
                        # Highlight the foreign key fields
                        if key.startswith('fk_'):
                            print(f"  → {key}: {value}")
                        else:
                            print(f"  {key}: {value}")
            else:
                print("No data rows retrieved.")
        else:
            print("❌ FAILURE: Foreign key relations were NOT included in the fallback query.")

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
        # Also print foreign keys if any
        if info['foreign_keys']:
            print(f"  Foreign keys:")
            for fk in info['foreign_keys']:
                print(f"    - {', '.join(fk.get('constrained_columns', []))} -> {fk.get('referred_table', '')}.{', '.join(fk.get('referred_columns', []))}")
    
    # Run tests
    test_query_with_valid_tables_invalid_columns(validator)
    test_query_with_correct_structure(validator)
    test_foreign_key_inclusion(validator)

if __name__ == "__main__":
    main()
