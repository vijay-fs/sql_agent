#!/usr/bin/env python3
"""
Test script to verify the enhanced foreign key handling for projects table
"""

from sqlalchemy import create_engine, text
from database.schema_validator import SchemaValidator
import json
import os

def setup_test_db():
    """Set up a test database with a structure similar to the real projects database"""
    # Use a file-based SQLite database to ensure persistence
    test_db_path = 'test_project_db.db'
    
    # Remove the database file if it exists to start fresh
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        
    # Create engine with echo for debugging
    engine = create_engine(f'sqlite:///{test_db_path}', echo=False)
    
    # Start a transaction to ensure proper commits
    with engine.begin() as conn:
        # Enable foreign keys in SQLite
        conn.execute(text('PRAGMA foreign_keys = ON;'))
        
        # Drop all tables if they exist
        tables = ['task_users', 'tasks', 'projects', 'clients', 'project_categories']
        
        # Try to drop tables first (ignore errors if tables don't exist)
        for table in tables:
            try:
                conn.execute(text(f'DROP TABLE IF EXISTS {table}'))
            except Exception as e:
                print(f"Note: couldn't drop table {table}: {e}")
        
        # Create project_categories table first since it will be referenced
        conn.execute(text('''
        CREATE TABLE project_categories (
            id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL,
            description TEXT
        )
        '''))
        
        # Create clients table since it will be referenced
        conn.execute(text('''
        CREATE TABLE clients (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            description TEXT
        )
        '''))
        
        # Now create projects table with foreign key constraints
        conn.execute(text('''
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            project_name TEXT NOT NULL,
            project_summary TEXT,
            project_admin INTEGER,
            start_date TEXT,
            deadline TEXT,
            notes TEXT,
            category_id INTEGER,
            client_id INTEGER,
            team_id INTEGER,
            feedback TEXT,
            manual_timelog TEXT,
            client_view_task TEXT,
            allow_client_notification TEXT,
            completion_percent INTEGER,
            calculate_task_progress BOOLEAN,
            created_at TEXT,
            updated_at TEXT,
            deleted_at TEXT,
            project_budget REAL,
            currency_id INTEGER,
            hours_allocated REAL,
            status TEXT,
            added_by INTEGER,
            last_updated_by INTEGER,
            hash TEXT,
            public BOOLEAN,
            FOREIGN KEY (category_id) REFERENCES project_categories(id),
            FOREIGN KEY (client_id) REFERENCES clients(id)
        )
        '''))
        
        # Create tasks table with foreign key to projects
        conn.execute(text('''
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            project_id INTEGER,
            status TEXT,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
        '''))
        
        # Create task_users table with foreign key to projects
        conn.execute(text('''
        CREATE TABLE task_users (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            project_id INTEGER,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
        '''))
        
        # Create project_milestones table with foreign key to projects
        conn.execute(text('''
        CREATE TABLE project_milestones (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            summary TEXT,
            project_id INTEGER,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
        '''))
        
        # Insert sample data
        project_categories = [
            (1, "Web Development", "All web development projects"),
            (2, "Mobile App", "Mobile application development"),
            (3, "Design", "Design projects"),
            (4, "Marketing", "Marketing campaigns")
        ]
        
        clients = [
            (1, "Acme Inc", "contact@acme.com", "Our biggest client"),
            (2, "Globex Corp", "info@globex.com", "International client"),
            (3, "Stark Industries", "tony@stark.com", "Tech industry leader")
        ]
        
        projects = [
            (1, "Payment Billing System", "Expedita quo quibusdam nostrum nemo iure eaque est.", 
             None, "2024-09-03", "2025-01-03", "Project notes here", 3, 1, None, 
             "Client feedback", "disable", "disable", "disable", 44, True, 
             "2025-02-03 17:42:09", "2025-02-03 17:42:09", None, None, 1, None, 
             "in progress", 1, None, "OCNikrfsJMBuHvaVV7ym5m8PXIw9OVOf", False),
            (2, "Website Copier Project", "Tempora voluptatem nulla corporis dolores non.", 
             None, "2024-10-03", "2025-02-03", "More notes", 2, 2, None, 
             "More feedback", "disable", "disable", "disable", 54, True, 
             "2025-02-03 17:42:09", "2025-02-03 17:42:09", None, None, 1, None, 
             "in progress", 1, None, "gc8EC0c85TL37TJQt1wNsdsHH7F4veww", False)
        ]
        
        tasks = [
            (1, "Create homepage", "Design the homepage", 1, "in progress"),
            (2, "Setup payment gateway", "Integrate with Stripe", 1, "pending"),
            (3, "Mobile design", "Create mobile mockups", 2, "completed")
        ]
        
        task_users = [
            (1, 101, 1),
            (2, 102, 1)
        ]
        
        project_milestones = [
            (1, "Alpha Release", "First version with basic features", 1),
            (2, "Beta Testing", "User acceptance testing", 2)
        ]
        
        # Insert data using individual inserts
        # Project categories
        for category in project_categories:
            conn.execute(text("""
                INSERT INTO project_categories (id, category_name, description) 
                VALUES (:id, :name, :desc)
            """), {"id": category[0], "name": category[1], "desc": category[2]})
            
        # Clients
        for client in clients:
            conn.execute(text("""
                INSERT INTO clients (id, name, email, description) 
                VALUES (:id, :name, :email, :desc)
            """), {"id": client[0], "name": client[1], "email": client[2], "desc": client[3]})
            
        # Projects - do them one by one due to complexity
        for project in projects:
            conn.execute(text("""
                INSERT INTO projects 
                (id, project_name, project_summary, project_admin, start_date, deadline, notes, 
                category_id, client_id, team_id, feedback, manual_timelog, client_view_task, 
                allow_client_notification, completion_percent, calculate_task_progress, created_at, 
                updated_at, deleted_at, project_budget, currency_id, hours_allocated, status, 
                added_by, last_updated_by, hash, public) 
                VALUES 
                (:id, :name, :summary, :admin, :start, :deadline, :notes, :cat_id, :client_id, 
                :team_id, :feedback, :manual, :client_view, :client_notif, :completion, :calc_progress, 
                :created, :updated, :deleted, :budget, :currency, :hours, :status, :added_by, 
                :last_updated, :hash, :public)
            """), {
                "id": project[0], "name": project[1], "summary": project[2], "admin": project[3],
                "start": project[4], "deadline": project[5], "notes": project[6], "cat_id": project[7],
                "client_id": project[8], "team_id": project[9], "feedback": project[10], "manual": project[11],
                "client_view": project[12], "client_notif": project[13], "completion": project[14],
                "calc_progress": project[15], "created": project[16], "updated": project[17], "deleted": project[18],
                "budget": project[19], "currency": project[20], "hours": project[21], "status": project[22],
                "added_by": project[23], "last_updated": project[24], "hash": project[25], "public": project[26]
            })
            
        # Tasks
        for task in tasks:
            conn.execute(text("""
                INSERT INTO tasks (id, title, description, project_id, status) 
                VALUES (:id, :title, :desc, :project_id, :status)
            """), {
                "id": task[0], "title": task[1], "desc": task[2], 
                "project_id": task[3], "status": task[4]
            })
        
        # Task users
        for task_user in task_users:
            conn.execute(text("""
                INSERT INTO task_users (id, user_id, project_id) 
                VALUES (:id, :user_id, :project_id)
            """), {
                "id": task_user[0], "user_id": task_user[1], "project_id": task_user[2]
            })
            
        # Project milestones
        for milestone in project_milestones:
            conn.execute(text("""
                INSERT INTO project_milestones (id, title, summary, project_id) 
                VALUES (:id, :title, :summary, :project_id)
            """), {
                "id": milestone[0], "title": milestone[1], "summary": milestone[2], "project_id": milestone[3]
            })
        
        # Verify data was inserted correctly
        result = conn.execute(text("SELECT * FROM projects"))
        print("Inserted projects:")
        for row in result.fetchall():
            print(f"ID: {row[0]}, Name: {row[1]}")
    
    return engine

def test_project_foreign_keys():
    """Test that foreign keys are correctly resolved in projects table"""
    engine = setup_test_db()
    validator = SchemaValidator(engine)
    
    # Show the database schema for debugging
    print("\nDatabase Schema Details:")
    print("Tables:", list(validator.tables_info.keys()))
    
    # Test 1: Query with correct column names
    print("\nTest 1: Original query with correct column names")
    query = "SELECT id, project_name FROM projects"
    result, warnings = validator.execute_query_safely(query)
    print(f"Query: {query}")
    if result and result.get('rows'):
        print(f"Found {len(result.get('rows'))} rows")
        print("First row sample:", result.get('rows')[0])
    else:
        print("No results found or error occurred.")
        print("Result:", result)
    
    # Test 2: Query with invalid column name (should trigger fallback) - no joins
    print("\nTest 2: Query with invalid column name (should trigger fallback)")
    query = "SELECT id, name FROM projects"  # 'name' doesn't exist, should use project_name
    result, warnings = validator.execute_query_safely(query)
    print(f"Query: {query}")
    
    if 'data' in result and result['data']:
        print(f"Found {len(result['data'])} rows in fallback data")
        print("First row sample:", json.dumps(result['data'][0], indent=2, default=str))
    else:
        print("No fallback data found")
        print("Result:", result)
    
    print("\nWarnings for Test 2:")
    for warning in warnings:
        print(f"- {warning}")
    
    # Test 3: Query with foreign keys (should include descriptive values from related tables)
    print("\nTest 3: Query with foreign keys (testing automatic resolution)")
    query = "SELECT id, project_name, category_id, client_id FROM projects"
    result, warnings = validator.execute_query_safely(query)
    print(f"Query: {query}")
    print("Result data sample:")
    print(json.dumps(result['data'][:1], indent=2, default=str))
    
    # Test 3b: Deliberately use a column name that doesn't exist to trigger fallback with FK resolution
    print("\nTest 3b: Deliberately trigger fallback with FK resolution")
    query = "SELECT id, name, category_id, client_id FROM projects"  # 'name' doesn't exist
    result, warnings = validator.execute_query_safely(query)
    print(f"Query: {query}")
    
    if 'enhanced_data' in result and result['enhanced_data']:
        print(f"Found {len(result['enhanced_data'])} rows with enhanced data")
        print("First enhanced row with relationships:")
        print(json.dumps(result['enhanced_data'][0], indent=2, default=str))
        
        # Extract relationship info
        sample_row = result['enhanced_data'][0]
        print("\nExtracted foreign key relationships:")
        
        # Check for project_categories relationship
        if 'category_id' in sample_row and '_related' in sample_row.get('category_id', ''):
            print(f"Category: {sample_row['category_id']}")
        elif 'project_categories' in sample_row:
            cat_data = sample_row['project_categories']
            print(f"Category: ID={cat_data['id']}, Name={cat_data.get('display', '')}")
            
        # Check for clients relationship
        if 'client_id' in sample_row and '_related' in sample_row.get('client_id', ''):
            print(f"Client: {sample_row['client_id']}")
        elif 'clients' in sample_row:
            client_data = sample_row['clients']
            print(f"Client: ID={client_data['id']}, Name={client_data.get('display', '')}")
        
        # Check for incoming relationships (collections)
        for rel_key in sample_row.keys():
            if rel_key.endswith('_collection') and isinstance(sample_row[rel_key], list) and sample_row[rel_key]:
                collection = sample_row[rel_key]
                print(f"\n{rel_key.replace('_collection', '')} references (count: {len(collection)}):")
                for i, item in enumerate(collection[:2]):  # Show first 2 only
                    print(f"  {i+1}. {item.get('display', 'Unknown')} ({item.get('table', 'Unknown')})")
                if len(collection) > 2:
                    print(f"  ... and {len(collection)-2} more items")
    else:
        print("No enhanced data found")
        print("Result:", result)
    
    # Test 4: Erroneous query referencing non-existent table
    print("\nTest 4: Query with invalid table joining another table")
    query = """
    SELECT p.id, p.name, pc.category_name 
    FROM proj AS p  -- 'proj' doesn't exist
    JOIN project_categories AS pc ON p.category_id = pc.id
    """
    result, warnings = validator.execute_query_safely(query)
    print(f"Query: {query}")
    
    if 'enhanced_data' in result and result['enhanced_data']:
        print(f"Found {len(result['enhanced_data'])} rows with enhanced data")
        print("First enhanced row:")
        print(json.dumps(result['enhanced_data'][0], indent=2, default=str))
    else:
        print("No enhanced data found or error occurred")
        if 'result' in result:
            print("Result info:", result['result'])
    
    print("\nWarnings for Test 4:")
    for warning in warnings:
        print(f"- {warning}")

if __name__ == "__main__":
    test_project_foreign_keys()
