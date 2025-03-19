from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Float,
    insert,
    inspect,
    text,
)

def create_database():
    """Create and populate the database with sample data"""
    engine = create_engine("sqlite:///:memory:")
    metadata_obj = MetaData()

    # Create receipts table
    receipts = Table(
        "receipts",
        metadata_obj,
        Column("receipt_id", Integer, primary_key=True),
        Column("customer_name", String(16), primary_key=True),
        Column("price", Float),
        Column("tip", Float),
    )

    # Create waiters table
    waiters = Table(
        "waiters",
        metadata_obj,
        Column("receipt_id", Integer, primary_key=True),
        Column("waiter_name", String(16), primary_key=True),
    )

    # Create all tables in the database
    metadata_obj.create_all(engine)

    # Insert sample data into receipts table
    receipt_rows = [
        {"receipt_id": 1, "customer_name": "Alan Payne", "price": 12.06, "tip": 1.20},
        {"receipt_id": 2, "customer_name": "Alex Mason", "price": 23.86, "tip": 0.24},
        {"receipt_id": 3, "customer_name": "Woodrow Wilson", "price": 53.43, "tip": 5.43},
        {"receipt_id": 4, "customer_name": "Margaret James", "price": 21.11, "tip": 1.00},
    ]
    
    for row in receipt_rows:
        stmt = insert(receipts).values(**row)
        with engine.begin() as connection:
            connection.execute(stmt)

    # Insert sample data into waiters table
    waiter_rows = [
        {"receipt_id": 1, "waiter_name": "Corey Johnson"},
        {"receipt_id": 2, "waiter_name": "Michael Watts"},
        {"receipt_id": 3, "waiter_name": "Michael Watts"},
        {"receipt_id": 4, "waiter_name": "Margaret James"},
    ]
    
    for row in waiter_rows:
        stmt = insert(waiters).values(**row)
        with engine.begin() as connection:
            connection.execute(stmt)
    
    return engine

def get_tables_description(engine):
    """Generate a description of all tables in the database"""
    description = "Allows you to perform SQL queries on the tables. Returns a string representation of the result.\nIt can use the following tables:"

    inspector = inspect(engine)
    for table in ["receipts", "waiters"]:
        columns_info = [(col["name"], col["type"]) for col in inspector.get_columns(table)]
        
        table_description = f"\n\nTable '{table}':\nColumns:"
        for name, col_type in columns_info:
            table_description += f"\n  - {name}: {col_type}"
        
        description += table_description
    
    return description

def execute_query(engine, query):
    """Execute a SQL query and return the results as a string"""
    output = ""
    try:
        with engine.connect() as con:
            rows = con.execute(text(query))
            for row in rows:
                output += "\n" + str(row)
        return output.strip() if output else "Query executed successfully. No rows returned."
    except Exception as e:
        return f"Error executing query: {str(e)}"