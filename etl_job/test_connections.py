from connection_factory import get_source_connection, get_target_connection
from sqlalchemy import text

print("Testing database connections...")

# Test source connection
print("\nTesting source (MySQL) connection...")
with get_source_connection().connect() as conn:
    # Test basic connection
    result = conn.execute(text("SELECT 1"))
    print("✓ Source connection successful")
    
    # Get current user and host
    result = conn.execute(text("SELECT CURRENT_USER()"))
    current_user = result.scalar()
    print(f"\nConnected as: {current_user}")
    
    # Show available databases with details
    print("\nMySQL Database Details:")
    print("-" * 50)
    result = conn.execute(text("""
        SELECT 
            table_schema as database_name,
            COUNT(*) as table_count,
            SUM(table_rows) as estimated_rows,
            SUM(data_length + index_length) as total_size_bytes
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'sys')
        GROUP BY table_schema
    """))
    
    for row in result:
        db_name, table_count, est_rows, total_size = row
        size_mb = round(total_size / (1024 * 1024), 2)
        print(f"Database: {db_name}")
        print(f"Tables: {table_count}")
        print(f"Estimated Rows: {est_rows:,}")
        print(f"Total Size: {size_mb} MB")
        print("-" * 50)
    
    # Show user privileges
    print("\nUser Privileges:")
    print("-" * 50)
    result = conn.execute(text("SHOW GRANTS"))
    for row in result:
        print(row[0])
    print("-" * 50)

# Test target connection
print("\nTesting target (PostgreSQL) connection...")
with get_target_connection().connect() as conn:
    # Test basic connection
    result = conn.execute(text("SELECT 1"))
    print("✓ Target connection successful")
    
    # Show available schemas with more details
    print("\nAvailable PostgreSQL schemas:")
    result = conn.execute(text("""
        SELECT 
            schema_name,
            schema_owner,
            CASE 
                WHEN schema_name = 'public' THEN 'Default schema'
                WHEN schema_name LIKE 'pg_%' THEN 'System schema'
                ELSE 'User schema'
            END as schema_type
        FROM information_schema.schemata 
        WHERE schema_name NOT LIKE 'pg_temp%'
        AND schema_name NOT LIKE 'pg_toast%'
        ORDER BY 
            CASE 
                WHEN schema_name = 'public' THEN 1
                WHEN schema_name LIKE 'pg_%' THEN 2
                ELSE 3
            END,
            schema_name
    """))
    
    print("\nSchema Details:")
    print("-" * 50)
    for row in result:
        schema_name, owner, schema_type = row
        print(f"Schema: {schema_name}")
        print(f"Owner: {owner}")
        print(f"Type: {schema_type}")
        print("-" * 50)

print("\nAll connections verified successfully!")
