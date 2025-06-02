from connection_factory import get_source_connection, get_staging_connection, get_target_connection
from sqlalchemy import text
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def print_header(title):
    """Print a formatted header."""
    print(f"\n{'='*60}")
    print(f"{title.upper()}")
    print(f"{'='*60}")

def print_section(title):
    """Print a formatted section header."""
    print(f"\n{title}")
    print("-" * 50)

def test_source_connection():
    """Test the source OpenDental MySQL connection (readonly)."""
    print_header("Testing Source Connection (OpenDental MySQL - ReadOnly)")
    
    try:
        with get_source_connection(readonly=True).connect() as conn:
            # Test basic connection
            result = conn.execute(text("SELECT 1"))
            print("✓ Source connection successful")
            
            # Connection details
            print_section("Connection Details")
            
            # Current user and host
            result = conn.execute(text("SELECT CURRENT_USER()"))
            current_user = result.scalar()
            print(f"Connected as: {current_user}")
            
            result = conn.execute(text("SELECT @@hostname"))
            hostname = result.scalar()
            print(f"MySQL Host: {hostname}")
            
            result = conn.execute(text("SELECT @@port"))
            port = result.scalar()
            print(f"MySQL Port: {port}")
            
            result = conn.execute(text("SELECT VERSION()"))
            version = result.scalar()
            print(f"MySQL Version: {version}")
            
            # Environment configuration
            print_section("Environment Configuration")
            print(f"OPENDENTAL_SOURCE_HOST: {os.getenv('OPENDENTAL_SOURCE_HOST')}")
            print(f"OPENDENTAL_SOURCE_PORT: {os.getenv('OPENDENTAL_SOURCE_PORT')}")
            print(f"OPENDENTAL_SOURCE_DB: {os.getenv('OPENDENTAL_SOURCE_DB')}")
            print(f"OPENDENTAL_SOURCE_PW: {'SET' if os.getenv('OPENDENTAL_SOURCE_PW') else 'MISSING'}")  # Changed to show if password is set
            
            # Show database details
            print_section("Database Details")
            result = conn.execute(text(f"""
                SELECT 
                    table_schema as database_name,
                    COUNT(*) as table_count,
                    COALESCE(SUM(table_rows), 0) as estimated_rows,
                    COALESCE(SUM(data_length + index_length), 0) as total_size_bytes
                FROM information_schema.tables 
                WHERE table_schema = '{os.getenv('OPENDENTAL_SOURCE_DB')}'
                AND table_type = 'BASE TABLE'
                GROUP BY table_schema
            """))
            
            for row in result:
                db_name, table_count, est_rows, total_size = row
                size_mb = round(total_size / (1024 * 1024), 2) if total_size else 0
                print(f"Database: {db_name}")
                print(f"Tables: {table_count}")
                print(f"Estimated Rows: {est_rows:,}")
                print(f"Total Size: {size_mb} MB")
            
            # Show key OpenDental tables
            print_section("Key OpenDental Tables")
            result = conn.execute(text(f"""
                SELECT 
                    table_name,
                    COALESCE(table_rows, 0) as estimated_rows,
                    COALESCE(ROUND((data_length + index_length) / 1024 / 1024, 2), 0) as size_mb
                FROM information_schema.tables 
                WHERE table_schema = '{os.getenv('OPENDENTAL_SOURCE_DB')}'
                AND table_name IN ('patient', 'appointment', 'procedurelog', 'claimproc', 'provider')
                ORDER BY table_rows DESC
            """))
            
            for row in result:
                table_name, est_rows, size_mb = row
                print(f"{table_name}: {est_rows:,} rows, {size_mb} MB")
            
            # Show user privileges
            print_section("User Privileges")
            result = conn.execute(text("SHOW GRANTS"))
            for row in result:
                print(row[0])
            
            # Test read-only enforcement
            print_section("Read-Only Access Verification")
            try:
                conn.execute(text("CREATE TABLE test_readonly_violation (id INT)"))
                print("❌ WARNING: Write access is NOT properly restricted!")
            except Exception as e:
                print("✓ Read-only access properly enforced")
                print(f"  Error (expected): {str(e)[:100]}...")
            
    except Exception as e:
        print(f"❌ Source connection failed: {str(e)}")
        return False
    
    return True

def test_target_mysql_connection():
    """Test the target MySQL connection (staging/local MySQL)."""
    print_header("Testing Target MySQL Connection (Local/Staging)")
    
    try:
        with get_staging_connection().connect() as conn:
            # Test basic connection
            result = conn.execute(text("SELECT 1"))
            print("✓ Target MySQL connection successful")
            
            # Connection details
            print_section("Connection Details")
            
            # Current user and host
            result = conn.execute(text("SELECT CURRENT_USER()"))
            current_user = result.scalar()
            print(f"Connected as: {current_user}")
            
            result = conn.execute(text("SELECT @@hostname"))
            hostname = result.scalar()
            print(f"MySQL Host: {hostname}")
            
            result = conn.execute(text("SELECT @@port"))
            port = result.scalar()
            print(f"MySQL Port: {port}")
            
            result = conn.execute(text("SELECT VERSION()"))
            version = result.scalar()
            print(f"MySQL Version: {version}")
            
            # Environment configuration
            print_section("Environment Configuration")
            print(f"STAGING_MYSQL_HOST: {os.getenv('STAGING_MYSQL_HOST')}")
            print(f"STAGING_MYSQL_PORT: {os.getenv('STAGING_MYSQL_PORT')}")
            print(f"STAGING_MYSQL_DATABASE: {os.getenv('STAGING_MYSQL_DB')}")
            print(f"STAGING_MYSQL_USER: {os.getenv('STAGING_MYSQL_USER')}")
            
            # Use the staging database
            conn.execute(text(f"USE {os.getenv('STAGING_MYSQL_DB')}"))
            
            # Show database details
            print_section("Database Details")
            result = conn.execute(text(f"""
                SELECT 
                    table_schema as database_name,
                    COUNT(*) as table_count,
                    COALESCE(SUM(table_rows), 0) as estimated_rows,
                    COALESCE(SUM(data_length + index_length), 0) as total_size_bytes
                FROM information_schema.tables 
                WHERE table_schema = '{os.getenv('STAGING_MYSQL_DB')}'
                AND table_type = 'BASE TABLE'
                GROUP BY table_schema
            """))
            
            row = result.first()
            if row:
                db_name, table_count, est_rows, total_size = row
                size_mb = round(total_size / (1024 * 1024), 2) if total_size else 0
                print(f"Database: {db_name}")
                print(f"Tables: {table_count}")
                print(f"Estimated Rows: {est_rows:,}")
                print(f"Total Size: {size_mb} MB")
            else:
                print(f"Database: {os.getenv('STAGING_MYSQL_DB')} (empty)")
            
            # Show existing tables
            print_section("Existing Tables")
            result = conn.execute(text(f"""
                SELECT 
                    table_name,
                    COALESCE(table_rows, 0) as estimated_rows,
                    COALESCE(ROUND((data_length + index_length) / 1024 / 1024, 2), 0) as size_mb,
                    create_time
                FROM information_schema.tables 
                WHERE table_schema = '{os.getenv('STAGING_MYSQL_DB')}'
                AND table_type = 'BASE TABLE'
                ORDER BY create_time DESC
                LIMIT 10
            """))
            
            tables = list(result)
            if tables:
                for row in tables:
                    table_name, est_rows, size_mb, create_time = row
                    print(f"{table_name}: {est_rows:,} rows, {size_mb} MB (created: {create_time})")
            else:
                print("No tables found (new database)")
            
            # Test write access
            print_section("Write Access Verification")
            try:
                conn.execute(text("CREATE TABLE IF NOT EXISTS test_write_access (id INT, test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"))
                conn.execute(text("INSERT INTO test_write_access (id) VALUES (1)"))
                result = conn.execute(text("SELECT COUNT(*) FROM test_write_access"))
                count = result.scalar()
                conn.execute(text("DROP TABLE test_write_access"))
                print(f"✓ Write access verified (inserted and deleted {count} test record)")
            except Exception as e:
                print(f"❌ Write access test failed: {str(e)}")
            
    except Exception as e:
        print(f"❌ Target MySQL connection failed: {str(e)}")
        return False
    
    return True

def test_target_postgres_connection():
    """Test the target PostgreSQL connection."""
    print_header("Testing Target PostgreSQL Connection (Analytics)")
    
    try:
        with get_target_connection().connect() as conn:
            # Test basic connection
            result = conn.execute(text("SELECT 1"))
            print("✓ Target PostgreSQL connection successful")
            
            # Connection details
            print_section("Connection Details")
            
            # Current user and database
            result = conn.execute(text("SELECT CURRENT_USER"))
            current_user = result.scalar()
            print(f"Connected as: {current_user}")
            
            result = conn.execute(text("SELECT CURRENT_DATABASE()"))
            current_db = result.scalar()
            print(f"Current Database: {current_db}")
            
            result = conn.execute(text("SELECT inet_server_addr(), inet_server_port()"))
            row = result.first()
            if row:
                server_addr, server_port = row
                print(f"Server Address: {server_addr}")
                print(f"Server Port: {server_port}")
            
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"PostgreSQL Version: {version}")
            
            # Environment configuration
            print_section("Environment Configuration")
            print(f"TARGET_POSTGRES_HOST: {os.getenv('TARGET_POSTGRES_HOST')}")
            print(f"TARGET_POSTGRES_PORT: {os.getenv('TARGET_POSTGRES_PORT')}")
            print(f"TARGET_POSTGRES_DB: {os.getenv('TARGET_POSTGRES_DB')}")
            print(f"TARGET_POSTGRES_USER: {os.getenv('TARGET_POSTGRES_USER')}")
            print(f"TARGET_POSTGRES_SCHEMA: {os.getenv('TARGET_POSTGRES_SCHEMA')}")
            
            # Show available schemas
            print_section("Available Schemas")
            result = conn.execute(text("""
                SELECT 
                    nspname as schema_name,
                    rolname as owner,
                    CASE 
                        WHEN nspname = 'raw' THEN 'ELT schema'
                        WHEN nspname = 'public' THEN 'Default schema'
                        WHEN nspname = 'pg_catalog' THEN 'System schema'
                        WHEN nspname = 'information_schema' THEN 'User schema'
                        ELSE 'User schema'
                    END as description
                FROM pg_namespace
                JOIN pg_roles ON pg_roles.oid = pg_namespace.nspowner
                WHERE nspname NOT LIKE 'pg_%'
                OR nspname = 'pg_catalog'
                ORDER BY nspname
            """))
            
            for row in result:
                schema_name, owner, description = row
                print(f"{schema_name}: {description} (owner: {owner})")
            
            # Show ELT schema tables
            print_section("ELT Schema Tables")
            result = conn.execute(text("""
                SELECT 
                    table_schema,
                    table_name,
                    table_type
                FROM information_schema.tables
                WHERE table_schema = 'raw'
                ORDER BY table_schema, table_name
            """))
            
            for row in result:
                schema, table, type = row
                print(f"\n{schema} schema:")
                print(f"  {table} ({type})")
            
            # Show ELT tracking tables
            print_section("ELT Tracking Tables")
            result = conn.execute(text("""
                SELECT 
                    table_schema,
                    table_name,
                    (SELECT COUNT(*) FROM raw.etl_load_status) as load_status_count,
                    (SELECT COUNT(*) FROM raw.etl_transform_status) as transform_status_count
                FROM information_schema.tables
                WHERE table_schema = 'raw'
                AND table_name IN ('etl_load_status', 'etl_transform_status')
                ORDER BY table_schema, table_name
            """))
            
            for row in result:
                schema, table, load_count, transform_count = row
                if table == 'etl_load_status':
                    print(f"{schema}.{table}: {load_count} records")
                else:
                    print(f"{schema}.{table}: {transform_count} records")
            
            # Test table access
            print_section("Table Access Verification")
            try:
                # Test read access
                conn.execute(text("SELECT COUNT(*) FROM raw.etl_load_status"))
                print("✓ Read access verified")
                
                # Test write access
                conn.execute(text("""
                    INSERT INTO raw.etl_load_status (table_name, last_extracted, rows_extracted, extraction_status)
                    VALUES ('test_table', CURRENT_TIMESTAMP, 0, 'test')
                    ON CONFLICT (table_name) DO NOTHING
                """))
                print("✓ Write access verified")
            except Exception as e:
                print(f"❌ Table access test failed: {str(e)}")
            
            # Show database statistics
            print_section("Database Statistics")
            try:
                result = conn.execute(text("""
                    SELECT
                        pg_size_pretty(pg_database_size(current_database())) as database_size,
                        (SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog = current_database()) as total_tables,
                        (SELECT COUNT(*) FROM information_schema.schemata WHERE catalog_name = current_database()) as total_schemas
                """))
                
                row = result.first()
                if row:
                    db_size, total_tables, total_schemas = row
                    print(f"Database Size: {db_size}")
                    print(f"Total Tables: {total_tables}")
                    print(f"Total Schemas: {total_schemas}")
            except Exception as e:
                print(f"❌ Database statistics query failed: {str(e)}")
            
    except Exception as e:
        print(f"❌ Target PostgreSQL connection failed: {str(e)}")
        return False
    
    return True

def main():
    """Main function to test all connections."""
    print("🔌 TESTING DATABASE CONNECTIONS FOR ELT PIPELINE")
    print("=" * 60)
    
    results = {
        'source': False,
        'target_mysql': False, 
        'target_postgres': False
    }
    
    # Test all connections
    results['source'] = test_source_connection()
    results['target_mysql'] = test_target_mysql_connection()
    results['target_postgres'] = test_target_postgres_connection()
    
    # Summary
    print_header("Connection Test Summary")
    
    success_count = sum(results.values())
    total_count = len(results)
    
    print(f"Source MySQL (OpenDental):     {'✓ PASS' if results['source'] else '❌ FAIL'}")
    print(f"Target MySQL (Local/Staging):  {'✓ PASS' if results['target_mysql'] else '❌ FAIL'}")
    print(f"Target PostgreSQL (Analytics): {'✓ PASS' if results['target_postgres'] else '❌ FAIL'}")
    
    print(f"\nOverall Status: {success_count}/{total_count} connections successful")
    
    if success_count == total_count:
        print("🎉 All database connections are working properly!")
        print("✅ ELT pipeline is ready to run.")
    else:
        print("⚠️  Some connections failed. Please check configuration.")
        print("❌ ELT pipeline may not work correctly.")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)