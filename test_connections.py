# Updated to use the core connections module directly
from sqlalchemy import text
import os
from dotenv import load_dotenv
from etl_pipeline.core.connections import ConnectionFactory
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import sys

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

def get_env_with_fallback(new_var, old_var):
    """Get environment variable with fallback and show which one is used."""
    new_val = os.getenv(new_var)
    old_val = os.getenv(old_var)
    
    if new_val:
        return new_val, f"{new_var} (template)"
    elif old_val:
        return old_val, f"{old_var} (legacy fallback)"
    else:
        return None, f"MISSING (checked {new_var}, {old_var})"

def test_opendental_source_connection():
    """Test connection to OpenDental source MySQL database."""
    print("\nTesting OpenDental source MySQL connection...")
    
    # Print environment variables with template naming
    host, host_source = get_env_with_fallback('OPENDENTAL_SOURCE_HOST', 'SOURCE_MYSQL_HOST')
    port, port_source = get_env_with_fallback('OPENDENTAL_SOURCE_PORT', 'SOURCE_MYSQL_PORT')
    database, db_source = get_env_with_fallback('OPENDENTAL_SOURCE_DB', 'SOURCE_MYSQL_DB')
    user, user_source = get_env_with_fallback('OPENDENTAL_SOURCE_USER', 'SOURCE_MYSQL_USER')
    password, pwd_source = get_env_with_fallback('OPENDENTAL_SOURCE_PASSWORD', 'SOURCE_MYSQL_PASSWORD')
    
    print(f"HOST: {host} [{host_source}]")
    print(f"PORT: {port} [{port_source}]")
    print(f"DATABASE: {database} [{db_source}]")
    print(f"USER: {user} [{user_source}]")
    print(f"PASSWORD: {'SET' if password else 'MISSING'} [{pwd_source}]")
    
    try:
        # Test both new and legacy connection methods
        print("\n🔧 Testing improved ConnectionFactory method...")
        new_engine = ConnectionFactory.get_opendental_source_connection()
        with new_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ New method (get_opendental_source_connection) successful")
        
        print("\n🔧 Testing legacy ConnectionFactory method...")
        legacy_engine = ConnectionFactory.get_source_connection()
        with legacy_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Legacy method (get_source_connection) successful")
        
        # List tables using the new connection
        with new_engine.connect() as conn:
            conn.execute(text(f"USE {database}"))
            result = conn.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :db_name
                    LIMIT 5
                """),
                {"db_name": database}
            )
            
            tables = [row[0] for row in result]
            if tables:
                print(f"\n📋 Sample tables in {database}:")
                for table in tables:
                    print(f"  - {table}")
                print("  ... (showing first 5 tables)")
            else:
                print(f"\nDatabase: {database} (empty)")
        
        # Cleanup
        new_engine.dispose()
        legacy_engine.dispose()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ OpenDental source connection failed: {str(e)}")
        return False

def test_mysql_replication_connection():
    """Test connection to MySQL replication database."""
    print("\nTesting MySQL replication connection...")
    
    # Print environment variables with template naming
    host, host_source = get_env_with_fallback('MYSQL_REPLICATION_HOST', 'REPLICATION_MYSQL_HOST')
    port, port_source = get_env_with_fallback('MYSQL_REPLICATION_PORT', 'REPLICATION_MYSQL_PORT')
    database, db_source = get_env_with_fallback('MYSQL_REPLICATION_DB', 'REPLICATION_MYSQL_DB')
    user, user_source = get_env_with_fallback('MYSQL_REPLICATION_USER', 'REPLICATION_MYSQL_USER')
    password, pwd_source = get_env_with_fallback('MYSQL_REPLICATION_PASSWORD', 'REPLICATION_MYSQL_PASSWORD')
    
    print(f"HOST: {host} [{host_source}]")
    print(f"PORT: {port} [{port_source}]")
    print(f"DATABASE: {database} [{db_source}]")
    print(f"USER: {user} [{user_source}]")
    print(f"PASSWORD: {'SET' if password else 'MISSING'} [{pwd_source}]")
    
    try:
        # Test both new and legacy connection methods
        print("\n🔧 Testing improved ConnectionFactory method...")
        new_engine = ConnectionFactory.get_mysql_replication_connection()
        with new_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ New method (get_mysql_replication_connection) successful")
        
        print("\n🔧 Testing legacy ConnectionFactory methods...")
        staging_engine = ConnectionFactory.get_staging_connection()
        with staging_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Legacy method (get_staging_connection) successful")
        
        replication_engine = ConnectionFactory.get_replication_connection()
        with replication_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Legacy method (get_replication_connection) successful")
        
        # List tables using the new connection
        with new_engine.connect() as conn:
            conn.execute(text(f"USE {database}"))
            result = conn.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :db_name
                    LIMIT 5
                """),
                {"db_name": database}
            )
            
            tables = [row[0] for row in result]
            if tables:
                print(f"\n📋 Sample tables in {database}:")
                for table in tables:
                    print(f"  - {table}")
                print("  ... (showing first 5 tables)")
            else:
                print(f"\nDatabase: {database} (empty)")
        
        # Cleanup
        new_engine.dispose()
        staging_engine.dispose()
        replication_engine.dispose()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ MySQL replication connection failed: {str(e)}")
        return False

def test_postgres_analytics_connection():
    """Test connection to PostgreSQL analytics database."""
    print("\nTesting PostgreSQL analytics connection...")
    
    # Print environment variables with template naming
    host, host_source = get_env_with_fallback('POSTGRES_ANALYTICS_HOST', 'ANALYTICS_POSTGRES_HOST')
    port, port_source = get_env_with_fallback('POSTGRES_ANALYTICS_PORT', 'ANALYTICS_POSTGRES_PORT')
    database, db_source = get_env_with_fallback('POSTGRES_ANALYTICS_DB', 'ANALYTICS_POSTGRES_DB')
    user, user_source = get_env_with_fallback('POSTGRES_ANALYTICS_USER', 'ANALYTICS_POSTGRES_USER')
    password, pwd_source = get_env_with_fallback('POSTGRES_ANALYTICS_PASSWORD', 'ANALYTICS_POSTGRES_PASSWORD')
    schema, schema_source = get_env_with_fallback('POSTGRES_ANALYTICS_SCHEMA', 'ANALYTICS_POSTGRES_SCHEMA')
    
    print(f"HOST: {host} [{host_source}]")
    print(f"PORT: {port} [{port_source}]")
    print(f"DATABASE: {database} [{db_source}]")
    print(f"USER: {user} [{user_source}]")
    print(f"SCHEMA: {schema} [{schema_source}]")
    print(f"PASSWORD: {'SET' if password else 'MISSING'} [{pwd_source}]")
    
    try:
        # Test both new and legacy connection methods
        print("\n🔧 Testing improved ConnectionFactory method...")
        new_engine = ConnectionFactory.get_postgres_analytics_connection()
        with new_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ New method (get_postgres_analytics_connection) successful")
        
        print("\n🔧 Testing legacy ConnectionFactory methods...")
        target_engine = ConnectionFactory.get_target_connection()
        with target_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Legacy method (get_target_connection) successful")
        
        analytics_engine = ConnectionFactory.get_analytics_connection()
        with analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Legacy method (get_analytics_connection) successful")
        
        # List tables using the new connection
        with new_engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :schema_name
                    LIMIT 5
                """),
                {"schema_name": schema or 'raw'}
            )
            
            tables = [row[0] for row in result]
            if tables:
                print(f"\n📋 Sample tables in {schema or 'raw'} schema:")
                for table in tables:
                    print(f"  - {table}")
                print("  ... (showing first 5 tables)")
            else:
                print(f"\nSchema: {schema or 'raw'} (empty)")
        
        # Cleanup
        new_engine.dispose()
        target_engine.dispose()
        analytics_engine.dispose()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ PostgreSQL analytics connection failed: {str(e)}")
        return False

def test_connection_factory_batch():
    """Test the ConnectionFactory.test_connections() method."""
    print("\nTesting ConnectionFactory.test_connections() method...")
    
    try:
        results = ConnectionFactory.test_connections()
        
        print("\n📊 ConnectionFactory Test Results:")
        print("Template naming convention results:")
        print(f"  - opendental_source: {'✅' if results.get('opendental_source') else '❌'}")
        print(f"  - mysql_replication: {'✅' if results.get('mysql_replication') else '❌'}")
        print(f"  - postgres_analytics: {'✅' if results.get('postgres_analytics') else '❌'}")
        
        print("\nLegacy compatibility results:")
        print(f"  - source: {'✅' if results.get('source') else '❌'}")
        print(f"  - replication: {'✅' if results.get('replication') else '❌'}")
        print(f"  - analytics: {'✅' if results.get('analytics') else '❌'}")
        
        return all([
            results.get('opendental_source', False),
            results.get('mysql_replication', False),
            results.get('postgres_analytics', False)
        ])
        
    except Exception as e:
        print(f"❌ ConnectionFactory.test_connections() failed: {str(e)}")
        return False

def main():
    """Run all connection tests."""
    print_header("Database Connection Tests - Template Naming Conventions")
    print("🔍 Testing database connections with template naming conventions...")
    print("📝 This test shows both template and legacy environment variable usage")
    
    # Test OpenDental source MySQL
    print_section("OpenDental Source Database (MySQL)")
    source_success = test_opendental_source_connection()
    
    # Test MySQL replication
    print_section("MySQL Replication Database")
    replication_success = test_mysql_replication_connection()
    
    # Test PostgreSQL analytics
    print_section("PostgreSQL Analytics Database")
    postgres_success = test_postgres_analytics_connection()
    
    # Test batch connection method
    print_section("ConnectionFactory Batch Test")
    batch_success = test_connection_factory_batch()
    
    # Print final summary
    print_header("Connection Test Summary")
    print(f"OpenDental Source (MySQL): {'✅' if source_success else '❌'}")
    print(f"MySQL Replication: {'✅' if replication_success else '❌'}")
    print(f"PostgreSQL Analytics: {'✅' if postgres_success else '❌'}")
    print(f"ConnectionFactory Batch Test: {'✅' if batch_success else '❌'}")
    
    all_success = all([source_success, replication_success, postgres_success, batch_success])
    
    if all_success:
        print("\n🎉 All connection tests passed!")
        print("✅ Template naming conventions are working correctly")
        print("✅ Legacy compatibility is maintained")
    else:
        print("\n❌ Some connection tests failed")
        print("💡 Check your .env file and ensure all required environment variables are set")
    
    return all_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)