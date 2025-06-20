import os
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import sys
from etl_pipeline.core.connections import ConnectionFactory

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

def test_opendental_source_connection():
    """Test connection to OpenDental source MySQL database."""
    print("\nTesting OpenDental source MySQL connection...")
    
    try:
        # Get source engine from ConnectionFactory
        engine = ConnectionFactory.get_opendental_source_connection()
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Connection successful (readonly)")
        
        # List tables and get count
        with engine.connect() as conn:
            # Get table count
            count_result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = :db_name
            """), {"db_name": os.getenv('SOURCE_MYSQL_DB')})
            table_count = count_result.scalar()
            
            # Get sample tables
            result = conn.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :db_name
                    LIMIT 5
                """),
                {"db_name": os.getenv('SOURCE_MYSQL_DB')}
            )
            
            tables = [row[0] for row in result]
            if tables:
                print(f"\n📋 Sample tables in {os.getenv('SOURCE_MYSQL_DB')}:")
                for table in tables:
                    print(f"  - {table}")
                print(f"  ... (showing first 5 of {table_count} total tables)")
            else:
                print(f"\nDatabase: {os.getenv('SOURCE_MYSQL_DB')} (empty)")
        
        # Cleanup
        engine.dispose()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ OpenDental source connection failed: {str(e)}")
        return False

def test_mysql_replication_connection():
    """Test connection to MySQL replication database."""
    print("\nTesting MySQL replication connection...")
    
    try:
        # Get replication engine from ConnectionFactory
        engine = ConnectionFactory.get_mysql_replication_connection()
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Connection successful")
        
        # List tables and get count
        with engine.connect() as conn:
            # Get table count
            count_result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = :db_name
            """), {"db_name": os.getenv('REPLICATION_MYSQL_DB')})
            table_count = count_result.scalar()
            
            # Get sample tables
            result = conn.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :db_name
                    LIMIT 5
                """),
                {"db_name": os.getenv('REPLICATION_MYSQL_DB')}
            )
            
            tables = [row[0] for row in result]
            if tables:
                print(f"\n📋 Sample tables in {os.getenv('REPLICATION_MYSQL_DB')}:")
                for table in tables:
                    print(f"  - {table}")
                print(f"  ... (showing first 5 of {table_count} total tables)")
            else:
                print(f"\nDatabase: {os.getenv('REPLICATION_MYSQL_DB')} (empty)")
        
        # Cleanup
        engine.dispose()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ MySQL replication connection failed: {str(e)}")
        return False

def test_postgres_analytics_connection():
    """Test connection to PostgreSQL analytics database."""
    print("\nTesting PostgreSQL analytics connection...")
    
    try:
        # Get analytics engine from ConnectionFactory
        engine = ConnectionFactory.get_postgres_analytics_connection()
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Connection successful")
        
        # List tables and get count
        with engine.connect() as conn:
            # Get table count
            count_result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = :schema_name
            """), {"schema_name": os.getenv('POSTGRES_SCHEMA', 'raw')})
            table_count = count_result.scalar()
            
            # Get sample tables
            result = conn.execute(
                text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :schema_name
                    LIMIT 5
                """),
                {"schema_name": os.getenv('POSTGRES_SCHEMA', 'raw')}
            )
            
            tables = [row[0] for row in result]
            if tables:
                print(f"\n📋 Sample tables in {os.getenv('POSTGRES_SCHEMA', 'raw')}:")
                for table in tables:
                    print(f"  - {table}")
                print(f"  ... (showing first 5 of {table_count} total tables)")
            else:
                print(f"\nSchema: {os.getenv('POSTGRES_SCHEMA', 'raw')} (empty)")
        
        # Cleanup
        engine.dispose()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ PostgreSQL analytics connection failed: {str(e)}")
        return False

def main():
    """Run all connection tests."""
    print_header("Database Connection Tests")
    
    # Test source connection
    if not test_opendental_source_connection():
        print("\n❌ Source connection test failed")
        sys.exit(1)
    
    # Test replication connection
    if not test_mysql_replication_connection():
        print("\n❌ Replication connection test failed")
        sys.exit(1)
    
    # Test analytics connection
    if not test_postgres_analytics_connection():
        print("\n❌ Analytics connection test failed")
        sys.exit(1)
    
    print("\n✅ All connection tests passed!")

if __name__ == "__main__":
    main() 