from etl_pipeline.connection_factory import get_source_connection, get_staging_connection, get_target_connection
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

def test_source_mysql_connection():
    """Test connection to source MySQL database."""
    print("\nTesting source MySQL connection...")
    
    # Print environment variables
    print(f"SOURCE_MYSQL_HOST: {os.getenv('SOURCE_MYSQL_HOST')}")
    print(f"SOURCE_MYSQL_PORT: {os.getenv('SOURCE_MYSQL_PORT')}")
    print(f"SOURCE_MYSQL_DB: {os.getenv('SOURCE_MYSQL_DB')}")
    print(f"SOURCE_MYSQL_USER: {os.getenv('SOURCE_MYSQL_USER')}")
    print(f"SOURCE_MYSQL_PASSWORD: {'SET' if os.getenv('SOURCE_MYSQL_PASSWORD') else 'MISSING'}")
    
    try:
        # Create connection
        conn_str = (
            f"mysql+pymysql://{os.getenv('SOURCE_MYSQL_USER')}:{os.getenv('SOURCE_MYSQL_PASSWORD')}@"
            f"{os.getenv('SOURCE_MYSQL_HOST')}:{os.getenv('SOURCE_MYSQL_PORT')}/{os.getenv('SOURCE_MYSQL_DB')}"
        )
        engine = create_engine(conn_str)
        conn = engine.connect()
        
        # Test connection
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
        print("✅ Source MySQL connection successful")
        
        # List tables
        conn.execute(text(f"USE {os.getenv('SOURCE_MYSQL_DB')}"))
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
        """, (os.getenv('SOURCE_MYSQL_DB'),)))
        
        tables = [row[0] for row in result]
        if tables:
            print(f"📋 Tables in {os.getenv('SOURCE_MYSQL_DB')}:")
            for table in tables:
                print(f"  - {table}")
        else:
            print(f"Database: {os.getenv('SOURCE_MYSQL_DB')} (empty)")
        
        # Close connection
        conn.close()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ Source MySQL connection failed: {str(e)}")
        return False

def test_replication_mysql_connection():
    """Test connection to replication MySQL database."""
    print("\nTesting replication MySQL connection...")
    
    # Print environment variables
    print(f"REPLICATION_MYSQL_HOST: {os.getenv('REPLICATION_MYSQL_HOST')}")
    print(f"REPLICATION_MYSQL_PORT: {os.getenv('REPLICATION_MYSQL_PORT')}")
    print(f"REPLICATION_MYSQL_DB: {os.getenv('REPLICATION_MYSQL_DB')}")
    print(f"REPLICATION_MYSQL_USER: {os.getenv('REPLICATION_MYSQL_USER')}")
    print(f"REPLICATION_MYSQL_PASSWORD: {'SET' if os.getenv('REPLICATION_MYSQL_PASSWORD') else 'MISSING'}")
    
    try:
        # Create connection
        conn_str = (
            f"mysql+pymysql://{os.getenv('REPLICATION_MYSQL_USER')}:{os.getenv('REPLICATION_MYSQL_PASSWORD')}@"
            f"{os.getenv('REPLICATION_MYSQL_HOST')}:{os.getenv('REPLICATION_MYSQL_PORT')}/{os.getenv('REPLICATION_MYSQL_DB')}"
        )
        engine = create_engine(conn_str)
        conn = engine.connect()
        
        # Test connection
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
        print("✅ Replication MySQL connection successful")
        
        # List tables
        conn.execute(text(f"USE {os.getenv('REPLICATION_MYSQL_DB')}"))
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
        """, (os.getenv('REPLICATION_MYSQL_DB'),)))
        
        tables = [row[0] for row in result]
        if tables:
            print(f"📋 Tables in {os.getenv('REPLICATION_MYSQL_DB')}:")
            for table in tables:
                print(f"  - {table}")
        else:
            print(f"Database: {os.getenv('REPLICATION_MYSQL_DB')} (empty)")
        
        # Close connection
        conn.close()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ Replication MySQL connection failed: {str(e)}")
        return False

def test_analytics_postgres_connection():
    """Test connection to analytics PostgreSQL database."""
    print("\nTesting analytics PostgreSQL connection...")
    
    # Print environment variables
    print(f"ANALYTICS_POSTGRES_HOST: {os.getenv('ANALYTICS_POSTGRES_HOST')}")
    print(f"ANALYTICS_POSTGRES_PORT: {os.getenv('ANALYTICS_POSTGRES_PORT')}")
    print(f"ANALYTICS_POSTGRES_DB: {os.getenv('ANALYTICS_POSTGRES_DB')}")
    print(f"ANALYTICS_POSTGRES_USER: {os.getenv('ANALYTICS_POSTGRES_USER')}")
    print(f"ANALYTICS_POSTGRES_SCHEMA: {os.getenv('ANALYTICS_POSTGRES_SCHEMA')}")
    print(f"ANALYTICS_POSTGRES_PASSWORD: {'SET' if os.getenv('ANALYTICS_POSTGRES_PASSWORD') else 'MISSING'}")
    
    try:
        # Create connection
        conn_str = (
            f"postgresql://{os.getenv('ANALYTICS_POSTGRES_USER')}:{os.getenv('ANALYTICS_POSTGRES_PASSWORD')}@"
            f"{os.getenv('ANALYTICS_POSTGRES_HOST')}:{os.getenv('ANALYTICS_POSTGRES_PORT')}/{os.getenv('ANALYTICS_POSTGRES_DB')}"
        )
        engine = create_engine(conn_str)
        conn = engine.connect()
        
        # Test connection
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
        print("✅ Analytics PostgreSQL connection successful")
        
        # List tables
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
        """, (os.getenv('ANALYTICS_POSTGRES_SCHEMA'),)))
        
        tables = [row[0] for row in result]
        if tables:
            print(f"📋 Tables in {os.getenv('ANALYTICS_POSTGRES_SCHEMA')} schema:")
            for table in tables:
                print(f"  - {table}")
        else:
            print(f"Schema: {os.getenv('ANALYTICS_POSTGRES_SCHEMA')} (empty)")
        
        # Close connection
        conn.close()
        return True
        
    except SQLAlchemyError as e:
        print(f"❌ Analytics PostgreSQL connection failed: {str(e)}")
        return False

def main():
    """Run all connection tests."""
    print("🔍 Testing database connections...")
    
    # Test source MySQL
    mysql_success = test_source_mysql_connection()
    
    # Test replication MySQL
    replication_success = test_replication_mysql_connection()
    
    # Test analytics PostgreSQL
    postgres_success = test_analytics_postgres_connection()
    
    # Print summary
    print("\n📊 Connection Test Summary:")
    print(f"Source MySQL: {'✅' if mysql_success else '❌'}")
    print(f"Replication MySQL: {'✅' if replication_success else '❌'}")
    print(f"Analytics PostgreSQL: {'✅' if postgres_success else '❌'}")
    
    # Return success if all connections work
    return all([mysql_success, replication_success, postgres_success])

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)