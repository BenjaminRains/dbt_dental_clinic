#!/usr/bin/env python3
"""
Setup script for PostgreSQL integration tests.
This script helps configure the PostgreSQL test environment.
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def check_postgres_connection():
    """Check if PostgreSQL is accessible with current settings."""
    host = os.getenv('TEST_POSTGRES_HOST', 'localhost')
    port = os.getenv('TEST_POSTGRES_PORT', '5432')
    user = os.getenv('TEST_POSTGRES_USER', 'postgres')
    password = os.getenv('TEST_POSTGRES_PASSWORD', 'postgres')
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database='postgres'
        )
        conn.close()
        print(f"✅ PostgreSQL connection successful to {host}:{port}")
        return True
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False


def create_test_database():
    """Create the test database if it doesn't exist."""
    host = os.getenv('TEST_POSTGRES_HOST', 'localhost')
    port = os.getenv('TEST_POSTGRES_PORT', '5432')
    user = os.getenv('TEST_POSTGRES_USER', 'postgres')
    password = os.getenv('TEST_POSTGRES_PASSWORD', 'postgres')
    db_name = os.getenv('TEST_POSTGRES_DB', 'test_analytics')
    
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if test database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        if cursor.fetchone():
            print(f"✅ Test database '{db_name}' already exists")
        else:
            cursor.execute(f"CREATE DATABASE {db_name}")
            print(f"✅ Created test database '{db_name}'")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Failed to create test database: {e}")
        return False


def check_user_permissions():
    """Check if the test user has necessary permissions."""
    host = os.getenv('TEST_POSTGRES_HOST', 'localhost')
    port = os.getenv('TEST_POSTGRES_PORT', '5432')
    user = os.getenv('TEST_POSTGRES_USER', 'postgres')
    password = os.getenv('TEST_POSTGRES_PASSWORD', 'postgres')
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database='postgres'
        )
        cursor = conn.cursor()
        
        # Check if user can create databases
        cursor.execute("""
            SELECT rolcreatedb 
            FROM pg_roles 
            WHERE rolname = %s
        """, (user,))
        
        result = cursor.fetchone()
        if result and result[0]:
            print(f"✅ User '{user}' has CREATEDB permission")
        else:
            print(f"⚠️  User '{user}' does not have CREATEDB permission")
            print("   Tests may fail if test database doesn't exist")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Failed to check user permissions: {e}")
        return False


def setup_environment():
    """Set up the test environment."""
    print("🔧 Setting up PostgreSQL test environment...")
    print()
    
    # Check current environment variables
    print("Current environment variables:")
    for var in ['TEST_POSTGRES_HOST', 'TEST_POSTGRES_PORT', 'TEST_POSTGRES_USER', 'TEST_POSTGRES_DB']:
        value = os.getenv(var, 'NOT SET')
        print(f"  {var}: {value}")
    print()
    
    # Check connection
    if not check_postgres_connection():
        print("\n💡 To fix connection issues:")
        print("1. Ensure PostgreSQL server is running")
        print("2. Set environment variables:")
        print("   export TEST_POSTGRES_HOST=your_host")
        print("   export TEST_POSTGRES_PORT=your_port")
        print("   export TEST_POSTGRES_USER=your_user")
        print("   export TEST_POSTGRES_PASSWORD=your_password")
        return False
    
    # Check permissions
    check_user_permissions()
    
    # Create test database
    if not create_test_database():
        return False
    
    print("\n✅ PostgreSQL test environment is ready!")
    print("\nYou can now run the integration tests:")
    print("  python -m pytest tests/unit/core/test_postgres_schema_simple.py -v -m integration")
    
    return True


if __name__ == "__main__":
    success = setup_environment()
    sys.exit(0 if success else 1) 