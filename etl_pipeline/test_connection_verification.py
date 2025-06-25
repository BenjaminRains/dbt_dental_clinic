#!/usr/bin/env python3
"""
Connection Verification Test

This script tests the connection handling for the new analyze_opendental_schema.py script
to ensure it works correctly with the enhanced SchemaDiscovery and ConnectionManager.
"""

import sys
import os
from pathlib import Path

# Add the etl_pipeline directory to Python path
etl_pipeline_dir = Path(__file__).parent
sys.path.insert(0, str(etl_pipeline_dir))

# Load environment variables from .env file
from dotenv import load_dotenv
env_path = etl_pipeline_dir / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print(f"Loaded environment from: {env_path}")
else:
    print(f"No .env file found at: {env_path}")

from unittest.mock import MagicMock
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.connections import ConnectionFactory, ConnectionManager
from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer

def test_connection_handling():
    """Test connection handling and initialization."""
    print("Testing Connection Handling")
    print("=" * 40)
    
    try:
        # Test environment loading
        env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print(f"Loaded environment from: {env_path}")
        else:
            print(f"No .env file found at: {env_path}")
        
        # Test connection creation
        source_engine = ConnectionFactory.get_opendental_source_connection()
        connection_manager = ConnectionManager()
        source_db = os.getenv('OPENDENTAL_SOURCE_DB', 'opendental')
        
        print(f"   Source engine created: {type(source_engine)}")
        print(f"   ConnectionManager created: {type(connection_manager)}")
        print(f"   Source DB: {source_db}")
        
        # Test SchemaDiscovery initialization
        schema_discovery = SchemaDiscovery()
        print(f"   SchemaDiscovery created: {type(schema_discovery)}")
        
        # Test analyzer initialization
        analyzer = OpenDentalSchemaAnalyzer()
        print(f"   Analyzer created: {type(analyzer)}")
        print(f"   Analyzer source DB: {analyzer.source_db}")
        print(f"   Analyzer schema discovery: {type(analyzer.schema_discovery)}")
        
        # Test table discovery
        tables = schema_discovery.discover_all_tables()
        print(f"   Found {len(tables)} tables")
        
        if tables:
            first_table = tables[0]
            print(f"   Testing with table: {first_table}")
            
            # Test schema retrieval
            schema = schema_discovery.get_table_schema(first_table)
            print(f"   Schema retrieved: {len(schema.get('columns', []))} columns")
        
        print("\nAll connection tests passed!")
        return True
        
    except Exception as e:
        print(f"\nConnection test failed: {str(e)}")
        return False

def test_environment_variables():
    """Test that all required environment variables are set."""
    print("\nTesting Environment Variables")
    print("=" * 40)
    
    required_vars = [
        'OPENDENTAL_SOURCE_HOST',
        'OPENDENTAL_SOURCE_PORT',
        'OPENDENTAL_SOURCE_USER',
        'OPENDENTAL_SOURCE_PASSWORD',
        'OPENDENTAL_SOURCE_DB',
        'OPENDENTAL_REPLICATION_HOST',
        'OPENDENTAL_REPLICATION_PORT',
        'OPENDENTAL_REPLICATION_USER',
        'OPENDENTAL_REPLICATION_PASSWORD',
        'OPENDENTAL_REPLICATION_DB',
        'OPENDENTAL_ANALYTICS_HOST',
        'OPENDENTAL_ANALYTICS_PORT',
        'OPENDENTAL_ANALYTICS_USER',
        'OPENDENTAL_ANALYTICS_PASSWORD',
        'OPENDENTAL_ANALYTICS_DB'
    ]
    
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"   {var}: {'*' * len(value)} (hidden)")
        else:
            print(f"   {var}: NOT SET")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\nMissing environment variables: {missing_vars}")
        return False
    else:
        print("\nAll required environment variables are set!")
        return True

def main():
    """Main test function."""
    print("Connection Verification Test Suite")
    print("=" * 50)
    
    # Test connection handling
    connection_success = test_connection_handling()
    
    # Test environment variables
    env_success = test_environment_variables()
    
    if connection_success and env_success:
        print("\nAll tests passed! Ready to run analyze_opendental_schema.py")
        return True
    else:
        print("\nConnection tests failed. Please check your database configuration.")
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        print("\nAll tests passed! Ready to run analyze_opendental_schema.py")
        sys.exit(0)
    else:
        print("\nConnection tests failed. Please check your database configuration.")
        sys.exit(1) 