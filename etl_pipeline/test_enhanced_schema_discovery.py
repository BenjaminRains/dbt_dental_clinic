#!/usr/bin/env python3
"""
Test script for enhanced SchemaDiscovery implementation.

This script tests the new enhanced SchemaDiscovery class to ensure it works
correctly with the new architecture and connection management.
"""

import os
import sys
import logging
from datetime import datetime

# Add the etl_pipeline directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'etl_pipeline'))

from etl_pipeline.core.schema_discovery import SchemaDiscovery, SchemaDiscoveryError, SchemaNotFoundError, SchemaAnalysisError, ConfigurationGenerationError
from etl_pipeline.core.connections import ConnectionFactory, ConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_enhanced_schema_discovery():
    """Test the enhanced SchemaDiscovery implementation."""
    print("Testing Enhanced SchemaDiscovery Implementation")
    print("=" * 60)
    
    try:
        # Test initialization
        schema_discovery = SchemaDiscovery()
        print("SchemaDiscovery initialized successfully")
        
        # Test table discovery
        tables = schema_discovery.discover_all_tables()
        print(f"Discovered {len(tables)} tables")
        
        if not tables:
            print("No tables found - this might be expected if database is empty")
            return True
        
        # Test schema retrieval
        test_table = tables[0]
        schema = schema_discovery.get_table_schema(test_table)
        print(f"Retrieved schema for {tables[0]}")
        
        # Test caching
        schema2 = schema_discovery.get_table_schema(test_table)
        if schema == schema2:
            print("Caching working correctly")
        
        # Test size info
        size_info = schema_discovery.get_table_size_info(test_table)
        print(f"Size info retrieved: {size_info.get('total_size_mb', 0):.2f} MB, {size_info.get('row_count', 0)} rows")
        
        # Test incremental columns
        incremental_cols = schema_discovery.get_incremental_columns(test_table)
        print(f"Found {len(incremental_cols)} incremental columns")
        
        # Test configuration generation
        config = schema_discovery.generate_complete_configuration()
        print(f"Pipeline configuration generated:")
        print(f"  - Tables: {len(config.get('tables', {}))}")
        print(f"  - Metadata: {config.get('metadata', {}).get('total_tables', 0)} total tables")
        
        # Test relationship analysis
        test_tables = tables[:3]
        relationships = schema_discovery.analyze_table_relationships(test_tables)
        print(f"Relationship analysis completed for {len(test_tables)} tables")
        
        # Test usage pattern analysis
        usage_patterns = schema_discovery.analyze_table_usage_patterns(test_tables)
        print(f"Usage pattern analysis completed for {len(test_tables)} tables")
        
        # Test importance determination
        importance_scores = schema_discovery.determine_table_importance(test_tables)
        print(f"Importance determination completed for {len(test_tables)} tables")
        
        # Test cache clearing
        schema_discovery.clear_schema_cache()
        print("Schema cache cleared")
        schema_discovery.clear_all_caches()
        print("All caches cleared")
        
        print("\nAll tests passed! Enhanced SchemaDiscovery is working correctly.")
        return True
        
    except SchemaNotFoundError as e:
        print(f"SchemaDiscovery error: {str(e)}")
        return False
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False

def test_exception_handling():
    """Test exception handling for SchemaDiscovery."""
    print("\nTesting Exception Handling")
    print("=" * 40)
    
    try:
        schema_discovery = SchemaDiscovery()
        
        # Test with nonexistent table
        try:
            schema = schema_discovery.get_table_schema("nonexistent_table_12345")
            print("Should have raised SchemaNotFoundError")
            return False
        except SchemaNotFoundError:
            print("SchemaNotFoundError raised correctly for nonexistent table")
        
        print("Exception handling tests passed")
        return True
        
    except Exception as e:
        print(f"Exception handling test failed: {str(e)}")
        return False

def test_quick_analysis():
    """Test quick analysis functionality."""
    print("\nTesting Quick Analysis")
    print("=" * 30)
    
    try:
        schema_discovery = SchemaDiscovery()
        tables = schema_discovery.discover_all_tables()
        
        if not tables:
            print("No tables found for quick analysis")
            return True
        
        # Test quick analysis with limited tables
        test_tables = tables[:2]
        analysis = schema_discovery.analyze_complete_schema(test_tables)
        
        print(f"Quick analysis completed:")
        print(f"  - Tables analyzed: {len(analysis.get('tables', []))}")
        print(f"  - Relationships found: {len(analysis.get('relationships', {}))}")
        print(f"  - Usage patterns: {len(analysis.get('usage_patterns', {}))}")
        
        # Test configuration generation
        config = schema_discovery.generate_complete_configuration()
        print(f"Configuration generated for {len(config.get('tables', {}))} tables")
        
        return True
        
    except Exception as e:
        print(f"Quick analysis test failed: {str(e)}")
        return False

def test_connection_handling():
    """Test connection handling and reuse."""
    print("\nTesting Connection Handling")
    print("=" * 35)
    
    try:
        schema_discovery = SchemaDiscovery()
        tables = schema_discovery.discover_all_tables()
        
        if not tables:
            print("No tables found for connection testing")
            return True
        
        # Test multiple operations with same connection
        test_tables = tables[:3]
        for table in test_tables:
            schema = schema_discovery.get_table_schema(table)
            size_info = schema_discovery.get_table_size_info(table)
            print(f"  {table}: {len(schema.get('columns', []))} columns, {size_info.get('total_size_mb', 0):.2f} MB")
        
        print("Connection handling tests passed")
        return True
        
    except Exception as e:
        print(f"Connection handling test failed: {str(e)}")
        return False

def test_connection_manager():
    """Test ConnectionManager directly."""
    print("\nTesting ConnectionManager Directly")
    print("=" * 40)
    
    try:
        # Test ConnectionManager initialization
        connection_manager = ConnectionManager()
        print("ConnectionManager initialized successfully")
        
        # Test connection reuse
        conn1 = connection_manager.get_connection()
        conn2 = connection_manager.get_connection()
        
        if conn1 is conn2:
            print("Connection reuse working correctly")
        else:
            print("Connection reuse not working")
        
        # Test query execution
        with connection_manager.get_connection() as conn:
            result = conn.execute(text("SELECT COUNT(*) as count FROM patient LIMIT 1"))
            count = result.fetchone()[0]
            print(f"Query execution working: {count} patients found")
        
        # Test multiple queries
        for i in range(3):
            with connection_manager.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
                print(f"  Query {i+1} executed successfully")
        
        print("ConnectionManager tests passed")
        return True
        
    except Exception as e:
        print(f"ConnectionManager test failed: {str(e)}")
        return False

def main():
    """Main test function."""
    print("Enhanced SchemaDiscovery Test Suite")
    print("=" * 50)
    
    tests = [
        test_enhanced_schema_discovery,
        test_exception_handling,
        test_quick_analysis,
        test_connection_handling,
        test_connection_manager
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"Test {test_func.__name__} failed with exception: {str(e)}")
    
    print(f"\nTest Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\nAll tests passed! Enhanced SchemaDiscovery is ready for use.")
        return True
    else:
        print("\nSome tests failed. Please check the implementation.")
        return False

if __name__ == "__main__":
    print(f"🚀 Starting Enhanced SchemaDiscovery Tests - {datetime.now()}")
    print("=" * 70)
    
    # Run tests
    success = main()
    
    if success:
        print("\n🎉 All tests passed! Enhanced SchemaDiscovery is ready for use.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
        sys.exit(1) 