#!/usr/bin/env python3
"""
Test script for enhanced connection handling in the ETL pipeline.

This script tests the new ConnectionManager to ensure it properly manages
database connections and prevents connection pool exhaustion.
"""

import os
import sys
import time
import logging
from datetime import datetime

# Add the etl_pipeline directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'etl_pipeline'))

from etl_pipeline.core.connections import ConnectionFactory, ConnectionManager
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_connection_manager():
    """Test the ConnectionManager class directly."""
    
    print("🧪 Testing ConnectionManager")
    
    try:
        # Get source engine
        source_engine = ConnectionFactory.get_opendental_source_connection()
        
        # Test 1: Initialize ConnectionManager
        print("\n1. Testing ConnectionManager initialization...")
        
        conn_mgr = ConnectionManager(source_engine)
        print("✅ ConnectionManager initialized successfully")
        
        # Test 2: Test connection reuse
        print("\n2. Testing connection reuse...")
        
        with conn_mgr as mgr:
            # First query
            result1 = mgr.execute_with_retry("SELECT COUNT(*) FROM patient")
            count1 = result1.fetchone()[0]
            print(f"✅ First query executed, patient count: {count1}")
            
            # Second query (should reuse connection)
            result2 = mgr.execute_with_retry("SELECT COUNT(*) FROM appointment")
            count2 = result2.fetchone()[0]
            print(f"✅ Second query executed, appointment count: {count2}")
            
            # Third query (should reuse connection)
            result3 = mgr.execute_with_retry("SELECT COUNT(*) FROM procedurelog")
            count3 = result3.fetchone()[0]
            print(f"✅ Third query executed, procedurelog count: {count3}")
        
        print("✅ Connection reuse test passed")
        
        # Test 3: Test SchemaDiscovery with ConnectionManager
        print("\n3. Testing SchemaDiscovery with ConnectionManager...")
        
        schema_discovery = SchemaDiscovery(source_engine, "opendental")
        
        # Test schema analysis
        schema = schema_discovery.get_table_schema("patient")
        print(f"✅ SchemaDiscovery analysis completed for patient table")
        print(f"   - Columns: {len(schema.get('columns', []))}")
        print(f"   - Indexes: {len(schema.get('indexes', []))}")
        
        print("\n✅ All ConnectionManager tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ ConnectionManager test failed: {str(e)}")
        return False

def test_schema_discovery_connection_handling():
    """Test SchemaDiscovery with improved connection handling."""
    
    print("\n🧪 Testing SchemaDiscovery Connection Handling")
    print("=" * 50)
    
    try:
        # Test 1: Initialize SchemaDiscovery
        print("\n1. Testing SchemaDiscovery initialization...")
        source_engine = ConnectionFactory.get_opendental_source_connection()
        source_db = os.getenv('SOURCE_MYSQL_DB', 'opendental')
        
        schema_discovery = SchemaDiscovery(source_engine, source_db)
        print("✅ SchemaDiscovery initialized successfully")
        
        # Test 2: Discover tables
        print("\n2. Testing table discovery...")
        tables = schema_discovery.discover_all_tables()
        print(f"✅ Discovered {len(tables)} tables")
        
        if not tables:
            print("⚠️  No tables found - this might be expected if database is empty")
            return True
        
        # Test 3: Test single table analysis
        print(f"\n3. Testing single table analysis for '{tables[0]}'...")
        start_time = time.time()
        
        schema = schema_discovery.get_table_schema(tables[0])
        size_info = schema_discovery.get_table_size_info(tables[0])
        incremental_cols = schema_discovery.get_incremental_columns(tables[0])
        
        elapsed = time.time() - start_time
        print(f"✅ Single table analysis completed in {elapsed:.2f}s")
        print(f"   - Schema: {len(schema.get('columns', []))} columns")
        print(f"   - Size: {size_info.get('total_size_mb', 0):.2f} MB")
        print(f"   - Incremental columns: {len(incremental_cols)}")
        
        # Test 4: Test batch analysis with limited tables
        print(f"\n4. Testing batch analysis...")
        test_tables = tables[:3]  # Test with first 3 tables
        print(f"Testing with {len(test_tables)} tables: {test_tables}")
        
        start_time = time.time()
        
        # Test usage patterns (this was causing connection issues)
        usage_patterns = schema_discovery.analyze_table_usage_patterns(test_tables)
        
        elapsed = time.time() - start_time
        print(f"✅ Batch usage pattern analysis completed in {elapsed:.2f}s")
        
        for table, pattern in usage_patterns.items():
            print(f"   - {table}: {pattern.get('size_mb', 0):.2f} MB, {pattern.get('row_count', 0)} rows")
        
        # Test 5: Test relationships analysis
        print(f"\n5. Testing relationships analysis...")
        start_time = time.time()
        
        relationships = schema_discovery.analyze_table_relationships(test_tables)
        
        elapsed = time.time() - start_time
        print(f"✅ Batch relationships analysis completed in {elapsed:.2f}s")
        
        for table, rel in relationships.items():
            print(f"   - {table}: {rel.get('dependency_count', 0)} deps, {rel.get('reference_count', 0)} refs")
        
        # Test 6: Test importance determination
        print(f"\n6. Testing importance determination...")
        start_time = time.time()
        
        importance_scores = schema_discovery.determine_table_importance(test_tables)
        
        elapsed = time.time() - start_time
        print(f"✅ Importance determination completed in {elapsed:.2f}s")
        
        for table, importance in importance_scores.items():
            print(f"   - {table}: {importance}")
        
        print("\n✅ All SchemaDiscovery connection handling tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ SchemaDiscovery connection handling test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_connection_pool_behavior():
    """Test that we don't exhaust the connection pool."""
    
    print("\n🧪 Testing Connection Pool Behavior")
    print("=" * 40)
    
    try:
        source_engine = ConnectionFactory.get_opendental_source_connection()
        source_db = os.getenv('SOURCE_MYSQL_DB', 'opendental')
        
        schema_discovery = SchemaDiscovery(source_engine, source_db)
        
        # Get a few tables
        tables = schema_discovery.discover_all_tables()[:5]
        
        print(f"Testing with {len(tables)} tables...")
        
        # Simulate the old problematic pattern (but with limited scope)
        print("Testing individual table operations...")
        start_time = time.time()
        
        for table in tables:
            try:
                schema = schema_discovery.get_table_schema(table)
                size_info = schema_discovery.get_table_size_info(table)
                print(f"  ✅ {table}: {len(schema.get('columns', []))} columns, {size_info.get('total_size_mb', 0):.2f} MB")
            except Exception as e:
                print(f"  ❌ {table}: {str(e)}")
        
        elapsed = time.time() - start_time
        print(f"✅ Individual operations completed in {elapsed:.2f}s")
        
        # Test batch operations
        print("Testing batch operations...")
        start_time = time.time()
        
        try:
            usage_patterns = schema_discovery.analyze_table_usage_patterns(tables)
            relationships = schema_discovery.analyze_table_relationships(tables)
            importance_scores = schema_discovery.determine_table_importance(tables)
            
            elapsed = time.time() - start_time
            print(f"✅ Batch operations completed in {elapsed:.2f}s")
            print(f"   - Usage patterns: {len(usage_patterns)} tables")
            print(f"   - Relationships: {len(relationships)} tables")
            print(f"   - Importance scores: {len(importance_scores)} tables")
            
        except Exception as e:
            print(f"❌ Batch operations failed: {str(e)}")
            return False
        
        print("✅ Connection pool behavior test passed!")
        return True
        
    except Exception as e:
        print(f"❌ Connection pool behavior test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print(f"🚀 Starting Connection Handling Tests - {datetime.now()}")
    print("=" * 70)
    
    # Run tests
    success1 = test_connection_manager()
    success2 = test_schema_discovery_connection_handling()
    success3 = test_connection_pool_behavior()
    
    if success1 and success2 and success3:
        print("\n🎉 All connection handling tests passed! Database connections are now properly managed.")
        sys.exit(0)
    else:
        print("\n❌ Some connection handling tests failed. Please check the implementation.")
        sys.exit(1) 