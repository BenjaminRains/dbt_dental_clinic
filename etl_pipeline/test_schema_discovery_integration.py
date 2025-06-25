#!/usr/bin/env python3
"""
Quick test script to verify SchemaDiscovery integration with ExactMySQLReplicator.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from unittest.mock import MagicMock
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator

def test_exact_mysql_replicator_integration():
    """Test ExactMySQLReplicator integration with SchemaDiscovery."""
    print("Testing SchemaDiscovery Integration")
    print("=" * 50)
    
    try:
        # Test ExactMySQLReplicator initialization with SchemaDiscovery
        replicator = ExactMySQLReplicator()
        print("ExactMySQLReplicator instantiated successfully with SchemaDiscovery")
        
        # Verify SchemaDiscovery is properly set
        if hasattr(replicator, 'schema_discovery') and replicator.schema_discovery is not None:
            print("SchemaDiscovery dependency properly set")
        else:
            print("SchemaDiscovery dependency not properly set")
            return False
        
        # Verify old schema analysis attributes are removed
        old_attrs = ['source_engine', 'source_db', 'schema_cache', 'size_cache']
        for attr in old_attrs:
            if hasattr(replicator, attr):
                print(f"Old attribute {attr} still exists")
                return False
        
        print("Old schema analysis attributes properly removed")
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def test_table_processor_integration():
    """Test TableProcessor integration with SchemaDiscovery."""
    print("\nTesting TableProcessor Integration")
    print("=" * 50)
    
    try:
        # Test TableProcessor initialization with SchemaDiscovery
        processor = TableProcessor()
        print("TableProcessor initialized successfully with SchemaDiscovery")
        
        # Verify SchemaDiscovery is properly set
        if hasattr(processor, 'schema_discovery') and processor.schema_discovery is not None:
            print("SchemaDiscovery properly set in TableProcessor")
        else:
            print("SchemaDiscovery not properly set in TableProcessor")
            return False
        
        # Verify old schema analysis attributes are removed
        old_attrs = ['source_engine', 'source_db', 'schema_cache', 'size_cache']
        for attr in old_attrs:
            if hasattr(processor, attr):
                print(f"Old attribute {attr} still exists in TableProcessor")
                return False
        
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def test_priority_processor_integration():
    """Test PriorityProcessor integration with SchemaDiscovery."""
    print("\nTesting PriorityProcessor Integration")
    print("=" * 50)
    
    try:
        # Test PriorityProcessor initialization with SchemaDiscovery
        processor = PriorityProcessor()
        print("PriorityProcessor initialized successfully with SchemaDiscovery")
        
        # Verify SchemaDiscovery is properly set
        if hasattr(processor, 'schema_discovery') and processor.schema_discovery is not None:
            print("SchemaDiscovery properly set in PriorityProcessor")
        else:
            print("SchemaDiscovery not properly set in PriorityProcessor")
            return False
        
        # Verify old schema analysis attributes are removed
        old_attrs = ['source_engine', 'source_db', 'schema_cache', 'size_cache']
        for attr in old_attrs:
            if hasattr(processor, attr):
                print(f"Old attribute {attr} still exists in PriorityProcessor")
                return False
        
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def main():
    """Main test function."""
    print("SchemaDiscovery Integration Test Suite")
    print("=" * 60)
    
    tests = [
        ("ExactMySQLReplicator Integration", test_exact_mysql_replicator_integration),
        ("TableProcessor Integration", test_table_processor_integration),
        ("PriorityProcessor Integration", test_priority_processor_integration)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\nRunning: {test_name}")
        try:
            if test_func():
                passed += 1
                print("PASSED")
            else:
                print("FAILED")
        except Exception as e:
            print(f"ERROR: {str(e)}")
    
    print(f"\nTest Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("\nAll integration tests passed!")
        print("SchemaDiscovery integration is working correctly")
        return True
    else:
        print("\nSome integration tests failed")
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        print("\nAll integration tests passed!")
        print("SchemaDiscovery integration is working correctly")
        sys.exit(0)
    else:
        print("\nSome integration tests failed")
        sys.exit(1) 