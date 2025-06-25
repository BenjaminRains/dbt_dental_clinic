#!/usr/bin/env python3
"""
Test script for Phase 5 updates - SchemaDiscovery dependencies and dbt model discovery.
"""

import sys
import os
from pathlib import Path

# Add the etl_pipeline to the path
sys.path.insert(0, str(Path(__file__).parent / "etl_pipeline"))

from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.config.settings import settings
from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.orchestration.priority_processor import PriorityProcessor
from etl_pipeline.core.connections import ConnectionFactory

def test_schema_discovery_dbt_integration():
    """Test that SchemaDiscovery correctly identifies dbt models."""
    print("Testing SchemaDiscovery DBT integration...")
    
    try:
        # Create SchemaDiscovery with dbt project root
        project_root = Path(__file__).parent
        source_engine = ConnectionFactory.get_opendental_source_connection()
        source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db=source_db,
            dbt_project_root=str(project_root)
        )
        
        # Test dbt model discovery
        modeled_tables = schema_discovery._discover_dbt_models()
        print(f"Discovered {len(modeled_tables)} tables with dbt models")
        
        # Test specific tables
        test_tables = ['patient', 'appointment', 'procedurelog', 'nonexistent_table']
        for table_name in test_tables:
            is_modeled = schema_discovery.is_table_modeled_by_dbt(table_name)
            model_types = schema_discovery.get_dbt_model_types(table_name)
            print(f"  {table_name}: is_modeled={is_modeled}, model_types={model_types}")
        
        # Test pipeline configuration generation
        for table_name in ['patient', 'appointment']:
            config = schema_discovery.get_pipeline_configuration(table_name)
            print(f"  {table_name} config: is_modeled={config.get('is_modeled')}, dbt_model_types={config.get('dbt_model_types')}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: SchemaDiscovery DBT integration test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_table_processor_schema_discovery_dependency():
    """Test that TableProcessor requires SchemaDiscovery."""
    print("\nTesting TableProcessor SchemaDiscovery dependency...")
    
    try:
        # Create SchemaDiscovery
        project_root = Path(__file__).parent
        source_engine = ConnectionFactory.get_opendental_source_connection()
        source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db=source_db,
            dbt_project_root=str(project_root)
        )
        
        # Test that TableProcessor requires SchemaDiscovery
        table_processor = TableProcessor(schema_discovery)
        print("✅ TableProcessor created successfully with SchemaDiscovery")
        
        # Test that it fails without SchemaDiscovery
        try:
            # This should fail
            table_processor = TableProcessor(None)
            print("❌ TableProcessor should have failed with None SchemaDiscovery")
            return False
        except ValueError as e:
            print("✅ TableProcessor correctly rejected None SchemaDiscovery")
        
        return True
        
    except Exception as e:
        print(f"ERROR: TableProcessor dependency test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_priority_processor_schema_discovery_dependency():
    """Test that PriorityProcessor requires SchemaDiscovery."""
    print("\nTesting PriorityProcessor SchemaDiscovery dependency...")
    
    try:
        # Create SchemaDiscovery
        project_root = Path(__file__).parent
        source_engine = ConnectionFactory.get_opendental_source_connection()
        source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db=source_db,
            dbt_project_root=str(project_root)
        )
        
        # Test that PriorityProcessor requires SchemaDiscovery
        priority_processor = PriorityProcessor(schema_discovery)
        print("✅ PriorityProcessor created successfully with SchemaDiscovery")
        
        # Test that it fails without SchemaDiscovery
        try:
            # This should fail
            priority_processor = PriorityProcessor(None)
            print("❌ PriorityProcessor should have failed with None SchemaDiscovery")
            return False
        except ValueError as e:
            print("✅ PriorityProcessor correctly rejected None SchemaDiscovery")
        
        return True
        
    except Exception as e:
        print(f"ERROR: PriorityProcessor dependency test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_settings_new_structure():
    """Test that Settings works with the new tables.yml structure."""
    print("\nTesting Settings with new tables.yml structure...")
    
    try:
        # Test table configuration loading
        table_config = settings.get_table_config('patient')
        print(f"Patient config loaded: {len(table_config)} fields")
        
        # Test dbt modeling information
        is_modeled = table_config.get('is_modeled', False)
        dbt_model_types = table_config.get('dbt_model_types', [])
        print(f"Patient: is_modeled={is_modeled}, dbt_model_types={dbt_model_types}")
        
        # Test table listing
        all_tables = settings.list_tables()
        print(f"Total tables in config: {len(all_tables)}")
        
        # Test importance-based filtering
        important_tables = settings.get_tables_by_importance('important')
        print(f"Important tables: {len(important_tables)}")
        
        # Test dbt modeling statistics
        modeled_tables = []
        for table_name in all_tables[:10]:  # Check first 10 tables
            config = settings.get_table_config(table_name)
            if config.get('is_modeled', False):
                modeled_tables.append(table_name)
        
        print(f"Modeled tables (first 10): {len(modeled_tables)}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Settings test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_end_to_end_integration():
    """Test end-to-end integration of all components."""
    print("\nTesting end-to-end integration...")
    
    try:
        # Create SchemaDiscovery
        project_root = Path(__file__).parent
        source_engine = ConnectionFactory.get_opendental_source_connection()
        source_db = os.getenv('OPENDENTAL_SOURCE_DB')
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db=source_db,
            dbt_project_root=str(project_root)
        )
        
        # Create PriorityProcessor
        priority_processor = PriorityProcessor(schema_discovery)
        print("✅ PriorityProcessor created with SchemaDiscovery")
        
        # Test that we can get table configurations
        test_tables = ['patient', 'appointment']
        for table_name in test_tables:
            config = schema_discovery.get_pipeline_configuration(table_name)
            print(f"  {table_name}: is_modeled={config.get('is_modeled')}")
        
        print("✅ End-to-end integration test passed")
        return True
        
    except Exception as e:
        print(f"ERROR: End-to-end integration test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Phase 5 Updates Test")
    print("=" * 50)
    
    # Run all tests
    tests = [
        ("SchemaDiscovery DBT Integration", test_schema_discovery_dbt_integration),
        ("TableProcessor SchemaDiscovery Dependency", test_table_processor_schema_discovery_dependency),
        ("PriorityProcessor SchemaDiscovery Dependency", test_priority_processor_schema_discovery_dependency),
        ("Settings New Structure", test_settings_new_structure),
        ("End-to-End Integration", test_end_to_end_integration)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"ERROR: {test_name} failed with exception: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST RESULTS SUMMARY")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name}: {status}")
        if success:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        print("Phase 5 updates are working correctly.")
        print("The new SchemaDiscovery dependencies and dbt model discovery are functional.")
    else:
        print(f"\n⚠️  {total - passed} tests failed.")
        print("Please check the error messages above.")
        sys.exit(1) 