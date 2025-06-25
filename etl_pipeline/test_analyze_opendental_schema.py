#!/usr/bin/env python3
"""
Comprehensive Test for analyze_opendental_schema.py

This script tests all functionality of the new analyze_opendental_schema.py script
before running it on the live database to ensure everything works correctly.
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch
import json
import yaml

# Add the etl_pipeline directory to Python path
etl_pipeline_dir = Path(__file__).parent
sys.path.insert(0, str(etl_pipeline_dir))

# Load environment variables from .env file
from dotenv import load_dotenv
env_path = etl_pipeline_dir / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print(f"Loaded environment from: {env_path}")

from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.connections import ConnectionFactory
from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer

def test_analyzer_initialization():
    """Test OpenDentalSchemaAnalyzer initialization."""
    print("Testing Analyzer Initialization")
    print("=" * 50)
    
    try:
        analyzer = OpenDentalSchemaAnalyzer()
        print(f"   Analyzer created: {type(analyzer)}")
        print(f"   Source DB: {analyzer.source_db}")
        print(f"   Schema Discovery: {type(analyzer.schema_discovery)}")
        return True
    except Exception as e:
        print(f"   Initialization failed: {str(e)}")
        return False

def test_table_discovery():
    """Test table discovery functionality."""
    print("\nTesting Table Discovery")
    print("=" * 50)
    
    try:
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.schema_discovery.discover_all_tables()
        
        print(f"   Total tables found: {len(tables)}")
        
        # Test filtering
        excluded_patterns = ['tmp_', 'temp_', 'backup_', '#', 'test_']
        filtered_tables = [t for t in tables if not any(pattern in t.lower() for pattern in excluded_patterns)]
        
        print(f"   After filtering: {len(filtered_tables)} tables")
        
        if filtered_tables:
            print(f"   Sample tables: {filtered_tables[:5]}")
        
        return len(filtered_tables) > 0
        
    except Exception as e:
        print(f"   Table discovery failed: {str(e)}")
        return False

def test_schema_analysis_methods():
    """Test individual schema analysis methods."""
    print("\nTesting Schema Analysis Methods")
    print("=" * 50)
    
    try:
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.schema_discovery.discover_all_tables()
        
        if not tables:
            print("   No tables found to test")
            return False
        
        # Test with first table
        test_table = tables[0]
        print(f"   Testing with table: {test_table}")
        
        # Test get_table_schema
        schema = analyzer.schema_discovery.get_table_schema(test_table)
        print(f"   Schema retrieved: {len(schema.get('columns', []))} columns")
        
        # Test get_table_size_info
        size_info = analyzer.schema_discovery.get_table_size_info(test_table)
        print(f"   Size info: {size_info.get('row_count', 0)} rows, {size_info.get('total_size_mb', 0):.2f} MB")
        
        # Test get_incremental_columns
        incremental_cols = analyzer.schema_discovery.get_incremental_columns(test_table)
        print(f"   Incremental columns: {len(incremental_cols)} found")
        
        # Clean up cache
        analyzer.schema_discovery.clear_cache()
        print(f"   Cache cleared")
        
        return True
        
    except Exception as e:
        print(f"   Schema analysis failed: {str(e)}")
        return False

def test_complete_schema_analysis():
    """Test the complete schema analysis functionality."""
    print("\nTesting Complete Schema Analysis")
    print("=" * 50)
    
    try:
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.schema_discovery.discover_all_tables()
        
        # Limit to first 5 tables for testing
        test_tables = tables[:5]
        print(f"   Testing with {len(test_tables)} tables: {test_tables}")
        
        # Test analyze_complete_schema
        analysis = analyzer.schema_discovery.analyze_complete_schema(test_tables)
        
        print(f"   Analysis completed")
        print(f"   Tables analyzed: {len(analysis.get('tables', []))}")
        print(f"   Relationships: {len(analysis.get('relationships', {}))}")
        print(f"   Usage patterns: {len(analysis.get('usage_patterns', {}))}")
        print(f"   Importance scores: {len(analysis.get('importance_scores', {}))}")
        
        # Clean up cache
        analyzer.schema_discovery.clear_cache()
        print(f"   Cache cleared")
        
        return True
        
    except Exception as e:
        print(f"   Complete analysis failed: {str(e)}")
        return False

def test_configuration_generation():
    """Test configuration generation functionality."""
    print("\nTesting Configuration Generation")
    print("=" * 50)
    
    try:
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.schema_discovery.discover_all_tables()
        
        # Limit to first 3 tables for testing
        test_tables = tables[:3]
        print(f"   Testing with {len(test_tables)} tables: {test_tables}")
        
        # Test generate_complete_configuration
        config = analyzer.schema_discovery.generate_complete_configuration()
        
        print(f"   Configuration generated")
        print(f"   Tables configured: {len(config.get('tables', {}))}")
        
        # Test YAML serialization
        yaml_str = yaml.dump(config, default_flow_style=False, sort_keys=False)
        print(f"   YAML size: {len(yaml_str)} characters")
        
        # Test JSON serialization
        json_str = json.dumps(config, indent=2, default=str)
        print(f"   JSON size: {len(json_str)} characters")
        
        # Clean up cache
        analyzer.schema_discovery.clear_cache()
        print(f"   Cache cleared")
        
        return True
        
    except Exception as e:
        print(f"   Configuration generation failed: {str(e)}")
        return False

def test_file_output():
    """Test file output functionality with temporary directory."""
    print("\nTesting File Output")
    print("=" * 50)
    
    try:
        # Create temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"   Using temporary directory: {temp_dir}")
            
            analyzer = OpenDentalSchemaAnalyzer()
            tables = analyzer.schema_discovery.discover_all_tables()
            
            # Limit to first 2 tables for testing
            test_tables = tables[:2]
            print(f"   Testing with {len(test_tables)} tables: {test_tables}")
            
            # Mock the analyze_complete_schema to return test data
            with patch.object(analyzer.schema_discovery, 'analyze_complete_schema') as mock_analyze:
                mock_analyze.return_value = {
                    'metadata': {
                        'timestamp': '2025-01-15T10:30:00Z', 
                        'total_tables': len(test_tables),
                        'analysis_version': '3.0'  # Add the missing field
                    },
                    'tables': test_tables,
                    'relationships': {table: {} for table in test_tables},
                    'usage_patterns': {table: {} for table in test_tables},
                    'importance_scores': {table: 'standard' for table in test_tables},
                    'pipeline_configs': {table: {'incremental_column': None, 'batch_size': 5000} for table in test_tables}
                }
                
                # Test the complete analysis process
                results = analyzer.analyze_complete_schema(temp_dir)
                
                print(f"   Analysis completed")
                print(f"   Results: {results}")
                
                # Check if files were created
                tables_yml_path = results.get('tables_config')
                if tables_yml_path and os.path.exists(tables_yml_path):
                    print(f"   Tables config file created: {tables_yml_path}")
                    
                    # Test YAML content
                    with open(tables_yml_path, 'r') as f:
                        config = yaml.safe_load(f)
                    print(f"   Config contains {len(config.get('tables', {}))} tables")
                else:
                    print(f"   Tables config file not created")
                    return False
                
                # Clean up cache after test
                analyzer.schema_discovery.clear_cache()
                print(f"   Cache cleared")
                
                return True
                
    except Exception as e:
        print(f"   File output test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_error_handling():
    """Test error handling functionality."""
    print("\nTesting Error Handling")
    print("=" * 50)
    
    try:
        # Test with invalid table name
        analyzer = OpenDentalSchemaAnalyzer()
        
        # This should raise an exception
        try:
            schema = analyzer.schema_discovery.get_table_schema("nonexistent_table")
            print("   Should have raised exception for nonexistent table")
            return False
        except Exception as e:
            print(f"   Correctly handled nonexistent table: {type(e).__name__}")
        
        # Test with empty table list
        try:
            analysis = analyzer.schema_discovery.analyze_complete_schema([])
            print(f"   Handled empty table list: {len(analysis.get('tables', []))} tables")
        except Exception as e:
            print(f"   Failed to handle empty table list: {str(e)}")
            return False
        
        return True
        
    except Exception as e:
        print(f"   Error handling test failed: {str(e)}")
        return False

def main():
    """Main test function."""
    print("Comprehensive Test for analyze_opendental_schema.py")
    print("=" * 70)
    
    tests = [
        ("Analyzer Initialization", test_analyzer_initialization),
        ("Table Discovery", test_table_discovery),
        ("Schema Analysis Methods", test_schema_analysis_methods),
        ("Complete Schema Analysis", test_complete_schema_analysis),
        ("Configuration Generation", test_configuration_generation),
        ("File Output", test_file_output),
        ("Error Handling", test_error_handling)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\nRunning: {test_name}")
        try:
            result = test_func()
            results.append((test_name, result))
            status = "PASSED" if result else "FAILED"
            print(f"   {status}")
        except Exception as e:
            print(f"   ERROR: {str(e)}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        print(f"   {test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\nAll tests passed! Ready to run analyze_opendental_schema.py on live database.")
        return True
    else:
        print(f"\n{total - passed} tests failed. Please fix issues before running on live database.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 