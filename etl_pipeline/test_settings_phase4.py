#!/usr/bin/env python3
"""
Test script for Phase 4: Clean Configuration Structure

This script tests the updated Settings class to ensure it works correctly
with the new simplified tables.yml structure and removed backward compatibility.
"""

import sys
import os
import tempfile
import yaml
from pathlib import Path

# Add the etl_pipeline directory to Python path
etl_pipeline_dir = Path(__file__).parent
sys.path.insert(0, str(etl_pipeline_dir))

from etl_pipeline.config.settings import Settings

def test_simplified_structure_loading():
    """Test loading configuration with simplified structure."""
    print("Testing Simplified Structure Loading")
    print("=" * 50)
    
    try:
        # Create a temporary tables.yml with simplified structure
        with tempfile.TemporaryDirectory() as temp_dir:
            tables_config = {
                'tables': {
                    'patient': {
                        'incremental_column': 'DateTimeDeceased',
                        'batch_size': 3000,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'critical',
                        'estimated_size_mb': 25.4,
                        'estimated_rows': 50000,
                        'dependencies': ['clinic', 'provider'],
                        'is_modeled': True,
                        'monitoring': {
                            'alert_on_failure': True,
                            'max_extraction_time_minutes': 30,
                            'data_quality_threshold': 0.99
                        }
                    },
                    'appointment': {
                        'incremental_column': 'AptDateTime',
                        'batch_size': 2000,
                        'extraction_strategy': 'incremental',
                        'table_importance': 'important',
                        'estimated_size_mb': 15.2,
                        'estimated_rows': 25000,
                        'dependencies': ['patient', 'provider'],
                        'is_modeled': True,
                        'monitoring': {
                            'alert_on_failure': True,
                            'max_extraction_time_minutes': 20,
                            'data_quality_threshold': 0.98
                        }
                    }
                }
            }
            
            # Save temporary config
            config_path = os.path.join(temp_dir, 'tables.yml')
            with open(config_path, 'w') as f:
                yaml.dump(tables_config, f, default_flow_style=False, sort_keys=False)
            
            print(f"Created temporary config at: {config_path}")
            
            # Test Settings with temporary config
            settings = Settings()
            
            # Override the config path for testing
            settings.tables_config = tables_config
            
            # Test table listing
            tables = settings.list_tables()
            print(f"Tables found: {tables}")
            assert 'patient' in tables
            assert 'appointment' in tables
            print("✅ Table listing works correctly")
            
            # Test table configuration retrieval
            patient_config = settings.get_table_config('patient')
            print(f"Patient config: {patient_config.get('table_importance')}")
            assert patient_config.get('table_importance') == 'critical'
            assert patient_config.get('batch_size') == 3000
            print("✅ Table configuration retrieval works correctly")
            
            # Test importance filtering
            critical_tables = settings.get_tables_by_importance('critical')
            print(f"Critical tables: {critical_tables}")
            assert 'patient' in critical_tables
            print("✅ Importance filtering works correctly")
            
            # Test extraction strategy filtering
            incremental_tables = settings.get_tables_by_extraction_strategy('incremental')
            print(f"Incremental tables: {incremental_tables}")
            assert 'patient' in incremental_tables
            assert 'appointment' in incremental_tables
            print("✅ Extraction strategy filtering works correctly")
            
            # Test dependency retrieval
            patient_deps = settings.get_table_dependencies('patient')
            print(f"Patient dependencies: {patient_deps}")
            assert 'clinic' in patient_deps
            assert 'provider' in patient_deps
            print("✅ Dependency retrieval works correctly")
            
            # Test monitoring configuration
            monitoring_config = settings.get_monitoring_config('patient')
            print(f"Patient monitoring: {monitoring_config}")
            assert monitoring_config.get('alert_on_failure') == True
            assert monitoring_config.get('max_extraction_time_minutes') == 30
            print("✅ Monitoring configuration works correctly")
            
            # Test incremental loading check
            should_incremental = settings.should_use_incremental('patient')
            print(f"Patient should use incremental: {should_incremental}")
            assert should_incremental == True
            print("✅ Incremental loading check works correctly")
            
            # Test alert on failure check
            should_alert = settings.should_alert_on_failure('patient')
            print(f"Patient should alert on failure: {should_alert}")
            assert should_alert == True
            print("✅ Alert on failure check works correctly")
            
            return True
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False

def test_default_configuration():
    """Test default configuration for tables not in config."""
    print("\nTesting Default Configuration")
    print("=" * 40)
    
    try:
        settings = Settings()
        
        # Test with empty config
        settings.tables_config = {'tables': {}}
        
        # Test default config for nonexistent table
        default_config = settings.get_table_config('nonexistent_table')
        print(f"Default config: {default_config.get('table_importance')}")
        assert default_config.get('table_importance') == 'standard'
        assert default_config.get('batch_size') == 5000
        assert default_config.get('extraction_strategy') == 'full_table'
        print("✅ Default configuration works correctly")
        
        # Test incremental check with default config
        should_incremental = settings.should_use_incremental('nonexistent_table')
        print(f"Default should use incremental: {should_incremental}")
        assert should_incremental == False
        print("✅ Default incremental check works correctly")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False

def test_backward_compatibility_removal():
    """Test that backward compatibility methods are removed."""
    print("\nTesting Backward Compatibility Removal")
    print("=" * 45)
    
    try:
        settings = Settings()
        
        # Test that old method signatures are gone
        # These should raise AttributeError if backward compatibility is properly removed
        
        # Test old get_table_config signature
        try:
            # This should work with new signature
            config = settings.get_table_config('test_table')
            print("✅ New get_table_config signature works")
        except TypeError:
            print("❌ get_table_config still has old signature")
            return False
        
        # Test old list_tables signature
        try:
            # This should work with new signature
            tables = settings.list_tables()
            print("✅ New list_tables signature works")
        except TypeError:
            print("❌ list_tables still has old signature")
            return False
        
        # Test old get_tables_by_priority method is gone
        try:
            settings.get_tables_by_priority('critical', 'source_tables')
            print("❌ Old get_tables_by_priority method still exists")
            return False
        except AttributeError:
            print("✅ Old get_tables_by_priority method properly removed")
        
        # Test new get_tables_by_importance method
        try:
            tables = settings.get_tables_by_importance('critical')
            print("✅ New get_tables_by_importance method works")
        except AttributeError:
            print("❌ New get_tables_by_importance method missing")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False

def test_new_monitoring_methods():
    """Test new monitoring-related methods."""
    print("\nTesting New Monitoring Methods")
    print("=" * 35)
    
    try:
        # Create test config with monitoring
        test_config = {
            'tables': {
                'test_table': {
                    'monitoring': {
                        'alert_on_failure': True,
                        'max_extraction_time_minutes': 45,
                        'data_quality_threshold': 0.97
                    }
                }
            }
        }
        
        settings = Settings()
        settings.tables_config = test_config
        
        # Test monitoring config retrieval
        monitoring = settings.get_monitoring_config('test_table')
        assert monitoring.get('alert_on_failure') == True
        assert monitoring.get('max_extraction_time_minutes') == 45
        assert monitoring.get('data_quality_threshold') == 0.97
        print("✅ Monitoring config retrieval works")
        
        # Test individual monitoring methods
        assert settings.should_alert_on_failure('test_table') == True
        assert settings.get_max_extraction_time('test_table') == 45
        assert settings.get_data_quality_threshold('test_table') == 0.97
        print("✅ Individual monitoring methods work")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False

def main():
    """Main test function."""
    print("Phase 4: Clean Configuration Structure Test")
    print("=" * 60)
    
    tests = [
        ("Simplified Structure Loading", test_simplified_structure_loading),
        ("Default Configuration", test_default_configuration),
        ("Backward Compatibility Removal", test_backward_compatibility_removal),
        ("New Monitoring Methods", test_new_monitoring_methods)
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
        print("\nAll tests passed! Phase 4 implementation is working correctly.")
        return True
    else:
        print(f"\n{total - passed} tests failed. Please check the implementation.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 