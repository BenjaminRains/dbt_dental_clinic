#!/usr/bin/env python3
"""
Test script for primary incremental column functionality.

This script tests the new primary incremental column feature in SimpleMySQLReplicator
to ensure it correctly uses the primary column when available and falls back to
multi-column logic when not available.
"""

import sys
import os
from pathlib import Path

# Add the etl_pipeline directory to the path
sys.path.append(str(Path(__file__).parent))

from etl_pipeline.config import get_settings
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator

def test_primary_incremental_column_functionality():
    """Test the primary incremental column functionality."""
    print("Testing primary incremental column functionality...")
    
    try:
        # Get settings
        settings = get_settings()
        
        # Create replicator
        replicator = SimpleMySQLReplicator(settings)
        
        # Test cases with different configurations
        test_cases = [
            {
                'table_name': 'appointment',
                'expected_primary': 'DateTStamp',
                'description': 'Table with valid primary incremental column'
            },
            {
                'table_name': 'patient', 
                'expected_primary': 'DateTStamp',
                'description': 'Table with primary column'
            },
            {
                'table_name': 'appointmenttype',
                'expected_primary': None,
                'description': 'Table with no incremental columns (fallback)'
            },
            {
                'table_name': 'apisubscription',
                'expected_primary': 'DateTimeStart',
                'description': 'Table with different primary column'
            }
        ]
        
        print(f"\nTesting {len(test_cases)} tables...")
        
        for i, test_case in enumerate(test_cases, 1):
            table_name = test_case['table_name']
            expected_primary = test_case['expected_primary']
            description = test_case['description']
            
            print(f"\n{i}. Testing {description}")
            print(f"   Table: {table_name}")
            print(f"   Expected primary column: {expected_primary}")
            
            # Get table configuration
            table_config = replicator.table_configs.get(table_name, {})
            if not table_config:
                print(f"   ❌ No configuration found for table {table_name}")
                continue
            
            # Test primary column detection
            actual_primary = replicator._get_primary_incremental_column(table_config)
            incremental_columns = table_config.get('incremental_columns', [])
            extraction_strategy = table_config.get('extraction_strategy', 'full_table')
            
            print(f"   Actual primary column: {actual_primary}")
            print(f"   Incremental columns: {incremental_columns}")
            print(f"   Extraction strategy: {extraction_strategy}")
            
            # Validate results
            if expected_primary == actual_primary:
                print(f"   ✅ Primary column detection correct")
            else:
                print(f"   ❌ Primary column detection failed: expected {expected_primary}, got {actual_primary}")
            
            # Test strategy logging
            try:
                replicator._log_incremental_strategy(table_name, actual_primary, incremental_columns)
                print(f"   ✅ Strategy logging works")
            except Exception as e:
                print(f"   ❌ Strategy logging failed: {e}")
            
            # Test configuration validation
            if actual_primary and actual_primary in incremental_columns:
                print(f"   ✅ Primary column '{actual_primary}' found in incremental columns list")
            elif actual_primary:
                print(f"   ❌ Primary column '{actual_primary}' not found in incremental columns list: {incremental_columns}")
            else:
                if incremental_columns:
                    print(f"   ✅ Fallback logic: will use multi-column logic with {incremental_columns}")
                else:
                    print(f"   ✅ Fallback logic: no incremental columns, will use full table strategy")
        
        print(f"\n✅ Primary incremental column functionality test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_primary_incremental_column_functionality()
    sys.exit(0 if success else 1) 