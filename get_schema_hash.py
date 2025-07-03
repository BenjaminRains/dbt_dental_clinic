#!/usr/bin/env python3
"""
Simple script to get the current schema hash for the patient table.
"""

from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.connections import ConnectionFactory

def main():
    try:
        # Get connection to the test OpenDental database
        engine = ConnectionFactory.get_opendental_source_test_connection()
        
        # Create schema discovery instance
        discovery = SchemaDiscovery(engine, 'test_opendental')
        
        # Get schema for patient table
        schema = discovery.get_table_schema('patient')
        
        print(f"Current schema hash: {schema['schema_hash']}")
        
        # Also show the CREATE statement for debugging
        print(f"\nCREATE statement:")
        print(schema['create_statement'])
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 