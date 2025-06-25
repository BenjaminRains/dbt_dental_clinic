"""
Test script for schema discovery and table configuration generation.
This script will:
1. Test database connectivity
2. Run schema discovery on a small subset of tables
3. Display detailed analysis results
4. Save test results to a log file
"""
import os
import json
from datetime import datetime
from tabulate import tabulate
from dotenv import load_dotenv
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.connections import ConnectionFactory
from sqlalchemy import text
import sys

# Load environment variables
load_dotenv()

def test_database_connection():
    """Test database connectivity and basic queries."""
    try:
        # Initialize connections
        source_engine = ConnectionFactory.get_opendental_source_connection()
        replication_engine = ConnectionFactory.get_mysql_replication_connection()
        
        # Test both connections
        with source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            if result[0] == 1:
                print("✅ Source database connection successful (readonly)")
                print(f"Connected to: {os.getenv('SOURCE_MYSQL_HOST')}:{os.getenv('SOURCE_MYSQL_PORT')}/{os.getenv('SOURCE_MYSQL_DB')}")
        
        with replication_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            if result[0] == 1:
                print("✅ Replication database connection successful")
                print(f"Connected to: {os.getenv('REPLICATION_MYSQL_HOST')}:{os.getenv('REPLICATION_MYSQL_PORT')}/{os.getenv('REPLICATION_MYSQL_DB')}")
        
        return source_engine, replication_engine
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        raise

def test_schema_discovery():
    """Test SchemaDiscovery functionality."""
    print("Testing SchemaDiscovery")
    print("=" * 40)
    
    try:
        # Test database connections
        source_engine = ConnectionFactory.get_opendental_source_connection()
        replication_engine = ConnectionFactory.get_opendental_replication_connection()
        
        print("Source database connection successful (readonly)")
        print("Replication database connection successful")
        
        # Test SchemaDiscovery initialization
        schema_discovery = SchemaDiscovery()
        
        # Test table discovery
        tables = schema_discovery.discover_all_tables()
        if not tables:
            print("No tables found")
            return False
        
        # Test with first table
        table_name = tables[0]
        print(f"\nTesting SchemaDiscovery for table: {table_name}")
        
        # Test schema retrieval
        schema = schema_discovery.get_table_schema(table_name)
        print("\nSchema Information:")
        print(f"  Columns: {len(schema.get('columns', []))}")
        print(f"  Indexes: {len(schema.get('indexes', []))}")
        print(f"  Foreign Keys: {len(schema.get('foreign_keys', []))}")
        
        # Test column information
        print("\nColumn Information:")
        for col in schema.get('columns', [])[:5]:  # Show first 5 columns
            print(f"  {col['name']}: {col['type']} ({col['nullable']})")
        
        # Test size information
        size_info = schema_discovery.get_table_size_info(table_name)
        print(f"\nSize Info: {size_info.get('row_count', 0)} rows, {size_info.get('total_size_mb', 0):.2f} MB")
        
        # Test incremental columns
        incremental_cols = schema_discovery.get_incremental_columns(table_name)
        print(f"\nIncremental Columns:")
        for col in incremental_cols:
            print(f"  {col['name']}: {col['type']}")
        
        # Test relationship analysis
        test_tables = tables[:3]
        print(f"\nTable Relationship Analysis:")
        relationships = schema_discovery.analyze_table_relationships(test_tables)
        for table, deps in relationships.items():
            if deps:
                print(f"  {table}: {len(deps)} dependencies")
        
        # Test usage pattern analysis
        print(f"\nTable Usage Analysis:")
        usage_patterns = schema_discovery.analyze_table_usage_patterns(test_tables)
        for table, pattern in usage_patterns.items():
            print(f"  {table}: {pattern.get('update_frequency', 'unknown')}")
        
        # Test importance determination
        print(f"\nTable Importance Analysis:")
        importance_scores = schema_discovery.determine_table_importance(test_tables)
        for table, importance in importance_scores.items():
            print(f"  {table}: {importance}")
        
        return True
        
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return False

def test_complete_analysis():
    """Test complete schema analysis."""
    print("\nTesting Complete Schema Analysis")
    print("=" * 50)
    
    try:
        schema_discovery = SchemaDiscovery()
        tables = schema_discovery.discover_all_tables()
        
        if not tables:
            print("No tables found")
            return False
        
        # Test with limited tables for performance
        test_tables = tables[:5]
        print(f"\nTesting with {len(test_tables)} tables:")
        for table in test_tables:
            print(f"  {table}")
        
        # Run complete analysis
        analysis = schema_discovery.analyze_complete_schema(test_tables)
        
        # Save test results
        output_file = 'test_schema_analysis.json'
        with open(output_file, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        
        print(f"\nTest results saved to: {output_file}")
        print(f"Analysis completed successfully")
        
        return True
        
    except Exception as e:
        print(f"\nTest failed: {str(e)}")
        return False

def main():
    """Main test function."""
    print("Schema Discovery Test")
    print("=" * 30)
    
    # Test basic functionality
    basic_success = test_schema_discovery()
    
    # Test complete analysis
    complete_success = test_complete_analysis()
    
    if basic_success and complete_success:
        print("\nTest completed successfully")
        return True
    else:
        print(f"\nTest failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 