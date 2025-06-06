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
from etl_pipeline.scripts.generate_table_config import (
    get_optimized_source_engine,
    analyze_table_relationships,
    analyze_table_usage,
    determine_table_importance
)
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.connections import ConnectionFactory
from sqlalchemy import text

# Load environment variables
load_dotenv()

def test_database_connection():
    """Test database connectivity and basic queries."""
    try:
        # Get readonly source connection
        source_engine = ConnectionFactory.get_source_connection()
        replication_engine = ConnectionFactory.get_replication_connection()
        
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

def test_schema_discovery(source_engine, replication_engine, table_name: str):
    """Test SchemaDiscovery functionality for a single table."""
    print(f"\n🔍 Testing SchemaDiscovery for table: {table_name}")
    
    schema_discovery = SchemaDiscovery(
        source_engine, 
        replication_engine, 
        os.getenv('SOURCE_MYSQL_DB'), 
        os.getenv('REPLICATION_MYSQL_DB')
    )
    
    # Get schema information
    schema_info = schema_discovery.get_table_schema(table_name)
    print("\n📋 Schema Information:")
    print(f"- Engine: {schema_info['metadata']['engine']}")
    print(f"- Row Count: {schema_info['metadata']['row_count']}")
    print(f"- Indexes: {len(schema_info['indexes'])}")
    print(f"- Foreign Keys: {len(schema_info['foreign_keys'])}")
    
    # Display detailed column information
    print("\n📊 Column Information:")
    columns_data = []
    for col in schema_info['columns']:
        columns_data.append([
            col['name'],
            col['type'],
            'NULL' if col['is_nullable'] else 'NOT NULL',
            col['key_type'] or '',
            col['default'] or '',
            col['extra'] or ''
        ])
    
    print(tabulate(
        columns_data,
        headers=['Column', 'Type', 'Nullable', 'Key', 'Default', 'Extra'],
        tablefmt='grid'
    ))
    
    # Get incremental columns
    incremental_columns = schema_discovery.get_incremental_columns(table_name)
    print("\n⏱️ Incremental Columns:")
    for col in incremental_columns:
        print(f"- {col['column_name']} ({col['data_type']}) - Priority: {col['priority']}")
    
    return schema_info, incremental_columns

def get_test_tables(engine, limit: int = 10):
    """Get a small subset of tables for testing."""
    with engine.connect() as conn:
        # Get all tables from the database
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :db_name
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
            LIMIT :limit
        """)
        
        results = conn.execute(
            query, 
            {
                'db_name': os.getenv('SOURCE_MYSQL_DB'),
                'limit': limit
            }
        ).fetchall()
        return [row.table_name for row in results]

def display_relationship_analysis(relationships: dict):
    """Display table relationship analysis results."""
    print("\n📊 Table Relationship Analysis:")
    
    # Prepare data for display
    data = []
    for table, metrics in relationships.items():
        data.append([
            table,
            metrics['referenced_by_count'],
            metrics['references_count'],
            f"{metrics['pagerank_score']:.4f}",
            "Yes" if metrics['is_core_reference'] else "No"
        ])
    
    # Display table
    print(tabulate(
        data,
        headers=['Table', 'Referenced By', 'References', 'PageRank', 'Core Reference'],
        tablefmt='grid'
    ))

def display_usage_analysis(usage: dict):
    """Display table usage analysis results."""
    print("\n📈 Table Usage Analysis:")
    
    # Prepare data for display
    data = []
    for table, metrics in usage.items():
        data.append([
            table,
            f"{metrics['size_mb']:.2f} MB",
            metrics['row_count'],
            metrics['update_frequency'],
            "Yes" if metrics['has_audit_columns'] else "No",
            "Yes" if metrics['has_timestamp_columns'] else "No",
            "Yes" if metrics['has_soft_delete'] else "No"
        ])
    
    # Display table
    print(tabulate(
        data,
        headers=['Table', 'Size', 'Rows', 'Update Freq', 'Audit Cols', 'Timestamp Cols', 'Soft Delete'],
        tablefmt='grid'
    ))

def display_importance_analysis(importance: dict, relationships: dict, usage: dict):
    """Display table importance analysis results."""
    print("\n🎯 Table Importance Analysis:")
    
    # Calculate scores for display
    data = []
    for table in importance.keys():
        rel_metrics = relationships[table]
        usage_metrics = usage[table]
        
        # Calculate score components
        ref_score = rel_metrics['referenced_by_count'] * 2
        pagerank_score = rel_metrics['pagerank_score'] * 100
        core_bonus = 50 if rel_metrics['is_core_reference'] else 0
        audit_score = 30 if usage_metrics['has_audit_columns'] else 0
        timestamp_score = 20 if usage_metrics['has_timestamp_columns'] else 0
        update_score = 25 if usage_metrics['update_frequency'] == 'high' else 0
        size_score = 15 if usage_metrics['size_mb'] > 100 else 0
        
        total_score = ref_score + pagerank_score + core_bonus + audit_score + timestamp_score + update_score + size_score
        
        data.append([
            table,
            importance[table],
            total_score,
            ref_score,
            pagerank_score,
            core_bonus,
            audit_score + timestamp_score + update_score + size_score
        ])
    
    # Sort by total score
    data.sort(key=lambda x: x[2], reverse=True)
    
    # Display table
    print(tabulate(
        data,
        headers=['Table', 'Importance', 'Total Score', 'Ref Score', 'PageRank', 'Core Bonus', 'Usage Score'],
        tablefmt='grid'
    ))

def save_test_results(relationships: dict, usage: dict, importance: dict, schema_info: dict = None):
    """Save test results to a JSON file."""
    results = {
        'timestamp': datetime.now().isoformat(),
        'relationships': relationships,
        'usage': usage,
        'importance': importance
    }
    
    if schema_info:
        results['schema_info'] = schema_info
    
    # Create logs directory if it doesn't exist
    os.makedirs('etl_pipeline/logs', exist_ok=True)
    
    # Save results
    output_file = f'etl_pipeline/logs/schema_discovery_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Test results saved to: {output_file}")

def main():
    """Main test function."""
    print("🔍 Starting Schema Discovery Test")
    print("=" * 50)
    
    try:
        # Test database connections
        source_engine, replication_engine = test_database_connection()
        
        # Get test tables
        test_tables = get_test_tables(source_engine, limit=5)  # Reduced to 5 for clearer output
        print(f"\n📋 Testing with {len(test_tables)} tables:")
        print(", ".join(test_tables))
        
        # Test SchemaDiscovery for first table
        schema_info, incremental_columns = test_schema_discovery(source_engine, replication_engine, test_tables[0])
        
        # Run configuration analysis
        print("\n🔄 Running configuration analysis...")
        relationships = analyze_table_relationships(source_engine, os.getenv('SOURCE_MYSQL_DB'), test_tables)
        usage = analyze_table_usage(source_engine, os.getenv('SOURCE_MYSQL_DB'), test_tables)
        importance = determine_table_importance(relationships, usage)
        
        # Display results
        display_relationship_analysis(relationships)
        display_usage_analysis(usage)
        display_importance_analysis(importance, relationships, usage)
        
        # Save results
        save_test_results(relationships, usage, importance, schema_info)
        
        print("\n✅ Test completed successfully")
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 