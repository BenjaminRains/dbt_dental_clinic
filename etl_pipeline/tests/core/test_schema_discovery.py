"""Tests for schema discovery functionality.

RELATIONSHIP NOTE:
=================
This test suite complements scripts/test_schema_discovery.py, which serves as a
comprehensive analysis and demonstration script.

DIFFERENCE:
- This file (tests/): Unit/integration tests for CI/CD and regression testing
- scripts/test_schema_discovery.py: Analysis script for manual testing and demonstration

SUGGESTED ACTIONS:
1. Keep both files - they serve different purposes
2. Rename scripts/test_schema_discovery.py to scripts/analyze_schema_discovery.py
3. Add documentation to clarify the difference between automated testing and manual analysis
4. This test suite focuses on method validation, while the script provides comprehensive analysis

Both files are valuable: this for automated testing, the script for analysis and debugging.
"""
import pytest
from sqlalchemy import create_engine, text
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.connections import ConnectionFactory

@pytest.fixture
def schema_discovery():
    """Create a SchemaDiscovery instance for testing."""
    source_engine = ConnectionFactory.get_opendental_source_connection()
    target_engine = ConnectionFactory.get_mysql_replication_connection()
    
    # Get database names from the connection URLs
    source_db = source_engine.url.database
    target_db = target_engine.url.database
    
    return SchemaDiscovery(
        source_engine=source_engine,
        target_engine=target_engine,
        source_db=source_db,
        target_db=target_db
    )

def test_get_table_schema(schema_discovery):
    """Test getting schema information for a table."""
    schema = schema_discovery.get_table_schema('definition')
    assert schema is not None
    assert 'columns' in schema
    assert len(schema['columns']) > 0
    
    # Check first column structure
    col = schema['columns'][0]
    assert 'name' in col
    assert 'type' in col
    assert 'is_nullable' in col
    assert 'key_type' in col
    assert 'default' in col
    assert 'extra' in col

def test_schema_hash_calculation(schema_discovery):
    """Test schema hash calculation and comparison."""
    # Get schema for a real table
    schema1 = schema_discovery.get_table_schema('definition')
    schema2 = schema_discovery.get_table_schema('definition')
    
    # Same table should have same hash
    assert schema1['schema_hash'] == schema2['schema_hash']
    
    # Test non-existent table
    with pytest.raises(Exception):
        schema_discovery.get_table_schema('non_existent_table')

def test_discover_all_tables(schema_discovery):
    """Test discovering all tables in the database."""
    tables = schema_discovery.discover_all_tables()
    assert isinstance(tables, list)
    assert len(tables) > 0
    assert 'definition' in tables
    assert 'patient' in tables

def test_get_table_size_info(schema_discovery):
    """Test getting table size information."""
    size_info = schema_discovery.get_table_size_info('definition')
    assert size_info is not None
    assert 'row_count' in size_info
    assert 'data_size_bytes' in size_info
    assert 'index_size_bytes' in size_info
    assert 'total_size_bytes' in size_info
    assert 'data_size_mb' in size_info
    assert 'index_size_mb' in size_info
    assert 'total_size_mb' in size_info

def test_get_incremental_columns(schema_discovery):
    """Test getting incremental columns for a table."""
    columns = schema_discovery.get_incremental_columns('patient')
    assert columns is not None
    assert len(columns) > 0
    
    # Check column structure
    col = columns[0]
    assert 'column_name' in col
    assert 'data_type' in col
    assert 'default' in col
    assert 'extra' in col
    assert 'comment' in col
    assert 'priority' in col

def test_replicate_schema(schema_discovery):
    """Test replicating a table schema."""
    # Create a test table in the target database
    test_table = 'test_replica'
    
    # Drop the table if it exists
    with schema_discovery.target_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{test_table}`"))
    
    # Replicate the schema with a different target name
    success = schema_discovery.replicate_schema('definition', target_table=test_table)
    assert success is True
    
    # Verify the table was created
    with schema_discovery.target_engine.connect() as conn:
        result = conn.execute(text(f"SHOW TABLES LIKE '{test_table}'"))
        assert result.fetchone() is not None
        
        # Clean up
        with schema_discovery.target_engine.begin() as cleanup_conn:
            cleanup_conn.execute(text(f"DROP TABLE IF EXISTS `{test_table}`")) 