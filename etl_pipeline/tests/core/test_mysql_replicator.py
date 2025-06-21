"""
Tests for MySQL replication functionality.

NOTE: This is NOT testing actual MySQL database replication!
===========================================================
This test suite covers the ExactMySQLReplicator class, which despite its name,
does NOT perform real MySQL replication. Instead, it performs:
- One-time table copying between MySQL databases
- Exact schema recreation
- Data migration utilities
- Table-level copying operations

For details on what this actually does vs. what the name suggests,
see the main mysql_replicator.py file which contains a comprehensive
explanation of the misnomer and actual functionality.

These are integration tests that use real database connections to verify
table copying accuracy and schema fidelity.
"""

import pytest
from sqlalchemy import text
from etl_pipeline.core.config import PipelineConfig
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator

def test_mysql_replication():
    """Test basic MySQL replication functionality."""
    # Initialize connections
    source_engine = ConnectionFactory.get_opendental_source_connection()
    target_engine = ConnectionFactory.get_mysql_replication_connection()
    
    try:
        # Create replicator
        replicator = ExactMySQLReplicator(
            source_engine=source_engine,
            target_engine=target_engine,
            source_db='opendental',
            target_db='opendental_replication'
        )
        
        # Test with a small table first (e.g., 'definition')
        table_name = 'definition'
        
        # Get row count from source
        with source_engine.connect() as conn:
            source_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        
        # Create replica and copy data
        assert replicator.create_exact_replica(table_name), "Failed to create table replica"
        assert replicator.copy_table_data(table_name), "Failed to copy table data"
        
        # Verify the replica
        assert replicator.verify_exact_replica(table_name), "Replica verification failed"
        
    finally:
        # Cleanup
        source_engine.dispose()
        target_engine.dispose()

def test_mysql_replication_with_schema():
    """Test MySQL replication with schema creation."""
    # Initialize connections
    source_engine = ConnectionFactory.get_opendental_source_connection()
    target_engine = ConnectionFactory.get_mysql_replication_connection()
    
    try:
        # Create replicator
        replicator = ExactMySQLReplicator(
            source_engine=source_engine,
            target_engine=target_engine,
            source_db='opendental',
            target_db='opendental_replication'
        )
        
        # Test with a table that has a complex schema
        table_name = 'patient'
        
        # Get source schema
        source_schema = replicator.get_exact_table_schema(table_name, source_engine)
        
        # Create replica and copy data
        assert replicator.create_exact_replica(table_name), "Failed to create table replica"
        assert replicator.copy_table_data(table_name), "Failed to copy table data"
        
        # Get target schema
        target_schema = replicator.get_exact_table_schema(table_name, target_engine)
        
        # Compare schemas
        assert source_schema['schema_hash'] == target_schema['schema_hash'], \
            "Schema hash mismatch between source and target"
        
        # Verify the replica
        assert replicator.verify_exact_replica(table_name), "Replica verification failed"
        
    finally:
        # Cleanup
        source_engine.dispose()
        target_engine.dispose() 