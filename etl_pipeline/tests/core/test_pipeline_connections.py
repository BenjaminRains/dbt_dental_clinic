"""
Tests for database connectivity in the ETL pipeline.

REDUNDANCY NOTE:
================
This test file may be REDUNDANT and should be reviewed for deletion.

OVERLAP ANALYSIS:
- test_connections.py: Already tests ConnectionFactory extensively with mocks
- test_cli.py: Already tests the test-connections CLI command end-to-end
- This file: Only adds simple SELECT 1 queries and basic cleanup testing

RECOMMENDATION:
Consider deleting this file as it provides minimal unique value and adds
maintenance overhead. The connection testing functionality is already
adequately covered by the other test files mentioned above.

TODO: Review and potentially remove this redundant test file.
"""

import pytest
from sqlalchemy import text
from etl_pipeline.core.config import PipelineConfig
from etl_pipeline.core.pipeline import ETLPipeline

def test_database_connections():
    """Test that the pipeline can connect to all three databases."""
    # Initialize pipeline
    config = PipelineConfig()
    pipeline = ETLPipeline(config)
    
    try:
        # Initialize connections
        pipeline.initialize_connections()
        
        # Test source database connection
        with pipeline.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Source database connection successful")
        
        # Test replication database connection
        with pipeline.replication_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Replication database connection successful")
        
        # Test analytics database connection
        with pipeline.analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
            print("✅ Analytics database connection successful")
        
    finally:
        # Clean up connections
        pipeline.cleanup()

def test_connection_cleanup():
    """Test that connections are properly cleaned up."""
    config = PipelineConfig()
    pipeline = ETLPipeline(config)
    
    # Initialize connections
    pipeline.initialize_connections()
    
    # Verify connections exist
    assert pipeline.source_engine is not None
    assert pipeline.replication_engine is not None
    assert pipeline.analytics_engine is not None
    
    # Clean up
    pipeline.cleanup()
    
    # Verify connections are cleaned up
    assert pipeline.source_engine is None
    assert pipeline.replication_engine is None
    assert pipeline.analytics_engine is None

def test_context_manager():
    """Test that the pipeline works as a context manager."""
    config = PipelineConfig()
    
    with ETLPipeline(config) as pipeline:
        # Initialize connections
        pipeline.initialize_connections()
        
        # Verify connections exist
        assert pipeline.source_engine is not None
        assert pipeline.replication_engine is not None
        assert pipeline.analytics_engine is not None
    
    # Verify connections are cleaned up after context manager
    assert pipeline.source_engine is None
    assert pipeline.replication_engine is None
    assert pipeline.analytics_engine is None 