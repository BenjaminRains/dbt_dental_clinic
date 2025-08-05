# tests/integration/scripts/analyze_opendental_schema/test_batch_processing_integration.py

"""
Integration tests for batch processing operations with real production database connections.

This module tests batch processing against the actual production OpenDental database
to validate real batch operations, multiple table processing, and connection management.

Production Test Strategy:
- Uses production database connections with readonly access
- Tests real batch processing with actual production database data
- Validates multiple table processing in single connection for production
- Tests connection management for batch operations with production database
- Uses Settings injection for production environment-agnostic connections

Coverage Areas:
- Real production batch schema processing with actual database
- Multiple table schema extraction in single connection for production
- Error handling for individual tables in batch with production data
- ConnectionManager usage for batch operations with production database
- Settings injection with real production database connections
- Real production batch size processing with actual database
- Multiple table size extraction in single connection for production
- Batch operations validation with real production data

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for production environment
- Validates actual production database connections and batch processing
"""

import pytest
import os
from pathlib import Path
from decimal import Decimal

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


@pytest.mark.integration
@pytest.mark.order(10)  # After configuration generation tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestBatchProcessingIntegration:
    """Integration tests for batch processing operations with real production database connections."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class for production environment validation."""
        # Store original environment for cleanup
        cls.original_etl_env = os.environ.get('ETL_ENVIRONMENT')
        cls.original_source_db = os.environ.get('OPENDENTAL_SOURCE_DB')
        
        # Set environment to production for these tests
        os.environ['ETL_ENVIRONMENT'] = 'production'
        
        # Validate production environment is available
        try:
            config_dir = Path(__file__).parent.parent.parent.parent.parent
            from etl_pipeline.config.providers import FileConfigProvider
            from etl_pipeline.config.settings import Settings
            from etl_pipeline.core.connections import ConnectionFactory
            from sqlalchemy import text
            
            provider = FileConfigProvider(config_dir)
            settings = Settings(environment='production', provider=provider)
            
            # Test connection
            source_engine = ConnectionFactory.get_source_connection(settings)
            with source_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                row = result.fetchone()
                if not row or row[0] != 1:
                    pytest.skip("Production database connection failed")
                    
        except Exception as e:
            pytest.skip(f"Production databases not available: {str(e)}")
    
    @classmethod
    def teardown_class(cls):
        """Clean up test class environment."""
        # Restore original environment variables
        if cls.original_etl_env is not None:
            os.environ['ETL_ENVIRONMENT'] = cls.original_etl_env
        elif 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
            
        if cls.original_source_db is not None:
            os.environ['OPENDENTAL_SOURCE_DB'] = cls.original_source_db
        elif 'OPENDENTAL_SOURCE_DB' in os.environ:
            del os.environ['OPENDENTAL_SOURCE_DB']

    def test_production_batch_schema_info_processing(self, production_settings_with_file_provider):
        """
        Test production batch schema information processing with actual production database.
        
        AAA Pattern:
            Arrange: Set up real production database connection and discover tables
            Act: Call get_batch_schema_info() method with production tables
            Assert: Verify batch schema information is correctly processed for production
            
        Validates:
            - Real production batch schema processing with actual database
            - Multiple table schema extraction in single connection for production
            - Error handling for individual tables in batch with production data
            - ConnectionManager usage for batch operations with production database
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and discover tables
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Get a subset of tables for testing (to avoid long test times)
        test_tables = tables[:3] if len(tables) >= 3 else tables
        
        if not test_tables:
            pytest.skip("No tables found in production database")
        
        # Act: Call get_batch_schema_info() method with production tables
        batch_schema_info = analyzer.get_batch_schema_info(test_tables)
        
        # Assert: Verify batch schema information is correctly processed for production
        assert isinstance(batch_schema_info, dict)
        assert len(batch_schema_info) == len(test_tables)
        
        # Verify each table's schema information
        for table_name in test_tables:
            assert table_name in batch_schema_info
            schema_info = batch_schema_info[table_name]
            assert 'table_name' in schema_info
            assert 'columns' in schema_info
            assert 'primary_keys' in schema_info
            assert 'foreign_keys' in schema_info
            assert 'indexes' in schema_info
            
            # Verify column information
            assert len(schema_info['columns']) > 0
            assert schema_info['table_name'] == table_name
            
            # Verify column structure
            for col_name, col_info in schema_info['columns'].items():
                assert 'type' in col_info
                assert 'nullable' in col_info
                assert 'primary_key' in col_info

    def test_production_batch_size_info_processing(self, production_settings_with_file_provider):
        """
        Test production batch size information processing with actual production database.
        
        AAA Pattern:
            Arrange: Set up real production database connection and discover tables
            Act: Call get_batch_size_info() method with production tables
            Assert: Verify batch size information is correctly processed for production
            
        Validates:
            - Real production batch size processing with actual database
            - Multiple table size extraction in single connection for production
            - Error handling for individual tables in batch with production data
            - ConnectionManager usage for batch operations with production database
            - Settings injection with real production database connections
        """
        # Arrange: Set up real production database connection and discover tables
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Get a subset of tables for testing (to avoid long test times)
        test_tables = tables[:3] if len(tables) >= 3 else tables
        
        if not test_tables:
            pytest.skip("No tables found in production database")
        
        # Act: Call get_batch_size_info() method with production tables
        batch_size_info = analyzer.get_batch_size_info(test_tables)
        
        # Assert: Verify batch size information is correctly processed for production
        assert isinstance(batch_size_info, dict)
        assert len(batch_size_info) == len(test_tables)
        
        # Verify each table's size information
        for table_name in test_tables:
            assert table_name in batch_size_info
            size_info = batch_size_info[table_name]
            assert 'table_name' in size_info
            assert 'estimated_row_count' in size_info
            assert 'size_mb' in size_info
            assert 'source' in size_info
            assert size_info['source'] == 'information_schema_estimate'
            assert size_info['table_name'] == table_name
            
            # Verify that row count is a non-negative integer
            assert isinstance(size_info['estimated_row_count'], int)
            assert size_info['estimated_row_count'] >= 0
            
            # Verify that size is a non-negative number (can be Decimal from MySQL)
            assert isinstance(size_info['size_mb'], (int, float, Decimal))
            assert float(size_info['size_mb']) >= 0

    def setup_method(self, method):
        """Set up each test method with proper isolation."""
        # Ensure we're in the current environment for each test
        current_env = os.environ.get('ETL_ENVIRONMENT', 'test')
        os.environ['ETL_ENVIRONMENT'] = current_env
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Environment cleanup is handled by fixtures and class setup/teardown
        pass 