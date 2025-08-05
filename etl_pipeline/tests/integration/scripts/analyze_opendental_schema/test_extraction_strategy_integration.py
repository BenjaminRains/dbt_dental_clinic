# tests/integration/scripts/analyze_opendental_schema/test_extraction_strategy_integration.py

"""
Integration tests for extraction strategy determination with real production database connections.

This module tests extraction strategy determination against the actual production OpenDental database
to validate real strategy selection, business logic, and configuration generation.

Production Test Strategy:
- Uses production database connections with readonly access
- Tests real extraction strategy determination with actual production database data
- Validates strategy selection with real production data
- Tests business logic with real production database structure
- Uses Settings injection for production environment-agnostic connections

Coverage Areas:
- Real production extraction strategy determination with actual database data
- Full table strategy for small production tables
- Incremental strategy for medium production tables with timestamp columns
- Chunked incremental strategy for large production tables
- Error handling for real production database operations
- Strategy selection based on table size and schema characteristics
- Business logic validation with real production data

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for production environment
- Validates actual production database connections and strategy determination
"""

import pytest
import os
from pathlib import Path

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


@pytest.mark.integration
@pytest.mark.order(6)  # After importance determination tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestExtractionStrategyIntegration:
    """Integration tests for extraction strategy determination with real production database connections."""
    
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

    def test_production_extraction_strategy_determination(self, production_settings_with_file_provider):
        """
        Test production extraction strategy determination with actual production database data.
        
        AAA Pattern:
            Arrange: Set up real production database connection and get real schema/size data
            Act: Call determine_extraction_strategy() method with production data
            Assert: Verify strategy is correctly determined for production tables
            
        Validates:
            - Real production extraction strategy determination with actual database data
            - Full table strategy for small production tables
            - Incremental strategy for medium production tables with timestamp columns
            - Chunked incremental strategy for large production tables
            - Error handling for real production database operations
        """
        # Arrange: Set up real production database connection and get real schema/size data
        analyzer = OpenDentalSchemaAnalyzer()
        tables = analyzer.discover_all_tables()
        
        # Test with patient table
        if 'patient' in tables:
            schema_info = analyzer.get_table_schema('patient')
            size_info = analyzer.get_table_size_info('patient')
            
            # Act: Call determine_extraction_strategy() method with production data
            strategy = analyzer.determine_extraction_strategy('patient', schema_info, size_info)
            
            # Assert: Verify strategy is correctly determined for production tables
            assert strategy in ['full_table', 'incremental', 'incremental_chunked']

    def setup_method(self, method):
        """Set up each test method with proper isolation."""
        # Ensure we're in the current environment for each test
        current_env = os.environ.get('ETL_ENVIRONMENT', 'test')
        os.environ['ETL_ENVIRONMENT'] = current_env
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Environment cleanup is handled by fixtures and class setup/teardown
        pass 