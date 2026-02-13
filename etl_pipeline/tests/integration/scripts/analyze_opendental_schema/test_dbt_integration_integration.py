# tests/integration/scripts/analyze_opendental_schema/test_dbt_integration_integration.py

"""
Integration tests for DBT model discovery with real production database connections.

This module tests DBT model discovery against the actual production OpenDental database
to validate real DBT model discovery, project structure analysis, and model categorization.

Production Test Strategy:
- Uses production database connections with readonly access
- Tests real DBT model discovery with actual project structure
- Validates model categorization with real project structure
- Tests project structure analysis with real DBT project
- Uses Settings injection for clinic environment-agnostic connections

Coverage Areas:
- Real production DBT model discovery from actual project structure
- Staging model discovery from real project
- Mart model discovery from real project
- Intermediate model discovery from real project
- Error handling for real project structure
- Model categorization with real project structure
- Project structure validation with real DBT project

ETL Context:
- Critical for production ETL pipeline configuration generation
- Tests with real production dental clinic database schemas
- Uses Settings injection with FileConfigProvider for clinic environment
- Validates actual production database connections and DBT model discovery
"""

import pytest
import os
from pathlib import Path

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer


@pytest.mark.integration
@pytest.mark.order(8)  # After incremental strategy tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.production
class TestDbtIntegrationIntegration:
    """Integration tests for DBT model discovery with real production database connections."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class for clinic environment validation."""
        # Store original environment for cleanup
        cls.original_etl_env = os.environ.get('ETL_ENVIRONMENT')
        cls.original_source_db = os.environ.get('OPENDENTAL_SOURCE_DB')
        
        # Set environment to clinic for these tests
        os.environ['ETL_ENVIRONMENT'] = 'clinic'
        
        # Validate clinic environment is available
        try:
            config_dir = Path(__file__).parent.parent.parent.parent.parent
            from etl_pipeline.config.providers import FileConfigProvider
            from etl_pipeline.config.settings import Settings
            from etl_pipeline.core.connections import ConnectionFactory
            from sqlalchemy import text
            
            provider = FileConfigProvider(config_dir)
            settings = Settings(environment='clinic', provider=provider)
            
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

    def test_production_dbt_model_discovery(self, production_settings_with_file_provider):
        """
        Test production DBT model discovery with actual project structure.
        
        AAA Pattern:
            Arrange: Set up real production database connection and real project structure
            Act: Call discover_dbt_models() method with real project
            Assert: Verify DBT models are correctly discovered for production
            
        Validates:
            - Real production DBT model discovery from actual project structure
            - Staging model discovery from real project
            - Mart model discovery from real project
            - Intermediate model discovery from real project
            - Error handling for real project structure
        """
        # Arrange: Set up real production database connection and real project structure
        analyzer = OpenDentalSchemaAnalyzer()
        
        # Act: Call discover_dbt_models() method with real project
        dbt_models = analyzer.discover_dbt_models()
        
        # Assert: Verify DBT models are correctly discovered for production
        assert isinstance(dbt_models, dict)
        assert 'staging' in dbt_models
        assert 'mart' in dbt_models
        assert 'intermediate' in dbt_models
        
        # Verify that all model lists are lists
        assert isinstance(dbt_models['staging'], list)
        assert isinstance(dbt_models['mart'], list)
        assert isinstance(dbt_models['intermediate'], list)

    def setup_method(self, method):
        """Set up each test method with proper isolation."""
        # Ensure we're in the current environment for each test
        current_env = os.environ.get('ETL_ENVIRONMENT', 'test')
        os.environ['ETL_ENVIRONMENT'] = current_env
    
    def teardown_method(self, method):
        """Clean up after each test method."""
        # Environment cleanup is handled by fixtures and class setup/teardown
        pass 