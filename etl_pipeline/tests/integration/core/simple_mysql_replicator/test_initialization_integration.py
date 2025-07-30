# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for SimpleMySQLReplicator initialization and configuration.

This module tests:
- Replicator initialization with real settings
- Configuration loading from tables.yml
- Database connection establishment
- Settings injection with FileConfigProvider
- Environment-agnostic configuration handling
"""

import pytest
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from sqlalchemy import text, Result
from datetime import datetime

from sqlalchemy.engine import Engine
from sqlalchemy import text

# Import ETL pipeline components
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.config import (
    Settings,
    DatabaseType
)
from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.core.connections import ConnectionFactory

# Import custom exceptions for specific error handling
from etl_pipeline.exceptions import (
    ConfigurationError,
    EnvironmentError,
    DatabaseConnectionError,
    DatabaseQueryError,
    DataExtractionError,
    SchemaValidationError
)

# Import fixtures for test data
from tests.fixtures.integration_fixtures import (
    populated_test_databases,
    test_settings_with_file_provider
)
from tests.fixtures.config_fixtures import temp_tables_config_dir

logger = logging.getLogger(__name__)

@pytest.fixture
def replicator_with_real_settings(test_settings_with_file_provider):
    """
    Create SimpleMySQLReplicator with real settings for integration testing.
    This fixture follows the connection architecture by:
    - Using Settings injection for environment-agnostic connections
    - Using FileConfigProvider for real configuration loading
    - Creating real database connections using ConnectionFactory
    - Supporting real database replication testing
    - Using test environment configuration
    """
    try:
        logger.info("Creating SimpleMySQLReplicator with test settings...")
        
        # Create replicator with real settings loaded from .env_test
        replicator = SimpleMySQLReplicator(settings=test_settings_with_file_provider)
        
        # Validate that replicator has proper configuration
        assert replicator.settings is not None
        assert replicator.table_configs is not None
        assert len(replicator.table_configs) > 0
        
        logger.info(f"Replicator created with {len(replicator.table_configs)} table configurations")
        logger.info(f"Source engine: {replicator.source_engine}")
        logger.info(f"Target engine: {replicator.target_engine}")
        
        # Validate that we can connect to test databases
        logger.info("Testing source database connection...")
        with replicator.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            if not row or row[0] != 1:
                pytest.skip("Test source database connection failed")
        
        logger.info("Testing target database connection...")
        with replicator.target_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            if not row or row[0] != 1:
                pytest.skip("Test target database connection failed")
        
        logger.info("Successfully created SimpleMySQLReplicator with working database connections")
        return replicator
        
    except Exception as e:
        logger.error(f"Failed to create replicator: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        pytest.skip(f"Test databases not available: {str(e)}")

@pytest.mark.integration
@pytest.mark.order(2)
@pytest.mark.config
def test_simplemysqlreplicator_loads_actual_tables_yml(caplog):
    """
    Test that SimpleMySQLReplicator loads the actual tables.yml file from config directory.
    
    This integration test validates that the replicator can load the real tables.yml
    configuration file used by the ETL pipeline.
    """
    import os
    
    # Get the actual config directory path
    config_dir = os.path.join(os.path.dirname(__file__), '../../../../etl_pipeline/config')
    tables_yml_path = os.path.join(config_dir, 'tables.yml')
    
    # Skip test if tables.yml doesn't exist
    if not os.path.exists(tables_yml_path):
        pytest.skip("tables.yml not found in config directory")
    
    # Use caplog to capture logs
    with caplog.at_level('INFO'):
        # Create replicator - should load the actual tables.yml
        replicator = SimpleMySQLReplicator()
        
        # Should load the actual tables.yml file
        assert replicator.tables_config_path.endswith('tables.yml')
        assert os.path.exists(replicator.tables_config_path)
        
        # Should have loaded table configurations
        assert replicator.table_configs is not None
        assert len(replicator.table_configs) > 0
        
        # Validate that the log shows the correct file
        assert 'SimpleMySQLReplicator using tables config' in caplog.text
        assert 'tables.yml' in caplog.text
        
        # Log what tables were loaded for debugging
        logger.info(f"Loaded {len(replicator.table_configs)} tables from {replicator.tables_config_path}")
        for table_name in replicator.table_configs.keys():
            logger.info(f"  - {table_name}")

@pytest.mark.integration
@pytest.mark.order(2)  # After configuration tests, before data loading tests
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestSimpleMySQLReplicatorInitializationIntegration:
    """Integration tests for SimpleMySQLReplicator initialization with real database connections."""

    def test_replicator_initialization_with_real_config(self, test_settings_with_file_provider):
        """
        Test SimpleMySQLReplicator initialization with real configuration.
        
        Validates:
            - Real configuration loading from .env_test file
            - Settings injection with FileConfigProvider
            - Table configuration loading from tables.yml
            - Database connection establishment
            - Configuration validation and error handling
            
        ETL Pipeline Context:
            - Critical for nightly ETL pipeline execution
            - Supports MariaDB v11.6 source and MySQL replication
            - Uses FileConfigProvider for real test environment
            - Implements Settings injection for environment-agnostic connections
        """
        try:
            # Create replicator with real settings loaded from .env_test
            replicator = SimpleMySQLReplicator(settings=test_settings_with_file_provider)
            
            # Validate settings injection
            assert replicator.settings is not None
            assert replicator.settings == test_settings_with_file_provider
            
            # Validate table configurations loaded
            assert replicator.table_configs is not None
            assert len(replicator.table_configs) > 0
            
            # Validate database connections
            assert replicator.source_engine is not None
            assert replicator.target_engine is not None
            
            # Test source connection
            with replicator.source_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row is not None and row[0] == 1
            
            # Test target connection
            with replicator.target_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row is not None and row[0] == 1
            
            logger.info("Successfully initialized SimpleMySQLReplicator with real configuration")
            
        except (DatabaseConnectionError, DatabaseQueryError, ConfigurationError) as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_table_configuration_loading(self, replicator_with_real_settings):
        """
        Test loading table configurations from real tables.yml file.
        
        Validates:
            - Table configuration loading from real tables.yml
            - Configuration structure and required fields
            - Incremental column configuration
            - Batch size configuration
            - Table importance levels
            - Extraction strategy configuration
            
        ETL Pipeline Context:
            - Critical for ETL pipeline configuration management
            - Supports dental clinic table configurations
            - Uses static configuration approach for performance
            - Enables incremental copy logic
        """
        replicator = replicator_with_real_settings
        
        # Validate table configurations are loaded
        assert replicator.table_configs is not None
        assert len(replicator.table_configs) > 0
        
        # Test specific table configurations
        for table_name, config in replicator.table_configs.items():
            # Validate required configuration fields
            # Schema analyzer uses incremental_columns (plural) as a list
            assert 'incremental_columns' in config, f"Missing incremental_columns for {table_name}"
            assert 'batch_size' in config, f"Missing batch_size for {table_name}"
            assert 'extraction_strategy' in config, f"Missing extraction_strategy for {table_name}"
            assert 'table_importance' in config, f"Missing table_importance for {table_name}"
            
            # Validate configuration values
            assert config['batch_size'] > 0, f"Invalid batch_size for {table_name}"
            assert config['extraction_strategy'] in ['incremental', 'full_table', 'chunked_incremental'], f"Invalid extraction_strategy for {table_name}"
            # Accept all importance values from schema analyzer
            assert config['table_importance'] in ['important', 'standard', 'audit', 'reference', 'critical'], f"Invalid table_importance for {table_name}"
        
        logger.info(f"Successfully loaded {len(replicator.table_configs)} table configurations") 