# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for SimpleMySQLReplicator initialization and configuration.

This module tests:
- Replicator initialization with real configuration
- Database connection establishment
- Configuration loading and validation
- Settings injection and validation
- Environment variable handling
- Configuration provider integration
"""

import pytest
import logging
import time
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

# Known test tables that exist in the test database
KNOWN_TEST_TABLES = ['patient', 'appointment', 'procedurelog']

@pytest.mark.integration
@pytest.mark.order(1)  # First tests to run
@pytest.mark.mysql
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestSimpleMySQLReplicatorInitializationIntegration:
    """Integration tests for SimpleMySQLReplicator initialization with real database connections."""

    def test_replicator_initialization_with_real_config(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test SimpleMySQLReplicator initialization with real configuration.
        
        Validates:
            - Replicator initialization with real settings
            - Database connection establishment
            - Configuration loading and validation
            - Settings injection and validation
            
        ETL Pipeline Context:
            - Critical for ETL pipeline startup
            - Supports dental clinic configuration management
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            test_data_manager.setup_procedure_data()
            
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
            - Performance-based prioritization (replaces table_importance)
            - Batch size configuration
            - Extraction strategy configuration
            
        ETL Pipeline Context:
            - Critical for ETL pipeline configuration management
            - Supports dental clinic table configurations
            - Uses static configuration approach for performance
            - Enables incremental copy logic
            - Uses size-based prioritization instead of table_importance
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
            assert 'performance_category' in config, f"Missing performance_category for {table_name}"
            assert 'processing_priority' in config, f"Missing processing_priority for {table_name}"
            
            # Validate configuration values
            assert config['batch_size'] > 0, f"Invalid batch_size for {table_name}"
            assert config['extraction_strategy'] in ['incremental', 'full_table', 'incremental_chunked'], f"Invalid extraction_strategy for {table_name}"
            assert config['performance_category'] in ['tiny', 'small', 'medium', 'large'], f"Invalid performance_category for {table_name}"
            assert config['processing_priority'] in ['low', 'medium', 'high'], f"Invalid processing_priority for {table_name}"
        
        logger.info(f"Successfully loaded {len(replicator.table_configs)} table configurations") 