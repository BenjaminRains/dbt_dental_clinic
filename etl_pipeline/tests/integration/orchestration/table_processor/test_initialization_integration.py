# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for TableProcessor initialization and configuration.

This module tests:
- TableProcessor initialization with real configuration
- Database connection establishment and validation
- Configuration loading and validation
- Settings injection and validation
- Environment variable handling
- Configuration provider integration
- TableProcessingContext initialization
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
from etl_pipeline.orchestration.table_processor import TableProcessor, TableProcessingContext
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    ConfigReader
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
    DataLoadingError,
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
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestTableProcessorInitializationIntegration:
    """Integration tests for TableProcessor initialization with real database connections."""

    def test_table_processor_initialization_with_real_config(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor initialization with real configuration.
        
        Validates:
            - TableProcessor initialization with real settings
            - Settings injection and validation
            - ConfigReader integration with real configuration
            - Provider pattern integration with real settings
            - Environment validation with real configuration
            
        ETL Pipeline Context:
            - Critical for ETL pipeline initialization
            - Supports dental clinic configuration management
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            
            table_processor = TableProcessor()
            
            # Validate that TableProcessor has settings
            assert table_processor.settings is not None
            
            # Validate settings properties instead of object identity
            assert table_processor.settings.environment == test_settings_with_file_provider.environment
            # Validate that settings have the required methods and configurations
            assert hasattr(table_processor.settings, 'get_source_connection_config')
            assert hasattr(table_processor.settings, 'get_replication_connection_config')
            assert hasattr(table_processor.settings, 'get_analytics_connection_config')
            
            # Validate that ConfigReader is initialized
            assert table_processor.config_reader is not None
            
            # Validate that metrics collector is initialized
            assert table_processor.metrics is not None
            
            logger.info("TableProcessor initialization with real config successful")
            
        except Exception as e:
            logger.error(f"TableProcessor initialization with real config test failed: {str(e)}")
            raise

    def test_table_processor_environment_validation(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor environment validation with real configuration.
        
        Validates:
            - Environment validation with real settings
            - Configuration validation for test environment
            - Settings injection for environment-agnostic operation
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Critical for ETL pipeline environment validation
            - Supports dental clinic environment management
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            
            table_processor = TableProcessor()
            
            # Validate environment validation method exists
            assert hasattr(table_processor, '_validate_environment')
            
            # Validate that environment validation passes
            # This should not raise an exception with valid test environment
            table_processor._validate_environment()
            
            # Validate settings are properly injected
            assert table_processor.settings.environment == 'test'
            
            logger.info("TableProcessor environment validation successful")
            
        except Exception as e:
            logger.error(f"TableProcessor environment validation test failed: {str(e)}")
            raise

    def test_table_processor_config_reader_integration(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor ConfigReader integration with real configuration.
        
        Validates:
            - ConfigReader initialization and integration
            - Table configuration loading from real config files
            - Configuration validation for test tables
            - Settings injection for configuration management
            
        ETL Pipeline Context:
            - Critical for ETL pipeline configuration management
            - Supports dental clinic table configuration
            - Uses real configuration files for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            
            table_processor = TableProcessor()
            
            # Validate ConfigReader is properly initialized
            assert table_processor.config_reader is not None
            
            # Test table configuration loading for known test tables
            for table_name in KNOWN_TEST_TABLES:
                config = table_processor.config_reader.get_table_config(table_name)
                assert config is not None, f"No configuration found for table: {table_name}"
                
                # Validate required configuration fields
                assert 'performance_category' in config
                assert 'processing_priority' in config
                assert 'estimated_size_mb' in config
                assert 'estimated_rows' in config
                
                logger.info(f"Configuration loaded successfully for table: {table_name}")
            
            logger.info("TableProcessor ConfigReader integration successful")
            
        except Exception as e:
            logger.error(f"TableProcessor ConfigReader integration test failed: {str(e)}")
            raise

    def test_table_processor_metrics_collector_integration(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor metrics collector integration.
        
        Validates:
            - Metrics collector initialization
            - Performance tracking setup
            - Settings injection for metrics collection
            - Provider pattern integration for metrics
            
        ETL Pipeline Context:
            - Critical for ETL pipeline performance monitoring
            - Supports dental clinic performance tracking
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            
            table_processor = TableProcessor()
            
            # Validate metrics collector is properly initialized
            assert table_processor.metrics is not None
            
            # Validate metrics collector has required methods
            assert hasattr(table_processor.metrics, 'record_performance_metric')
            
            # Validate settings injection for metrics
            assert table_processor.metrics.settings is not None
            assert table_processor.metrics.settings.environment == test_settings_with_file_provider.environment
            
            logger.info("TableProcessor metrics collector integration successful")
            
        except Exception as e:
            logger.error(f"TableProcessor metrics collector integration test failed: {str(e)}")
            raise

    def test_table_processor_database_connection_validation(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor database connection validation.
        
        Validates:
            - Database connection factory integration
            - Source database connection validation
            - Replication database connection validation
            - Analytics database connection validation
            - Connection factory pattern integration
            
        ETL Pipeline Context:
            - Critical for ETL pipeline database connectivity
            - Supports dental clinic database operations
            - Uses real connections for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            
            table_processor = TableProcessor()
            
            # Validate that TableProcessor can access connection factory
            # Note: TableProcessor doesn't directly manage connections, but uses components that do
            from etl_pipeline.core.connections import ConnectionFactory
            
            # Validate source database connection using static methods
            source_engine = ConnectionFactory.get_source_connection(test_settings_with_file_provider)
            assert source_engine is not None
            
            # Validate replication database connection using static methods
            replication_engine = ConnectionFactory.get_replication_connection(test_settings_with_file_provider)
            assert replication_engine is not None
            
            # Validate analytics database connection using static methods
            analytics_engine = ConnectionFactory.get_analytics_connection(test_settings_with_file_provider)
            assert analytics_engine is not None
            
            logger.info("TableProcessor database connection validation successful")
            
        except Exception as e:
            logger.error(f"TableProcessor database connection validation test failed: {str(e)}")
            raise

    def test_table_processor_with_custom_config_path(self, test_settings_with_file_provider, populated_test_databases, temp_tables_config_dir):
        """
        Test TableProcessor initialization with custom configuration path.
        
        Validates:
            - Custom configuration path handling
            - ConfigReader initialization with custom path
            - Settings injection with custom configuration
            - Provider pattern integration with custom config
            
        ETL Pipeline Context:
            - Critical for ETL pipeline configuration flexibility
            - Supports dental clinic configuration management
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            
            # Create custom config path and file
            custom_config_path = str(temp_tables_config_dir / "custom_tables.yml")
            
            # Create a minimal custom tables configuration
            custom_config_content = """
tables:
  patient:
    extraction_strategy: full
    performance_category: tiny
    incremental_columns: []
  appointment:
    extraction_strategy: full
    performance_category: small
    incremental_columns: []
"""
            
            # Write the custom config file
            with open(custom_config_path, 'w') as f:
                f.write(custom_config_content)
            
            table_processor = TableProcessor(config_path=custom_config_path)
            
            # Validate custom config path is set
            assert table_processor.config_path == custom_config_path
            
            # Validate ConfigReader is initialized with custom path
            assert table_processor.config_reader is not None
            
            logger.info("TableProcessor custom config path initialization successful")
            
        except Exception as e:
            logger.error(f"TableProcessor custom config path test failed: {str(e)}")
            raise

    def test_table_processor_with_custom_config_reader(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor initialization with custom ConfigReader.
        
        Validates:
            - Custom ConfigReader injection
            - ConfigReader integration with custom instance
            - Settings injection with custom ConfigReader
            - Provider pattern integration with custom ConfigReader
            
        ETL Pipeline Context:
            - Critical for ETL pipeline configuration flexibility
            - Supports dental clinic configuration management
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            
            # Create custom ConfigReader
            custom_config_reader = ConfigReader()
            
            table_processor = TableProcessor(config_reader=custom_config_reader)
            
            # Validate custom ConfigReader is used
            assert table_processor.config_reader == custom_config_reader
            
            # Validate ConfigReader functionality
            assert table_processor.config_reader is not None
            
            logger.info("TableProcessor custom ConfigReader initialization successful")
            
        except Exception as e:
            logger.error(f"TableProcessor custom ConfigReader test failed: {str(e)}") 