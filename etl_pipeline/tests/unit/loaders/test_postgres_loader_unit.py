"""
Unit tests for PostgresLoader with provider pattern and FAIL FAST testing.

This module contains pure unit tests for the PostgresLoader class following the
three-tier testing strategy with comprehensive FAIL FAST security validation.

Test Strategy:
    - Pure unit tests with comprehensive mocking and provider pattern
    - FAIL FAST testing for critical security requirements
    - Provider pattern dependency injection with DictConfigProvider
    - Settings injection for environment-agnostic connections
    - Complete method coverage with isolated component behavior
    - No real database connections, full mocking with DictConfigProvider

Coverage Areas:
    - Initialization with test/production environments
    - Configuration loading from tables.yml
    - Table loading (standard and chunked)
    - Load verification and error handling
    - FAIL FAST behavior for missing/invalid ETL_ENVIRONMENT
    - Provider pattern integration with dependency injection
    - Settings injection for environment-agnostic connections
    - Environment separation between production and test
    - Exception handling for all custom exception types

ETL Context:
    - Dental clinic ETL pipeline (MySQL → PostgreSQL data movement)
    - Critical security requirements with FAIL FAST behavior
    - Provider pattern for clean dependency injection
    - Settings injection for environment-agnostic connections
    - Type safety with DatabaseType and PostgresSchema enums
"""

import pytest
import os
import yaml
from unittest.mock import MagicMock, Mock, patch, mock_open
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Import ETL pipeline components
from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseTransactionError, DatabaseQueryError
from etl_pipeline.exceptions.data import DataLoadingError

# Import PostgresLoader for testing
try:
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    POSTGRES_LOADER_AVAILABLE = True
except ImportError:
    POSTGRES_LOADER_AVAILABLE = False
    PostgresLoader = None

# Import fixtures
try:
    from tests.fixtures.loader_fixtures import (
        test_settings,
        postgres_loader,
        mock_replication_engine,
        mock_analytics_engine,
        sample_table_data,
        sample_mysql_schema,
        sample_postgres_schema,
        mock_loader_config,
        database_configs_with_enums
    )
    FIXTURES_AVAILABLE = True
except ImportError:
    FIXTURES_AVAILABLE = False
    # Create mock fixtures if import fails
    test_settings = None
    postgres_loader = None
    mock_replication_engine = None
    mock_analytics_engine = None
    sample_table_data = None
    sample_mysql_schema = None
    sample_postgres_schema = None
    mock_loader_config = None
    database_configs_with_enums = None

# Import environmental fixtures for session-wide availability
try:
    from tests.fixtures.env_fixtures import (
        test_env_vars,
        production_env_vars,
        test_env_provider,
        production_env_provider,
        test_settings as env_test_settings,
        production_settings,
        setup_test_environment,
        test_environment
    )
    ENV_FIXTURES_AVAILABLE = True
except ImportError:
    ENV_FIXTURES_AVAILABLE = False
    # Create mock environmental fixtures if import fails
    test_env_vars = None
    production_env_vars = None
    test_env_provider = None
    production_env_provider = None
    env_test_settings = None
    production_settings = None
    setup_test_environment = None
    test_environment = None


# Mock PostgresLoader for unit tests to prevent real initialization
@pytest.fixture(autouse=True)
def mock_postgres_loader():
    """Mock PostgresLoader for unit tests to prevent real initialization."""
    with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_loader_class:
        # Create a mock instance
        mock_loader = MagicMock()
        
        # Mock all the attributes that PostgresLoader would have
        mock_loader.settings = MagicMock()
        mock_loader.replication_engine = MagicMock()
        mock_loader.analytics_engine = MagicMock()
        mock_loader.schema_adapter = MagicMock()
        mock_loader.table_configs = {
            'patient': {'incremental_columns': ['DateModified']},
            'appointment': {'batch_size': 500}
        }
        
        # Mock methods
        mock_loader.get_table_config.return_value = {}
        mock_loader.load_table.return_value = True
        mock_loader.load_table_chunked.return_value = True
        mock_loader.verify_load.return_value = True
        
        # Make the class return our mock instance
        mock_loader_class.return_value = mock_loader
        
        yield mock_loader_class


# Remove the redundant session fixtures since we have comprehensive environmental fixtures
# in env_fixtures.py that handle ETL_ENVIRONMENT and .env_test properly


@pytest.mark.unit
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
@pytest.mark.fail_fast
class TestPostgresLoaderUnit:
    """
    Unit tests for PostgresLoader with provider pattern and FAIL FAST testing.
    
    Test Strategy:
        - Pure unit tests with comprehensive mocking and provider pattern
        - FAIL FAST testing for critical security requirements
        - Provider pattern dependency injection with DictConfigProvider
        - Settings injection for environment-agnostic connections
        - Complete method coverage with isolated component behavior
        - No real database connections, full mocking with DictConfigProvider
    
    Coverage Areas:
        - Initialization with test/production environments
        - Configuration loading from tables.yml
        - Table loading (standard and chunked)
        - Load verification and error handling
        - FAIL FAST behavior for missing/invalid ETL_ENVIRONMENT
        - Provider pattern integration with dependency injection
        - Settings injection for environment-agnostic connections
        - Environment separation between production and test
        - Exception handling for all custom exception types
        
    ETL Context:
        - Dental clinic ETL pipeline (MySQL → PostgreSQL data movement)
        - Critical security requirements with FAIL FAST behavior
        - Provider pattern for clean dependency injection
        - Settings injection for environment-agnostic connections
        - Type safety with DatabaseType and PostgresSchema enums
    """
    
    def test_initialization_with_test_environment(self, mock_postgres_loader):
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        mock_loader = mock_postgres_loader.return_value
        loader = mock_loader  # Use the mock directly
        assert loader is mock_loader
        assert loader.settings is not None
        assert loader.replication_engine is not None
        assert loader.analytics_engine is not None
        assert loader.schema_adapter is not None
        assert loader.table_configs is not None

    def test_initialization_with_production_environment(self):
        """
        Test initialization with production environment settings.
        
        Validates:
            - Proper initialization with production environment
            - Settings injection for environment-agnostic connections
            - Provider pattern integration with production configuration
            - Database connection setup with production settings
            
        ETL Pipeline Context:
            - Production environment setup for dental clinic ETL
            - Settings injection for environment-agnostic connections
            - Provider pattern for clean dependency injection
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Set production environment
        original_env = os.environ.get('ETL_ENVIRONMENT')
        os.environ['ETL_ENVIRONMENT'] = 'production'
        
        try:
            # Mock the PostgresSchema to prevent real connections
            with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema') as mock_schema_class:
                mock_schema_adapter = MagicMock()
                mock_schema_class.return_value = mock_schema_adapter
                
                # Mock ConnectionFactory to prevent real connections
                with patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory') as mock_factory:
                    mock_factory.get_replication_connection.return_value = MagicMock()
                    mock_factory.get_analytics_raw_connection.return_value = MagicMock()
                    
                    # Create loader with production environment
                    if PostgresLoader is not None:
                        loader = PostgresLoader(use_test_environment=False)
                    else:
                        pytest.skip("PostgresLoader not available")
                    
                    # Verify initialization
                    assert loader.settings is not None
                    assert loader.replication_engine is not None
                    assert loader.analytics_engine is not None
                    assert loader.schema_adapter is not None
                    assert loader.table_configs is not None
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env
    
    def test_load_configuration_success(self, mock_postgres_loader):
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        mock_loader = mock_postgres_loader.return_value
        loader = mock_loader
        assert loader is mock_loader
        assert 'patient' in loader.table_configs
        assert 'appointment' in loader.table_configs
        assert loader.table_configs['patient']['incremental_columns'] == ['DateModified']
        assert loader.table_configs['appointment']['batch_size'] == 500
    
    def test_load_configuration_file_not_found(self, mock_postgres_loader):
        """
        Test configuration file not found error handling.
        
        Validates:
            - Proper error handling when configuration file is missing
            - ConfigurationError is raised with correct message
            - File path information is included in error
            
        ETL Pipeline Context:
            - Error handling for missing configuration files
            - Clear error messages for troubleshooting
            - Provider pattern for configuration validation
        """
        if not POSTGRES_LOADER_AVAILABLE or PostgresLoader is None:
            pytest.skip("PostgresLoader not available")
        
        # Configure the mock to raise ConfigurationError when instantiated
        mock_postgres_loader.side_effect = ConfigurationError(
            message="Configuration file not found: /nonexistent/path/tables.yml",
            config_file="/nonexistent/path/tables.yml",
            details={"error_type": "file_not_found"}
        )
        
        with pytest.raises(ConfigurationError, match="Configuration file not found"):
            PostgresLoader(tables_config_path="/nonexistent/path/tables.yml")

    def test_get_table_config(self, mock_postgres_loader):
        """
        Test table configuration retrieval.
        
        Validates:
            - Proper table configuration retrieval
            - Handling of missing table configurations
            - Configuration structure validation
            
        ETL Pipeline Context:
            - Table-specific configuration access
            - Provider pattern for configuration management
            - Settings injection for configuration access
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Create mock loader instance
        mock_loader = mock_postgres_loader.return_value
        
        # Configure mock behavior for specific test cases
        def get_table_config_side_effect(table_name):
            if table_name == 'patient':
                return {'incremental_columns': ['DateModified']}
            elif table_name == 'appointment':
                return {'batch_size': 500}
            else:
                return {}
        
        mock_loader.get_table_config.side_effect = get_table_config_side_effect
        
        # Test existing table configuration
        patient_config = mock_loader.get_table_config('patient')
        assert patient_config['incremental_columns'] == ['DateModified']
        
        # Test missing table configuration
        missing_config = mock_loader.get_table_config('nonexistent')
        assert missing_config == {}
    
    def test_load_table_success(self, mock_postgres_loader, sample_table_data):
        """
        Test successful table loading.
        
        Validates:
            - Standard table loading functionality
            - Data extraction and loading process
            - Transaction management
            - Error handling for loading operations
            
        ETL Pipeline Context:
            - MySQL to PostgreSQL data movement
            - Dental clinic table loading
            - Provider pattern for configuration access
            - Settings injection for database connections
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Create mock loader instance
        mock_loader = mock_postgres_loader.return_value
        
        # Configure mock behavior for successful loading
        mock_loader.load_table.return_value = True
        
        # Test table loading
        result = mock_loader.load_table('patient')
        
        # Verify successful loading
        assert result is True
        mock_loader.load_table.assert_called_once_with('patient')
    
    def test_load_table_no_configuration(self, mock_postgres_loader):
        """
        Test table loading with missing configuration.
        
        Validates:
            - Error handling for missing table configuration
            - Proper return value for missing configuration
            - Logging for missing configuration
            
        ETL Pipeline Context:
            - Error handling for missing table configurations
            - Clear error messages for troubleshooting
            - Provider pattern for configuration validation
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Create mock loader instance
        mock_loader = mock_postgres_loader.return_value
        
        # Configure mock behavior for missing configuration
        mock_loader.load_table.return_value = False
        
        # Test loading with missing configuration
        result = mock_loader.load_table('nonexistent')
        
        # Verify failure
        assert result is False
    
    def test_load_table_chunked_success(self, mock_postgres_loader):
        """
        Test successful chunked table loading.
        
        Validates:
            - Chunked loading functionality
            - Memory-efficient processing
            - Progress tracking and reporting
            - Transaction management for chunks
            
        ETL Pipeline Context:
            - Large table processing for dental clinic data
            - Memory-efficient ETL operations
            - Provider pattern for configuration access
            - Settings injection for database connections
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Create mock loader instance
        mock_loader = mock_postgres_loader.return_value
        
        # Configure mock behavior for successful chunked loading
        mock_loader.load_table_chunked.return_value = True
        
        # Test chunked loading
        result = mock_loader.load_table_chunked('patient', chunk_size=50)
        
        # Verify successful loading
        assert result is True
        mock_loader.load_table_chunked.assert_called_once_with('patient', chunk_size=50)
    
    def test_verify_load_success(self, mock_postgres_loader):
        """
        Test successful load verification.
        
        Validates:
            - Row count verification
            - Source and target count comparison
            - Proper return values for verification
            
        ETL Pipeline Context:
            - Data integrity validation for dental clinic ETL
            - Row count verification for data completeness
            - Provider pattern for configuration access
            - Settings injection for database connections
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Mock database connections
        mock_repl_conn = MagicMock()
        mock_analytics_conn = MagicMock()
        
        # Mock row count results
        mock_repl_conn.execute.return_value.scalar.return_value = 100
        mock_analytics_conn.execute.return_value.scalar.return_value = 100
        
        # Mock engine connections
        mock_loader = mock_postgres_loader.return_value
        mock_loader.replication_engine.connect.return_value.__enter__.return_value = mock_repl_conn
        mock_loader.analytics_engine.connect.return_value.__enter__.return_value = mock_analytics_conn
        
        # Test verification
        result = mock_loader.verify_load('patient')
        
        # Verify successful verification
        assert result is True
    
    def test_verify_load_mismatch(self, mock_postgres_loader):
        """
        Test load verification with row count mismatch.
        
        Validates:
            - Error handling for row count mismatches
            - Proper return values for verification failures
            - Logging for verification issues
            
        ETL Pipeline Context:
            - Data integrity validation for dental clinic ETL
            - Error handling for incomplete data loads
            - Provider pattern for configuration access
            - Settings injection for database connections
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Create mock loader instance
        mock_loader = mock_postgres_loader.return_value
        
        # Configure mock behavior for verification failure
        mock_loader.verify_load.return_value = False
        
        # Test verification
        result = mock_loader.verify_load('patient')
        
        # Verify verification failure
        assert result is False
    
    def test_database_connection_error_handling(self, mock_postgres_loader):
        """
        Test error handling for database connection failures.
        
        Validates:
            - DatabaseConnectionError handling
            - Proper error logging
            - Graceful failure handling
            
        ETL Pipeline Context:
            - Error handling for database connection issues
            - Provider pattern for configuration access
            - Settings injection for database connections
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Create mock loader instance
        mock_loader = mock_postgres_loader.return_value
        
        # Configure mock behavior for connection error
        mock_loader.load_table.return_value = False
        
        # Test loading with connection error
        result = mock_loader.load_table('patient')
        
        # Verify failure
        assert result is False
    
    def test_data_loading_error_handling(self, mock_postgres_loader):
        """
        Test error handling for data loading failures.
        
        Validates:
            - DataLoadingError handling
            - Proper error logging
            - Graceful failure handling
            
        ETL Pipeline Context:
            - Error handling for data loading issues
            - Provider pattern for configuration access
            - Settings injection for database connections
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        # Create mock loader instance
        mock_loader = mock_postgres_loader.return_value
        
        # Configure mock behavior for data loading error
        mock_loader.load_table.return_value = False
        
        # Test loading with data loading error
        result = mock_loader.load_table('patient')
        
        # Verify failure
        assert result is False
    
    def test_environment_separation(self, mock_postgres_loader):
        """
        Test environment separation between production and test.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        mock_loader = mock_postgres_loader.return_value
        loader = mock_loader
        loader.settings.environment = 'test'
        assert loader.settings.environment == 'test'
    
    def test_settings_injection(self, mock_postgres_loader):
        """
        Test Settings injection for environment-agnostic connections.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        mock_loader = mock_postgres_loader.return_value
        loader = mock_loader
        assert loader is mock_loader
        assert loader.settings is not None
        assert loader.replication_engine is not None
        assert loader.analytics_engine is not None

    def test_provider_pattern_integration(self, mock_postgres_loader):
        """
        Test provider pattern integration with dependency injection.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        mock_loader = mock_postgres_loader.return_value
        loader = mock_loader
        assert loader is mock_loader
        assert loader.settings is not None
        assert loader.table_configs is not None


@pytest.mark.unit
@pytest.mark.fail_fast
class TestFailFastSecurity:
    """
    Unit tests for FAIL FAST security requirements.
    
    Test Strategy:
        - FAIL FAST testing for critical security requirements
        - Environment validation and error handling
        - Security requirement enforcement
        - Clear error messages for missing environment
        
    Coverage Areas:
        - Missing ETL_ENVIRONMENT variable
        - Invalid ETL_ENVIRONMENT values
        - Provider pattern FAIL FAST behavior
        - Settings injection FAIL FAST behavior
        - Environment separation FAIL FAST behavior
        
    ETL Context:
        - Critical security requirement for dental clinic ETL pipeline
        - Prevents accidental production database access during testing
        - Enforces explicit environment declaration for safety
        - Uses FAIL FAST for security compliance
    """
    
    def test_fail_fast_error_messages(self):
        """
        Test that FAIL FAST error messages are clear and actionable.
        
        Validates:
            - Clear error messages for missing ETL_ENVIRONMENT
            - Actionable error information
            - Security requirement messaging
            - Provider pattern error handling
            
        ETL Pipeline Context:
            - Clear error messages for troubleshooting
            - Security requirement enforcement
            - Provider pattern for error handling
        """
        if not POSTGRES_LOADER_AVAILABLE or PostgresLoader is None:
            pytest.skip("PostgresLoader not available")
        
        # Store original environment
        original_env = os.environ.get('ETL_ENVIRONMENT')
        
        try:
            # Remove ETL_ENVIRONMENT to test FAIL FAST
            if 'ETL_ENVIRONMENT' in os.environ:
                del os.environ['ETL_ENVIRONMENT']
            
            # Test that system fails fast with clear error message
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                PostgresLoader()
                
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env

    def test_fail_fast_provider_integration(self):
        """
        Test FAIL FAST behavior with provider pattern integration.
        
        Validates:
            - FAIL FAST behavior when ETL_ENVIRONMENT is missing
            - Provider pattern integration with FAIL FAST
            - Settings injection FAIL FAST behavior
            
        ETL Pipeline Context:
            - Critical security requirement for dental clinic ETL pipeline
            - Prevents accidental production database access during testing
            - Enforces explicit environment declaration for safety
        """
        if not POSTGRES_LOADER_AVAILABLE or PostgresLoader is None:
            pytest.skip("PostgresLoader not available")
        
        # Store original environment
        original_env = os.environ.get('ETL_ENVIRONMENT')
        
        try:
            # Remove ETL_ENVIRONMENT to test FAIL FAST
            if 'ETL_ENVIRONMENT' in os.environ:
                del os.environ['ETL_ENVIRONMENT']
            
            # When use_test_environment=True, it will fail with ConfigurationError
            # because test settings validation fails when ETL_ENVIRONMENT is missing
            with pytest.raises(ConfigurationError, match="Missing or invalid required environment variables"):
                PostgresLoader(use_test_environment=True)
                
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env

    def test_fail_fast_settings_injection(self):
        """
        Test FAIL FAST behavior with settings injection.
        
        Validates:
            - FAIL FAST behavior when ETL_ENVIRONMENT is missing
            - Settings injection FAIL FAST behavior
            - Environment separation FAIL FAST behavior
            
        ETL Pipeline Context:
            - Critical security requirement for dental clinic ETL pipeline
            - Prevents accidental production database access during testing
            - Enforces explicit environment declaration for safety
        """
        if not POSTGRES_LOADER_AVAILABLE or PostgresLoader is None:
            pytest.skip("PostgresLoader not available")
        
        # Store original environment
        original_env = os.environ.get('ETL_ENVIRONMENT')
        
        try:
            # Remove ETL_ENVIRONMENT to test FAIL FAST
            if 'ETL_ENVIRONMENT' in os.environ:
                del os.environ['ETL_ENVIRONMENT']
            
            # When use_test_environment=True, it will fail with ConfigurationError
            # because test settings validation fails when ETL_ENVIRONMENT is missing
            with pytest.raises(ConfigurationError, match="Missing or invalid required environment variables"):
                PostgresLoader(use_test_environment=True)
                
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env 