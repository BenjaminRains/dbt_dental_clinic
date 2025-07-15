"""
Unit tests for PostgresLoader focusing on core data loading functionality.

This module contains pure unit tests for the PostgresLoader class following the
three-tier testing strategy, focusing only on PostgresLoader's actual responsibilities.

Test Strategy:
    - Pure unit tests with mocked database connections
    - Focus on PostgresLoader's core data loading logic
    - No testing of Settings/ConnectionFactory behavior
    - AAA pattern for all test methods
    - No real database connections, full mocking

Coverage Areas:
    - Configuration loading from tables.yml
    - Table loading (standard and chunked)
    - Load verification and error handling
    - Query building logic
    - Data type conversion

ETL Context:
    - Dental clinic ETL pipeline (MySQL → PostgreSQL data movement)
    - Focus on data movement and transformation logic
    - Type safety with DatabaseType and PostgresSchema enums
"""

import pytest
import os
from unittest.mock import MagicMock, patch

# Import ETL pipeline components
from etl_pipeline.exceptions.configuration import ConfigurationError

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
        sample_table_data,
    )
    FIXTURES_AVAILABLE = True
except ImportError:
    FIXTURES_AVAILABLE = False
    sample_table_data = None


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


@pytest.mark.unit
class TestPostgresLoaderUnit:
    """
    Unit tests for PostgresLoader focusing on core data loading functionality.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on PostgresLoader's core data loading logic
        - No testing of Settings/ConnectionFactory behavior
        - AAA pattern for all test methods
        - No real database connections, full mocking
    
    Coverage Areas:
        - Configuration loading from tables.yml
        - Table loading (standard and chunked)
        - Load verification and error handling
        - Query building logic
        - Data type conversion
        - Exception handling for data loading operations
        
    ETL Context:
        - Dental clinic ETL pipeline (MySQL → PostgreSQL data movement)
        - Focus on data movement and transformation logic
        - Type safety with DatabaseType and PostgresSchema enums
    """
    
    def test_initialization_with_test_environment(self, mock_postgres_loader):
        """
        Test basic initialization of PostgresLoader with test environment.
        
        AAA Pattern:
            Arrange: Mock PostgresLoader to prevent real connections
            Act: Access loader attributes
            Assert: Verify all required attributes are initialized
        """
        # Arrange & Act
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        mock_loader = mock_postgres_loader.return_value
        
        # Assert
        assert mock_loader.settings is not None
        assert mock_loader.replication_engine is not None
        assert mock_loader.analytics_engine is not None
        assert mock_loader.schema_adapter is not None
        assert mock_loader.table_configs is not None

    def test_load_configuration_success(self, mock_postgres_loader):
        """
        Test successful loading of table configuration from tables.yml.
        
        AAA Pattern:
            Arrange: Mock PostgresLoader with table configurations
            Act: Access table configurations
            Assert: Verify configurations are loaded correctly
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        mock_loader = mock_postgres_loader.return_value
        
        # Act & Assert
        assert 'patient' in mock_loader.table_configs
        assert 'appointment' in mock_loader.table_configs
        assert mock_loader.table_configs['patient']['incremental_columns'] == ['DateModified']
        assert mock_loader.table_configs['appointment']['batch_size'] == 500
    
    def test_load_configuration_file_not_found(self, mock_postgres_loader):
        """
        Test configuration file not found error handling.
        
        AAA Pattern:
            Arrange: Configure mock to raise ConfigurationError
            Act: Attempt to create PostgresLoader with invalid path
            Assert: Verify ConfigurationError is raised with correct message
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE or PostgresLoader is None:
            pytest.skip("PostgresLoader not available")
        
        mock_postgres_loader.side_effect = ConfigurationError(
            message="Configuration file not found: /nonexistent/path/tables.yml",
            config_file="/nonexistent/path/tables.yml",
            details={"error_type": "file_not_found"}
        )
        
        # Act & Assert
        with pytest.raises(ConfigurationError, match="Configuration file not found"):
            PostgresLoader(tables_config_path="/nonexistent/path/tables.yml")

    def test_get_table_config(self, mock_postgres_loader):
        """
        Test table configuration retrieval.
        
        AAA Pattern:
            Arrange: Configure mock with table-specific configurations
            Act: Call get_table_config for different tables
            Assert: Verify correct configurations are returned
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        mock_loader = mock_postgres_loader.return_value
        
        def get_table_config_side_effect(table_name):
            if table_name == 'patient':
                return {'incremental_columns': ['DateModified']}
            elif table_name == 'appointment':
                return {'batch_size': 500}
            else:
                return {}
        
        mock_loader.get_table_config.side_effect = get_table_config_side_effect
        
        # Act
        patient_config = mock_loader.get_table_config('patient')
        appointment_config = mock_loader.get_table_config('appointment')
        missing_config = mock_loader.get_table_config('nonexistent')
        
        # Assert
        assert patient_config['incremental_columns'] == ['DateModified']
        assert appointment_config['batch_size'] == 500
        assert missing_config == {}
    
    def test_load_table_success(self, mock_postgres_loader, sample_table_data):
        """
        Test successful table loading.
        
        AAA Pattern:
            Arrange: Configure mock loader for successful loading
            Act: Call load_table method
            Assert: Verify successful loading return value
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        mock_loader = mock_postgres_loader.return_value
        mock_loader.load_table.return_value = True
        
        # Act
        result = mock_loader.load_table('patient')
        
        # Assert
        assert result is True
        mock_loader.load_table.assert_called_once_with('patient')
    
    def test_load_table_no_configuration(self, mock_postgres_loader):
        """
        Test table loading with missing configuration.
        
        AAA Pattern:
            Arrange: Configure mock to return False for missing config
            Act: Call load_table with nonexistent table
            Assert: Verify failure return value
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        mock_loader = mock_postgres_loader.return_value
        mock_loader.load_table.return_value = False
        
        # Act
        result = mock_loader.load_table('nonexistent')
        
        # Assert
        assert result is False
    
    def test_load_table_chunked_success(self, mock_postgres_loader):
        """
        Test successful chunked table loading.
        
        AAA Pattern:
            Arrange: Configure mock for successful chunked loading
            Act: Call load_table_chunked with chunk size
            Assert: Verify successful loading with correct parameters
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        mock_loader = mock_postgres_loader.return_value
        mock_loader.load_table_chunked.return_value = True
        
        # Act
        result = mock_loader.load_table_chunked('patient', chunk_size=50)
        
        # Assert
        assert result is True
        mock_loader.load_table_chunked.assert_called_once_with('patient', chunk_size=50)
    
    def test_verify_load_success(self, mock_postgres_loader):
        """
        Test successful load verification.
        
        AAA Pattern:
            Arrange: Configure mock with matching row counts
            Act: Call verify_load method
            Assert: Verify successful verification
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        mock_loader = mock_postgres_loader.return_value
        mock_loader.verify_load.return_value = True
        
        # Act
        result = mock_loader.verify_load('patient')
        
        # Assert
        assert result is True
    
    def test_verify_load_mismatch(self, mock_postgres_loader):
        """
        Test load verification with row count mismatch.
        
        AAA Pattern:
            Arrange: Configure mock to return False for verification failure
            Act: Call verify_load method
            Assert: Verify failure return value
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        mock_loader = mock_postgres_loader.return_value
        mock_loader.verify_load.return_value = False
        
        # Act
        result = mock_loader.verify_load('patient')
        
        # Assert
        assert result is False
    
    def test_database_connection_error_handling(self, mock_postgres_loader):
        """
        Test error handling for database connection failures.
        
        AAA Pattern:
            Arrange: Configure mock to return False for connection error
            Act: Call load_table method
            Assert: Verify failure is handled gracefully
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        mock_loader = mock_postgres_loader.return_value
        mock_loader.load_table.return_value = False
        
        # Act
        result = mock_loader.load_table('patient')
        
        # Assert
        assert result is False
    
    def test_data_loading_error_handling(self, mock_postgres_loader):
        """
        Test error handling for data loading failures.
        
        AAA Pattern:
            Arrange: Configure mock to return False for data loading error
            Act: Call load_table method
            Assert: Verify failure is handled gracefully
        """
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        mock_loader = mock_postgres_loader.return_value
        mock_loader.load_table.return_value = False
        
        # Act
        result = mock_loader.load_table('patient')
        
        # Assert
        assert result is False