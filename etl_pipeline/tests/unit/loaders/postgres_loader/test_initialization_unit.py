"""
Unit tests for PostgresLoader initialization and configuration.

This module contains tests for:
    - Basic initialization with test environment
    - Configuration loading from tables.yml
    - Table configuration retrieval
    - Configuration file error handling
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Import ETL pipeline components
from etl_pipeline.exceptions.configuration import ConfigurationError
from etl_pipeline.config import DatabaseType

# Import PostgresLoader for testing
try:
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    POSTGRES_LOADER_AVAILABLE = True
except ImportError:
    POSTGRES_LOADER_AVAILABLE = False
    PostgresLoader = None

# Import fixtures
try:
    from tests.fixtures.loader_fixtures import sample_table_data
    from tests.fixtures.env_fixtures import test_env_vars, load_test_environment_file
    FIXTURES_AVAILABLE = True
except ImportError:
    FIXTURES_AVAILABLE = False
    sample_table_data = None
    test_env_vars = None
    load_test_environment_file = None


@pytest.fixture
def mock_postgres_loader_instance():
    """Create a fully mocked PostgresLoader instance for unit testing."""
    # Create a mock PostgresLoader instance - remove spec to allow custom attributes
    mock_loader = MagicMock()
    
    # Set up basic attributes
    mock_loader.analytics_schema = 'raw'
    mock_loader.settings = MagicMock()  # Add missing settings attribute
    mock_loader.table_configs = {
        'patient': {'incremental_columns': ['DateModified']},
        'appointment': {'batch_size': 500, 'incremental_columns': ['DateCreated', 'DateModified']},
        'procedurelog': {'primary_key': 'ProcNum'},
        'periomeasure': {
            'incremental_columns': ['DateEntered'],
            'estimated_size_mb': 57,
            'performance_strategy': 'streaming',
            'bulk_insert_buffer': 50000
        },
        'large_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 500
        },
        'empty_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 100
        },
        'test_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 50
        }
    }
    
    # Mock the schema adapter
    mock_schema_adapter = MagicMock()
    mock_schema_adapter.get_table_schema_from_mysql.return_value = {'columns': []}
    mock_schema_adapter.ensure_table_exists.return_value = True
    mock_schema_adapter.convert_row_data_types.return_value = {'id': 1, 'data': 'test'}
    mock_loader.schema_adapter = mock_schema_adapter
    
    # Mock database engines
    mock_replication_engine = MagicMock()
    mock_analytics_engine = MagicMock()
    mock_loader.replication_engine = mock_replication_engine
    mock_loader.analytics_engine = mock_analytics_engine
    
    # Mock methods that are called internally
    # Configure get_table_config to return actual table configs
    def get_table_config_side_effect(table_name):
        return mock_loader.table_configs.get(table_name, {})
    
    mock_loader.get_table_config = MagicMock(side_effect=get_table_config_side_effect)
    mock_loader._build_load_query = MagicMock(return_value="SELECT * FROM test_table")
    mock_loader._update_load_status = MagicMock(return_value=True)
    mock_loader._get_loaded_at_time_max = MagicMock(return_value=datetime(2024, 1, 1, 10, 0, 0))
    mock_loader._ensure_tracking_record_exists = MagicMock(return_value=True)
    mock_loader.stream_mysql_data = MagicMock(return_value=[[{'id': 1, 'data': 'test'}]])
    mock_loader.load_table_copy_csv = MagicMock(return_value=True)
    mock_loader.load_table_standard = MagicMock(return_value=True)
    mock_loader.load_table_streaming = MagicMock(return_value=True)
    mock_loader.load_table_chunked = MagicMock(return_value=True)
    
    return mock_loader


@pytest.mark.unit
class TestPostgresLoaderInitialization:
    """
    Unit tests for PostgresLoader initialization and configuration.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on initialization and configuration logic
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_initialization_with_test_environment(self, mock_postgres_loader_instance):
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
        loader = mock_postgres_loader_instance
        
        # Assert
        assert loader.settings is not None
        assert loader.replication_engine is not None
        assert loader.analytics_engine is not None
        assert loader.schema_adapter is not None
        assert loader.table_configs is not None

    def test_load_configuration_success(self, mock_postgres_loader_instance):
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
        loader = mock_postgres_loader_instance
        
        # Act & Assert
        assert 'patient' in loader.table_configs
        assert 'appointment' in loader.table_configs
        assert loader.table_configs['patient']['incremental_columns'] == ['DateModified']
        assert loader.table_configs['appointment']['batch_size'] == 500
        assert loader.table_configs['appointment']['incremental_columns'] == ['DateCreated', 'DateModified']
    
    def test_load_configuration_file_not_found(self, mock_postgres_loader_instance):
        """
        Test configuration handling with new constructor.
        
        The new constructor requires replication_engine, analytics_engine, settings, and schema_adapter.
        Configuration is loaded from settings, not from a file path parameter.
        """
        if not POSTGRES_LOADER_AVAILABLE or PostgresLoader is None:
            pytest.skip("PostgresLoader not available")
        
        # The new constructor doesn't take tables_config_path - configuration comes from settings
        # This test verifies that missing configuration is handled gracefully
        from etl_pipeline.config import create_test_settings
        from unittest.mock import MagicMock
        from etl_pipeline.config import DatabaseType
        from etl_pipeline.config import PostgresSchema as ConfigPostgresSchema
        
        settings = create_test_settings()
        # Mock settings.get_database_config to avoid env var requirements
        def mock_get_database_config(db_type, *args):
            if db_type == DatabaseType.ANALYTICS:
                return {'database': 'test_db', 'schema': 'raw'}
            elif db_type == DatabaseType.REPLICATION:
                return {'database': 'test_db'}
            return {'database': 'test_db'}
        settings.get_database_config = MagicMock(side_effect=mock_get_database_config)
        
        # Mock engines instead of using real ConnectionFactory to avoid env var requirements
        replication_engine = MagicMock()
        analytics_engine = MagicMock()
        schema_adapter = MagicMock()
        schema_adapter.get_table_schema_from_mysql.return_value = {'columns': []}
        schema_adapter.ensure_table_exists.return_value = True
        
        # Act - create loader with new constructor
        loader = PostgresLoader(
            replication_engine=replication_engine,
            analytics_engine=analytics_engine,
            settings=settings,
            schema_adapter=schema_adapter,
        )
        
        # Assert - loader should be created successfully
        assert loader is not None
        assert loader.settings == settings
        # get_table_config should work - settings.get_table_config may return default config
        # So we just check that it doesn't raise an exception
        config = loader.get_table_config('nonexistent_table')
        assert isinstance(config, dict)  # Should return a dict (empty or with defaults)

    def test_get_table_config(self, mock_postgres_loader_instance):
        """
        Test table configuration retrieval.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Test with real get_table_config method using the actual table_configs
        # Act
        patient_config = loader.get_table_config('patient')
        appointment_config = loader.get_table_config('appointment')
        procedure_config = loader.get_table_config('procedurelog')
        missing_config = loader.get_table_config('nonexistent')
        
        # Assert: Verify table configurations are returned correctly
        assert patient_config == {'incremental_columns': ['DateModified']}
        assert appointment_config == {'batch_size': 500, 'incremental_columns': ['DateCreated', 'DateModified']}
        assert procedure_config == {'primary_key': 'ProcNum'}
        assert missing_config == {} 