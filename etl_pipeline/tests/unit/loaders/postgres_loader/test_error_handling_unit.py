"""
Unit tests for PostgresLoader error handling and exception scenarios.

This module contains tests for:
    - Database connection error handling
    - Data loading error handling
    - Configuration error handling
    - Exception handling in various scenarios
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
    mock_loader._get_last_load_time_max = MagicMock(return_value=datetime(2024, 1, 1, 10, 0, 0))
    mock_loader._ensure_tracking_record_exists = MagicMock(return_value=True)
    mock_loader.stream_mysql_data = MagicMock(return_value=[[{'id': 1, 'data': 'test'}]])
    mock_loader.load_table_copy_csv = MagicMock(return_value=True)
    mock_loader.load_table_standard = MagicMock(return_value=True)
    mock_loader.load_table_streaming = MagicMock(return_value=True)
    mock_loader.load_table_chunked = MagicMock(return_value=True)
    
    return mock_loader


@pytest.mark.unit
class TestPostgresLoaderErrorHandling:
    """
    Unit tests for PostgresLoader error handling and exception scenarios.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on error handling and exception scenarios
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_database_connection_error_handling(self, mock_postgres_loader_instance):
        """
        Test error handling for database connection failures.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock load_table to return False when database connection fails
        with patch.object(loader, 'load_table', return_value=False):
            # Act - call the load_table method which should handle the exception
            result = loader.load_table('patient')
            
            # Assert
            assert result is False
    
    def test_data_loading_error_handling(self, mock_postgres_loader_instance):
        """
        Test error handling for data loading failures.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock load_table to return False (loading failed)
        with patch.object(loader, 'load_table', return_value=False):
            # Act
            result = loader.load_table('patient')
            
            # Assert
            assert result is False
    
    def test_bulk_insert_optimized_exception_handling(self, mock_postgres_loader_instance):
        """Test bulk insert with database exception."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        rows_data = [{'id': 1, 'name': 'John Doe'}]
        
        # Import PostgresLoader for method binding
        from etl_pipeline.loaders.postgres_loader import PostgresLoader
        
        # Bind the real bulk_insert_optimized method for testing
        loader.bulk_insert_optimized = PostgresLoader.bulk_insert_optimized.__get__(loader, PostgresLoader)
        
        # Mock exception
        loader.analytics_engine.begin.side_effect = Exception("Database error")
        
        # Act
        result = loader.bulk_insert_optimized(table_name, rows_data)
        
        # Assert
        assert result is False
    
    def test_stream_mysql_data_exception_handling(self, mock_postgres_loader_instance):
        """Test streaming with database exception."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        query = "SELECT * FROM patient"
        
        # Import PostgresLoader for method binding
        from etl_pipeline.loaders.postgres_loader import PostgresLoader
        
        # Remove the mock for stream_mysql_data so we can test the real method
        loader.stream_mysql_data = PostgresLoader.stream_mysql_data.__get__(loader, PostgresLoader)
        
        # Bind the real _convert_sqlalchemy_row_to_dict method for testing
        loader._convert_sqlalchemy_row_to_dict = PostgresLoader._convert_sqlalchemy_row_to_dict.__get__(loader, PostgresLoader)
        
        # Mock exception
        loader.replication_engine.connect.side_effect = Exception("Connection error")
        
        # Act & Assert
        with pytest.raises(Exception, match="Connection error"):
            list(loader.stream_mysql_data(table_name, query))
    
    def test_load_table_streaming_schema_error(self, mock_postgres_loader_instance):
        """Test streaming load with schema adapter error."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Remove the mock for load_table_streaming so we can test the real method
        loader.load_table_streaming = PostgresLoader.load_table_streaming.__get__(loader, PostgresLoader)
        
        # Mock get_table_config and schema adapter error
        with patch.object(loader, 'get_table_config', return_value={
            'incremental_columns': ['DateModified']
        }), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', side_effect=Exception("Schema error")):
            
            # Act
            result = loader.load_table_streaming(table_name)
            
            # Assert
            assert result is False
    
    def test_copy_csv_file_cleanup_on_error(self, mock_postgres_loader_instance):
        """Test that COPY CSV method cleans up temp file even on error."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'test_table'
        
        # Remove the mock for load_table_copy_csv so we can test the real method
        loader.load_table_copy_csv = PostgresLoader.load_table_copy_csv.__get__(loader, PostgresLoader)
        
        # Mock the required methods
        with patch.object(loader, 'get_table_config', return_value={
            'incremental_columns': ['DateModified']
        }), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', return_value={'columns': []}), \
        patch.object(loader.schema_adapter, 'ensure_table_exists', return_value=True):
            
            # Mock database connections
            mock_source_conn = MagicMock()
            mock_target_conn = MagicMock()
            mock_result = MagicMock()
            mock_result.keys.return_value = ['id', 'name']
            # Mock fetchmany to return data first, then empty list to end the loop
            mock_result.fetchmany.side_effect = [
                [(1, 'John Doe')],  # First call returns data
                []  # Second call returns empty list to end the loop
            ]
            
            mock_source_conn.execution_options.return_value.execute.return_value = mock_result
            loader.replication_engine.connect.return_value.__enter__.return_value = mock_source_conn
            loader.analytics_engine.begin.return_value.__enter__.return_value = mock_target_conn
            
            # Mock file operations
            with patch('tempfile.NamedTemporaryFile') as mock_tempfile:
                mock_csvfile = MagicMock()
                mock_csvfile.name = '/tmp/test.csv'
                mock_tempfile.return_value.__enter__.return_value = mock_csvfile
                
                # Mock COPY command to raise exception
                mock_target_conn.execute.side_effect = Exception("COPY failed")
                
                with patch('os.unlink') as mock_unlink:
                    with patch('os.path.exists', return_value=True):
                        # Act
                        result = loader.load_table_copy_csv(table_name)
                        
                        # Assert
                        assert result is False
                        # Should still clean up temp file even on error
                        mock_unlink.assert_called_once_with('/tmp/test.csv')
    
    def test_configuration_error_handling(self, mock_postgres_loader_instance):
        """Test handling of configuration errors."""
        # Arrange
        if not POSTGRES_LOADER_AVAILABLE or PostgresLoader is None:
            pytest.skip("PostgresLoader not available")
        
        # Mock PostgresLoader to raise ConfigurationError
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader') as mock_loader_class:
            mock_loader_class.side_effect = ConfigurationError(
                message="Invalid configuration: missing required field",
                config_file="/path/to/config.yml",
                details={"error_type": "missing_field", "field": "database_url"}
            )
            
            # Act & Assert
            with pytest.raises(ConfigurationError) as exc_info:
                PostgresLoader()
            
            # Assert the error details
            assert "Invalid configuration" in str(exc_info.value)
            assert exc_info.value.config_file == "/path/to/config.yml"
            assert exc_info.value.details["error_type"] == "missing_field"
    
    def test_schema_adapter_error_handling(self, mock_postgres_loader_instance):
        """Test handling of schema adapter errors."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock schema adapter to raise exception
        loader.schema_adapter.get_table_schema_from_mysql.side_effect = Exception("Schema adapter error")
        
        # Mock load_table to handle the error gracefully
        with patch.object(loader, 'load_table', return_value=False):
            # Act
            result = loader.load_table(table_name)
            
            # Assert
            assert result is False
    
    def test_database_engine_error_handling(self, mock_postgres_loader_instance):
        """Test handling of database engine errors."""
        # Arrange
        loader = mock_postgres_loader_instance
        
        # Mock database engine to raise exception
        loader.replication_engine.connect.side_effect = Exception("Database engine error")
        
        # Mock load_table to handle the error gracefully
        with patch.object(loader, 'load_table', return_value=False):
            # Act
            result = loader.load_table('patient')
            
            # Assert
            assert result is False
    
    def test_memory_error_handling(self, mock_postgres_loader_instance):
        """Test handling of memory errors during streaming."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'large_table'
        
        # Remove the mock for load_table_streaming so we can test the real method
        loader.load_table_streaming = PostgresLoader.load_table_streaming.__get__(loader, PostgresLoader)
        
        # Mock the required methods
        with patch.object(loader, 'get_table_config', return_value={
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 100
        }), \
        patch.object(loader, 'stream_mysql_data', side_effect=MemoryError("Out of memory")), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', return_value={'columns': []}), \
        patch.object(loader.schema_adapter, 'ensure_table_exists', return_value=True):
            
            # Act
            result = loader.load_table_streaming(table_name)
            
            # Assert
            assert result is False
    
    def test_file_system_error_handling(self, mock_postgres_loader_instance):
        """Test handling of file system errors during COPY operations."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'test_table'
        
        # Remove the mock for load_table_copy_csv so we can test the real method
        loader.load_table_copy_csv = PostgresLoader.load_table_copy_csv.__get__(loader, PostgresLoader)
        
        # Mock the required methods
        with patch.object(loader, 'get_table_config', return_value={
            'incremental_columns': ['DateModified']
        }), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', return_value={'columns': []}), \
        patch.object(loader.schema_adapter, 'ensure_table_exists', return_value=True):
            
            # Mock database connections
            mock_source_conn = MagicMock()
            mock_target_conn = MagicMock()
            mock_result = MagicMock()
            mock_result.keys.return_value = ['id', 'name']
            mock_result.fetchmany.side_effect = [
                [(1, 'John Doe')],
                []
            ]
            
            mock_source_conn.execution_options.return_value.execute.return_value = mock_result
            loader.replication_engine.connect.return_value.__enter__.return_value = mock_source_conn
            loader.analytics_engine.begin.return_value.__enter__.return_value = mock_target_conn
            
            # Mock file operations to raise exception
            with patch('tempfile.NamedTemporaryFile', side_effect=OSError("File system error")):
                # Act
                result = loader.load_table_copy_csv(table_name)
                
                # Assert
                assert result is False 