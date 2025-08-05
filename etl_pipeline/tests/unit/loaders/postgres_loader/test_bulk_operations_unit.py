"""
Unit tests for PostgresLoader bulk operations and streaming.

This module contains tests for:
    - Bulk insert optimization
    - Streaming MySQL data
    - Large chunk handling
    - Memory monitoring
    - Empty data handling
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
def real_postgres_loader_instance():
    """Create a PostgresLoader instance with mocked dependencies for testing real methods."""
    if not POSTGRES_LOADER_AVAILABLE:
        pytest.skip("PostgresLoader not available")
    
    # Create a mock PostgresLoader instance that allows testing real methods
    mock_loader = MagicMock()
    
    # Set up basic attributes
    mock_loader.analytics_schema = 'raw'
    mock_loader.settings = MagicMock()
    mock_loader.table_configs = {
        'patient': {'incremental_columns': ['DateModified'], 'primary_key': 'PatientNum'},
        'appointment': {'batch_size': 500, 'incremental_columns': ['DateCreated', 'DateModified'], 'primary_key': 'AptNum'},
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
    
    # Configure get_table_config to return actual table configs
    def get_table_config_side_effect(table_name):
        return mock_loader.table_configs.get(table_name, {})
    
    mock_loader.get_table_config = MagicMock(side_effect=get_table_config_side_effect)
    
    # Mock methods that are called internally but don't need testing
    mock_loader._build_load_query = MagicMock(return_value="SELECT * FROM test_table")
    mock_loader._update_load_status = MagicMock(return_value=True)
    mock_loader._get_last_load_time_max = MagicMock(return_value=datetime(2024, 1, 1, 10, 0, 0))
    mock_loader._ensure_tracking_record_exists = MagicMock(return_value=True)
    mock_loader.stream_mysql_data = MagicMock(return_value=[[{'id': 1, 'data': 'test'}]])
    mock_loader.load_table_copy_csv = MagicMock(return_value=True)
    mock_loader.load_table_standard = MagicMock(return_value=True)
    mock_loader.load_table_streaming = MagicMock(return_value=True)
    mock_loader.load_table_chunked = MagicMock(return_value=True)
    
    # Import the real PostgresLoader class to get access to its methods
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    import logging
    
    # Set up logger for the mock loader
    mock_loader.logger = logging.getLogger('etl_pipeline.loaders.postgres_loader')
    
    # Bind real methods to the mock loader for testing
    mock_loader._filter_valid_incremental_columns = PostgresLoader._filter_valid_incremental_columns.__get__(mock_loader, PostgresLoader)
    mock_loader._build_upsert_sql = PostgresLoader._build_upsert_sql.__get__(mock_loader, PostgresLoader)
    mock_loader._validate_incremental_integrity = PostgresLoader._validate_incremental_integrity.__get__(mock_loader, PostgresLoader)
    mock_loader.bulk_insert_optimized = PostgresLoader.bulk_insert_optimized.__get__(mock_loader, PostgresLoader)
    mock_loader._convert_sqlalchemy_row_to_dict = PostgresLoader._convert_sqlalchemy_row_to_dict.__get__(mock_loader, PostgresLoader)
    
    return mock_loader


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
class TestPostgresLoaderBulkOperations:
    """
    Unit tests for PostgresLoader bulk operations and streaming.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on bulk insert and streaming operations
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_bulk_insert_optimized_success(self, real_postgres_loader_instance):
        """Test successful bulk insert with optimized chunking."""
        # Arrange
        loader = real_postgres_loader_instance
        table_name = 'patient'
        rows_data = [
            {'id': 1, 'name': 'John Doe', 'DateModified': '2024-01-01'},
            {'id': 2, 'name': 'Jane Smith', 'DateModified': '2024-01-02'},
            {'id': 3, 'name': 'Bob Johnson', 'DateModified': '2024-01-03'}
        ]
        chunk_size = 2
        
        # Mock the analytics engine connection with proper context manager
        mock_conn = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        loader.analytics_engine.begin.return_value = mock_context
        
        # Act
        result = loader.bulk_insert_optimized(table_name, rows_data, chunk_size)
        
        # Assert
        assert result is True
        # Should be called twice (2 chunks of 2 rows each, with 3 total rows)
        assert mock_conn.execute.call_count == 2
        
        # Verify the SQL was built correctly
        calls = mock_conn.execute.call_args_list
        for call in calls:
            args, kwargs = call
            sql = args[0].text
            assert 'INSERT INTO raw.patient' in sql
            assert '"id"' in sql
            assert '"name"' in sql
            assert '"DateModified"' in sql
    
    def test_bulk_insert_optimized_empty_data(self, real_postgres_loader_instance):
        """Test bulk insert with empty data."""
        # Arrange
        loader = real_postgres_loader_instance
        table_name = 'patient'
        rows_data = []
        
        # Act
        result = loader.bulk_insert_optimized(table_name, rows_data)
        
        # Assert
        assert result is True
        # Should not call execute since no data
        loader.analytics_engine.begin.assert_not_called()
    
    def test_bulk_insert_optimized_exception_handling(self, real_postgres_loader_instance):
        """Test bulk insert with database exception."""
        # Arrange
        loader = real_postgres_loader_instance
        table_name = 'patient'
        rows_data = [{'id': 1, 'name': 'John Doe'}]
        
        # Mock exception
        loader.analytics_engine.begin.side_effect = Exception("Database error")
        
        # Act
        result = loader.bulk_insert_optimized(table_name, rows_data)
        
        # Assert
        assert result is False
    
    def test_stream_mysql_data_success(self, mock_postgres_loader_instance):
        """Test successful streaming of MySQL data."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        query = "SELECT * FROM patient"
        chunk_size = 2
        
        # Import PostgresLoader for method binding
        from etl_pipeline.loaders.postgres_loader import PostgresLoader
        
        # Remove the mock for stream_mysql_data so we can test the real method
        loader.stream_mysql_data = PostgresLoader.stream_mysql_data.__get__(loader, PostgresLoader)
        
        # Bind the real _convert_sqlalchemy_row_to_dict method for testing
        loader._convert_sqlalchemy_row_to_dict = PostgresLoader._convert_sqlalchemy_row_to_dict.__get__(loader, PostgresLoader)
        
        # Mock the replication engine connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name', 'DateModified']
        mock_result.fetchmany.side_effect = [
            [(1, 'John Doe', '2024-01-01'), (2, 'Jane Smith', '2024-01-02')],
            [(3, 'Bob Johnson', '2024-01-03')],
            []  # End of data
        ]
        mock_conn.execution_options.return_value.execute.return_value = mock_result
        
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        chunks = list(loader.stream_mysql_data(table_name, query, chunk_size))
        
        # Assert
        assert len(chunks) == 2
        assert len(chunks[0]) == 2  # First chunk
        assert len(chunks[1]) == 1  # Second chunk
        
        # Verify data structure
        assert chunks[0][0]['id'] == 1
        assert chunks[0][0]['name'] == 'John Doe'
        assert chunks[1][0]['id'] == 3
    
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
    
    def test_bulk_insert_optimized_large_chunks(self, real_postgres_loader_instance):
        """Test bulk insert with large chunk sizes."""
        # Arrange
        loader = real_postgres_loader_instance
        table_name = 'large_table'
        rows_data = [{'id': i, 'data': f'test{i}'} for i in range(100000)]  # 100k rows
        chunk_size = 50000
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        loader.analytics_engine.begin.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader.bulk_insert_optimized(table_name, rows_data, chunk_size)
        
        # Assert
        assert result is True
        # Should be called twice (2 chunks of 50k rows each)
        assert mock_conn.execute.call_count == 2
        
        # Verify chunk sizes - check the actual data passed to execute
        calls = mock_conn.execute.call_args_list
        for call in calls:
            args, kwargs = call
            # The data is passed as the second argument to execute
            if len(args) > 1:
                data = args[1]  # The actual data passed to execute
                assert len(data) == 50000  # Should have 50k rows in each chunk
    
    def test_stream_mysql_data_empty_result(self, mock_postgres_loader_instance):
        """Test streaming with empty result set."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'empty_table'
        query = "SELECT * FROM empty_table"
        
        # Remove the mock for stream_mysql_data so we can test the real method
        loader.stream_mysql_data = PostgresLoader.stream_mysql_data.__get__(loader, PostgresLoader)
        
        # Mock the replication engine connection and result
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name']
        mock_result.fetchmany.return_value = []  # Empty result
        
        mock_conn.execution_options.return_value.execute.return_value = mock_result
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        chunks = list(loader.stream_mysql_data(table_name, query))
        
        # Assert
        assert len(chunks) == 0  # No chunks should be yielded 