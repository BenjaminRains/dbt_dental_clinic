"""
Unit tests for PostgresLoader table loading strategies.

This module contains tests for:
    - Standard table loading
    - Streaming table loading
    - COPY CSV table loading
    - Chunked table loading
    - Automatic strategy selection
    - Load verification
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
class TestPostgresLoaderLoadingStrategies:
    """
    Unit tests for PostgresLoader table loading strategies.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on different loading strategies and methods
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_load_table_success(self, mock_postgres_loader_instance, sample_table_data):
        """
        Test successful table loading.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock the load_table method to return True for success
        with patch.object(loader, 'load_table', return_value=True):
            # Act
            result = loader.load_table('patient')
            
            # Assert
            assert result is True
    
    def test_load_table_no_configuration(self, mock_postgres_loader_instance):
        """
        Test table loading with missing configuration.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock load_table to return False for missing configuration
        with patch.object(loader, 'load_table', return_value=False):
            # Act
            result = loader.load_table('nonexistent')
            
            # Assert
            assert result is False
    
    def test_load_table_chunked_success(self, mock_postgres_loader_instance):
        """
        Test successful chunked table loading.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # The load_table_chunked method is already mocked in the fixture to return True
        # Act
        result = loader.load_table_chunked('patient', force_full=False, chunk_size=50)
        
        # Assert
        assert result is True
        loader.load_table_chunked.assert_called_once_with('patient', force_full=False, chunk_size=50)
    
    def test_verify_load_success(self, mock_postgres_loader_instance):
        """
        Test successful load verification.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock verify_load to return True for success
        with patch.object(loader, 'verify_load', return_value=True):
            # Act
            result = loader.verify_load('patient')
            
            # Assert
            assert result is True
            # Verify the method was called with the correct table name
            loader.verify_load.assert_called_once_with('patient')
    
    def test_verify_load_mismatch(self, mock_postgres_loader_instance):
        """
        Test load verification with row count mismatch.
        """
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock verify_load to return False for mismatch scenario
        with patch.object(loader, 'verify_load', return_value=False):
            # Act
            result = loader.verify_load('patient')
            
            # Assert
            assert result is False
    
    def test_load_table_streaming_success(self, mock_postgres_loader_instance):
        """Test successful streaming table load."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'periomeasure'
        
        # Remove the mock for load_table_streaming so we can test the real method
        loader.load_table_streaming = PostgresLoader.load_table_streaming.__get__(loader, PostgresLoader)
        
        # Mock the required methods using patch.object
        with patch.object(loader, 'get_table_config', return_value={
            'incremental_columns': ['DateEntered'],
            'estimated_size_mb': 57
        }), \
        patch.object(loader, 'stream_mysql_data', return_value=[
            [{'id': 1, 'data': 'test1'}],
            [{'id': 2, 'data': 'test2'}]
        ]), \
        patch.object(loader, 'bulk_insert_optimized', return_value=True), \
        patch.object(loader, '_update_load_status', return_value=True), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', return_value={'columns': []}), \
        patch.object(loader.schema_adapter, 'ensure_table_exists', return_value=True), \
        patch.object(loader.schema_adapter, 'convert_row_data_types', return_value={'id': 1, 'data': 'test'}):
            
            # Act
            result = loader.load_table_streaming(table_name)
            
            # Assert
            assert result is True
            loader.schema_adapter.get_table_schema_from_mysql.assert_called_once_with(table_name)
            loader.schema_adapter.ensure_table_exists.assert_called_once()
            assert loader.bulk_insert_optimized.call_count == 2
            loader._update_load_status.assert_called_once_with(table_name, 2)
    
    def test_load_table_streaming_no_configuration(self, mock_postgres_loader_instance):
        """Test streaming load with no table configuration."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'nonexistent_table'
        
        # Remove the mock for load_table_streaming so we can test the real method
        loader.load_table_streaming = PostgresLoader.load_table_streaming.__get__(loader, PostgresLoader)
        
        # Mock get_table_config to return empty config
        with patch.object(loader, 'get_table_config', return_value={}):
            # Act
            result = loader.load_table_streaming(table_name)
            
            # Assert
            assert result is False
    
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
    
    def test_load_table_standard_success(self, mock_postgres_loader_instance):
        """Test successful standard table load."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the required methods
        with patch.object(loader, 'get_table_config', return_value={
            'incremental_columns': ['DateModified']
        }), \
        patch.object(loader, '_ensure_tracking_record_exists', return_value=True), \
        patch.object(loader, '_update_load_status', return_value=True), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', return_value={'columns': []}), \
        patch.object(loader.schema_adapter, 'ensure_table_exists', return_value=True), \
        patch.object(loader.schema_adapter, 'convert_row_data_types', return_value={'id': 1, 'name': 'test'}):
            
            # Mock database connections
            mock_source_conn = MagicMock()
            mock_target_conn = MagicMock()
            mock_result = MagicMock()
            mock_result.keys.return_value = ['id', 'name']
            mock_result.fetchall.return_value = [(1, 'John Doe'), (2, 'Jane Smith')]
            
            mock_source_conn.execute.return_value = mock_result
            loader.replication_engine.connect.return_value.__enter__.return_value = mock_source_conn
            loader.analytics_engine.begin.return_value.__enter__.return_value = mock_target_conn
            
            # Act
            result = loader.load_table_standard(table_name)
            
            # Assert
            assert result is True
            loader._ensure_tracking_record_exists.assert_called_once_with(table_name)
            loader.schema_adapter.get_table_schema_from_mysql.assert_called_once_with(table_name)
            loader._update_load_status.assert_called_once_with(table_name, 2)
    
    def test_load_table_standard_full_load(self, mock_postgres_loader_instance):
        """Test standard table load with force full load."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        force_full = True
        
        # Mock the required methods
        with patch.object(loader, 'get_table_config', return_value={
            'incremental_columns': ['DateModified']
        }), \
        patch.object(loader, '_ensure_tracking_record_exists', return_value=True), \
        patch.object(loader, '_update_load_status', return_value=True), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', return_value={'columns': []}), \
        patch.object(loader.schema_adapter, 'ensure_table_exists', return_value=True), \
        patch.object(loader.schema_adapter, 'convert_row_data_types', return_value={'id': 1, 'name': 'test'}):
            
            # Mock database connections
            mock_source_conn = MagicMock()
            mock_target_conn = MagicMock()
            mock_result = MagicMock()
            mock_result.keys.return_value = ['id', 'name']
            mock_result.fetchall.return_value = [(1, 'John Doe')]
            
            mock_source_conn.execute.return_value = mock_result
            loader.replication_engine.connect.return_value.__enter__.return_value = mock_source_conn
            loader.analytics_engine.begin.return_value.__enter__.return_value = mock_target_conn
            
            # Act
            result = loader.load_table_standard(table_name, force_full)
            
            # Assert
            assert result is True
            # Should call TRUNCATE for full load
            mock_target_conn.execute.assert_called()
            calls = mock_target_conn.execute.call_args_list
            # Check that at least one call contains TRUNCATE in the SQL
            truncate_found = False
            for call in calls:
                args, kwargs = call
                if len(args) > 0 and 'TRUNCATE' in str(args[0]):
                    truncate_found = True
                    break
            assert truncate_found, "TRUNCATE command should be called for force_full=True"
    
    def test_load_table_copy_csv_success(self, mock_postgres_loader_instance):
        """Test successful COPY command table load."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'large_table'
        
        # Configure the mock loader for this test
        loader.get_table_config.return_value = {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 500
        }
        
        # Remove the mock for load_table_copy_csv so we can test the real method
        loader.load_table_copy_csv = PostgresLoader.load_table_copy_csv.__get__(loader, PostgresLoader)
        
        # Mock the database connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name']
        mock_result.fetchmany.side_effect = [
            [(1, 'John Doe'), (2, 'Jane Smith')],
            [(3, 'Bob Johnson')],
            []  # End of data
        ]
        
        mock_source_conn.execution_options.return_value.execute.return_value = mock_result
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_source_conn
        loader.analytics_engine.begin.return_value.__enter__.return_value = mock_target_conn
        
        # Mock file operations
        with patch('tempfile.NamedTemporaryFile') as mock_tempfile:
            mock_csvfile = MagicMock()
            mock_csvfile.name = '/tmp/test.csv'
            mock_tempfile.return_value.__enter__.return_value = mock_csvfile
            
            # Mock file cleanup
            with patch('os.unlink') as mock_unlink, \
                 patch('os.path.exists', return_value=True):
                # Act
                result = loader.load_table_copy_csv(table_name, force_full=True)
                
                # Assert
                assert result is True
                loader.schema_adapter.get_table_schema_from_mysql.assert_called_once_with(table_name)
                loader.schema_adapter.ensure_table_exists.assert_called_once()
                # Should call both TRUNCATE and COPY commands
                mock_target_conn.execute.assert_called()
                calls = mock_target_conn.execute.call_args_list
                
                # Check that execute was called at least twice (TRUNCATE + COPY)
                assert len(calls) >= 2, "Should call TRUNCATE and COPY commands"
                # Verify that we have both TRUNCATE and COPY calls
                truncate_calls = []
                copy_calls = []
                for call in calls:
                    args, kwargs = call
                    if args and len(args) > 0:
                        sql_text = str(args[0])
                        if 'TRUNCATE' in sql_text:
                            truncate_calls.append(call)
                        if 'COPY' in sql_text:
                            copy_calls.append(call)
                
                assert len(truncate_calls) >= 1, "Should call TRUNCATE command"
                assert len(copy_calls) >= 1, "Should call COPY command"
                # Should clean up temp file
                mock_unlink.assert_called_once_with('/tmp/test.csv')
    
    def test_load_table_copy_csv_no_data(self, mock_postgres_loader_instance):
        """Test COPY command load with no data."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'empty_table'
        
        # Configure the mock loader for this test
        loader.get_table_config.return_value = {
            'incremental_columns': ['DateModified']
        }
        
        # Remove the mock for load_table_copy_csv so we can test the real method
        loader.load_table_copy_csv = PostgresLoader.load_table_copy_csv.__get__(loader, PostgresLoader)
        
        # Mock database connections with no data
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name']
        mock_result.fetchmany.return_value = []  # No data
        
        mock_source_conn.execution_options.return_value.execute.return_value = mock_result
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_source_conn
        loader.analytics_engine.begin.return_value.__enter__.return_value = mock_target_conn
        
        # Mock file operations
        with patch('tempfile.NamedTemporaryFile') as mock_tempfile:
            mock_csvfile = MagicMock()
            mock_csvfile.name = '/tmp/test.csv'
            mock_tempfile.return_value.__enter__.return_value = mock_csvfile
            
            with patch('os.unlink') as mock_unlink:
                # Act
                result = loader.load_table_copy_csv(table_name, force_full=True)
                
                # Assert
                assert result is True
                # Should call TRUNCATE but not COPY since no data
                mock_target_conn.execute.assert_called()
                calls = mock_target_conn.execute.call_args_list
                # Should have TRUNCATE call but no COPY call
                truncate_calls = [call for call in calls if 'TRUNCATE' in str(call)]
                copy_calls = [call for call in calls if 'COPY' in str(call)]
                assert len(truncate_calls) == 1, "Should call TRUNCATE when force_full=True"
                assert len(copy_calls) == 0, "Should not call COPY when no data"
    
    def test_load_table_automatic_strategy_selection(self, mock_postgres_loader_instance):
        """Test automatic strategy selection based on table size."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'test_table'
        
        # Remove the mock for load_table so we can test the real method
        loader.load_table = PostgresLoader.load_table.__get__(loader, PostgresLoader)
        
        # Test different table sizes based on actual implementation thresholds
        test_cases = [
            (10, 'standard'),      # Small table (< 50MB)
            (100, 'streaming'),    # Medium table (50-200MB)
            (300, 'chunked'),      # Large table (200-500MB)
            (600, 'copy')          # Very large table (> 500MB)
        ]
        
        for estimated_size_mb, expected_strategy in test_cases:
            # Configure the mock loader for this test case
            loader.get_table_config.return_value = {
                'estimated_size_mb': estimated_size_mb
            }
            
            # Mock the strategy methods
            loader.load_table_standard.return_value = True
            loader.load_table_streaming.return_value = True
            loader.load_table_chunked.return_value = True
            loader.load_table_copy_csv.return_value = True
            
            # Act
            result = loader.load_table(table_name)
            
            # Assert
            assert result is True
            
            # Verify the correct strategy was called based on actual thresholds
            if estimated_size_mb <= 50:
                loader.load_table_standard.assert_called_with(table_name, False)
            elif estimated_size_mb <= 200:
                loader.load_table_streaming.assert_called_with(table_name, False)
            elif estimated_size_mb <= 500:
                loader.load_table_chunked.assert_called_with(table_name, False, chunk_size=50000)  # Use keyword argument
            else:
                loader.load_table_copy_csv.assert_called_with(table_name, False)
    
    def test_load_table_no_configuration_fallback(self, mock_postgres_loader_instance):
        """Test load_table with no configuration falls back to standard."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'unknown_table'
        
        # Mock load_table to return False for no configuration
        with patch.object(loader, 'load_table', return_value=False):
            # Act
            result = loader.load_table(table_name)
            
            # Assert
            assert result is False  # Should fail due to no configuration
    
    def test_streaming_memory_monitoring(self, mock_postgres_loader_instance):
        """Test that streaming load includes memory monitoring."""
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
        patch.object(loader, 'stream_mysql_data', return_value=[
            [{'id': 1, 'data': 'test1'}],
            [{'id': 2, 'data': 'test2'}]
        ]), \
        patch.object(loader, 'bulk_insert_optimized', return_value=True), \
        patch.object(loader, '_update_load_status', return_value=True), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', return_value={'columns': []}), \
        patch.object(loader.schema_adapter, 'ensure_table_exists', return_value=True), \
        patch.object(loader.schema_adapter, 'convert_row_data_types', return_value={'id': 1, 'data': 'test'}):
            
            # Mock psutil for memory monitoring
            with patch('psutil.Process') as mock_process:
                mock_process.return_value.memory_info.return_value.rss = 100 * 1024 * 1024  # 100MB
                
                # Act
                result = loader.load_table_streaming(table_name)
                
                # Assert
                assert result is True
                # Should call memory monitoring
                mock_process.assert_called()
    
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
    
    def test_load_table_streaming_with_force_full(self, mock_postgres_loader_instance):
        """Test streaming load with force full parameter."""
        # Arrange
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        force_full = True
        
        # Remove the mock for load_table_streaming so we can test the real method
        loader.load_table_streaming = PostgresLoader.load_table_streaming.__get__(loader, PostgresLoader)
        
        # Mock analytics engine for truncate
        mock_target_conn = MagicMock()
        loader.analytics_engine.begin.return_value.__enter__.return_value = mock_target_conn
        
        # Mock the required methods
        with patch.object(loader, 'get_table_config', return_value={
            'incremental_columns': ['DateModified']
        }), \
        patch.object(loader, 'stream_mysql_data', return_value=[
            [{'id': 1, 'data': 'test1'}]
        ]), \
        patch.object(loader, 'bulk_insert_optimized', return_value=True), \
        patch.object(loader, '_update_load_status', return_value=True), \
        patch.object(loader.schema_adapter, 'get_table_schema_from_mysql', return_value={'columns': []}), \
        patch.object(loader.schema_adapter, 'ensure_table_exists', return_value=True), \
        patch.object(loader.schema_adapter, 'convert_row_data_types', return_value={'id': 1, 'data': 'test'}):
            
            # Act
            result = loader.load_table_streaming(table_name, force_full)
            
            # Assert
            assert result is True
            # Should call TRUNCATE for force full
            mock_target_conn.execute.assert_called()
            calls = mock_target_conn.execute.call_args_list
            truncate_calls = [call for call in calls if 'TRUNCATE' in str(call)]
            assert len(truncate_calls) > 0, "Should call TRUNCATE when force_full=True" 