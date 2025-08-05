"""
Unit tests for PostgresLoader advanced loading functionality.

This module contains tests for:
    - Parallel loading methods
    - Data validation methods
    - Advanced loading strategies
    - Data completeness validation
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
class TestPostgresLoaderAdvancedLoading:
    """
    Unit tests for PostgresLoader advanced loading functionality.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on advanced loading strategies and data validation
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_load_table_parallel_success(self, mock_postgres_loader_instance):
        """Test successful parallel table loading."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'large_table'
        force_full = False
        
        # Mock the replication engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 10000  # Total rows
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock the analytics engine connection
        mock_analytics_conn = MagicMock()
        mock_analytics_conn.execute.return_value.rowcount = 1  # Insert successful
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_analytics_conn
        
        # Act
        success, details = loader.load_table_parallel(table_name, force_full)
        
        # Assert
        assert success is True
        assert isinstance(details, dict)
        mock_conn.execute.assert_called()
        mock_analytics_conn.execute.assert_called()
    
    def test_load_table_parallel_force_full(self, mock_postgres_loader_instance):
        """Test parallel table loading with force full load."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'large_table'
        force_full = True
        
        # Mock the replication engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 10000  # Total rows
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock the analytics engine connection
        mock_analytics_conn = MagicMock()
        mock_analytics_conn.execute.return_value.rowcount = 1  # Insert successful
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_analytics_conn
        
        # Act
        success, details = loader.load_table_parallel(table_name, force_full)
        
        # Assert
        assert success is True
        assert isinstance(details, dict)
        mock_conn.execute.assert_called()
        mock_analytics_conn.execute.assert_called()
    
    def test_process_chunk_success(self, mock_postgres_loader_instance):
        """Test successful chunk processing."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        chunk_start = 0
        chunk_end = 1000
        chunk_id = 1
        
        # Mock the replication engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [{'id': i} for i in range(1000)]
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock the analytics engine connection
        mock_analytics_conn = MagicMock()
        mock_analytics_conn.execute.return_value.rowcount = 1000  # Insert successful
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_analytics_conn
        
        # Act
        success, rows_processed = loader.process_chunk(chunk_start, chunk_end, chunk_id)
        
        # Assert
        assert success is True
        assert rows_processed == 1000
        mock_conn.execute.assert_called()
        mock_analytics_conn.execute.assert_called()
    
    def test_process_chunk_empty_result(self, mock_postgres_loader_instance):
        """Test chunk processing with empty result."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        chunk_start = 0
        chunk_end = 1000
        chunk_id = 1
        
        # Mock the replication engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []  # Empty result
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        success, rows_processed = loader.process_chunk(chunk_start, chunk_end, chunk_id)
        
        # Assert
        assert success is True
        assert rows_processed == 0
        mock_conn.execute.assert_called()
    
    def test_validate_data_completeness_success(self, mock_postgres_loader_instance):
        """Test successful data completeness validation."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        expected_count = 1000
        actual_count = 1000
        
        # Act
        result = loader._validate_data_completeness(table_name, expected_count, actual_count)
        
        # Assert
        assert result is True
    
    def test_validate_data_completeness_mismatch(self, mock_postgres_loader_instance):
        """Test data completeness validation with count mismatch."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        expected_count = 1000
        actual_count = 950  # Mismatch
        
        # Act
        result = loader._validate_data_completeness(table_name, expected_count, actual_count)
        
        # Assert
        assert result is False
    
    def test_validate_data_completeness_within_tolerance(self, mock_postgres_loader_instance):
        """Test data completeness validation within tolerance."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        expected_count = 1000
        actual_count = 995  # Within tolerance
        
        # Act
        result = loader._validate_data_completeness(table_name, expected_count, actual_count)
        
        # Assert
        assert result is True
    
    def test_load_table_parallel_with_error_handling(self, mock_postgres_loader_instance):
        """Test parallel table loading with error handling."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'large_table'
        force_full = False
        
        # Mock the replication engine connection to raise an exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database connection error")
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act & Assert
        with pytest.raises(Exception, match="Database connection error"):
            loader.load_table_parallel(table_name, force_full)
    
    def test_process_chunk_with_database_error(self, mock_postgres_loader_instance):
        """Test chunk processing with database error."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        chunk_start = 0
        chunk_end = 1000
        chunk_id = 1
        
        # Mock the replication engine connection to raise an exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Query execution error")
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act & Assert
        with pytest.raises(Exception, match="Query execution error"):
            loader.process_chunk(chunk_start, chunk_end, chunk_id)
    
    def test_load_table_parallel_with_large_dataset(self, mock_postgres_loader_instance):
        """Test parallel table loading with large dataset."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'large_table'
        force_full = False
        
        # Mock the replication engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 1000000  # 1 million rows
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock the analytics engine connection
        mock_analytics_conn = MagicMock()
        mock_analytics_conn.execute.return_value.rowcount = 1  # Insert successful
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_analytics_conn
        
        # Act
        success, details = loader.load_table_parallel(table_name, force_full)
        
        # Assert
        assert success is True
        assert isinstance(details, dict)
        # Should handle large datasets efficiently
        mock_conn.execute.assert_called()
        mock_analytics_conn.execute.assert_called()
    
    def test_process_chunk_with_memory_optimization(self, mock_postgres_loader_instance):
        """Test chunk processing with memory optimization."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        chunk_start = 0
        chunk_end = 50000  # Large chunk
        chunk_id = 1
        
        # Mock the replication engine connection
        mock_conn = MagicMock()
        # Return a large dataset that should trigger memory optimization
        mock_conn.execute.return_value.fetchall.return_value = [{'id': i, 'data': 'x' * 1000} for i in range(50000)]
        loader.replication_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock the analytics engine connection
        mock_analytics_conn = MagicMock()
        mock_analytics_conn.execute.return_value.rowcount = 50000  # Insert successful
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_analytics_conn
        
        # Act
        success, rows_processed = loader.process_chunk(chunk_start, chunk_end, chunk_id)
        
        # Assert
        assert success is True
        assert rows_processed == 50000
        mock_conn.execute.assert_called()
        mock_analytics_conn.execute.assert_called()
    
    def test_validate_data_completeness_with_zero_counts(self, mock_postgres_loader_instance):
        """Test data completeness validation with zero counts."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'empty_table'
        expected_count = 0
        actual_count = 0
        
        # Act
        result = loader._validate_data_completeness(table_name, expected_count, actual_count)
        
        # Assert
        assert result is True  # Zero counts should be considered valid
    
    def test_validate_data_completeness_with_very_large_counts(self, mock_postgres_loader_instance):
        """Test data completeness validation with very large counts."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'large_table'
        expected_count = 1000000  # 1 million
        actual_count = 999995  # Within tolerance
        
        # Act
        result = loader._validate_data_completeness(table_name, expected_count, actual_count)
        
        # Assert
        assert result is True  # Should be within tolerance even for large counts 