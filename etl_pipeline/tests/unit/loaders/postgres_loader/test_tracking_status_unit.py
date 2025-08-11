"""
Unit tests for PostgresLoader tracking and status functionality.

This module contains tests for:
    - Tracking record management
    - Load status updates
    - Tracking table validation
    - Performance metrics tracking
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
class TestPostgresLoaderTrackingStatus:
    """
    Unit tests for PostgresLoader tracking and status functionality.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on tracking record management and status updates
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_ensure_tracking_record_exists_success(self, mock_postgres_loader_instance):
        """Test successful creation of tracking record."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.rowcount = 0  # No existing record
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._ensure_tracking_record_exists(table_name)
        
        # Assert
        assert result is True
        mock_conn.execute.assert_called()
    
    def test_ensure_tracking_record_exists_already_exists(self, mock_postgres_loader_instance):
        """Test tracking record that already exists."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.rowcount = 1  # Record already exists
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._ensure_tracking_record_exists(table_name)
        
        # Assert
        assert result is True
        mock_conn.execute.assert_called()
    
    def test_update_load_status_success(self, mock_postgres_loader_instance):
        """Test successful load status update."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        rows_loaded = 1000
        load_start = datetime(2024, 1, 1, 10, 0, 0)
        load_end = datetime(2024, 1, 1, 10, 5, 0)
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.rowcount = 1  # Update successful
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._update_load_status(table_name, rows_loaded, load_start, load_end)
        
        # Assert
        assert result is True
        mock_conn.execute.assert_called()
    
    def test_update_load_status_failure(self, mock_postgres_loader_instance):
        """Test load status update failure."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        rows_loaded = 1000
        load_start = datetime(2024, 1, 1, 10, 0, 0)
        load_end = datetime(2024, 1, 1, 10, 5, 0)
        
        # Mock the analytics engine connection to fail
        mock_conn = MagicMock()
        mock_conn.execute.return_value.rowcount = 0  # Update failed
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._update_load_status(table_name, rows_loaded, load_start, load_end)
        
        # Assert
        assert result is False
        mock_conn.execute.assert_called()
    
    def test_validate_tracking_tables_exist_success(self, mock_postgres_loader_instance):
        """Test successful validation of tracking tables."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = ('etl_load_status',)  # Table exists
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._validate_tracking_tables_exist()
        
        # Assert
        assert result is True
        mock_conn.execute.assert_called()
    
    def test_validate_tracking_tables_exist_failure(self, mock_postgres_loader_instance):
        """Test validation failure when tracking tables don't exist."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None  # Table doesn't exist
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._validate_tracking_tables_exist()
        
        # Assert
        assert result is False
        mock_conn.execute.assert_called()
    
    def test_track_performance_metrics_success(self, mock_postgres_loader_instance):
        """Test successful performance metrics tracking."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        strategy = 'streaming'
        duration = 5.5
        memory_mb = 150.0
        rows_processed = 10000
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.rowcount = 1  # Insert successful
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        loader.track_performance_metrics(table_name, strategy, duration, memory_mb, rows_processed)
        
        # Assert
        mock_conn.execute.assert_called()
    
    def test_get_performance_report_success(self, mock_postgres_loader_instance):
        """Test successful performance report generation."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ('patient', 'streaming', 5.5, 150.0, 10000, datetime(2024, 1, 1, 10, 0, 0)),
            ('appointment', 'standard', 3.2, 75.0, 5000, datetime(2024, 1, 1, 11, 0, 0))
        ]
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader.get_performance_report()
        
        # Assert
        assert isinstance(result, str)
        assert 'patient' in result
        assert 'appointment' in result
        assert 'streaming' in result
        assert 'standard' in result
        mock_conn.execute.assert_called()
    
    def test_get_performance_report_empty(self, mock_postgres_loader_instance):
        """Test performance report generation with no data."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []  # No performance data
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader.get_performance_report()
        
        # Assert
        assert isinstance(result, str)
        assert 'No performance data available' in result or 'No records found' in result
        mock_conn.execute.assert_called()
    
    def test_tracking_record_creation_with_error_handling(self, mock_postgres_loader_instance):
        """Test tracking record creation with database error handling."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection to raise an exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database connection error")
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act & Assert
        with pytest.raises(Exception, match="Database connection error"):
            loader._ensure_tracking_record_exists(table_name)
    
    def test_load_status_update_with_transaction_rollback(self, mock_postgres_loader_instance):
        """Test load status update with transaction rollback on error."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        rows_loaded = 1000
        load_start = datetime(2024, 1, 1, 10, 0, 0)
        load_end = datetime(2024, 1, 1, 10, 5, 0)
        
        # Mock the analytics engine connection with transaction context
        mock_conn = MagicMock()
        mock_transaction = MagicMock()
        mock_conn.begin.return_value = mock_transaction
        mock_conn.execute.side_effect = Exception("Update failed")
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act & Assert
        with pytest.raises(Exception, match="Update failed"):
            loader._update_load_status(table_name, rows_loaded, load_start, load_end)
        
        # Verify transaction rollback was called
        mock_transaction.rollback.assert_called()
    
    def test_performance_metrics_with_large_dataset(self, mock_postgres_loader_instance):
        """Test performance metrics tracking for large datasets."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'large_table'
        strategy = 'chunked'
        duration = 120.5  # 2 minutes
        memory_mb = 2048.0  # 2GB
        rows_processed = 1000000  # 1 million rows
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.rowcount = 1  # Insert successful
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        loader.track_performance_metrics(table_name, strategy, duration, memory_mb, rows_processed)
        
        # Assert
        mock_conn.execute.assert_called()
        # Verify the call contains the large dataset parameters
        call_args = mock_conn.execute.call_args[0][0]
        assert 'large_table' in call_args
        assert 'chunked' in call_args 