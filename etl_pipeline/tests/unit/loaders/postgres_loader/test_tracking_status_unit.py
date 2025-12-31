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
    mock_loader.target_schema = 'raw'  # Used by tracking methods
    mock_loader._get_loaded_at_time_max = MagicMock(return_value=datetime(2024, 1, 1, 10, 0, 0))
    mock_loader.load_table = MagicMock(return_value=(True, {}))
    
    # Import the real PostgresLoader class to bind real methods for testing
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    import logging
    
    # Set up logger for the mock loader
    mock_loader.logger = logging.getLogger('etl_pipeline.loaders.postgres_loader')
    
    # Bind real tracking methods that exist in the new architecture
    mock_loader._ensure_tracking_record_exists = PostgresLoader._ensure_tracking_record_exists.__get__(mock_loader, PostgresLoader)
    mock_loader._update_load_status = PostgresLoader._update_load_status.__get__(mock_loader, PostgresLoader)
    
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
        # First call returns 0 (no existing record), second call is the INSERT
        mock_conn.execute.return_value.scalar.return_value = 0  # No existing record
        mock_conn.execute.return_value.rowcount = 1  # INSERT successful
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        loader.analytics_engine.connect.return_value = mock_context
        
        # Act
        result = loader._ensure_tracking_record_exists(table_name)
        
        # Assert
        assert result is True
        assert mock_conn.execute.call_count >= 1  # At least SELECT and possibly INSERT
    
    def test_ensure_tracking_record_exists_already_exists(self, mock_postgres_loader_instance):
        """Test tracking record that already exists."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        # Record already exists (count > 0)
        mock_conn.execute.return_value.scalar.return_value = 1  # Record already exists
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        loader.analytics_engine.connect.return_value = mock_context
        
        # Act
        result = loader._ensure_tracking_record_exists(table_name)
        
        # Assert
        assert result is True
        assert mock_conn.execute.call_count >= 1  # At least the SELECT
    
    def test_update_load_status_success(self, mock_postgres_loader_instance):
        """Test successful load status update."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        rows_loaded = 1000
        status = 'success'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.rowcount = 1  # Update successful
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        loader.analytics_engine.connect.return_value = mock_context
        
        # Act - signature: (table_name, rows_loaded, status)
        result = loader._update_load_status(table_name, rows_loaded, status)
        
        # Assert
        assert result is True
        assert mock_conn.execute.call_count >= 1  # Should call execute
        mock_conn.commit.assert_called()  # Method calls commit() explicitly
    
    def test_update_load_status_failure(self, mock_postgres_loader_instance):
        """Test load status update failure."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        rows_loaded = 1000
        status = 'success'
        
        # Mock the analytics engine connection to raise exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database error")
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        loader.analytics_engine.connect.return_value = mock_context
        
        # Act - signature: (table_name, rows_loaded, status)
        result = loader._update_load_status(table_name, rows_loaded, status)
        
        # Assert - method catches exception and returns False
        assert result is False
    
    @pytest.mark.skip(reason="_validate_tracking_tables_exist is not part of the new architecture - validation handled internally")
    def test_validate_tracking_tables_exist_success(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test successful validation of tracking tables.
        
        Tracking table validation is now handled internally.
        """
        pytest.skip("_validate_tracking_tables_exist is not part of the new architecture")
    
    @pytest.mark.skip(reason="_validate_tracking_tables_exist is not part of the new architecture - validation handled internally")
    def test_validate_tracking_tables_exist_failure(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test validation failure when tracking tables don't exist.
        
        Tracking table validation is now handled internally.
        """
        pytest.skip("_validate_tracking_tables_exist is not part of the new architecture")
    
    @pytest.mark.skip(reason="track_performance_metrics is not part of the new architecture - metrics handled internally")
    def test_track_performance_metrics_success(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test successful performance metrics tracking.
        
        Performance metrics tracking is now handled internally.
        """
        pytest.skip("track_performance_metrics is not part of the new architecture")
    
    @pytest.mark.skip(reason="get_performance_report is not part of the new architecture - reporting handled internally")
    def test_get_performance_report_success(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test successful performance report generation.
        
        Performance reporting is now handled internally.
        """
        pytest.skip("get_performance_report is not part of the new architecture")
    
    @pytest.mark.skip(reason="get_performance_report is not part of the new architecture - reporting handled internally")
    def test_get_performance_report_empty(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test performance report generation with no data.
        
        Performance reporting is now handled internally.
        """
        pytest.skip("get_performance_report is not part of the new architecture")
    
    def test_tracking_record_creation_with_error_handling(self, mock_postgres_loader_instance):
        """Test tracking record creation with database error handling."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection to raise an exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database connection error")
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        loader.analytics_engine.connect.return_value = mock_context
        
        # Act & Assert - method should catch exception and return False
        result = loader._ensure_tracking_record_exists(table_name)
        assert result is False
    
    def test_load_status_update_with_transaction_rollback(self, mock_postgres_loader_instance):
        """Test load status update with error handling."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        rows_loaded = 1000
        status = 'success'
        
        # Mock the analytics engine connection to raise exception
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Update failed")
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        loader.analytics_engine.connect.return_value = mock_context
        
        # Act & Assert - method should catch exception and return False
        result = loader._update_load_status(table_name, rows_loaded, status)
        assert result is False
    
    @pytest.mark.skip(reason="track_performance_metrics is not part of the new architecture - metrics handled internally")
    def test_performance_metrics_with_large_dataset(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test performance metrics tracking for large datasets.
        
        Performance metrics tracking is now handled internally.
        """
        pytest.skip("track_performance_metrics is not part of the new architecture")
        assert 'large_table' in call_args
        assert 'chunked' in call_args 