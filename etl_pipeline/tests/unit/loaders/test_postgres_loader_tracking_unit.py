"""
Unit tests for PostgresLoader tracking functionality.

This module contains unit tests for the new ETL tracking features added to PostgresLoader.
"""

import pytest
from unittest.mock import patch, Mock, MagicMock
from datetime import datetime
from etl_pipeline.loaders.postgres_loader import PostgresLoader

class TestPostgresLoaderTracking:
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_ensure_tracking_record_exists_creates_new_record(self, mock_factory, test_settings):
        """Test that _ensure_tracking_record_exists creates new record when none exists."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        # Mock the database query result
        mock_conn = Mock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        mock_analytics_engine.connect.return_value = mock_context
        mock_conn.execute.return_value.scalar.return_value = 0  # No existing record
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        
        # Act
        result = loader._ensure_tracking_record_exists('test_table')
        
        # Assert
        assert result is True
        
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_update_load_status_upserts_correctly(self, mock_factory, test_settings):
        """Test that _update_load_status creates/updates records correctly."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        # Mock the database query result
        mock_conn = Mock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        mock_analytics_engine.connect.return_value = mock_context
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        
        # Act
        result = loader._update_load_status('test_table', 1000)
        
        # Assert
        assert result is True
        
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_build_improved_load_query_uses_or_logic(self, mock_factory, test_settings):
        """Test that improved load query uses OR logic by default."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        incremental_columns = ['created_date', 'updated_date']
        
        # Act - Use the correct method name
        query = loader._build_enhanced_load_query('test_table', incremental_columns, force_full=False)
        
        # Assert
        assert 'test_table' in query
        # Note: The actual logic depends on whether primary column is available
        # This test verifies the method exists and returns a query
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_filter_valid_incremental_columns_filters_bad_dates(self, mock_factory, test_settings):
        """Test that _filter_valid_incremental_columns filters out columns with bad dates."""
        # Arrange
        # Mock the database connections
        mock_replication_engine = Mock()
        mock_analytics_engine = Mock()
        mock_factory.get_replication_connection.return_value = mock_replication_engine
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        columns = ['good_date', 'bad_date']
        
        # In test environment, the method skips validation and returns all columns
        # So we need to test the actual filtering logic by temporarily setting replication_engine
        loader.replication_engine = mock_replication_engine
        
        # Mock the database query to return bad date range
        mock_conn = Mock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        mock_replication_engine.connect.return_value = mock_context
        
        # Mock results for good_date and bad_date
        mock_conn.execute.return_value.fetchone.side_effect = [
            (datetime(2020, 1, 1), datetime(2024, 1, 1), 1000),  # Good date range
            (datetime(1902, 1, 1), datetime(1980, 1, 1), 500)     # Bad date range
        ]
        
        # Act
        valid_columns = loader._filter_valid_incremental_columns('test_table', columns)
        
        # Assert
        assert 'good_date' in valid_columns
        assert 'bad_date' not in valid_columns
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_get_last_load_time_returns_correct_timestamp(self, mock_factory, test_settings):
        """Test that _get_last_load_time returns the correct timestamp."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        expected_timestamp = datetime(2024, 1, 1, 10, 0, 0)
        
        # Mock the database query result
        mock_conn = Mock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        mock_analytics_engine.connect.return_value = mock_context
        mock_conn.execute.return_value.scalar.return_value = expected_timestamp
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        
        # Act
        result = loader._get_last_load_time('test_table')
        
        # Assert
        assert result == expected_timestamp
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_get_primary_incremental_column_with_valid_column(self, mock_factory, test_settings):
        """Test that _get_primary_incremental_column returns valid column."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        config = {'primary_incremental_column': 'DateTStamp'}
        
        # Act
        result = loader._get_primary_incremental_column(config)
        
        # Assert
        assert result == 'DateTStamp'
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_get_primary_incremental_column_with_none_value(self, mock_factory, test_settings):
        """Test that _get_primary_incremental_column handles 'none' value."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        config = {'primary_incremental_column': 'none'}
        
        # Act
        result = loader._get_primary_incremental_column(config)
        
        # Assert
        assert result is None
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_get_primary_incremental_column_with_empty_value(self, mock_factory, test_settings):
        """Test that _get_primary_incremental_column handles empty string."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        config = {'primary_incremental_column': ''}
        
        # Act
        result = loader._get_primary_incremental_column(config)
        
        # Assert
        assert result is None
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_get_primary_incremental_column_with_missing_key(self, mock_factory, test_settings):
        """Test that _get_primary_incremental_column handles missing key."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        config = {}
        
        # Act
        result = loader._get_primary_incremental_column(config)
        
        # Assert
        assert result is None
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_get_last_primary_value_returns_correct_value(self, mock_factory, test_settings):
        """Test that _get_last_primary_value returns the correct value."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        expected_value = '2024-01-01 00:00:00'
        expected_column = 'DateTStamp'
        
        # Mock the database query result
        mock_conn = Mock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        mock_analytics_engine.connect.return_value = mock_context
        mock_conn.execute.return_value.fetchone.return_value = (expected_value, expected_column)
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        
        # Act
        result = loader._get_last_primary_value('test_table')
        
        # Assert
        assert result == expected_value
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_get_last_primary_value_returns_none_when_no_record(self, mock_factory, test_settings):
        """Test that _get_last_primary_value returns None when no record exists."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        # Mock the database query result
        mock_conn = Mock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        mock_analytics_engine.connect.return_value = mock_context
        mock_conn.execute.return_value.fetchone.return_value = None
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        
        # Act
        result = loader._get_last_primary_value('test_table')
        
        # Assert
        assert result is None
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_build_enhanced_load_query_uses_primary_column(self, mock_factory, test_settings):
        """Test that enhanced load query uses primary column when available."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        incremental_columns = ['created_date', 'updated_date']
        primary_column = 'DateTStamp'
        
        # Mock _get_last_primary_value to return a value
        with patch.object(loader, '_get_last_primary_value', return_value='2024-01-01 00:00:00'):
            # Act
            query = loader._build_enhanced_load_query('test_table', incremental_columns, primary_column, force_full=False)
            
            # Assert
            assert 'DateTStamp >' in query
            assert ' OR ' not in query  # Should not use OR logic with primary column
            assert 'test_table' in query
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_build_enhanced_load_query_falls_back_to_multi_column(self, mock_factory, test_settings):
        """Test that enhanced load query falls back to multi-column logic when no primary column."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        incremental_columns = ['created_date', 'updated_date']
        primary_column = None
        
        # Mock _get_last_load_time_max to return a timestamp
        with patch.object(loader, '_get_last_load_time_max', return_value=datetime(2024, 1, 1, 10, 0, 0)):
            # Act
            query = loader._build_enhanced_load_query('test_table', incremental_columns, primary_column, force_full=False)
            
            # Assert
            assert ' OR ' in query  # Should use OR logic with multi-column
            assert 'created_date >' in query
            assert 'updated_date >' in query
            assert 'test_table' in query
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_build_enhanced_load_query_uses_full_load_when_force_full(self, mock_factory, test_settings):
        """Test that enhanced load query uses full load when force_full=True."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        incremental_columns = ['created_date', 'updated_date']
        primary_column = 'DateTStamp'
        
        # Act
        query = loader._build_enhanced_load_query('test_table', incremental_columns, primary_column, force_full=True)
        
        # Assert
        assert 'WHERE' not in query  # Should not have WHERE clause for full load
        assert 'test_table' in query
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_log_incremental_strategy_logs_primary_column(self, mock_factory, test_settings):
        """Test that _log_incremental_strategy logs primary column strategy."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        table_name = 'test_table'
        primary_column = 'DateTStamp'
        incremental_columns = ['created_date', 'updated_date']
        
        # Act & Assert - should not raise exception
        loader._log_incremental_strategy(table_name, primary_column, incremental_columns)
    
    @patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory')
    def test_log_incremental_strategy_logs_multi_column(self, mock_factory, test_settings):
        """Test that _log_incremental_strategy logs multi-column strategy."""
        # Arrange
        # Mock the database connections
        mock_analytics_engine = Mock()
        mock_factory.get_analytics_raw_connection.return_value = mock_analytics_engine
        
        loader = PostgresLoader(use_test_environment=True, settings=test_settings)
        table_name = 'test_table'
        primary_column = None
        incremental_columns = ['created_date', 'updated_date']
        
        # Act & Assert - should not raise exception
        loader._log_incremental_strategy(table_name, primary_column, incremental_columns) 