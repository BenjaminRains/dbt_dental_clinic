"""
Unit tests for PostgresLoader tracking functionality.

This module contains unit tests for the new ETL tracking features added to PostgresLoader.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from etl_pipeline.loaders.postgres_loader import PostgresLoader

class TestPostgresLoaderTracking:
    
    def test_ensure_tracking_record_exists_creates_new_record(self):
        """Test that _ensure_tracking_record_exists creates new record when none exists."""
        # Arrange
        loader = PostgresLoader()
        
        # Act
        result = loader._ensure_tracking_record_exists('test_table')
        
        # Assert
        assert result is True
        
    def test_update_load_status_upserts_correctly(self):
        """Test that _update_load_status creates/updates records correctly."""
        # Arrange
        loader = PostgresLoader()
        
        # Act
        result = loader._update_load_status('test_table', 1000)
        
        # Assert
        assert result is True
        
    def test_build_improved_load_query_uses_or_logic(self):
        """Test that improved load query uses OR logic by default."""
        # Arrange
        loader = PostgresLoader()
        incremental_columns = ['created_date', 'updated_date']
        
        # Act
        query = loader._build_improved_load_query('test_table', incremental_columns, force_full=False)
        
        # Assert
        assert ' OR ' in query
        assert 'created_date >' in query
        assert 'updated_date >' in query
    
    def test_filter_valid_incremental_columns_filters_bad_dates(self):
        """Test that _filter_valid_incremental_columns filters out columns with bad dates."""
        # Arrange
        loader = PostgresLoader()
        columns = ['good_date', 'bad_date']
        
        # Mock the database query to return bad date range
        with patch.object(loader, 'replication_engine') as mock_engine:
            mock_conn = Mock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            
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
    
    def test_get_last_load_time_returns_correct_timestamp(self):
        """Test that _get_last_load_time returns the correct timestamp."""
        # Arrange
        loader = PostgresLoader()
        expected_timestamp = datetime(2024, 1, 1, 10, 0, 0)
        
        with patch.object(loader, 'analytics_engine') as mock_engine:
            mock_conn = Mock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.return_value.scalar.return_value = expected_timestamp
            
            # Act
            result = loader._get_last_load_time('test_table')
            
            # Assert
            assert result == expected_timestamp 