"""
Unit tests for SimpleMySQLReplicator tracking functionality.

This module contains unit tests for the new ETL tracking features added to SimpleMySQLReplicator.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator

class TestSimpleMySQLReplicatorTracking:
    
    def test_ensure_tracking_tables_exist_creates_tables(self):
        """Test that _ensure_tracking_tables_exist creates tracking tables."""
        # Arrange
        replicator = SimpleMySQLReplicator()
        
        # Act
        result = replicator._ensure_tracking_tables_exist()
        
        # Assert
        assert result is True
        
    def test_update_copy_status_upserts_correctly(self):
        """Test that _update_copy_status creates/updates records correctly."""
        # Arrange
        replicator = SimpleMySQLReplicator()
        
        # Act
        result = replicator._update_copy_status('test_table', 1000)
        
        # Assert
        assert result is True
        
    def test_get_last_copy_time_returns_correct_timestamp(self):
        """Test that _get_last_copy_time returns the correct timestamp."""
        # Arrange
        replicator = SimpleMySQLReplicator()
        expected_timestamp = datetime(2024, 1, 1, 10, 0, 0)
        
        with patch.object(replicator, 'target_engine') as mock_engine:
            mock_conn = Mock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.return_value.scalar.return_value = expected_timestamp
            
            # Act
            result = replicator._get_last_copy_time('test_table')
            
            # Assert
            assert result == expected_timestamp
    
    def test_copy_table_updates_tracking_on_success(self):
        """Test that copy_table updates tracking when successful."""
        # Arrange
        replicator = SimpleMySQLReplicator()
        
        with patch.object(replicator, '_copy_full_table') as mock_copy:
            mock_copy.return_value = (True, 500)  # Success, 500 rows copied
            
            # Act
            result = replicator.copy_table('test_table')
            
            # Assert
            assert result is True
            mock_copy.assert_called_once()
    
    def test_copy_table_updates_tracking_on_failure(self):
        """Test that copy_table updates tracking when failed."""
        # Arrange
        replicator = SimpleMySQLReplicator()
        
        with patch.object(replicator, '_copy_full_table') as mock_copy:
            mock_copy.return_value = (False, 0)  # Failure, 0 rows copied
            
            # Act
            result = replicator.copy_table('test_table')
            
            # Assert
            assert result is False
            mock_copy.assert_called_once() 