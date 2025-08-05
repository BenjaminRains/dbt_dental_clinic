"""
Unit tests for SimpleMySQLReplicator tracking functionality.

This module contains unit tests for the new ETL tracking features added to SimpleMySQLReplicator.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
import yaml
from unittest.mock import mock_open

class TestSimpleMySQLReplicatorTracking:
    
    @pytest.fixture
    def replicator_with_mock_engines(self, test_settings):
        """Create replicator with mocked database engines for testing tracking functionality."""
        # Mock engines
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        # Mock engine.url attribute (Section 3.1 from debugging notes)
        mock_source_engine.url = Mock()
        mock_source_engine.url.database = 'opendental'
        mock_target_engine.url = Mock()
        mock_target_engine.url.database = 'replication'
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_source_connection', return_value=mock_source_engine), \
             patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection', return_value=mock_target_engine):
            
            # Mock configuration with test data
            mock_config = {
                'test_table': {
                    'incremental_columns': ['DateTStamp'],
                    'batch_size': 1000,
                    'estimated_size_mb': 50,
                    'extraction_strategy': 'incremental',
                    'performance_category': 'medium',
                    'processing_priority': 5,
                    'estimated_processing_time_minutes': 0.1,
                    'memory_requirements_mb': 10
                }
            }
            
            # Mock the _load_configuration method at the class level to intercept during __init__
            with patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._load_configuration', return_value=mock_config), \
                 patch('etl_pipeline.core.simple_mysql_replicator.SimpleMySQLReplicator._validate_tracking_tables_exist', return_value=True):
                
                # Create replicator instance
                replicator = SimpleMySQLReplicator(settings=test_settings)
                replicator.source_engine = mock_source_engine
                replicator.target_engine = mock_target_engine
                
                return replicator
    
    def test_validate_tracking_tables_exist_returns_true(self, replicator_with_mock_engines):
        """Test that _validate_tracking_tables_exist returns True when tables exist."""
        # Arrange
        replicator = replicator_with_mock_engines
        
        # Mock the database connection and queries
        mock_conn = Mock()
        
        # First query result (table exists)
        mock_result1 = Mock()
        mock_result1.scalar.return_value = 1  # Table exists
        
        # Second query result (columns exist)
        mock_result2 = Mock()
        mock_result2.scalar.return_value = 2  # Both columns exist
        
        # Mock execute to return different results for each call
        mock_conn.execute.side_effect = [mock_result1, mock_result2]
        
        # Mock context manager for connection
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn
        
        # Act
        result = replicator._validate_tracking_tables_exist()
        
        # Assert
        assert result is True
        assert mock_conn.execute.call_count == 2
        
    def test_update_copy_status_upserts_correctly(self, replicator_with_mock_engines):
        """Test that _update_copy_status creates/updates records correctly."""
        # Arrange
        replicator = replicator_with_mock_engines
        
        # Mock the database connection and execute
        mock_conn = Mock()
        mock_conn.execute.return_value = None
        mock_conn.commit.return_value = None
        
        # Mock context manager for connection
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn
        
        # Act
        result = replicator._update_copy_status('test_table', 1000)
        
        # Assert
        assert result is True
        
    def test_get_last_copy_time_returns_correct_timestamp(self, replicator_with_mock_engines):
        """Test that _get_last_copy_time returns the correct timestamp."""
        # Arrange
        replicator = replicator_with_mock_engines
        expected_timestamp = datetime(2024, 1, 1, 10, 0, 0)
        
        # Mock the database connection and query
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = expected_timestamp
        mock_conn.execute.return_value = mock_result
        
        # Mock context manager for connection (Section 2 from debugging notes)
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        replicator.target_engine.connect.return_value = mock_conn
        
        # Act
        result = replicator._get_last_copy_time('test_table')
        
        # Assert
        assert result == expected_timestamp
    
    def test_copy_table_updates_tracking_on_success(self, replicator_with_mock_engines):
        """Test that copy_table updates tracking when successful."""
        # Arrange
        replicator = replicator_with_mock_engines
        
        # Mock the methods that are called during copy_table execution
        with patch.object(replicator, '_execute_copy_operation', return_value=(True, 500)) as mock_execute, \
             patch.object(replicator, '_update_copy_status', return_value=True) as mock_update, \
             patch.object(replicator, '_get_primary_incremental_column', return_value='DateTStamp'), \
             patch.object(replicator.performance_optimizer, '_track_performance_optimized'), \
             patch.object(replicator, 'get_extraction_strategy', return_value='full_table'), \
             patch.object(replicator, 'get_copy_method', return_value='medium'), \
             patch.object(replicator, '_log_incremental_strategy'), \
             patch.object(replicator.performance_optimizer, 'should_use_full_refresh', return_value=False):
            
            # Act
            result = replicator.copy_table('test_table')
            
            # Assert
            assert result is True
            mock_execute.assert_called_once()
            mock_update.assert_called_once()
    
    def test_copy_table_updates_tracking_on_failure(self, replicator_with_mock_engines):
        """Test that copy_table updates tracking when failed."""
        # Arrange
        replicator = replicator_with_mock_engines
        
        # Mock the methods that are called during copy_table execution
        with patch.object(replicator, '_execute_copy_operation', return_value=(False, 0)) as mock_execute, \
             patch.object(replicator, '_update_copy_status', return_value=True) as mock_update, \
             patch.object(replicator, '_get_primary_incremental_column', return_value='DateTStamp'), \
             patch.object(replicator, 'get_extraction_strategy', return_value='full_table'), \
             patch.object(replicator, 'get_copy_method', return_value='medium'), \
             patch.object(replicator, '_log_incremental_strategy'), \
             patch.object(replicator.performance_optimizer, 'should_use_full_refresh', return_value=False):
            
            # Act
            result = replicator.copy_table('test_table')
            
            # Assert
            assert result is False
            mock_execute.assert_called_once()
            mock_update.assert_called_once() 