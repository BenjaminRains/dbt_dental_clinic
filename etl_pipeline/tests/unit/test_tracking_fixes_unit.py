"""
Unit tests for tracking table fixes.

This module tests the fixes implemented for tracking table issues:
1. Proper primary value retrieval in MySQL replication
2. Error tracking updates in all load methods
3. Tracking validation in test environments
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.loaders.postgres_loader import PostgresLoader
from etl_pipeline.config.settings import Settings


class TestTrackingFixes:
    """Test class for tracking table fixes."""
    
    @pytest.fixture
    def mock_settings(self):
        """Create mock settings for testing."""
        settings = Mock(spec=Settings)
        settings.get_database_config.return_value = {
            'host': 'localhost',
            'port': 3306,
            'database': 'test_db',
            'username': 'test_user',
            'password': 'test_pass'
        }
        return settings
    
    @pytest.fixture
    def mock_replicator(self, mock_settings):
        """Create mock SimpleMySQLReplicator."""
        with patch('etl_pipeline.core.simple_mysql_replicator.ConnectionFactory'):
            replicator = SimpleMySQLReplicator(settings=mock_settings)
            replicator.target_engine = Mock()
            replicator.table_configs = {
                'test_table': {
                    'primary_incremental_column': 'id',
                    'incremental_columns': ['id', 'updated_at']
                }
            }
            return replicator
    
    @pytest.fixture
    def mock_loader(self, mock_settings):
        """Create mock PostgresLoader."""
        with patch('etl_pipeline.loaders.postgres_loader.ConnectionFactory'):
            loader = PostgresLoader(settings=mock_settings, use_test_environment=True)
            loader.analytics_engine = Mock()
            loader.table_configs = {
                'test_table': {
                    'primary_incremental_column': 'id',
                    'incremental_columns': ['id', 'updated_at']
                }
            }
            return loader

    def test_get_max_primary_value_from_copied_data(self, mock_replicator):
        """Test that _get_max_primary_value_from_copied_data works correctly."""
        # Mock the database connection and query result
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.scalar.return_value = "1000"
        mock_conn.execute.return_value = mock_result
        mock_replicator.target_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Test the method
        result = mock_replicator._get_max_primary_value_from_copied_data('test_table', 'id')
        
        # Verify the result
        assert result == "1000"
        mock_conn.execute.assert_called_once()
        
    def test_copy_table_updates_tracking_with_primary_value(self, mock_replicator):
        """Test that copy_table properly updates tracking with primary value."""
        # Mock the copy operation to succeed
        mock_replicator._execute_table_copy = Mock(return_value=(True, 100))
        mock_replicator._get_max_primary_value_from_copied_data = Mock(return_value="1000")
        mock_replicator._update_copy_status = Mock(return_value=True)
        mock_replicator.performance_optimizer = Mock()
        
        # Test the copy operation
        success, metadata = mock_replicator.copy_table('test_table')
        
        # Verify tracking was updated with primary value
        assert success is True
        assert metadata['last_primary_value'] == "1000"
        mock_replicator._update_copy_status.assert_called_once_with(
            'test_table', 100, 'success', "1000", 'id'
        )
    
    def test_load_methods_update_tracking_on_failure(self, mock_loader):
        """Test that load methods update tracking on failure."""
        # Mock the _update_load_status method
        mock_loader._update_load_status = Mock(return_value=True)
        
        # Test streaming load failure
        with patch.object(mock_loader, 'stream_mysql_data', side_effect=Exception("Test error")):
            success, metadata = mock_loader.load_table_streaming('test_table')
            
            # Verify tracking was updated with failure status
            assert success is False
            mock_loader._update_load_status.assert_called_with('test_table', 0, 'failed', None, None)
    
    def test_tracking_validation_in_test_environment(self, mock_loader):
        """Test that tracking validation works in test environment."""
        # Mock the validation method
        mock_loader._validate_tracking_tables_exist = Mock(return_value=True)
        
        # Test that validation is called in test environment
        with patch('etl_pipeline.loaders.postgres_loader.logger') as mock_logger:
            # Re-initialize the loader to trigger validation
            mock_loader.__init__(settings=mock_loader.settings, use_test_environment=True)
            
            # Verify validation was called and logged
            mock_loader._validate_tracking_tables_exist.assert_called_once()
            mock_logger.info.assert_called_with("PostgreSQL tracking tables validated in test environment")
    
    def test_error_tracking_in_all_load_methods(self, mock_loader):
        """Test that all load methods update tracking on error."""
        # Mock the _update_load_status method
        mock_loader._update_load_status = Mock(return_value=True)
        
        # Test each load method with error
        load_methods = [
            'load_table_streaming',
            'load_table_standard', 
            'load_table_chunked',
            'load_table_copy_csv',
            'load_table_parallel'
        ]
        
        for method_name in load_methods:
            method = getattr(mock_loader, method_name)
            
            # Mock the method to raise an exception
            with patch.object(mock_loader, method_name, side_effect=Exception("Test error")):
                try:
                    method('test_table')
                except Exception:
                    pass
                
                # Verify tracking was updated
                mock_loader._update_load_status.assert_called_with('test_table', 0, 'failed', None, None)
                mock_loader._update_load_status.reset_mock()


if __name__ == "__main__":
    pytest.main([__file__]) 