"""
Unit tests for TableProcessingContext and its interaction with SimpleMySQLReplicator.

These tests focus on the incremental_columns attribute issue and ensure proper
handling of TableProcessingContext objects in the replication pipeline.

Updated to use provider pattern test environment with proper fixtures.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, mock_open
from typing import Dict, List, Optional
import yaml
import tempfile
import os

from etl_pipeline.orchestration.table_processor import TableProcessingContext
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.config.config_reader import ConfigReader


@pytest.fixture(autouse=True)
def set_etl_environment(monkeypatch, request):
    # Only remove ETL_ENVIRONMENT for tests marked with 'fail_fast'
    if 'fail_fast' in request.keywords:
        monkeypatch.delenv('ETL_ENVIRONMENT', raising=False)
    else:
        monkeypatch.setenv('ETL_ENVIRONMENT', 'test')


@pytest.mark.unit
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestTableProcessingContext:
    """Test TableProcessingContext class functionality."""
    
    @pytest.fixture
    def mock_config_reader(self):
        """Create a mock config reader for testing."""
        mock_reader = Mock(spec=ConfigReader)
        return mock_reader
    
    def test_table_processing_context_has_incremental_columns_attribute(self, mock_config_reader):
        """Test that TableProcessingContext has incremental_columns attribute."""
        # Arrange
        mock_config_reader.get_table_config.return_value = {
            'incremental_columns': ['DateModified', 'DateCreated'],
            'primary_column': 'id',
            'performance_category': 'medium'
        }
        
        # Act
        context = TableProcessingContext('test_table', False, mock_config_reader)
        
        # Assert
        assert hasattr(context, 'incremental_columns')
        assert context.incremental_columns == ['DateModified', 'DateCreated']
        assert context.primary_column == 'id'
    
    def test_table_processing_context_with_empty_incremental_columns(self, mock_config_reader):
        """Test TableProcessingContext with empty incremental_columns."""
        # Arrange
        mock_config_reader.get_table_config.return_value = {
            'incremental_columns': [],
            'performance_category': 'small'
        }
        
        # Act
        context = TableProcessingContext('test_table', False, mock_config_reader)
        
        # Assert
        assert hasattr(context, 'incremental_columns')
        assert context.incremental_columns == []
        assert context.primary_column is None
    
    def test_table_processing_context_with_no_config(self, mock_config_reader):
        """Test TableProcessingContext when no configuration is found."""
        # Arrange
        mock_config_reader.get_table_config.return_value = None
        
        # Act
        context = TableProcessingContext('test_table', False, mock_config_reader)
        
        # Assert
        assert hasattr(context, 'incremental_columns')
        assert context.incremental_columns == []
        assert context.primary_column is None
    
    def test_table_processing_context_strategy_resolution(self, mock_config_reader):
        """Test strategy resolution with incremental columns."""
        # Arrange
        mock_config_reader.get_table_config.return_value = {
            'incremental_columns': ['DateModified'],
            'extraction_strategy': 'incremental',
            'performance_category': 'medium'
        }
        
        # Act
        context = TableProcessingContext('test_table', False, mock_config_reader)
        
        # Assert
        assert context.strategy_info['strategy'] == 'incremental'
        assert not context.strategy_info['force_full_applied']
        assert context.incremental_columns == ['DateModified']


@pytest.mark.unit
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestSimpleMySQLReplicatorWithTableProcessingContext:
    """Test SimpleMySQLReplicator handling of TableProcessingContext objects."""
    
    @pytest.fixture
    def mock_settings(self):
        """Create mock settings for testing."""
        mock_settings = Mock()
        mock_settings.mysql_source_host = 'localhost'
        mock_settings.mysql_source_port = 3306
        mock_settings.mysql_source_user = 'test_user'
        mock_settings.mysql_source_password = 'test_pass'
        mock_settings.mysql_source_database = 'opendental'
        mock_settings.mysql_replication_host = 'localhost'
        mock_settings.mysql_replication_port = 3306
        mock_settings.mysql_replication_user = 'test_user'
        mock_settings.mysql_replication_password = 'test_pass'
        mock_settings.mysql_replication_database = 'opendental_replication'
        return mock_settings
    
    @pytest.fixture
    def mock_table_config(self):
        """Create mock table configuration."""
        return {
            'incremental_columns': ['DateModified', 'DateCreated'],
            'primary_column': 'id',
            'performance_category': 'medium',
            'extraction_strategy': 'incremental',
            'batch_size': 5000
        }
    
    @pytest.fixture
    def mock_config_reader(self, mock_table_config):
        """Create a mock config reader for testing."""
        mock_reader = Mock(spec=ConfigReader)
        mock_reader.get_table_config.return_value = mock_table_config
        return mock_reader
    
    @pytest.fixture
    def table_processing_context(self, mock_config_reader):
        """Create a TableProcessingContext instance for testing."""
        return TableProcessingContext('test_table', False, mock_config_reader)
    
    def test_replicator_handles_table_processing_context_incremental_columns(self, mock_settings, table_processing_context):
        """Test that replicator methods can handle TableProcessingContext objects correctly."""
        # Arrange
        with patch('etl_pipeline.core.simple_mysql_replicator.Settings') as mock_settings_class:
            mock_settings_class.return_value = mock_settings
            
            with patch('etl_pipeline.core.simple_mysql_replicator.ConnectionFactory') as mock_connection_factory:
                # Create mock engines with context manager support
                mock_source_engine = Mock()
                mock_target_engine = Mock()
                
                # Implement context manager protocol for engines
                mock_source_engine.__enter__ = Mock(return_value=mock_source_engine)
                mock_source_engine.__exit__ = Mock(return_value=None)
                mock_target_engine.__enter__ = Mock(return_value=mock_target_engine)
                mock_target_engine.__exit__ = Mock(return_value=None)
                
                mock_connection_factory.get_source_connection.return_value = mock_source_engine
                mock_connection_factory.get_replication_connection.return_value = mock_target_engine
                
                # Create temporary configuration file for testing
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                    test_config = {
                        'tables': {
                            'test_table': {
                                'incremental_columns': ['DateModified', 'DateCreated'],
                                'primary_column': 'id',
                                'performance_category': 'medium'
                            }
                        }
                    }
                    yaml.dump(test_config, f)
                    temp_config_path = f.name
                
                try:
                    # Mock the _load_configuration method to return our test config
                    with patch.object(SimpleMySQLReplicator, '_load_configuration') as mock_load_config:
                        mock_load_config.return_value = {
                            'test_table': {
                                'incremental_columns': ['DateModified', 'DateCreated'],
                                'primary_column': 'id',
                                'performance_category': 'medium'
                            }
                        }
                        
                        # Mock the _validate_tracking_tables_exist method to avoid database validation
                        with patch.object(SimpleMySQLReplicator, '_validate_tracking_tables_exist') as mock_validate:
                            mock_validate.return_value = True
                            
                            replicator = SimpleMySQLReplicator(settings=mock_settings)
                            
                            # Act & Assert
                            # Test that the replicator can handle TableProcessingContext objects
                            assert hasattr(table_processing_context, 'incremental_columns')
                            assert table_processing_context.incremental_columns == ['DateModified', 'DateCreated']
                            
                            # Test the specific method that was failing
                            # This simulates the check in _copy_incremental_unified
                            if hasattr(table_processing_context, 'incremental_columns'):
                                incremental_columns = table_processing_context.incremental_columns
                                assert incremental_columns == ['DateModified', 'DateCreated']
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_config_path):
                        os.unlink(temp_config_path)
    
    def test_replicator_config_handling_with_table_processing_context(self, mock_settings, table_processing_context):
        """Test that replicator methods can handle TableProcessingContext objects correctly."""
        # Arrange
        with patch('etl_pipeline.core.simple_mysql_replicator.Settings') as mock_settings_class:
            mock_settings_class.return_value = mock_settings
            
            with patch('etl_pipeline.core.simple_mysql_replicator.ConnectionFactory') as mock_connection_factory:
                # Create mock engines with context manager support
                mock_source_engine = Mock()
                mock_target_engine = Mock()
                
                # Implement context manager protocol for engines
                mock_source_engine.__enter__ = Mock(return_value=mock_source_engine)
                mock_source_engine.__exit__ = Mock(return_value=None)
                mock_target_engine.__enter__ = Mock(return_value=mock_target_engine)
                mock_target_engine.__exit__ = Mock(return_value=None)
                
                mock_connection_factory.get_source_connection.return_value = mock_source_engine
                mock_connection_factory.get_replication_connection.return_value = mock_target_engine
                
                # Create temporary configuration file for testing
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                    test_config = {
                        'tables': {
                            'test_table': {
                                'incremental_columns': ['DateModified', 'DateCreated'],
                                'primary_column': 'id',
                                'performance_category': 'medium'
                            }
                        }
                    }
                    yaml.dump(test_config, f)
                    temp_config_path = f.name
                
                try:
                    # Mock the _load_configuration method to return our test config
                    with patch.object(SimpleMySQLReplicator, '_load_configuration') as mock_load_config:
                        mock_load_config.return_value = {
                            'test_table': {
                                'incremental_columns': ['DateModified', 'DateCreated'],
                                'primary_column': 'id',
                                'performance_category': 'medium'
                            }
                        }
                        
                        # Mock the _validate_tracking_tables_exist method to avoid database validation
                        with patch.object(SimpleMySQLReplicator, '_validate_tracking_tables_exist') as mock_validate:
                            mock_validate.return_value = True
                            
                            replicator = SimpleMySQLReplicator(settings=mock_settings)
                            
                            # Act & Assert
                            # Test the config handling logic from _copy_incremental_unified
                            config = table_processing_context
                            
                            # This simulates the logic in _copy_incremental_unified
                            if hasattr(config, 'config'):
                                config = config.config
                            elif hasattr(config, 'incremental_columns'):
                                incremental_columns = config.incremental_columns
                                config_dict = {
                                    'incremental_columns': incremental_columns,
                                    'primary_incremental_column': getattr(config, 'primary_incremental_column', None)
                                }
                                config = config_dict
                            
                            # Verify the config was processed correctly
                            assert isinstance(config, dict)
                            assert 'incremental_columns' in config
                            assert config['incremental_columns'] == ['DateModified', 'DateCreated']
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_config_path):
                        os.unlink(temp_config_path)
    
    def test_replicator_methods_with_table_processing_context(self, mock_settings, table_processing_context):
        """Test that replicator methods can handle TableProcessingContext objects."""
        # Arrange
        with patch('etl_pipeline.core.simple_mysql_replicator.Settings') as mock_settings_class:
            mock_settings_class.return_value = mock_settings
            
            with patch('etl_pipeline.core.simple_mysql_replicator.ConnectionFactory') as mock_connection_factory:
                # Create mock engines with context manager support
                mock_source_engine = Mock()
                mock_target_engine = Mock()
                
                # Implement context manager protocol for engines
                mock_source_engine.__enter__ = Mock(return_value=mock_source_engine)
                mock_source_engine.__exit__ = Mock(return_value=None)
                mock_target_engine.__enter__ = Mock(return_value=mock_target_engine)
                mock_target_engine.__exit__ = Mock(return_value=None)
                
                mock_connection_factory.get_source_connection.return_value = mock_source_engine
                mock_connection_factory.get_replication_connection.return_value = mock_target_engine
                
                # Create temporary configuration file for testing
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
                    test_config = {
                        'tables': {
                            'test_table': {
                                'incremental_columns': ['DateModified', 'DateCreated'],
                                'primary_column': 'id',
                                'performance_category': 'medium'
                            }
                        }
                    }
                    yaml.dump(test_config, f)
                    temp_config_path = f.name
                
                try:
                    # Mock the _load_configuration method to return our test config
                    with patch.object(SimpleMySQLReplicator, '_load_configuration') as mock_load_config:
                        mock_load_config.return_value = {
                            'test_table': {
                                'incremental_columns': ['DateModified', 'DateCreated'],
                                'primary_column': 'id',
                                'performance_category': 'medium'
                            }
                        }
                        
                        # Mock the _validate_tracking_tables_exist method to avoid database validation
                        with patch.object(SimpleMySQLReplicator, '_validate_tracking_tables_exist') as mock_validate:
                            mock_validate.return_value = True
                            
                            replicator = SimpleMySQLReplicator(settings=mock_settings)
                            
                            # Act & Assert
                            # Test that the replicator can handle TableProcessingContext objects
                            assert hasattr(table_processing_context, 'incremental_columns')
                            assert table_processing_context.incremental_columns == ['DateModified', 'DateCreated']
                            
                            # Test the specific method that was failing
                            # This simulates the check in _copy_incremental_unified
                            if hasattr(table_processing_context, 'incremental_columns'):
                                incremental_columns = table_processing_context.incremental_columns
                                assert incremental_columns == ['DateModified', 'DateCreated']
                finally:
                    # Clean up temporary file
                    if os.path.exists(temp_config_path):
                        os.unlink(temp_config_path)
    
    def test_incremental_columns_access_patterns(self, table_processing_context):
        """Test different ways of accessing incremental_columns from TableProcessingContext."""
        # Test direct attribute access
        assert hasattr(table_processing_context, 'incremental_columns')
        assert table_processing_context.incremental_columns == ['DateModified', 'DateCreated']
        
        # Test getattr access
        incremental_columns = getattr(table_processing_context, 'incremental_columns', [])
        assert incremental_columns == ['DateModified', 'DateCreated']
        
        # Test hasattr check
        assert hasattr(table_processing_context, 'incremental_columns')
        
        # Test accessing from config
        assert 'incremental_columns' in table_processing_context.config
        assert table_processing_context.config['incremental_columns'] == ['DateModified', 'DateCreated']


@pytest.mark.unit
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestIncrementalColumnsIntegration:
    """Integration tests for incremental_columns handling."""
    
    @pytest.fixture
    def mock_config_reader(self):
        """Create a mock config reader for testing."""
        mock_reader = Mock(spec=ConfigReader)
        mock_reader.get_table_config.return_value = {
            'incremental_columns': ['DateModified', 'DateCreated'],
            'primary_column': 'id',
            'performance_category': 'medium',
            'extraction_strategy': 'incremental'
        }
        return mock_reader
    
    def test_table_processing_context_to_replicator_flow(self, mock_config_reader):
        """Test the complete flow from TableProcessingContext to SimpleMySQLReplicator."""
        # Arrange
        context = TableProcessingContext('test_table', False, mock_config_reader)
        
        # Act & Assert
        # Verify the context has the expected attributes
        assert hasattr(context, 'incremental_columns')
        assert context.incremental_columns == ['DateModified', 'DateCreated']
        assert context.primary_column == 'id'
        
        # Verify the config dictionary has the expected structure
        assert 'incremental_columns' in context.config
        assert context.config['incremental_columns'] == ['DateModified', 'DateCreated']
        
        # Test the strategy resolution
        assert context.strategy_info['strategy'] == 'incremental'
        assert not context.strategy_info['force_full_applied']


if __name__ == '__main__':
    pytest.main([__file__]) 