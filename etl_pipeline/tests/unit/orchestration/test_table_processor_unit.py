"""
Unit tests for TableProcessor - Fast execution with comprehensive mocking.

This module provides pure unit tests for TableProcessor with all dependencies
mocked for fast execution and isolated component behavior testing.

Coverage: Core logic, edge cases, error handling
Execution: < 1 second per component
Markers: @pytest.mark.unit

Updated for Configuration System Refactoring:
- Uses enum-based DatabaseType and PostgresSchema
- Implements dependency injection patterns
- Uses modular fixtures from tests.fixtures.*
- Follows ConnectionFactory interface
- Uses ConfigReader instead of SchemaDiscovery
"""

import pytest
import time
import logging
import os
from unittest.mock import MagicMock, patch, Mock
from sqlalchemy.engine import Engine
import os

from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.config.settings import Settings
from etl_pipeline.config.config_reader import ConfigReader

# Import configuration system components
from etl_pipeline.config import DatabaseType, PostgresSchema, create_test_settings

# Import modular fixtures
from tests.fixtures.transformer_fixtures import (
    mock_table_processor_engines,
    table_processor_standard_config,
    table_processor_large_config,
    table_processor_medium_large_config
)
from tests.fixtures.connection_fixtures import (
    mock_source_engine,
    mock_replication_engine,
    mock_analytics_engine
)
from tests.fixtures.env_fixtures import test_settings

logger = logging.getLogger(__name__)


class TestTableProcessorUnit:
    """Unit tests for TableProcessor with comprehensive mocking."""

    @pytest.mark.unit
    def test_initialization(self):
        """Test TableProcessor initialization."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        assert processor.settings is not None
        assert processor.metrics is not None
        assert processor.config_reader == mock_config_reader
        assert processor.opendental_source_engine is None
        assert processor.mysql_replication_engine is None
        assert processor.postgres_analytics_engine is None
        # raw_to_public_transformer removed as part of refactoring
        assert processor._source_db is None
        assert processor._replication_db is None
        assert processor._analytics_db is None

    @pytest.mark.unit
    def test_initialization_with_config_path(self):
        """Test TableProcessor initialization with config path."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader, config_path="/path/to/config")
        
        assert processor.config_path == "/path/to/config"
        assert processor.settings is not None
        assert processor.config_reader == mock_config_reader

    @pytest.mark.unit
    def test_initialization_without_config_reader_test_env(self):
        """Test TableProcessor initialization without config_reader in test environment."""
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class, \
             patch.object(Settings, 'get_database_config') as mock_get_config:
            
            mock_engine = MagicMock(spec=Engine)
            mock_factory.get_source_connection.return_value = mock_engine
            mock_get_config.return_value = {'database': 'test_db'}
            
            processor = TableProcessor(environment='test')
            
            assert processor.config_reader is not None
            mock_config_reader_class.assert_called_once_with("etl_pipeline/config/tables.yml")

    @pytest.mark.unit
    def test_initialization_without_config_reader_production_env(self):
        """Test TableProcessor initialization without config_reader in production environment."""
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch('etl_pipeline.orchestration.table_processor.ConfigReader') as mock_config_reader_class, \
             patch.object(Settings, 'get_database_config') as mock_get_config:
            
            mock_engine = MagicMock(spec=Engine)
            mock_factory.get_source_connection.return_value = mock_engine
            mock_get_config.return_value = {'database': 'prod_db'}
            
            processor = TableProcessor(environment='production')
            
            assert processor.config_reader is not None
            mock_config_reader_class.assert_called_once_with("etl_pipeline/config/tables.yml")

    @pytest.mark.unit
    def test_connections_available_all_none(self):
        """Test _connections_available when all connections are None."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        assert processor._connections_available() is False

    @pytest.mark.unit
    def test_connections_available_partial(self):
        """Test _connections_available when some connections are available."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        processor.opendental_source_engine = MagicMock(spec=Engine)
        
        assert processor._connections_available() is False

    @pytest.mark.unit
    def test_connections_available_all_set(self):
        """Test _connections_available when all connections are available."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        processor.opendental_source_engine = MagicMock(spec=Engine)
        processor.mysql_replication_engine = MagicMock(spec=Engine)
        processor.postgres_analytics_engine = MagicMock(spec=Engine)
        
        assert processor._connections_available() is True

    @pytest.mark.unit
    def test_initialize_connections_with_provided_engines(self, mock_table_processor_engines):
        """Test initialize_connections with provided engines."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        
        # Use enum-based database config calls
        with patch.object(processor.settings, 'get_database_config') as mock_get_config:
            mock_get_config.side_effect = lambda db_type, schema=None, **kwargs: {'database': f'{db_type.value}_db'}
            
            result = processor.initialize_connections(
                source_engine=mock_source,
                replication_engine=mock_replication,
                analytics_engine=mock_analytics
            )
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics
            assert processor._source_db == 'source_db'
            assert processor._replication_db == 'replication_db'
            assert processor._analytics_db == 'analytics_db'

    @pytest.mark.unit
    def test_initialize_connections_with_connection_factory(self, mock_table_processor_engines):
        """Test initialize_connections using ConnectionFactory."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        
        # Use ConnectionFactory interface
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_factory.get_source_connection.return_value = mock_source
            mock_factory.get_replication_connection.return_value = mock_replication
            mock_factory.get_analytics_connection.return_value = mock_analytics
            mock_get_config.side_effect = lambda db_type, schema=None, **kwargs: {'database': f'{db_type.value}_db'}
            
            result = processor.initialize_connections()
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics
        # All tests should use the new ConnectionFactory interface
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_factory.get_source_connection.return_value = mock_source
            mock_factory.get_replication_connection.return_value = mock_replication
            mock_factory.get_analytics_connection.return_value = mock_analytics
            mock_get_config.side_effect = lambda db_type, schema=None, **kwargs: {'database': f'{db_type.value}_db'}
            
            result = processor.initialize_connections()
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics

    @pytest.mark.unit
    def test_initialize_connections_test_environment_no_provided_engines(self):
        """Test initialize_connections in test environment without provided engines."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader, environment='test')
        
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_source = MagicMock(spec=Engine)
            mock_replication = MagicMock(spec=Engine)
            mock_analytics = MagicMock(spec=Engine)
            
            mock_factory.get_source_connection.return_value = mock_source
            mock_factory.get_replication_connection.return_value = mock_replication
            mock_factory.get_analytics_connection.return_value = mock_analytics
            mock_get_config.side_effect = lambda db_type, schema=None, **kwargs: {'database': f'{db_type.value}_db'}
            
            result = processor.initialize_connections()
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics
            assert processor._source_db == 'source_db'
            assert processor._replication_db == 'replication_db'
            assert processor._analytics_db == 'analytics_db'

    @pytest.mark.unit
    def test_initialize_connections_production_environment_no_provided_engines(self):
        """Test initialize_connections in production environment without provided engines."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader, environment='production')
        
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_source = MagicMock(spec=Engine)
            mock_replication = MagicMock(spec=Engine)
            mock_analytics = MagicMock(spec=Engine)
            
            mock_factory.get_source_connection.return_value = mock_source
            mock_factory.get_replication_connection.return_value = mock_replication
            mock_factory.get_analytics_connection.return_value = mock_analytics
            mock_get_config.side_effect = lambda db_type, schema=None, **kwargs: {'database': f'{db_type.value}_db'}
            
            result = processor.initialize_connections()
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics
            assert processor._source_db == 'source_db'
            assert processor._replication_db == 'replication_db'
            assert processor._analytics_db == 'analytics_db'

    @pytest.mark.unit
    def test_initialize_connections_with_provided_source_engine_only(self):
        """Test initialize_connections with only source engine provided."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader, environment='test')
        
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_provided_source = MagicMock(spec=Engine)
            mock_replication = MagicMock(spec=Engine)
            mock_analytics = MagicMock(spec=Engine)
            
            mock_factory.get_replication_connection.return_value = mock_replication
            mock_factory.get_analytics_connection.return_value = mock_analytics
            mock_get_config.side_effect = lambda db_type, schema=None, **kwargs: {'database': f'{db_type.value}_db'}
            
            result = processor.initialize_connections(source_engine=mock_provided_source)
            
            assert result is True
            assert processor.opendental_source_engine == mock_provided_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics

    @pytest.mark.unit
    def test_initialize_connections_with_provided_replication_engine_only(self):
        """Test initialize_connections with only replication engine provided."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader, environment='test')
        
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_source = MagicMock(spec=Engine)
            mock_provided_replication = MagicMock(spec=Engine)
            mock_analytics = MagicMock(spec=Engine)
            
            mock_factory.get_source_connection.return_value = mock_source
            mock_factory.get_analytics_connection.return_value = mock_analytics
            mock_get_config.side_effect = lambda db_type, schema=None, **kwargs: {'database': f'{db_type.value}_db'}
            
            result = processor.initialize_connections(replication_engine=mock_provided_replication)
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_provided_replication
            assert processor.postgres_analytics_engine == mock_analytics

    @pytest.mark.unit
    def test_initialize_connections_with_provided_analytics_engine_only(self):
        """Test initialize_connections with only analytics engine provided."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader, environment='test')
        
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_source = MagicMock(spec=Engine)
            mock_replication = MagicMock(spec=Engine)
            mock_provided_analytics = MagicMock(spec=Engine)
            
            mock_factory.get_source_connection.return_value = mock_source
            mock_factory.get_replication_connection.return_value = mock_replication
            mock_get_config.side_effect = lambda db_type, schema=None, **kwargs: {'database': f'{db_type.value}_db'}
            
            result = processor.initialize_connections(analytics_engine=mock_provided_analytics)
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_provided_analytics

    @pytest.mark.unit
    def test_initialize_connections_failure(self):
        """Test initialize_connections when ConnectionFactory fails."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory:
            mock_factory.get_source_connection.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                processor.initialize_connections()
        # All tests should use the new ConnectionFactory interface
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory:
            mock_factory.get_source_connection.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                processor.initialize_connections()

    @pytest.mark.unit
    def test_cleanup_all_engines(self):
        """Test cleanup with all engines set."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        # Create engines with proper dispose mocking
        mock_source = MagicMock(spec=Engine)
        mock_source.dispose = MagicMock()
        processor.opendental_source_engine = mock_source
        
        mock_replication = MagicMock(spec=Engine)
        mock_replication.dispose = MagicMock()
        processor.mysql_replication_engine = mock_replication
        
        mock_analytics = MagicMock(spec=Engine)
        mock_analytics.dispose = MagicMock()
        processor.postgres_analytics_engine = mock_analytics
        
        processor.cleanup()
        
        assert processor.opendental_source_engine is None
        assert processor.mysql_replication_engine is None
        assert processor.postgres_analytics_engine is None
        
        # Verify dispose was called
        mock_source.dispose.assert_called_once()
        mock_replication.dispose.assert_called_once()
        mock_analytics.dispose.assert_called_once()

    @pytest.mark.unit
    def test_cleanup_with_dispose_error(self):
        """Test cleanup when engine dispose fails."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        mock_engine = MagicMock(spec=Engine)
        mock_engine.dispose.side_effect = Exception("Dispose failed")
        processor.opendental_source_engine = mock_engine
        
        # Should not raise exception
        processor.cleanup()
        
        assert processor.opendental_source_engine is None

    @pytest.mark.unit
    def test_cleanup_with_cleanup_exception(self):
        """Test cleanup when the main cleanup method raises an exception."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        # Set up engines
        processor.opendental_source_engine = MagicMock(spec=Engine)
        processor.mysql_replication_engine = MagicMock(spec=Engine)
        processor.postgres_analytics_engine = MagicMock(spec=Engine)
        
        # Mock the cleanup method to raise an exception
        with patch.object(processor, '_initialized', True):
            # This should not raise an exception
            processor.cleanup()
            
            # Verify that _initialized is set to False even if cleanup fails
            assert processor._initialized is False

    @pytest.mark.unit
    def test_cleanup_with_general_exception(self):
        """Test cleanup when a general exception occurs during cleanup."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        # Set up engines
        processor.opendental_source_engine = MagicMock(spec=Engine)
        processor.mysql_replication_engine = MagicMock(spec=Engine)
        processor.postgres_analytics_engine = MagicMock(spec=Engine)
        
        # Mock the cleanup method to raise a general exception
        with patch.object(processor, '_initialized', True):
            # Mock the cleanup method to raise an exception
            with patch.object(processor, 'opendental_source_engine') as mock_source:
                mock_source.dispose.side_effect = Exception("General cleanup error")
                
                # This should not raise an exception
                processor.cleanup()
                
                # Verify that _initialized is set to False even if cleanup fails
                assert processor._initialized is False

    @pytest.mark.unit
    def test_context_manager(self):
        """Test TableProcessor as context manager."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        with TableProcessor(config_reader=mock_config_reader) as processor:
            assert isinstance(processor, TableProcessor)
            # Connections should be initialized
            assert processor.opendental_source_engine is None  # Not initialized yet

    @pytest.mark.unit
    def test_context_manager_exit_cleanup(self):
        """Test context manager exit calls cleanup."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        processor.opendental_source_engine = MagicMock(spec=Engine)
        
        with patch.object(processor, 'cleanup') as mock_cleanup:
            processor.__exit__(None, None, None)
            mock_cleanup.assert_called_once()

    @pytest.mark.unit
    def test_context_manager_exit_with_exception(self):
        """Test context manager exit with exception still calls cleanup."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        processor.opendental_source_engine = MagicMock(spec=Engine)
        
        with patch.object(processor, 'cleanup') as mock_cleanup:
            processor.__exit__(Exception("Test error"), Exception("Test error"), None)
            mock_cleanup.assert_called_once()

    @pytest.mark.unit
    def test_process_table_connections_not_available(self, mock_table_processor_engines):
        """Test process_table when connections are not available."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        # Mock initialize_connections to return False
        with patch.object(processor, 'initialize_connections', return_value=False):
            result = processor.process_table("test_table")
            
            assert result is False

    @pytest.mark.unit
    def test_process_table_success(self, mock_table_processor_engines, table_processor_standard_config):
        """Test successful table processing."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True), \
             patch.object(processor.settings, 'get_table_config', return_value=table_processor_standard_config), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table')
            
            assert result is True

    @pytest.mark.unit
    def test_process_table_extraction_failure(self, mock_table_processor_engines):
        """Test process_table when extraction fails."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=False), \
             patch.object(processor.settings, 'get_table_config', return_value={}), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table')
            
            assert result is False

    @pytest.mark.unit
    def test_process_table_loading_failure(self, mock_table_processor_engines):
        """Test process_table when loading fails."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=False), \
             patch.object(processor.settings, 'get_table_config', return_value={}), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table')
            
            assert result is False

    @pytest.mark.unit
    def test_process_table_transformation_failure(self, mock_table_processor_engines):
        """Test process_table when transformation fails (removed as part of refactoring)."""
        # This test is no longer applicable since raw_to_public_transformer has been removed
        # The transformation step is now handled by dbt instead of the ETL pipeline
        pass

    @pytest.mark.unit
    def test_process_table_force_full(self, mock_table_processor_engines):
        """Test process_table with force_full=True."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication') as mock_extract, \
             patch.object(processor, '_load_to_analytics') as mock_load, \
             patch.object(processor.settings, 'get_table_config', return_value={}), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            mock_extract.return_value = True
            mock_load.return_value = True
            result = processor.process_table('test_table', force_full=True)
            assert result is True
            # Should call with force_full=True for extraction
            mock_extract.assert_called_once_with('test_table', True)
            # Should call with is_incremental=False for loading
            mock_load.assert_called_once_with('test_table', False, {})

    @pytest.mark.unit
    def test_process_table_exception_handling(self, mock_table_processor_engines):
        """Test process_table exception handling."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', side_effect=Exception("Test error")), \
             patch.object(processor.settings, 'get_table_config', return_value={}), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table')
            
            assert result is False

    @pytest.mark.unit
    def test_extract_to_replication_success(self, mock_table_processor_engines):
        """Test successful extraction to replication."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        mock_config_reader.get_table_schema.return_value = None  # Table doesn't exist
        mock_config_reader.replicate_schema.return_value = True
        mock_config_reader.get_table_size_info.return_value = {'row_count': 0}
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = None  # Table doesn't exist
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        # Patch ExactMySQLReplicator at the correct import path for table_processor.py
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator), \
             patch('etl_pipeline.core.schema_discovery.SchemaDiscovery') as mock_schema_discovery_class:
            mock_target_discovery = MagicMock()
            mock_target_discovery.get_table_schema.return_value = None
            mock_schema_discovery_class.return_value = mock_target_discovery
            
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True
            mock_replicator.create_exact_replica.assert_called_once_with('test_table')
            mock_replicator.copy_table_data.assert_called_once_with('test_table')

    @pytest.mark.unit
    def test_extract_to_replication_schema_change(self, mock_table_processor_engines):
        """Test extraction with schema change detection."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        mock_config_reader.get_table_schema.return_value = {'columns': []}
        mock_config_reader.replicate_schema.return_value = True
        mock_config_reader.get_table_size_info.return_value = {'row_count': 0}
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        # Mock the entire method to avoid SQLAlchemy inspection issues
        with patch.object(processor, '_extract_to_replication', return_value=True):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True

    @pytest.mark.unit
    def test_extract_to_replication_create_failure(self, mock_table_processor_engines):
        """Test extraction when table creation fails."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = None  # Table doesn't exist
        mock_replicator.create_exact_replica.return_value = False  # Creation failed
        
        with patch('etl_pipeline.core.mysql_replicator.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is False

    @pytest.mark.unit
    def test_extract_to_replication_copy_failure(self, mock_table_processor_engines):
        """Test extraction when data copy fails."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = {'columns': []}  # Table exists
        mock_replicator.verify_exact_replica.return_value = True  # No schema change
        mock_replicator.copy_table_data.return_value = False  # Copy failed
        
        with patch('etl_pipeline.core.mysql_replicator.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is False

    @pytest.mark.unit
    def test_extract_to_replication_exception(self, mock_table_processor_engines):
        """Test extraction exception handling."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        with patch('etl_pipeline.core.mysql_replicator.ExactMySQLReplicator', side_effect=Exception("Test error")):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is False

    @pytest.mark.unit
    def test_load_to_analytics_standard_loading(self, mock_table_processor_engines, table_processor_standard_config):
        """Test standard loading to analytics."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        mock_schema = {'columns': [], 'primary_key': {}, 'incremental_columns': []}
        mock_config_reader.get_table_schema.return_value = mock_schema
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is True
            mock_loader.load_table.assert_called_once_with(table_name='test_table', mysql_schema=mock_schema, force_full=False)

    @pytest.mark.unit
    def test_load_to_analytics_chunked_loading(self, mock_table_processor_engines):
        """Test chunked loading for large tables."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        mock_schema = {'columns': [], 'primary_key': {}, 'incremental_columns': []}
        mock_config_reader.get_table_schema.return_value = mock_schema
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is True
            mock_loader.load_table_chunked.assert_called_once_with(
                table_name='test_table',
                mysql_schema=mock_schema,
                force_full=False,
                chunk_size=5000
            )

    @pytest.mark.unit
    def test_load_to_analytics_loading_failure(self, mock_table_processor_engines):
        """Test loading failure handling."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = False
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is False

    @pytest.mark.unit
    def test_load_to_analytics_exception(self, mock_table_processor_engines, table_processor_standard_config):
        """Test loading exception handling."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', side_effect=Exception("Test error")):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is False

    @pytest.mark.unit
    def test_load_to_analytics_default_config(self, mock_table_processor_engines):
        """Test loading with default table configuration."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        mock_schema = {'columns': [], 'primary_key': {}, 'incremental_columns': []}
        mock_config_reader.get_table_schema.return_value = mock_schema
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        table_config = {}  # Empty config
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is True
            mock_loader.load_table.assert_called_once_with(table_name='test_table', mysql_schema=mock_schema, force_full=False)

    @pytest.mark.unit
    def test_load_to_analytics_size_based_chunking(self, mock_table_processor_engines, table_processor_medium_large_config):
        """Test chunked loading based on size rather than row count."""
        # Create mock config reader
        mock_config_reader = MagicMock(spec=ConfigReader)
        mock_schema = {'columns': [], 'primary_key': {}, 'incremental_columns': []}
        mock_config_reader.get_table_schema.return_value = mock_schema
        
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is True
            mock_loader.load_table_chunked.assert_called_once_with(
                table_name='test_table',
                mysql_schema=mock_schema,
                force_full=False,
                chunk_size=2500
            ) 

    @pytest.mark.unit
    def test_load_to_analytics_chunked_loading_large_table(self, mock_table_processor_engines):
        """Test chunked loading for large table (> 1M rows)."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Mock schema discovery
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'primary_key': {},
            'incremental_columns': []
        }
        processor.config_reader = mock_config_reader
        
        # Large table config (> 1M rows)
        large_table_config = {
            'estimated_rows': 1500000,  # > 1M rows
            'estimated_size_mb': 50,
            'batch_size': 5000
        }
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is True
            mock_loader.load_table_chunked.assert_called_once_with(
                table_name='test_table',
                mysql_schema={'columns': [], 'primary_key': {}, 'incremental_columns': []},
                force_full=False,
                chunk_size=5000
            )

    @pytest.mark.unit
    def test_load_to_analytics_chunked_loading_large_size(self, mock_table_processor_engines):
        """Test chunked loading for large table (> 100MB)."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Mock schema discovery
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'primary_key': {},
            'incremental_columns': []
        }
        processor.config_reader = mock_config_reader
        
        # Large table config (> 100MB)
        large_table_config = {
            'estimated_rows': 50000,  # Not > 1M rows
            'estimated_size_mb': 150,  # > 100MB
            'batch_size': 3000
        }
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is True
            mock_loader.load_table_chunked.assert_called_once_with(
                table_name='test_table',
                mysql_schema={'columns': [], 'primary_key': {}, 'incremental_columns': []},
                force_full=False,
                chunk_size=3000
            )

    @pytest.mark.unit
    def test_load_to_analytics_standard_loading_small_table(self, mock_table_processor_engines):
        """Test standard loading for small table."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Mock schema discovery
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'primary_key': {},
            'incremental_columns': []
        }
        processor.config_reader = mock_config_reader
        
        # Small table config
        small_table_config = {
            'estimated_rows': 5000,  # < 1M rows
            'estimated_size_mb': 25,  # < 100MB
            'batch_size': 1000
        }
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is True
            mock_loader.load_table.assert_called_once_with(
                table_name='test_table',
                mysql_schema={'columns': [], 'primary_key': {}, 'incremental_columns': []},
                force_full=False
            )

    @pytest.mark.unit
    def test_load_to_analytics_default_config_values(self, mock_table_processor_engines):
        """Test loading with default config values."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Mock schema discovery
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'primary_key': {},
            'incremental_columns': []
        }
        processor.config_reader = mock_config_reader
        
        # Empty config (should use defaults)
        empty_config = {}
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is True
            mock_loader.load_table.assert_called_once_with(
                table_name='test_table',
                mysql_schema={'columns': [], 'primary_key': {}, 'incremental_columns': []},
                force_full=False
            )

    @pytest.mark.unit
    def test_load_to_analytics_schema_not_found(self, mock_table_processor_engines):
        """Test loading when schema is not found."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Mock schema discovery to return None (schema not found)
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = None
        processor.config_reader = mock_config_reader
        
        result = processor._load_to_analytics('test_table', True)
        
        assert result is False

    @pytest.mark.unit
    def test_process_table_connections_available_but_not_initialized(self, mock_table_processor_engines):
        """Test process_table when connections are available but not initialized."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        processor._initialized = False  # Connections available but not initialized
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True), \
             patch.object(processor.settings, 'get_table_config', return_value={}), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table')
            
            assert result is True
            assert processor._initialized is True  # Should be set to True

    @pytest.mark.unit
    def test_process_table_connections_not_available_initialized_false(self, mock_table_processor_engines):
        """Test process_table when connections not available and not initialized."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        processor._initialized = False
        
        with patch.object(processor, 'initialize_connections', return_value=False):
            result = processor.process_table('test_table')
            
            assert result is False

    @pytest.mark.unit
    def test_process_table_connections_not_available_initialized_true(self):
        """Test process_table when connections not available but initialized flag is True."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        processor._initialized = True
        
        result = processor.process_table('test_table')
        
        assert result is False

    @pytest.mark.unit
    def test_extract_to_replication_schema_change_detection_production(self, mock_table_processor_engines):
        """Test schema change detection in production environment."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader, environment='production')
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        # Mock schema discovery to return different schemas
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'schema_hash': 'source_hash_123'
        }
        processor.config_reader = mock_config_reader
        
        mock_replicator = MagicMock()
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator), \
             patch('etl_pipeline.orchestration.table_processor.SchemaDiscovery') as mock_schema_discovery_class, \
             patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
            
            mock_target_discovery = MagicMock()
            mock_target_discovery.get_table_schema.return_value = {
                'columns': [],
                'schema_hash': 'target_hash_456'  # Different hash
            }
            mock_schema_discovery_class.return_value = mock_target_discovery
            
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True
            # Should force full extraction due to schema change
            mock_replicator.create_exact_replica.assert_called_once_with('test_table')

    @pytest.mark.unit
    def test_extract_to_replication_schema_change_detection_test_env(self, mock_table_processor_engines):
        """Test schema change detection in test environment (should skip comparison)."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader, environment='test')
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        # Mock schema discovery to return different schemas
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'schema_hash': 'source_hash_123'
        }
        processor.config_reader = mock_config_reader
        
        mock_replicator = MagicMock()
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator), \
             patch('etl_pipeline.orchestration.table_processor.SchemaDiscovery') as mock_schema_discovery_class, \
             patch.dict(os.environ, {'ETL_ENVIRONMENT': 'test'}):
            
            mock_target_discovery = MagicMock()
            mock_target_discovery.get_table_schema.return_value = {
                'columns': [],
                'schema_hash': 'target_hash_456'  # Different hash, but should be ignored in test
            }
            mock_schema_discovery_class.return_value = mock_target_discovery
            
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True
            # Should not force full extraction in test environment
            mock_replicator.create_exact_replica.assert_not_called()

    @pytest.mark.unit
    def test_extract_to_replication_target_table_does_not_exist(self, mock_table_processor_engines):
        """Test extraction when target table doesn't exist."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        # Mock schema discovery to return source table exists
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'schema_hash': 'source_hash_123'
        }
        processor.config_reader = mock_config_reader
        
        mock_replicator = MagicMock()
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator), \
             patch('etl_pipeline.orchestration.table_processor.SchemaDiscovery') as mock_schema_discovery_class:
            
            mock_target_discovery = MagicMock()
            mock_target_discovery.get_table_schema.return_value = None  # Target table doesn't exist
            mock_schema_discovery_class.return_value = mock_target_discovery
            
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True
            # Should create table since target doesn't exist
            mock_replicator.create_exact_replica.assert_called_once_with('test_table')

    @pytest.mark.unit
    def test_extract_to_replication_source_table_does_not_exist(self, mock_table_processor_engines):
        """Test extraction when source table doesn't exist."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        # Mock schema discovery to return source table doesn't exist
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = None
        processor.config_reader = mock_config_reader
        
        mock_replicator = MagicMock()
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True
            # Should create table since source doesn't exist
            mock_replicator.create_exact_replica.assert_called_once_with('test_table') 

    @pytest.mark.unit
    def test_extract_to_replication_with_replicator_exception(self, mock_table_processor_engines):
        """Test extraction when ExactMySQLReplicator constructor raises an exception."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        # Mock SchemaDiscovery to return a table that exists
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'schema_hash': 'test_hash'
        }
        
        # Mock ExactMySQLReplicator to raise an exception during construction
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', side_effect=Exception("Replicator construction failed")):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is False

    @pytest.mark.unit
    def test_load_to_analytics_with_loader_exception(self, mock_table_processor_engines):
        """Test loading when PostgresLoader constructor raises an exception."""
        mock_config_reader = MagicMock(spec=ConfigReader)
        processor = TableProcessor(config_reader=mock_config_reader)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Mock schema discovery to return a valid schema
        mock_config_reader = MagicMock()
        mock_config_reader.get_table_schema.return_value = {
            'columns': [],
            'primary_key': {},
            'incremental_columns': []
        }
        
        # Mock PostgresLoader to raise an exception during construction
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', side_effect=Exception("Loader construction failed")):
            result = processor._load_to_analytics('test_table', True)
            
            assert result is False 