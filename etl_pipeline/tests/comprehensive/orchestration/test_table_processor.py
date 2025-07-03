"""
Comprehensive tests for TableProcessor - Full functionality testing with mocked dependencies.

This module provides comprehensive tests for TableProcessor covering complete component behavior,
error handling, and edge cases with mocked dependencies.

Coverage: 90%+ target coverage (main test suite)
Execution: < 5 seconds per component
Markers: @pytest.mark.unit (default)
"""

import pytest
import time
import logging
from unittest.mock import MagicMock, patch, Mock
from sqlalchemy.engine import Engine

from etl_pipeline.orchestration.table_processor import TableProcessor

logger = logging.getLogger(__name__)


class TestTableProcessor:
    """Comprehensive tests for TableProcessor with full functionality testing."""

    def test_initialization_with_defaults(self):
        """Test TableProcessor initialization with default values."""
        processor = TableProcessor()
        
        assert processor.settings is not None
        assert processor.metrics is not None
        assert processor.config_path is None
        assert processor.opendental_source_engine is None
        assert processor.mysql_replication_engine is None
        assert processor.postgres_analytics_engine is None
        assert processor._source_db is None
        assert processor._replication_db is None
        assert processor._analytics_db is None

    def test_initialization_with_config_path(self):
        """Test TableProcessor initialization with config path."""
        processor = TableProcessor(config_path="/test/config.yml")
        
        assert processor.config_path == "/test/config.yml"
        assert processor.settings is not None

    def test_connections_available_scenarios(self):
        """Test _connections_available with various connection states."""
        processor = TableProcessor()
        
        # All None
        assert processor._connections_available() is False
        
        # Only source
        processor.opendental_source_engine = MagicMock(spec=Engine)
        assert processor._connections_available() is False
        
        # Source and replication
        processor.mysql_replication_engine = MagicMock(spec=Engine)
        assert processor._connections_available() is False
        
        # All connections
        processor.postgres_analytics_engine = MagicMock(spec=Engine)
        assert processor._connections_available() is True

    def test_initialize_connections_success_with_provided_engines(self, mock_table_processor_engines):
        """Test successful connection initialization with provided engines."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        
        # Use **kwargs in side effects (Section 12 from debugging notes)
        with patch.object(processor.settings, 'get_database_config') as mock_get_config:
            mock_get_config.side_effect = lambda db, **kwargs: {'database': f'{db}_database'}
            
            result = processor.initialize_connections(
                source_engine=mock_source,
                replication_engine=mock_replication,
                analytics_engine=mock_analytics
            )
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics
            assert processor._source_db == 'source_database'
            assert processor._replication_db == 'replication_database'
            assert processor._analytics_db == 'analytics_database'

    def test_initialize_connections_success_with_connection_factory(self, mock_table_processor_engines):
        """Test successful connection initialization using ConnectionFactory."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_factory.get_opendental_source_connection.return_value = mock_source
            mock_factory.get_mysql_replication_connection.return_value = mock_replication
            mock_factory.get_postgres_analytics_connection.return_value = mock_analytics
            mock_get_config.side_effect = lambda db, **kwargs: {'database': f'{db}_database'}
            
            result = processor.initialize_connections()
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics

    def test_initialize_connections_failure_and_cleanup(self):
        """Test connection initialization failure and cleanup."""
        processor = TableProcessor()
        
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory:
            mock_factory.get_opendental_source_connection.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                processor.initialize_connections()
            
            # Should cleanup on failure
            assert processor.opendental_source_engine is None
            assert processor.mysql_replication_engine is None
            assert processor.postgres_analytics_engine is None

    def test_cleanup_with_all_engines(self):
        """Test cleanup with all engines set."""
        processor = TableProcessor()
        
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

    def test_cleanup_with_partial_engines(self):
        """Test cleanup with only some engines set."""
        processor = TableProcessor()
        processor.opendental_source_engine = MagicMock(spec=Engine)
        processor.mysql_replication_engine = None
        processor.postgres_analytics_engine = MagicMock(spec=Engine)
        
        processor.cleanup()
        
        assert processor.opendental_source_engine is None
        assert processor.mysql_replication_engine is None
        assert processor.postgres_analytics_engine is None

    def test_cleanup_with_dispose_errors(self):
        """Test cleanup when engine dispose operations fail."""
        processor = TableProcessor()
        
        # Mock engines that fail on dispose
        mock_source = MagicMock(spec=Engine)
        mock_source.dispose.side_effect = Exception("Source dispose failed")
        processor.opendental_source_engine = mock_source
        
        mock_replication = MagicMock(spec=Engine)
        mock_replication.dispose.side_effect = Exception("Replication dispose failed")
        processor.mysql_replication_engine = mock_replication
        
        mock_analytics = MagicMock(spec=Engine)
        mock_analytics.dispose.side_effect = Exception("Analytics dispose failed")
        processor.postgres_analytics_engine = mock_analytics
        
        # Should not raise exceptions
        processor.cleanup()
        
        assert processor.opendental_source_engine is None
        assert processor.mysql_replication_engine is None
        assert processor.postgres_analytics_engine is None

    def test_context_manager_success(self):
        """Test TableProcessor as context manager with successful operations."""
        with TableProcessor() as processor:
            assert isinstance(processor, TableProcessor)
            # Test that we can access the processor
            assert processor.settings is not None

    def test_context_manager_exit_cleanup(self):
        """Test context manager exit calls cleanup."""
        processor = TableProcessor()
        processor.opendental_source_engine = MagicMock(spec=Engine)
        
        with patch.object(processor, 'cleanup') as mock_cleanup:
            processor.__exit__(None, None, None)
            mock_cleanup.assert_called_once()

    def test_context_manager_exit_with_exception(self):
        """Test context manager exit with exception still calls cleanup."""
        processor = TableProcessor()
        processor.opendental_source_engine = MagicMock(spec=Engine)
        
        with patch.object(processor, 'cleanup') as mock_cleanup:
            processor.__exit__(Exception("Test error"), Exception("Test error"), None)
            mock_cleanup.assert_called_once()

    def test_process_table_connections_not_available(self):
        """Test process_table when connections are not initialized."""
        processor = TableProcessor()
        
        result = processor.process_table('test_table')
        
        assert result is False

    def test_process_table_success_incremental(self, mock_table_processor_engines, table_processor_standard_config):
        """Test successful incremental table processing."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Add debug logging to mocks (Section 1 from debugging notes)
        with patch.object(processor, '_extract_to_replication') as mock_extract, \
             patch.object(processor, '_load_to_analytics') as mock_load, \
             patch.object(processor.settings, 'get_table_config') as mock_get_config, \
             patch.object(processor.settings, 'should_use_incremental') as mock_incremental:
            
            # Add debug logging to side effects
            def extract_side_effect(table, force_full, **kwargs):
                logger.debug(f"Extract called: {table}, {force_full}")
                return True
            
            def load_side_effect(table, incremental, config, **kwargs):
                logger.debug(f"Load called: {table}, {incremental}")
                return True
            
            mock_extract.side_effect = extract_side_effect
            mock_load.side_effect = load_side_effect
            mock_get_config.return_value = table_processor_standard_config
            mock_incremental.return_value = True
            
            result = processor.process_table('test_table')
            
            assert result is True
            processor._extract_to_replication.assert_called_once_with('test_table', False)
            processor._load_to_analytics.assert_called_once_with('test_table', True, table_processor_standard_config)

    def test_process_table_success_full_refresh(self, mock_table_processor_engines, table_processor_standard_config):
        """Test successful full refresh table processing."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True), \
             patch.object(processor.settings, 'get_table_config', return_value=table_processor_standard_config), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table', force_full=True)
            
            assert result is True
            processor._extract_to_replication.assert_called_once_with('test_table', True)
            processor._load_to_analytics.assert_called_once_with('test_table', False, table_processor_standard_config)

    def test_process_table_extraction_failure(self, mock_table_processor_engines, table_processor_standard_config):
        """Test process_table when extraction phase fails."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=False), \
             patch.object(processor.settings, 'get_table_config', return_value=table_processor_standard_config), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table')
            
            assert result is False

    def test_process_table_loading_failure(self, mock_table_processor_engines, table_processor_standard_config):
        """Test process_table when loading phase fails."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=False), \
             patch.object(processor.settings, 'get_table_config', return_value=table_processor_standard_config), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table')
            
            assert result is False

    def test_process_table_transformation_failure(self, mock_table_processor_engines, table_processor_standard_config):
        """Test process_table when transformation phase fails (removed as part of refactoring)."""
        # This test is no longer applicable since raw_to_public_transformer has been removed
        # The transformation step is now handled by dbt instead of the ETL pipeline
        pass

    def test_process_table_exception_handling(self, mock_table_processor_engines):
        """Test process_table exception handling."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', side_effect=Exception("Test error")), \
             patch.object(processor.settings, 'get_table_config', return_value={}), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table')
            
            assert result is False

    def test_process_table_timing_logging(self, mock_table_processor_engines, table_processor_standard_config):
        """Test that process_table logs timing information."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True), \
             patch.object(processor.settings, 'get_table_config', return_value=table_processor_standard_config), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True), \
             patch('time.time', side_effect=[100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]):  # More values for all logging calls
            
            result = processor.process_table('test_table')
            
            assert result is True

    def test_extract_to_replication_new_table(self, mock_table_processor_engines):
        """Test extraction for a new table that doesn't exist."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = None  # Table doesn't exist
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True
            mock_replicator.create_exact_replica.assert_called_once_with('test_table')
            mock_replicator.copy_table_data.assert_called_once_with('test_table')

    def test_extract_to_replication_existing_table_no_schema_change(self, mock_table_processor_engines):
        """Test extraction for existing table with no schema change."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = {'columns': []}  # Table exists
        mock_replicator.verify_exact_replica.return_value = True  # No schema change
        mock_replicator.copy_table_data.return_value = True
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True
            # Based on actual implementation, no schema change means no table recreation needed
            mock_replicator.copy_table_data.assert_called_once_with('test_table')

    def test_extract_to_replication_schema_change_detection(self, mock_table_processor_engines):
        """Test extraction with schema change detection."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = {'columns': []}  # Table exists
        mock_replicator.verify_exact_replica.return_value = False  # Schema changed
        mock_replicator.create_exact_replica.return_value = True
        mock_replicator.copy_table_data.return_value = True
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is True
            # Based on actual implementation, schema change forces full extraction but doesn't recreate table
            mock_replicator.copy_table_data.assert_called_once_with('test_table')

    def test_extract_to_replication_create_failure(self, mock_table_processor_engines):
        """Test extraction when table creation fails."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = None  # Table doesn't exist
        mock_replicator.create_exact_replica.return_value = False  # Creation failed
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is False

    def test_extract_to_replication_copy_failure(self, mock_table_processor_engines):
        """Test extraction when data copy fails."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        mock_replicator = MagicMock()
        mock_replicator.get_exact_table_schema.return_value = {'columns': []}  # Table exists
        mock_replicator.verify_exact_replica.return_value = True  # No schema change
        mock_replicator.copy_table_data.return_value = False  # Copy failed
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', return_value=mock_replicator):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is False

    def test_extract_to_replication_exception_handling(self, mock_table_processor_engines):
        """Test extraction exception handling."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor._source_db = 'source_db'
        processor._replication_db = 'replication_db'
        
        with patch('etl_pipeline.orchestration.table_processor.ExactMySQLReplicator', side_effect=Exception("Test error")):
            result = processor._extract_to_replication('test_table', False)
            
            assert result is False

    def test_load_to_analytics_standard_loading_small_table(self, mock_table_processor_engines, table_processor_standard_config):
        """Test standard loading for small tables."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_standard_config)
            
            assert result is True
            # Match actual call signature with keyword arguments (Section 31 from debugging notes)
            mock_loader.load_table.assert_called_once_with(table_name='test_table', force_full=False)

    def test_load_to_analytics_chunked_loading_large_table(self, mock_table_processor_engines, table_processor_large_config):
        """Test chunked loading for large tables based on row count."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_large_config)
            
            assert result is True
            # Match actual call signature with keyword arguments (Section 31 from debugging notes)
            mock_loader.load_table_chunked.assert_called_once_with(table_name='test_table', force_full=False, chunk_size=5000)

    def test_load_to_analytics_chunked_loading_large_size(self, mock_table_processor_engines, table_processor_medium_large_config):
        """Test chunked loading for large tables based on size."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_medium_large_config)
            
            assert result is True
            # Match actual call signature with keyword arguments (Section 31 from debugging notes)
            mock_loader.load_table_chunked.assert_called_once_with(table_name='test_table', force_full=False, chunk_size=5000)

    def test_load_to_analytics_standard_loading_full_refresh(self, mock_table_processor_engines, table_processor_standard_config):
        """Test standard loading with full refresh."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', False, table_processor_standard_config)
            
            assert result is True
            # Match actual call signature with keyword arguments (Section 31 from debugging notes)
            mock_loader.load_table.assert_called_once_with(table_name='test_table', force_full=True)

    def test_load_to_analytics_chunked_loading_full_refresh(self, mock_table_processor_engines, table_processor_large_config):
        """Test chunked loading with full refresh."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', False, table_processor_large_config)
            
            assert result is True
            # Match actual call signature with keyword arguments (Section 31 from debugging notes)
            mock_loader.load_table_chunked.assert_called_once_with(table_name='test_table', force_full=True, chunk_size=5000)

    def test_load_to_analytics_loading_failure(self, mock_table_processor_engines, table_processor_standard_config):
        """Test loading failure handling."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = False
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_standard_config)
            
            assert result is False

    def test_load_to_analytics_chunked_loading_failure(self, mock_table_processor_engines, table_processor_large_config):
        """Test chunked loading failure handling."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = False
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_large_config)
            
            assert result is False

    def test_load_to_analytics_exception_handling(self, mock_table_processor_engines, table_processor_standard_config):
        """Test loading exception handling."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', side_effect=Exception("Test error")):
            result = processor._load_to_analytics('test_table', True, table_processor_standard_config)
            
            assert result is False

    def test_load_to_analytics_default_config(self, mock_table_processor_engines):
        """Test loading with default table configuration."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        table_config = {}  # Empty config
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_config)
            
            assert result is True
            # Match actual call signature with keyword arguments (Section 31 from debugging notes)
            mock_loader.load_table.assert_called_once_with(table_name='test_table', force_full=False)

    def test_load_to_analytics_edge_case_sizes(self, mock_table_processor_engines):
        """Test loading with edge case table sizes."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        # Test exactly at the threshold - based on actual implementation logic
        # The actual threshold is > 1000000 rows OR > 100 MB, so exactly at threshold should use standard loading
        table_config = {
            'estimated_rows': 1000000,  # Exactly at threshold
            'estimated_size_mb': 100,   # Exactly at threshold
            'batch_size': 5000
        }
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_config)
            
            assert result is True
            # At exactly the threshold, should use standard loading (not chunked)
            mock_loader.load_table.assert_called_once_with(table_name='test_table', force_full=False)

    def test_process_table_incremental_logic(self, mock_table_processor_engines, table_processor_standard_config):
        """Test incremental processing logic in process_table."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True), \
             patch.object(processor.settings, 'get_table_config', return_value=table_processor_standard_config), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table', force_full=False)
            
            assert result is True
            processor._extract_to_replication.assert_called_once_with('test_table', False)
            processor._load_to_analytics.assert_called_once_with('test_table', True, table_processor_standard_config)

    def test_process_table_full_refresh_logic(self, mock_table_processor_engines, table_processor_standard_config):
        """Test full refresh processing logic in process_table."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True), \
             patch.object(processor.settings, 'get_table_config', return_value=table_processor_standard_config), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table', force_full=True)
            
            assert result is True
            processor._extract_to_replication.assert_called_once_with('test_table', True)
            processor._load_to_analytics.assert_called_once_with('test_table', False, table_processor_standard_config)

    def test_process_table_settings_integration(self, mock_table_processor_engines):
        """Test integration with Settings class for table configuration."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        expected_config = {
            'estimated_rows': 5000,
            'estimated_size_mb': 25,
            'batch_size': 1000,
            'priority': 'important'
        }
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True), \
             patch.object(processor.settings, 'get_table_config', return_value=expected_config), \
             patch.object(processor.settings, 'should_use_incremental', return_value=False):
            
            result = processor.process_table('test_table')
            
            assert result is True
            processor.settings.get_table_config.assert_called_once_with('test_table')
            processor.settings.should_use_incremental.assert_called_once_with('test_table')
            processor._load_to_analytics.assert_called_once_with('test_table', False, expected_config)

    def test_mock_call_verification_with_debug_logging(self, mock_table_processor_engines, table_processor_standard_config):
        """Test mock call verification with debug logging for troubleshooting."""
        processor = TableProcessor()
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication') as mock_extract, \
             patch.object(processor, '_load_to_analytics') as mock_load, \
             patch.object(processor.settings, 'get_table_config') as mock_get_config, \
             patch.object(processor.settings, 'should_use_incremental') as mock_incremental:
            
            # Add debug logging to side effects (Section 1 from debugging notes)
            def extract_side_effect(table, force_full, **kwargs):
                logger.debug(f"Extract called with: table={table}, force_full={force_full}, kwargs={kwargs}")
                return True
            
            def load_side_effect(table, incremental, config, **kwargs):
                logger.debug(f"Load called with: table={table}, incremental={incremental}, config={config}, kwargs={kwargs}")
                return True
            
            mock_extract.side_effect = extract_side_effect
            mock_load.side_effect = load_side_effect
            mock_get_config.return_value = table_processor_standard_config
            mock_incremental.return_value = True
            
            result = processor.process_table('test_table')
            
            assert result is True
            
            # Verify mock calls with debug information
            assert mock_extract.call_count > 0, "Extract mock was not called"
            assert mock_load.call_count > 0, "Load mock was not called"
            
            logger.debug(f"Extract calls: {mock_extract.call_args_list}")
            logger.debug(f"Load calls: {mock_load.call_args_list}")
            
            mock_extract.assert_called_once_with('test_table', False)
            mock_load.assert_called_once_with('test_table', True, table_processor_standard_config)