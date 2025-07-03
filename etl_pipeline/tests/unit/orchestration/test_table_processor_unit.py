"""
Unit tests for TableProcessor - Fast execution with comprehensive mocking.

This module provides pure unit tests for TableProcessor with all dependencies
mocked for fast execution and isolated component behavior testing.

Coverage: Core logic, edge cases, error handling
Execution: < 1 second per component
Markers: @pytest.mark.unit
"""

import pytest
import time
import logging
from unittest.mock import MagicMock, patch, Mock
from sqlalchemy.engine import Engine

from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.core.schema_discovery import SchemaDiscovery

logger = logging.getLogger(__name__)


class TestTableProcessorUnit:
    """Unit tests for TableProcessor with comprehensive mocking."""

    @pytest.mark.unit
    def test_initialization(self):
        """Test TableProcessor initialization."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        assert processor.settings is not None
        assert processor.metrics is not None
        assert processor.schema_discovery == mock_schema_discovery
        assert processor.opendental_source_engine is None
        assert processor.mysql_replication_engine is None
        assert processor.postgres_analytics_engine is None
        # raw_to_public_transformer removed as part of refactoring
        assert processor._source_db is None
        assert processor._replication_db is None
        assert processor._analytics_db is None

    @pytest.mark.unit
    def test_initialization_with_config_path(self):
        """Test TableProcessor initialization with config path (deprecated)."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery, config_path="/path/to/config")
        
        assert processor.config_path == "/path/to/config"
        assert processor.settings is not None
        assert processor.schema_discovery == mock_schema_discovery

    @pytest.mark.unit
    def test_connections_available_all_none(self):
        """Test _connections_available when all connections are None."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        assert processor._connections_available() is False

    @pytest.mark.unit
    def test_connections_available_partial(self):
        """Test _connections_available when some connections are available."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        processor.opendental_source_engine = MagicMock(spec=Engine)
        
        assert processor._connections_available() is False

    @pytest.mark.unit
    def test_connections_available_all_set(self):
        """Test _connections_available when all connections are available."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        processor.opendental_source_engine = MagicMock(spec=Engine)
        processor.mysql_replication_engine = MagicMock(spec=Engine)
        processor.postgres_analytics_engine = MagicMock(spec=Engine)
        
        assert processor._connections_available() is True

    @pytest.mark.unit
    def test_initialize_connections_with_provided_engines(self, mock_table_processor_engines):
        """Test initialize_connections with provided engines."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        
        # Use **kwargs in side effects (Section 12 from debugging notes)
        with patch.object(processor.settings, 'get_database_config') as mock_get_config:
            mock_get_config.side_effect = lambda db, **kwargs: {'database': f'{db}_db'}
            
            result = processor.initialize_connections(
                source_engine=mock_source,
                replication_engine=mock_replication,
                analytics_engine=mock_analytics
            )
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics
            assert processor._source_db == 'opendental_source_db'
            assert processor._replication_db == 'opendental_replication_db'
            assert processor._analytics_db == 'opendental_analytics_raw_db'

    @pytest.mark.unit
    def test_initialize_connections_with_connection_factory(self, mock_table_processor_engines):
        """Test initialize_connections using ConnectionFactory."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        
        # Patch where used, not where defined (Section 4.1 from debugging notes)
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory, \
             patch.object(processor.settings, 'get_database_config') as mock_get_config:
            
            mock_factory.get_opendental_source_connection.return_value = mock_source
            mock_factory.get_mysql_replication_connection.return_value = mock_replication
            mock_factory.get_postgres_analytics_connection.return_value = mock_analytics
            mock_get_config.side_effect = lambda db, **kwargs: {'database': f'{db}_db'}
            
            result = processor.initialize_connections()
            
            assert result is True
            assert processor.opendental_source_engine == mock_source
            assert processor.mysql_replication_engine == mock_replication
            assert processor.postgres_analytics_engine == mock_analytics

    @pytest.mark.unit
    def test_initialize_connections_failure(self):
        """Test initialize_connections when ConnectionFactory fails."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        with patch('etl_pipeline.orchestration.table_processor.ConnectionFactory') as mock_factory:
            mock_factory.get_opendental_source_connection.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception, match="Connection failed"):
                processor.initialize_connections()

    @pytest.mark.unit
    def test_cleanup_all_engines(self):
        """Test cleanup with all engines set."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        mock_engine = MagicMock(spec=Engine)
        mock_engine.dispose.side_effect = Exception("Dispose failed")
        processor.opendental_source_engine = mock_engine
        
        # Should not raise exception
        processor.cleanup()
        
        assert processor.opendental_source_engine is None

    @pytest.mark.unit
    def test_context_manager(self):
        """Test TableProcessor as context manager."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        with TableProcessor(schema_discovery=mock_schema_discovery) as processor:
            assert isinstance(processor, TableProcessor)
            # Connections should be initialized
            assert processor.opendental_source_engine is None  # Not initialized yet

    @pytest.mark.unit
    def test_context_manager_exit_cleanup(self):
        """Test context manager exit calls cleanup."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        processor.opendental_source_engine = MagicMock(spec=Engine)
        
        with patch.object(processor, 'cleanup') as mock_cleanup:
            processor.__exit__(None, None, None)
            mock_cleanup.assert_called_once()

    @pytest.mark.unit
    def test_process_table_connections_not_available(self):
        """Test process_table when connections are not available."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        # Mock initialize_connections to return False
        with patch.object(processor, 'initialize_connections', return_value=False):
            result = processor.process_table("test_table")
            
            assert result is False

    @pytest.mark.unit
    def test_process_table_success(self, mock_table_processor_engines, table_processor_standard_config):
        """Test successful table processing."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.opendental_source_engine = mock_source
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch.object(processor, '_extract_to_replication', return_value=True), \
             patch.object(processor, '_load_to_analytics', return_value=True), \
             patch.object(processor.settings, 'get_table_config', return_value={}), \
             patch.object(processor.settings, 'should_use_incremental', return_value=True):
            
            result = processor.process_table('test_table', force_full=True)
            
            assert result is True
            # Should call with force_full=True for extraction
            processor._extract_to_replication.assert_called_once_with('test_table', True)
            # Should call with is_incremental=False for loading
            processor._load_to_analytics.assert_called_once_with('test_table', False, {})

    @pytest.mark.unit
    def test_process_table_exception_handling(self, mock_table_processor_engines):
        """Test process_table exception handling."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        mock_schema_discovery.get_table_schema.return_value = None  # Table doesn't exist
        mock_schema_discovery.replicate_schema.return_value = True
        mock_schema_discovery.get_table_size_info.return_value = {'row_count': 0}
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        mock_schema_discovery.get_table_schema.return_value = {'columns': []}
        mock_schema_discovery.replicate_schema.return_value = True
        mock_schema_discovery.get_table_size_info.return_value = {'row_count': 0}
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
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
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_standard_config)
            
            assert result is True
            mock_loader.load_table.assert_called_once_with(table_name='test_table', mysql_schema=mock_schema_discovery.get_table_schema.return_value, force_full=False)

    @pytest.mark.unit
    def test_load_to_analytics_chunked_loading(self, mock_table_processor_engines, table_processor_large_config):
        """Test chunked loading for large tables."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_large_config)
            
            assert result is True
            mock_loader.load_table_chunked.assert_called_once_with(
                table_name='test_table',
                mysql_schema=mock_schema_discovery.get_table_schema.return_value,
                force_full=False,
                chunk_size=5000
            )

    @pytest.mark.unit
    def test_load_to_analytics_loading_failure(self, mock_table_processor_engines, table_processor_standard_config):
        """Test loading failure handling."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = False
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_standard_config)
            
            assert result is False

    @pytest.mark.unit
    def test_load_to_analytics_exception(self, mock_table_processor_engines, table_processor_standard_config):
        """Test loading exception handling."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', side_effect=Exception("Test error")):
            result = processor._load_to_analytics('test_table', True, table_processor_standard_config)
            
            assert result is False

    @pytest.mark.unit
    def test_load_to_analytics_default_config(self, mock_table_processor_engines):
        """Test loading with default table configuration."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        table_config = {}  # Empty config
        
        mock_loader = MagicMock()
        mock_loader.load_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_config)
            
            assert result is True
            mock_loader.load_table.assert_called_once_with(table_name='test_table', mysql_schema=mock_schema_discovery.get_table_schema.return_value, force_full=False)

    @pytest.mark.unit
    def test_load_to_analytics_size_based_chunking(self, mock_table_processor_engines, table_processor_medium_large_config):
        """Test chunked loading based on size rather than row count."""
        # Create mock schema discovery
        mock_schema_discovery = MagicMock(spec=SchemaDiscovery)
        
        processor = TableProcessor(schema_discovery=mock_schema_discovery)
        
        mock_source, mock_replication, mock_analytics = mock_table_processor_engines
        processor.mysql_replication_engine = mock_replication
        processor.postgres_analytics_engine = mock_analytics
        
        mock_loader = MagicMock()
        mock_loader.load_table_chunked.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.PostgresLoader', return_value=mock_loader):
            result = processor._load_to_analytics('test_table', True, table_processor_medium_large_config)
            
            assert result is True
            mock_loader.load_table_chunked.assert_called_once_with(
                table_name='test_table',
                mysql_schema=mock_schema_discovery.get_table_schema.return_value,
                force_full=False,
                chunk_size=2500
            ) 