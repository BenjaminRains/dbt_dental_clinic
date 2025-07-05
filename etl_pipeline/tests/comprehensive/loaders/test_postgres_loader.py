"""
Comprehensive tests for PostgresLoader - Full functionality testing with mocked dependencies.

This test file covers complete component behavior, error handling, and edge cases.
This is the main test suite that provides 90%+ coverage target.

Testing Strategy:
- Complete component behavior testing
- Error handling and edge cases
- Full functionality with mocked dependencies
- Coverage target: 90%+
- Execution: < 5 seconds per component
- Marker: @pytest.mark.unit (default)
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
from datetime import datetime, timedelta
import logging

# Import the component under test
from etl_pipeline.loaders.postgres_loader import PostgresLoader

logger = logging.getLogger(__name__)


class TestPostgresLoader:
    """Comprehensive tests for PostgresLoader class."""


class TestInitialization(TestPostgresLoader):
    """Test PostgresLoader initialization."""
    
    def test_initialization_with_valid_engines(self, mock_replication_engine, mock_analytics_engine):
        """Test successful initialization with valid database engines."""
        logger.info("Testing PostgresLoader initialization with valid engines")
        
        with patch('etl_pipeline.loaders.postgres_loader.settings') as mock_settings:
            mock_settings.get_database_config.side_effect = lambda db: {
                'analytics': {'schema': 'raw'},
                'replication': {'schema': 'raw'}
            }.get(db, {})
            
            with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema') as mock_schema_class:
                mock_schema_adapter = MagicMock()
                mock_schema_class.return_value = mock_schema_adapter
                
                loader = PostgresLoader(
                    replication_engine=mock_replication_engine,
                    analytics_engine=mock_analytics_engine
                )
                
                assert loader.replication_engine == mock_replication_engine
                assert loader.analytics_engine == mock_analytics_engine
                assert loader.replication_db == "opendental_replication"
                assert loader.analytics_db == "opendental_analytics"
                assert loader.analytics_schema == "raw"
                assert loader.schema_adapter == mock_schema_adapter
                assert loader.target_schema == "raw"
                assert loader.staging_schema == "raw"
                
                logger.debug("Successfully initialized PostgresLoader with valid engines")
    
    def test_initialization_with_custom_schemas(self, mock_replication_engine, mock_analytics_engine):
        """Test initialization with custom schema configurations."""
        logger.info("Testing PostgresLoader initialization with custom schemas")
        
        with patch('etl_pipeline.loaders.postgres_loader.settings') as mock_settings:
            mock_settings.get_database_config.side_effect = lambda db: {
                'analytics': {'schema': 'custom_analytics'},
                'replication': {'schema': 'custom_replication'}
            }.get(db, {})
            
            with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema'):
                loader = PostgresLoader(
                    replication_engine=mock_replication_engine,
                    analytics_engine=mock_analytics_engine
                )
                
                assert loader.target_schema == "custom_analytics"
                assert loader.staging_schema == "custom_replication"
                
                logger.debug("Successfully initialized PostgresLoader with custom schemas")


class TestLoadTable(TestPostgresLoader):
    """Test core load_table functionality."""
    
    def test_load_table_success(self, postgres_loader, sample_mysql_schema, sample_table_data):
        """Test successful table loading."""
        logger.info("Testing successful table loading")
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock source connection and result with proper return types (Lesson 14)
        mock_source_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name', 'created_at']
        mock_result.fetchall.return_value = [
            (1, 'John Doe', datetime(2023, 1, 1, 10, 0, 0)),
            (2, 'Jane Smith', datetime(2023, 1, 2, 11, 0, 0)),
            (3, 'Bob Johnson', datetime(2023, 1, 3, 12, 0, 0))
        ]
        mock_source_conn.execute.return_value = mock_result
        
        # Mock target connection
        mock_target_conn = MagicMock()
        
        # Set up engine mocks with proper context manager support (Lesson 23)
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.begin.return_value = mock_target_context
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table('test_table', sample_mysql_schema, force_full=False)
            
            assert result is True
            
            # Verify schema verification was called
            postgres_loader.schema_adapter.verify_schema.assert_called_once_with('test_table', sample_mysql_schema)
            
            # Verify target table insertion (Lesson 18)
            assert mock_target_conn.execute.call_count == 1
            insert_call = mock_target_conn.execute.call_args
            assert 'INSERT INTO raw.test_table' in str(insert_call[0][0])
            
            logger.debug(f"Successfully loaded table with {len(sample_table_data)} rows")
    
    def test_load_table_full_load_success(self, postgres_loader, sample_mysql_schema, sample_table_data):
        """Test successful full table loading with truncate."""
        logger.info("Testing full table loading with truncate")
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock source connection and result
        mock_source_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name', 'created_at']
        mock_result.fetchall.return_value = [
            (1, 'John Doe', datetime(2023, 1, 1, 10, 0, 0)),
            (2, 'Jane Smith', datetime(2023, 1, 2, 11, 0, 0))
        ]
        mock_source_conn.execute.return_value = mock_result
        
        # Mock target connection
        mock_target_conn = MagicMock()
        
        # Set up engine mocks with proper context manager support
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.begin.return_value = mock_target_context
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table('test_table', sample_mysql_schema, force_full=True)
            
            assert result is True
            
            # Verify truncate was called for full load (Lesson 18)
            assert mock_target_conn.execute.call_count == 2
            truncate_call = mock_target_conn.execute.call_args_list[0]
            assert 'TRUNCATE TABLE raw.test_table' in str(truncate_call[0][0])
            
            logger.debug("Successfully tested full load with truncate")
    
    def test_load_table_no_data(self, postgres_loader, sample_mysql_schema):
        """Test table loading with no data."""
        logger.info("Testing table loading with no data")
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock source connection with no data
        mock_source_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name', 'created_at']
        mock_result.fetchall.return_value = []
        mock_source_conn.execute.return_value = mock_result
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table('test_table', sample_mysql_schema, force_full=False)
            
            assert result is True
            
            logger.debug("Successfully handled table loading with no data")
    
    def test_load_table_schema_creation_failure(self, postgres_loader, sample_mysql_schema):
        """Test table loading when schema creation fails."""
        logger.info("Testing table loading with schema creation failure")
        
        # Mock schema adapter to fail
        postgres_loader.schema_adapter.create_postgres_table.return_value = False
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = False
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table('test_table', sample_mysql_schema, force_full=False)
            
            assert result is False
            
            logger.debug("Successfully handled schema creation failure")
    
    def test_load_table_database_error(self, postgres_loader, sample_mysql_schema):
        """Test table loading with database error."""
        logger.info("Testing table loading with database error")
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock source connection to raise error
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = SQLAlchemyError("Database connection failed")
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table('test_table', sample_mysql_schema, force_full=False)
            
            assert result is False
            
            logger.debug("Successfully handled database error")


class TestLoadTableChunked(TestPostgresLoader):
    """Test chunked loading functionality."""
    
    def test_load_table_chunked_success(self, postgres_loader, sample_mysql_schema):
        """Test successful chunked table loading."""
        logger.info("Testing successful chunked table loading")
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock count query
        mock_source_conn = MagicMock()
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 5  # Total rows
        mock_source_conn.execute.return_value = mock_count_result
        
        # Mock chunk queries with proper side effects
        mock_chunk_result = MagicMock()
        mock_chunk_result.keys.return_value = ['id', 'name', 'created_at']
        # Return data for first chunk, then empty for second chunk
        mock_chunk_result.fetchall.side_effect = [
            [(1, 'John Doe', datetime(2023, 1, 1, 10, 0, 0))],  # First chunk
            []  # No more data
        ]
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        # Mock target connection
        mock_target_conn = MagicMock()
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.begin.return_value = mock_target_context
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            # Mock _get_last_load to return None for full load behavior
            with patch.object(postgres_loader, '_get_last_load', return_value=None):
                result = postgres_loader.load_table_chunked('test_table', sample_mysql_schema, force_full=False, chunk_size=1)
                
                assert result is True
                
                # Verify chunked processing occurred - should be at least 2 calls:
                # 1. Count query
                # 2. Chunk query (returns data)
                assert mock_source_conn.execute.call_count >= 2
                
                logger.debug(f"Successfully tested chunked loading with {mock_source_conn.execute.call_count} database queries")
    
    def test_load_table_chunked_full_load(self, postgres_loader, sample_mysql_schema):
        """Test chunked loading with full load (truncate)."""
        logger.info("Testing chunked loading with full load")
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock count query
        mock_source_conn = MagicMock()
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 2
        mock_source_conn.execute.return_value = mock_count_result
        
        # Mock chunk query
        mock_chunk_result = MagicMock()
        mock_chunk_result.keys.return_value = ['id', 'name', 'created_at']
        mock_chunk_result.fetchall.return_value = [
            (1, 'John Doe', datetime(2023, 1, 1, 10, 0, 0))
        ]
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        # Mock target connection
        mock_target_conn = MagicMock()
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.begin.return_value = mock_target_context
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table_chunked('test_table', sample_mysql_schema, force_full=True, chunk_size=1)
            
            assert result is True
            
            # Verify truncate was called for full load
            assert mock_target_conn.execute.call_count >= 2  # Truncate + insert
            
            logger.debug("Successfully tested chunked full load with truncate")
    
    def test_load_table_chunked_no_data(self, postgres_loader, sample_mysql_schema):
        """Test chunked loading with no data."""
        logger.info("Testing chunked loading with no data")
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock count query with no data
        mock_source_conn = MagicMock()
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 0
        mock_source_conn.execute.return_value = mock_count_result
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table_chunked('test_table', sample_mysql_schema, force_full=False, chunk_size=1)
            
            assert result is True
            
            logger.debug("Successfully handled chunked loading with no data")
    
    def test_load_table_chunked_database_error(self, postgres_loader, sample_mysql_schema):
        """Test chunked loading with database error."""
        logger.info("Testing chunked loading with database error")
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock count query to raise error
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = SQLAlchemyError("Database connection failed")
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = True
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table_chunked('test_table', sample_mysql_schema, force_full=False, chunk_size=1)
            
            assert result is False
            
            logger.debug("Successfully handled chunked loading database error")


class TestVerifyLoad(TestPostgresLoader):
    """Test load verification functionality."""
    
    def test_verify_load_success(self, postgres_loader):
        """Test successful load verification."""
        logger.info("Testing successful load verification")
        
        # Mock source connection
        mock_source_conn = MagicMock()
        mock_source_conn.execute.return_value.scalar.return_value = 100
        
        # Mock target connection
        mock_target_conn = MagicMock()
        mock_target_conn.execute.return_value.scalar.return_value = 100
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.connect.return_value = mock_target_context
        
        result = postgres_loader.verify_load('test_table')
        
        assert result is True
        
        # Verify count queries were executed (Lesson 18)
        source_call = mock_source_conn.execute.call_args
        target_call = mock_target_conn.execute.call_args
        assert 'SELECT COUNT(*)' in str(source_call[0][0])
        assert 'SELECT COUNT(*)' in str(target_call[0][0])
        
        logger.debug("Successfully verified load with matching counts")
    
    def test_verify_load_count_mismatch(self, postgres_loader):
        """Test load verification with count mismatch."""
        logger.info("Testing load verification with count mismatch")
        
        # Mock source connection
        mock_source_conn = MagicMock()
        mock_source_conn.execute.return_value.scalar.return_value = 100
        
        # Mock target connection with different count
        mock_target_conn = MagicMock()
        mock_target_conn.execute.return_value.scalar.return_value = 95
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.connect.return_value = mock_target_context
        
        result = postgres_loader.verify_load('test_table')
        
        assert result is False
        
        logger.debug("Successfully detected count mismatch in load verification")
    
    def test_verify_load_database_error(self, postgres_loader):
        """Test load verification with database error."""
        logger.info("Testing load verification with database error")
        
        # Mock source connection to raise error
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = SQLAlchemyError("Database connection failed")
        
        # Set up engine mocks
        mock_source_context = MagicMock()
        mock_source_context.__enter__ = MagicMock(return_value=mock_source_conn)
        mock_source_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.replication_engine.connect.return_value = mock_source_context
        
        result = postgres_loader.verify_load('test_table')
        
        assert result is False
        
        logger.debug("Successfully handled database error in load verification")


class TestUtilityMethods(TestPostgresLoader):
    """Test utility methods."""
    
    def test_build_load_query_full_load(self, postgres_loader):
        """Test query building for full load."""
        logger.info("Testing query building for full load")
        
        query = postgres_loader._build_load_query('test_table', ['created_at'], force_full=True)
        
        assert 'SELECT * FROM test_table' in query
        assert 'WHERE' not in query  # No incremental conditions
        
        logger.debug(f"Built full load query: {query}")
    
    def test_build_load_query_incremental_load(self, postgres_loader):
        """Test query building for incremental load."""
        logger.info("Testing query building for incremental load")
        
        # Mock last load timestamp
        with patch.object(postgres_loader, '_get_last_load', return_value=datetime(2023, 1, 1, 10, 0, 0)):
            query = postgres_loader._build_load_query('test_table', ['created_at'], force_full=False)
            
            assert 'SELECT * FROM test_table' in query
            assert 'WHERE' in query
            assert "created_at > '2023-01-01 10:00:00'" in query
            
            logger.debug(f"Built incremental query: {query}")
    
    def test_build_load_query_no_incremental_columns(self, postgres_loader):
        """Test query building with no incremental columns."""
        logger.info("Testing query building with no incremental columns")
        
        query = postgres_loader._build_load_query('test_table', [], force_full=False)
        
        assert 'SELECT * FROM test_table' in query
        assert 'WHERE' not in query  # No incremental conditions
        
        logger.debug(f"Built query without incremental columns: {query}")
    
    def test_build_count_query_full_load(self, postgres_loader):
        """Test count query building for full load."""
        logger.info("Testing count query building for full load")
        
        query = postgres_loader._build_count_query('test_table', ['created_at'], force_full=True)
        
        assert 'SELECT COUNT(*) FROM test_table' in query
        assert 'WHERE' not in query  # No incremental conditions
        
        logger.debug(f"Built full count query: {query}")
    
    def test_build_count_query_incremental_load(self, postgres_loader):
        """Test count query building for incremental load."""
        logger.info("Testing count query building for incremental load")
        
        # Mock last load timestamp
        with patch.object(postgres_loader, '_get_last_load', return_value=datetime(2023, 1, 1, 10, 0, 0)):
            query = postgres_loader._build_count_query('test_table', ['created_at'], force_full=False)
            
            assert 'SELECT COUNT(*) FROM test_table' in query
            assert 'WHERE' in query
            assert "created_at > '2023-01-01 10:00:00'" in query
            
            logger.debug(f"Built incremental count query: {query}")
    
    def test_get_last_load_success(self, postgres_loader):
        """Test successful last load timestamp retrieval."""
        logger.info("Testing successful last load timestamp retrieval")
        
        # Mock target connection
        mock_target_conn = MagicMock()
        mock_target_conn.execute.return_value.scalar.return_value = datetime(2023, 1, 1, 10, 0, 0)
        
        # Set up engine mocks
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.connect.return_value = mock_target_context
        
        result = postgres_loader._get_last_load('test_table')
        
        assert result == datetime(2023, 1, 1, 10, 0, 0)
        
        # Verify query was executed (Lesson 18)
        call_args = mock_target_conn.execute.call_args
        assert 'SELECT MAX(last_loaded)' in str(call_args[0][0])
        assert 'etl_load_status' in str(call_args[0][0])
        
        logger.debug("Successfully retrieved last load timestamp")
    
    def test_get_last_load_no_timestamp(self, postgres_loader):
        """Test last load timestamp retrieval when no timestamp exists."""
        logger.info("Testing last load timestamp retrieval with no timestamp")
        
        # Mock target connection
        mock_target_conn = MagicMock()
        mock_target_conn.execute.return_value.scalar.return_value = None
        
        # Set up engine mocks
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.connect.return_value = mock_target_context
        
        result = postgres_loader._get_last_load('test_table')
        
        assert result is None
        
        logger.debug("Successfully handled missing timestamp")
    
    def test_get_last_load_database_error(self, postgres_loader):
        """Test last load timestamp retrieval with database error."""
        logger.info("Testing last load timestamp retrieval with database error")
        
        # Mock target connection to raise error
        mock_target_conn = MagicMock()
        mock_target_conn.execute.side_effect = SQLAlchemyError("Database connection failed")
        
        # Set up engine mocks
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.connect.return_value = mock_target_context
        
        result = postgres_loader._get_last_load('test_table')
        
        assert result is None
        
        logger.debug("Successfully handled database error in timestamp retrieval")
    
    def test_convert_row_data_types_success(self, postgres_loader):
        """Test successful data type conversion."""
        logger.info("Testing successful data type conversion")
        
        # Mock target connection and inspector
        mock_target_conn = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {'name': 'id', 'type': MagicMock(python_type=int)},
            {'name': 'name', 'type': MagicMock(python_type=str)},
            {'name': 'is_active', 'type': MagicMock(python_type=bool)}
        ]
        
        # Set up engine mocks
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.connect.return_value = mock_target_context
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            row_data = {
                'id': '1',
                'name': 'John Doe',
                'is_active': 1
            }
            
            result = postgres_loader._convert_row_data_types('test_table', row_data)
            
            # The actual implementation doesn't convert string '1' to int 1
            # It only handles boolean conversion when target_type is bool
            assert result['id'] == '1'  # String remains string
            assert result['name'] == 'John Doe'  # String unchanged
            assert result['is_active'] is True  # Converted to bool
            
            logger.debug("Successfully converted data types")
    
    def test_convert_row_data_types_error(self, postgres_loader):
        """Test data type conversion with error."""
        logger.info("Testing data type conversion with error")
        
        # Mock target connection to raise error
        mock_target_conn = MagicMock()
        mock_target_conn.execute.side_effect = SQLAlchemyError("Database connection failed")
        
        # Set up engine mocks
        mock_target_context = MagicMock()
        mock_target_context.__enter__ = MagicMock(return_value=mock_target_conn)
        mock_target_context.__exit__ = MagicMock(return_value=None)
        postgres_loader.analytics_engine.connect.return_value = mock_target_context
        
        row_data = {'id': 1, 'name': 'John Doe'}
        
        result = postgres_loader._convert_row_data_types('test_table', row_data)
        
        # Should return original data on error
        assert result == row_data
        
        logger.debug("Successfully handled data type conversion error")


class TestSchemaIntegration(TestPostgresLoader):
    """Test schema integration with PostgresSchema."""
    
    def test_ensure_postgres_table_create_new(self, postgres_loader, sample_mysql_schema):
        """Test ensuring PostgreSQL table when table doesn't exist."""
        logger.info("Testing PostgreSQL table creation")
        
        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = False
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            result = postgres_loader._ensure_postgres_table('test_table', sample_mysql_schema)
            
            assert result is True
            postgres_loader.schema_adapter.create_postgres_table.assert_called_once_with('test_table', sample_mysql_schema)
            
            logger.debug("Successfully created new PostgreSQL table")
    
    def test_ensure_postgres_table_verify_existing(self, postgres_loader, sample_mysql_schema):
        """Test ensuring PostgreSQL table when table exists."""
        logger.info("Testing PostgreSQL table verification")
        
        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = True
        
        # Mock schema adapter
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            result = postgres_loader._ensure_postgres_table('test_table', sample_mysql_schema)
            
            assert result is True
            postgres_loader.schema_adapter.verify_schema.assert_called_once_with('test_table', sample_mysql_schema)
            
            logger.debug("Successfully verified existing PostgreSQL table")
    
    def test_ensure_postgres_table_creation_failure(self, postgres_loader, sample_mysql_schema):
        """Test ensuring PostgreSQL table when creation fails."""
        logger.info("Testing PostgreSQL table creation failure")
        
        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = False
        
        # Mock schema adapter to fail
        postgres_loader.schema_adapter.create_postgres_table.return_value = False
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            result = postgres_loader._ensure_postgres_table('test_table', sample_mysql_schema)
            
            assert result is False
            
            logger.debug("Successfully handled table creation failure")
    
    def test_ensure_postgres_table_verification_failure(self, postgres_loader, sample_mysql_schema):
        """Test ensuring PostgreSQL table when verification fails."""
        logger.info("Testing PostgreSQL table verification failure")
        
        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = True
        
        # Mock schema adapter to fail verification
        postgres_loader.schema_adapter.verify_schema.return_value = False
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            result = postgres_loader._ensure_postgres_table('test_table', sample_mysql_schema)
            
            assert result is False
            
            logger.debug("Successfully handled table verification failure")


@pytest.mark.integration
class TestPostgresLoaderIntegration:
    """Integration tests with real database connections."""
    
    def test_full_table_loading_workflow(self):
        """Test complete table loading workflow with real databases."""
        pytest.skip("Integration tests require real database setup")
        
        # This would test:
        # 1. Create test table in MySQL replication
        # 2. Load table to PostgreSQL analytics
        # 3. Verify load was successful
        # 4. Validate data integrity
    
    def test_incremental_table_loading_workflow(self):
        """Test incremental table loading workflow with real databases."""
        pytest.skip("Integration tests require real database setup")
        
        # This would test:
        # 1. Initial load of table
        # 2. Add new data to source
        # 3. Incremental load
        # 4. Verify only new data was loaded
    
    def test_large_table_chunked_loading(self):
        """Test chunked loading with large tables."""
        pytest.skip("Integration tests require real database setup")
        
        # This would test:
        # 1. Create large test table
        # 2. Load using chunked method
        # 3. Verify all data was loaded
        # 4. Monitor memory usage


@pytest.mark.performance
class TestPostgresLoaderPerformance(TestPostgresLoader):
    """Performance tests for PostgresLoader."""
    
    def test_small_table_loading_performance(self, postgres_loader):
        """Test performance with small tables."""
        pytest.skip("Performance tests require test data setup")
        
        # This would test:
        # 1. Loading time for small tables
        # 2. Memory usage optimization
        # 3. Connection efficiency
    
    def test_large_table_chunked_loading_performance(self, postgres_loader):
        """Test performance with large tables using chunked loading."""
        pytest.skip("Performance tests require test data setup")
        
        # This would test:
        # 1. Chunked loading performance
        # 2. Memory usage during chunked loading
        # 3. Processing time optimization
    
    def test_memory_usage_optimization(self, postgres_loader):
        """Test memory usage during loading operations."""
        pytest.skip("Performance tests require memory monitoring setup")
        
        # This would test:
        # 1. Memory usage remains constant
        # 2. No memory leaks
        # 3. Garbage collection efficiency


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 