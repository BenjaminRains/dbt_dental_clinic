"""
Unit tests for PostgresLoader - Fast execution with comprehensive mocking.

This test file focuses on pure unit tests with comprehensive mocking for fast execution.
Tests cover core logic, edge cases, and isolated component behavior.

Testing Strategy:
- Fast execution (< 1 second per component)
- Isolated component behavior
- Core logic and edge cases
- Comprehensive mocking
- Marker: @pytest.mark.unit

Refactored for new architecture:
- Uses new configuration system with enum-based types
- Imports fixtures from modular fixture files
- Uses dependency injection pattern
- Supports actual PostgresLoader constructor signature
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
from datetime import datetime, timedelta
import logging

# Import fixtures from modular fixture files
from tests.fixtures.config_fixtures import test_pipeline_config, test_tables_config
from tests.fixtures.env_fixtures import test_env_vars, test_settings
from tests.fixtures.connection_fixtures import (
    mock_source_engine, mock_replication_engine, mock_analytics_engine,
    database_types, postgres_schemas
)
from tests.fixtures.loader_fixtures import (
    postgres_loader, sample_mysql_schema, sample_table_data
)

# Import the component under test
from etl_pipeline.loaders.postgres_loader import PostgresLoader


@pytest.mark.unit
class TestPostgresLoaderUnit:
    """Unit tests for PostgresLoader class with comprehensive mocking."""
    
    # All fixtures moved to modular fixture files:
    # - mock_replication_engine (from connection_fixtures)
    # - mock_analytics_engine (from connection_fixtures)
    # - postgres_loader (from loader_fixtures)
    # - sample_mysql_schema (from loader_fixtures)
    # - sample_table_data (from loader_fixtures)


@pytest.mark.unit
class TestInitializationUnit:
    """Unit tests for PostgresLoader initialization."""
    
    def test_initialization_with_valid_engines(self, mock_replication_engine, mock_analytics_engine):
        """Test successful initialization with valid database engines."""
        with patch('etl_pipeline.loaders.postgres_loader.get_settings') as mock_get_settings:
            mock_settings = MagicMock()
            mock_settings.get_database_config.side_effect = lambda db_type, schema=None: {
                ('analytics', 'raw'): {'schema': 'raw'},
                ('replication', None): {'schema': 'raw'}
            }.get((db_type, schema), {})
            mock_get_settings.return_value = mock_settings
            
            with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema') as mock_schema_class:
                mock_schema_adapter = MagicMock()
                mock_schema_class.return_value = mock_schema_adapter
                
                # Use actual constructor signature: replication_engine and analytics_engine
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
    
    def test_initialization_with_custom_schemas(self, mock_replication_engine, mock_analytics_engine):
        """Test initialization with custom schema configurations."""
        with patch('etl_pipeline.loaders.postgres_loader.get_settings') as mock_get_settings:
            mock_settings = MagicMock()
            # Fix the side effect to return custom schemas using enum objects
            def get_config_side_effect(db_type, schema=None):
                # Import the actual enums for comparison
                from etl_pipeline.config import DatabaseType, PostgresSchema as ConfigPostgresSchema
                
                if db_type == DatabaseType.ANALYTICS and schema == ConfigPostgresSchema.RAW:
                    return {'schema': 'custom_analytics'}
                elif db_type == DatabaseType.REPLICATION:
                    return {'schema': 'custom_replication'}
                return {}
            
            mock_settings.get_database_config.side_effect = get_config_side_effect
            mock_get_settings.return_value = mock_settings
            
            with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema'):
                # Use actual constructor signature
                loader = PostgresLoader(
                    replication_engine=mock_replication_engine,
                    analytics_engine=mock_analytics_engine
                )
                
                assert loader.target_schema == "custom_analytics"
                assert loader.staging_schema == "custom_replication"


@pytest.mark.unit
class TestLoadTableUnit:
    """Unit tests for core load_table functionality."""
    
    def test_load_table_success(self, postgres_loader, sample_mysql_schema):
        """Test successful table loading with mocked dependencies."""
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock source connection and result
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
            
            result = postgres_loader.load_table('test_table', sample_mysql_schema, force_full=False)
            
            assert result is True
            
            # Verify schema verification was called
            postgres_loader.schema_adapter.verify_schema.assert_called_once_with('test_table', sample_mysql_schema)
            
            # Verify target table insertion
            assert mock_target_conn.execute.call_count == 1
            insert_call = mock_target_conn.execute.call_args
            assert 'INSERT INTO raw.test_table' in str(insert_call[0][0])
    
    def test_load_table_full_load_success(self, postgres_loader, sample_mysql_schema):
        """Test successful full table loading with truncate."""
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
            
            # Verify truncate was called for full load
            assert mock_target_conn.execute.call_count == 2
            truncate_call = mock_target_conn.execute.call_args_list[0]
            assert 'TRUNCATE TABLE raw.test_table' in str(truncate_call[0][0])
    
    def test_load_table_no_data(self, postgres_loader, sample_mysql_schema):
        """Test table loading with no data."""
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
    
    def test_load_table_schema_creation_failure(self, postgres_loader, sample_mysql_schema):
        """Test table loading when schema creation fails."""
        # Mock schema adapter to fail
        postgres_loader.schema_adapter.create_postgres_table.return_value = False
        
        # Mock table existence check
        with patch('etl_pipeline.loaders.postgres_loader.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.has_table.return_value = False
            mock_inspect.return_value = mock_inspector
            
            result = postgres_loader.load_table('test_table', sample_mysql_schema, force_full=False)
            
            assert result is False
    
    def test_load_table_database_error(self, postgres_loader, sample_mysql_schema):
        """Test table loading with database error."""
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


@pytest.mark.unit
class TestLoadTableChunkedUnit:
    """Unit tests for chunked loading functionality."""
    
    def test_load_table_chunked_success(self, postgres_loader, sample_mysql_schema):
        """Test successful chunked table loading."""
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
    
    def test_load_table_chunked_full_load(self, postgres_loader, sample_mysql_schema):
        """Test chunked loading with full load (truncate)."""
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
    
    def test_load_table_chunked_no_data(self, postgres_loader, sample_mysql_schema):
        """Test chunked loading with no data."""
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
    
    def test_load_table_chunked_database_error(self, postgres_loader, sample_mysql_schema):
        """Test chunked loading with database error."""
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


@pytest.mark.unit
class TestVerifyLoadUnit:
    """Unit tests for load verification functionality."""
    
    def test_verify_load_success(self, postgres_loader):
        """Test successful load verification."""
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
        
        # Verify count queries were executed
        source_call = mock_source_conn.execute.call_args
        target_call = mock_target_conn.execute.call_args
        assert 'SELECT COUNT(*)' in str(source_call[0][0])
        assert 'SELECT COUNT(*)' in str(target_call[0][0])
    
    def test_verify_load_count_mismatch(self, postgres_loader):
        """Test load verification with count mismatch."""
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
    
    def test_verify_load_database_error(self, postgres_loader):
        """Test load verification with database error."""
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


@pytest.mark.unit
class TestUtilityMethodsUnit:
    """Unit tests for utility methods."""
    
    def test_build_load_query_full_load(self, postgres_loader):
        """Test query building for full load."""
        query = postgres_loader._build_load_query('test_table', ['created_at'], force_full=True)
        
        assert 'SELECT * FROM test_table' in query
        assert 'WHERE' not in query  # No incremental conditions
    
    def test_build_load_query_incremental_load(self, postgres_loader):
        """Test query building for incremental load."""
        # Mock last load timestamp
        with patch.object(postgres_loader, '_get_last_load', return_value=datetime(2023, 1, 1, 10, 0, 0)):
            query = postgres_loader._build_load_query('test_table', ['created_at'], force_full=False)
            
            assert 'SELECT * FROM test_table' in query
            assert 'WHERE' in query
            assert "created_at > '2023-01-01 10:00:00'" in query
    
    def test_build_load_query_no_incremental_columns(self, postgres_loader):
        """Test query building with no incremental columns."""
        query = postgres_loader._build_load_query('test_table', [], force_full=False)
        
        assert 'SELECT * FROM test_table' in query
        assert 'WHERE' not in query  # No incremental conditions
    
    def test_build_count_query_full_load(self, postgres_loader):
        """Test count query building for full load."""
        query = postgres_loader._build_count_query('test_table', ['created_at'], force_full=True)
        
        assert 'SELECT COUNT(*) FROM test_table' in query
        assert 'WHERE' not in query  # No incremental conditions
    
    def test_build_count_query_incremental_load(self, postgres_loader):
        """Test count query building for incremental load."""
        # Mock last load timestamp
        with patch.object(postgres_loader, '_get_last_load', return_value=datetime(2023, 1, 1, 10, 0, 0)):
            query = postgres_loader._build_count_query('test_table', ['created_at'], force_full=False)
            
            assert 'SELECT COUNT(*) FROM test_table' in query
            assert 'WHERE' in query
            assert "created_at > '2023-01-01 10:00:00'" in query
    
    def test_get_last_load_success(self, postgres_loader):
        """Test successful last load timestamp retrieval."""
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
        
        # Verify query was executed
        call_args = mock_target_conn.execute.call_args
        assert 'SELECT MAX(last_loaded)' in str(call_args[0][0])
        assert 'etl_load_status' in str(call_args[0][0])
    
    def test_get_last_load_no_timestamp(self, postgres_loader):
        """Test last load timestamp retrieval when no timestamp exists."""
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
    
    def test_get_last_load_database_error(self, postgres_loader):
        """Test last load timestamp retrieval with database error."""
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
    
    def test_convert_row_data_types_success(self, postgres_loader):
        """Test successful data type conversion."""
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
    
    def test_convert_row_data_types_error(self, postgres_loader):
        """Test data type conversion with error."""
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


@pytest.mark.unit
class TestSchemaIntegrationUnit:
    """Unit tests for schema integration with PostgresSchema."""
    
    def test_ensure_postgres_table_create_new(self, postgres_loader, sample_mysql_schema):
        """Test ensuring PostgreSQL table when table doesn't exist."""
        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = False
        
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            result = postgres_loader._ensure_postgres_table('test_table', sample_mysql_schema)
            
            assert result is True
            postgres_loader.schema_adapter.create_postgres_table.assert_called_once_with('test_table', sample_mysql_schema)
    
    def test_ensure_postgres_table_verify_existing(self, postgres_loader, sample_mysql_schema):
        """Test ensuring PostgreSQL table when table exists."""
        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = True
        
        # Mock schema adapter
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            result = postgres_loader._ensure_postgres_table('test_table', sample_mysql_schema)
            
            assert result is True
            postgres_loader.schema_adapter.verify_schema.assert_called_once_with('test_table', sample_mysql_schema)
    
    def test_ensure_postgres_table_creation_failure(self, postgres_loader, sample_mysql_schema):
        """Test ensuring PostgreSQL table when creation fails."""
        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = False
        
        # Mock schema adapter to fail
        postgres_loader.schema_adapter.create_postgres_table.return_value = False
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            result = postgres_loader._ensure_postgres_table('test_table', sample_mysql_schema)
            
            assert result is False
    
    def test_ensure_postgres_table_verification_failure(self, postgres_loader, sample_mysql_schema):
        """Test ensuring PostgreSQL table when verification fails."""
        # Mock inspector
        mock_inspector = MagicMock()
        mock_inspector.has_table.return_value = True
        
        # Mock schema adapter to fail verification
        postgres_loader.schema_adapter.verify_schema.return_value = False
        
        with patch('etl_pipeline.loaders.postgres_loader.inspect', return_value=mock_inspector):
            result = postgres_loader._ensure_postgres_table('test_table', sample_mysql_schema)
            
            assert result is False


@pytest.mark.unit
class TestNewConfigurationSystemUnit:
    """Unit tests for new configuration system integration."""
    
    def test_initialization_with_new_config_system(self, test_settings, database_types, postgres_schemas):
        """Test PostgresLoader initialization with new configuration system."""
        # Mock engines using new configuration system
        mock_replication_engine = MagicMock(spec=Engine)
        mock_analytics_engine = MagicMock(spec=Engine)
        
        with patch('etl_pipeline.core.connections.ConnectionFactory.get_replication_connection') as mock_get_repl:
            mock_get_repl.return_value = mock_replication_engine
            
            with patch('etl_pipeline.core.connections.ConnectionFactory.get_analytics_connection') as mock_get_analytics:
                mock_get_analytics.return_value = mock_analytics_engine
                
                # Mock PostgresSchema to prevent inspection of mock engines
                with patch('etl_pipeline.loaders.postgres_loader.PostgresSchema') as mock_schema_class:
                    mock_schema_adapter = MagicMock()
                    mock_schema_class.return_value = mock_schema_adapter
                    
                    # Mock get_settings to return a mock settings object
                    with patch('etl_pipeline.loaders.postgres_loader.get_settings') as mock_get_settings:
                        mock_settings = MagicMock()
                        mock_settings.get_database_config.side_effect = lambda db_type, schema=None: {
                            ('analytics', 'raw'): {'schema': 'raw'},
                            ('replication', None): {'schema': 'raw'}
                        }.get((db_type, schema), {})
                        mock_get_settings.return_value = mock_settings
                        
                        # Test with actual constructor signature
                        loader = PostgresLoader(
                            replication_engine=mock_replication_engine,
                            analytics_engine=mock_analytics_engine
                        )
                        
                        assert loader.replication_engine == mock_replication_engine
                        assert loader.analytics_engine == mock_analytics_engine
    
    def test_load_table_with_enum_types(self, postgres_loader, sample_mysql_schema, database_types, postgres_schemas):
        """Test table loading using enum-based database types."""
        # Mock schema adapter
        postgres_loader.schema_adapter.create_postgres_table.return_value = True
        postgres_loader.schema_adapter.verify_schema.return_value = True
        
        # Mock source connection and result
        mock_source_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name', 'created_at']
        mock_result.fetchall.return_value = [
            (1, 'John Doe', datetime(2023, 1, 1, 10, 0, 0))
        ]
        mock_source_conn.execute.return_value = mock_result
        
        # Mock target connection
        mock_target_conn = MagicMock()
        
        # Set up engine mocks
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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "unit"]) 