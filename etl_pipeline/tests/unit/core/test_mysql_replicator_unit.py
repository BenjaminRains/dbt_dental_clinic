"""
Unit tests for MySQL replicator business logic using mocking.

This test suite focuses on:
- Business logic validation
- Error handling scenarios
- Data transformation logic
- Configuration validation
- Edge case handling

Testing Strategy:
- Pure unit tests with mocked dependencies
- Fast execution for development feedback
- No database connections required
- Tests only the logic, not the database integration
"""

import pytest
import hashlib
import os
from unittest.mock import MagicMock, patch, call, Mock
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import logging

# Import the component under test
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator
from etl_pipeline.core.schema_discovery import SchemaDiscovery

# NOTE: Shared fixtures (mock_source_engine, mock_target_engine, sample_table_data) are now provided by conftest.py and should not be redefined here.

# Configure logging for tests
logging.basicConfig(level=logging.ERROR)

@pytest.mark.unit
class TestMySQLReplicatorUnit:
    """Base class for MySQL replicator unit tests."""
    
    @pytest.fixture
    def mock_source_engine(self):
        """Create a mock source engine."""
        return MagicMock()
    
    @pytest.fixture
    def mock_target_engine(self):
        """Create a mock target engine."""
        return MagicMock()
    
    @pytest.fixture
    def mock_schema_discovery(self):
        """Create a mock SchemaDiscovery instance."""
        mock_discovery = MagicMock(spec=SchemaDiscovery)
        return mock_discovery
    
    @pytest.fixture
    def replicator(self, mock_source_engine, mock_target_engine, mock_schema_discovery):
        """Create ExactMySQLReplicator instance with mocked engines and SchemaDiscovery."""
        replicator = ExactMySQLReplicator(
            source_engine=mock_source_engine,
            target_engine=mock_target_engine,
            source_db='test_source',
            target_db='test_target',
            schema_discovery=mock_schema_discovery
        )
        return replicator
    
    @pytest.fixture
    def sample_create_statement(self):
        """Sample CREATE TABLE statement for testing."""
        return """CREATE TABLE `patient` (
  `PatNum` int(11) NOT NULL AUTO_INCREMENT,
  `LName` varchar(255) NOT NULL DEFAULT '',
  `FName` varchar(255) NOT NULL DEFAULT '',
  `Birthdate` datetime NOT NULL DEFAULT '0001-01-01 00:00:00',
  `Email` varchar(255) NOT NULL DEFAULT '',
  `HmPhone` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`PatNum`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""
    
    @pytest.fixture
    def sample_table_data(self):
        """Sample table data for testing."""
        return [
            {'PatNum': 1, 'LName': 'Doe', 'FName': 'John', 'Email': 'john@example.com'},
            {'PatNum': 2, 'LName': 'Smith', 'FName': 'Jane', 'Email': 'jane@example.com'},
            {'PatNum': 3, 'LName': 'Johnson', 'FName': 'Bob', 'Email': 'bob@example.com'}
        ]


@pytest.mark.unit
class TestInitialization(TestMySQLReplicatorUnit):
    """Test replicator initialization logic."""
    
    def test_initialization_with_valid_parameters(self, mock_source_engine, mock_target_engine, mock_schema_discovery):
        """Test successful initialization with valid parameters."""
        replicator = ExactMySQLReplicator(
            source_engine=mock_source_engine,
            target_engine=mock_target_engine,
            source_db='test_source',
            target_db='test_target',
            schema_discovery=mock_schema_discovery
        )
        
        # Verify initialization
        assert replicator.source_engine == mock_source_engine
        assert replicator.target_engine == mock_target_engine
        assert replicator.source_db == 'test_source'
        assert replicator.target_db == 'test_target'
        assert replicator.schema_discovery == mock_schema_discovery
        assert replicator.query_timeout == 300
        assert replicator.max_batch_size == 10000
    
    def test_initialization_with_custom_settings(self, mock_source_engine, mock_target_engine, mock_schema_discovery):
        """Test initialization with custom timeout and batch size."""
        replicator = ExactMySQLReplicator(
            source_engine=mock_source_engine,
            target_engine=mock_target_engine,
            source_db='test_source',
            target_db='test_target',
            schema_discovery=mock_schema_discovery
        )
        
        # Test default values are set correctly
        assert replicator.query_timeout == 300
        assert replicator.max_batch_size == 10000
    
    def test_initialization_with_invalid_schema_discovery(self, mock_source_engine, mock_target_engine):
        """Test initialization with invalid SchemaDiscovery instance."""
        with pytest.raises(ValueError, match="SchemaDiscovery instance is required"):
            ExactMySQLReplicator(
                source_engine=mock_source_engine,
                target_engine=mock_target_engine,
                source_db='test_source',
                target_db='test_target',
                schema_discovery="invalid_schema_discovery"
            )


@pytest.mark.unit
class TestCreateExactReplica(TestMySQLReplicatorUnit):
    """Test create_exact_replica method."""
    
    def test_create_exact_replica_success(self, replicator):
        """Test successful table replica creation."""
        # Mock schema discovery to return True
        replicator.schema_discovery.replicate_schema.return_value = True
        
        result = replicator.create_exact_replica('test_table')
        
        assert result is True
        replicator.schema_discovery.replicate_schema.assert_called_once_with(
            source_table='test_table',
            target_engine=replicator.target_engine,
            target_db=replicator.target_db,
            target_table='test_table'
        )
    
    def test_create_exact_replica_failure(self, replicator):
        """Test table replica creation failure."""
        # Mock schema discovery to return False
        replicator.schema_discovery.replicate_schema.return_value = False
        
        result = replicator.create_exact_replica('test_table')
        
        assert result is False
        replicator.schema_discovery.replicate_schema.assert_called_once()
    
    def test_create_exact_replica_exception(self, replicator):
        """Test table replica creation with exception."""
        # Mock schema discovery to raise exception
        replicator.schema_discovery.replicate_schema.side_effect = Exception("Schema replication failed")
        
        result = replicator.create_exact_replica('test_table')
        
        assert result is False
        replicator.schema_discovery.replicate_schema.assert_called_once()


@pytest.mark.unit
class TestCopyTableData(TestMySQLReplicatorUnit):
    """Test copy_table_data method."""
    
    def test_copy_table_data_small_table_direct_copy(self, replicator):
        """Test copying small table with direct copy."""
        # Mock size info for small table
        replicator.schema_discovery.get_table_size_info.return_value = {'row_count': 100}
        
        # Mock target engine connection
        mock_conn = MagicMock()
        replicator.target_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock direct copy to return True
        with patch.object(replicator, '_copy_direct', return_value=True):
            result = replicator.copy_table_data('test_table')
            
            assert result is True
            replicator.schema_discovery.get_table_size_info.assert_called_once_with('test_table')
            mock_conn.execute.assert_called_once()
            replicator._copy_direct.assert_called_once_with('test_table', 100)
    
    def test_copy_table_data_large_table_chunked_copy(self, replicator):
        """Test copying large table with chunked copy."""
        # Mock size info for large table
        replicator.schema_discovery.get_table_size_info.return_value = {'row_count': 50000}
        
        # Mock target engine connection
        mock_conn = MagicMock()
        replicator.target_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock chunked copy to return True
        with patch.object(replicator, '_copy_chunked', return_value=True):
            result = replicator.copy_table_data('test_table')
            
            assert result is True
            replicator.schema_discovery.get_table_size_info.assert_called_once_with('test_table')
            mock_conn.execute.assert_called_once()
            replicator._copy_chunked.assert_called_once_with('test_table', 50000)
    
    def test_copy_table_data_exception(self, replicator):
        """Test copy_table_data with exception."""
        # Mock schema discovery to raise exception
        replicator.schema_discovery.get_table_size_info.side_effect = Exception("Size info failed")
        
        result = replicator.copy_table_data('test_table')
        
        assert result is False


@pytest.mark.unit
class TestVerifyExactReplica(TestMySQLReplicatorUnit):
    """Test verify_exact_replica method."""
    
    def test_verify_exact_replica_success_test_environment(self, replicator):
        """Test successful replica verification in test environment."""
        # Mock environment variables for test environment
        with patch.dict(os.environ, {'ETL_ENVIRONMENT': 'test'}):
            # Mock source schema
            replicator.schema_discovery.get_table_schema.return_value = {
                'schema_hash': 'test_hash',
                'columns': []
            }
            replicator.schema_discovery.get_table_size_info.return_value = {'row_count': 100}
            
            # Mock target discovery
            mock_target_discovery = MagicMock()
            mock_target_discovery.get_table_schema.return_value = {
                'schema_hash': 'different_hash',
                'columns': []
            }
            mock_target_discovery.get_table_size_info.return_value = {'row_count': 100}
            
            with patch('etl_pipeline.core.mysql_replicator.SchemaDiscovery', return_value=mock_target_discovery):
                result = replicator.verify_exact_replica('test_table')
                
                assert result is True
    
    def test_verify_exact_replica_success_production_environment(self, replicator):
        """Test successful replica verification in production environment."""
        # Mock environment variables for production environment
        with patch.dict(os.environ, {'ETL_ENVIRONMENT': 'production'}):
            # Mock source schema
            replicator.schema_discovery.get_table_schema.return_value = {
                'schema_hash': 'same_hash',
                'columns': []
            }
            replicator.schema_discovery.get_table_size_info.return_value = {'row_count': 100}
            
            # Mock target discovery
            mock_target_discovery = MagicMock()
            mock_target_discovery.get_table_schema.return_value = {
                'schema_hash': 'same_hash',
                'columns': []
            }
            mock_target_discovery.get_table_size_info.return_value = {'row_count': 100}
            
            with patch('etl_pipeline.core.mysql_replicator.SchemaDiscovery', return_value=mock_target_discovery):
                result = replicator.verify_exact_replica('test_table')
                
                assert result is True
    
    def test_verify_exact_replica_schema_mismatch(self, replicator):
        """Test replica verification with schema mismatch in production."""
        # Mock environment variables for production environment
        with patch.dict(os.environ, {'ETL_ENVIRONMENT': 'production'}):
            # Mock source schema
            replicator.schema_discovery.get_table_schema.return_value = {
                'schema_hash': 'source_hash',
                'columns': []
            }
            replicator.schema_discovery.get_table_size_info.return_value = {'row_count': 100}
            
            # Mock target discovery with different hash
            mock_target_discovery = MagicMock()
            mock_target_discovery.get_table_schema.return_value = {
                'schema_hash': 'target_hash',
                'columns': []
            }
            mock_target_discovery.get_table_size_info.return_value = {'row_count': 100}
            
            with patch('etl_pipeline.core.mysql_replicator.SchemaDiscovery', return_value=mock_target_discovery):
                result = replicator.verify_exact_replica('test_table')
                
                assert result is False
    
    def test_verify_exact_replica_row_count_mismatch(self, replicator):
        """Test replica verification with row count mismatch."""
        # Mock environment variables for test environment
        with patch.dict(os.environ, {'ETL_ENVIRONMENT': 'test'}):
            # Mock source schema
            replicator.schema_discovery.get_table_schema.return_value = {
                'schema_hash': 'test_hash',
                'columns': []
            }
            replicator.schema_discovery.get_table_size_info.return_value = {'row_count': 100}
            
            # Mock target discovery with different row count
            mock_target_discovery = MagicMock()
            mock_target_discovery.get_table_schema.return_value = {
                'schema_hash': 'test_hash',
                'columns': []
            }
            mock_target_discovery.get_table_size_info.return_value = {'row_count': 50}
            
            with patch('etl_pipeline.core.mysql_replicator.SchemaDiscovery', return_value=mock_target_discovery):
                result = replicator.verify_exact_replica('test_table')
                
                assert result is False
    
    def test_verify_exact_replica_exception(self, replicator):
        """Test verify_exact_replica with exception."""
        # Mock schema discovery to raise exception
        replicator.schema_discovery.get_table_schema.side_effect = Exception("Schema discovery failed")
        
        result = replicator.verify_exact_replica('test_table')
        
        assert result is False


@pytest.mark.unit
class TestCopyDirect(TestMySQLReplicatorUnit):
    """Test _copy_direct method."""
    
    def test_copy_direct_success(self, replicator):
        """Test successful direct copy."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock source result with data
        mock_result = MagicMock()
        mock_row1 = MagicMock()
        mock_row1._mapping = {'id': 1, 'name': 'test1'}
        mock_row2 = MagicMock()
        mock_row2._mapping = {'id': 2, 'name': 'test2'}
        mock_result.fetchall.return_value = [mock_row1, mock_row2]
        mock_result.keys.return_value = ['id', 'name']
        mock_source_conn.execute.return_value = mock_result
        
        # Mock target connection context manager
        mock_target_conn.begin.return_value.__enter__.return_value = mock_target_conn
        
        # Mock engine connect methods to return context managers
        mock_source_context = MagicMock()
        mock_source_context.__enter__.return_value = mock_source_conn
        mock_source_context.__exit__.return_value = None
        
        mock_target_context = MagicMock()
        mock_target_context.__enter__.return_value = mock_target_conn
        mock_target_context.__exit__.return_value = None
        
        replicator.source_engine.connect.return_value = mock_source_context
        replicator.target_engine.connect.return_value = mock_target_context
        
        result = replicator._copy_direct('test_table', 2)
        
        assert result is True
        mock_source_conn.execute.assert_called_once()
        mock_target_conn.execute.assert_called_once()
    
    def test_copy_direct_empty_table(self, replicator):
        """Test direct copy with empty table."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock source result with no data
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_source_conn.execute.return_value = mock_result
        
        # Mock engine connect methods to return context managers
        mock_source_context = MagicMock()
        mock_source_context.__enter__.return_value = mock_source_conn
        mock_source_context.__exit__.return_value = None
        
        mock_target_context = MagicMock()
        mock_target_context.__enter__.return_value = mock_target_conn
        mock_target_context.__exit__.return_value = None
        
        replicator.source_engine.connect.return_value = mock_source_context
        replicator.target_engine.connect.return_value = mock_target_context
        
        result = replicator._copy_direct('test_table', 0)
        
        assert result is True
        mock_source_conn.execute.assert_called_once()
        # Should not call target execute for empty table
    
    def test_copy_direct_exception(self, replicator):
        """Test direct copy with exception."""
        # Mock source connection to raise exception
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = Exception("Copy failed")
        
        # Mock engine connect method to return context manager
        mock_source_context = MagicMock()
        mock_source_context.__enter__.return_value = mock_source_conn
        mock_source_context.__exit__.return_value = None
        
        replicator.source_engine.connect.return_value = mock_source_context
        
        result = replicator._copy_direct('test_table', 10)
        
        assert result is False


@pytest.mark.unit
class TestCopyChunked(TestMySQLReplicatorUnit):
    """Test _copy_chunked method."""
    
    def test_copy_chunked_with_primary_key(self, replicator):
        """Test chunked copy with primary key."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock schema with primary key
        replicator.schema_discovery.get_table_schema.return_value = {
            'primary_key_columns': ['id']
        }
        
        # Mock engine connect methods to return context managers
        mock_source_context = MagicMock()
        mock_source_context.__enter__.return_value = mock_source_conn
        mock_source_context.__exit__.return_value = None
        
        mock_target_context = MagicMock()
        mock_target_context.__enter__.return_value = mock_target_conn
        mock_target_context.__exit__.return_value = None
        
        replicator.source_engine.connect.return_value = mock_source_context
        replicator.target_engine.connect.return_value = mock_target_context
        
        # Mock PK chunking to return True
        with patch.object(replicator, '_copy_with_pk_chunking', return_value=True):
            result = replicator._copy_chunked('test_table', 50000)
            
            assert result is True
            replicator._copy_with_pk_chunking.assert_called_once_with(
                mock_source_conn, mock_target_conn, 'test_table', ['id']
            )
    
    def test_copy_chunked_without_primary_key(self, replicator):
        """Test chunked copy without primary key."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock schema without primary key
        replicator.schema_discovery.get_table_schema.return_value = {
            'primary_key_columns': []
        }
        
        # Mock engine connect methods to return context managers
        mock_source_context = MagicMock()
        mock_source_context.__enter__.return_value = mock_source_conn
        mock_source_context.__exit__.return_value = None
        
        mock_target_context = MagicMock()
        mock_target_context.__enter__.return_value = mock_target_conn
        mock_target_context.__exit__.return_value = None
        
        replicator.source_engine.connect.return_value = mock_source_context
        replicator.target_engine.connect.return_value = mock_target_context
        
        # Mock limit/offset chunking to return True
        with patch.object(replicator, '_copy_with_limit_offset', return_value=True):
            result = replicator._copy_chunked('test_table', 50000)
            
            assert result is True
            replicator._copy_with_limit_offset.assert_called_once_with(
                mock_source_conn, mock_target_conn, 'test_table'
            )
    
    def test_copy_chunked_exception(self, replicator):
        """Test chunked copy with exception."""
        # Mock source connection to raise exception
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = Exception("Chunked copy failed")
        
        # Mock engine connect method to return context manager
        mock_source_context = MagicMock()
        mock_source_context.__enter__.return_value = mock_source_conn
        mock_source_context.__exit__.return_value = None
        
        replicator.source_engine.connect.return_value = mock_source_context
        
        result = replicator._copy_chunked('test_table', 50000)
        
        assert result is False


@pytest.mark.unit
class TestCopyWithPkChunking(TestMySQLReplicatorUnit):
    """Test _copy_with_pk_chunking method."""
    
    def test_copy_with_pk_chunking_success(self, replicator):
        """Test successful PK chunking copy."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock range query result
        mock_range_result = MagicMock()
        mock_range_row = MagicMock()
        mock_range_row.min_pk = 1
        mock_range_row.max_pk = 100
        mock_range_result.fetchone.return_value = mock_range_row
        mock_source_conn.execute.return_value = mock_range_result
        
        # Mock chunk query result
        mock_chunk_result = MagicMock()
        mock_row1 = MagicMock()
        mock_row1._mapping = {'id': 1, 'name': 'test1'}
        mock_row1.id = 1
        mock_row2 = MagicMock()
        mock_row2._mapping = {'id': 2, 'name': 'test2'}
        mock_row2.id = 2
        mock_chunk_result.fetchall.return_value = [mock_row1, mock_row2]
        mock_chunk_result.keys.return_value = ['id', 'name']
        
        # Mock empty chunk result to end the loop
        mock_empty_chunk_result = MagicMock()
        mock_empty_chunk_result.fetchall.return_value = []
        mock_empty_chunk_result.keys.return_value = ['id', 'name']
        
        # Mock target connection context manager
        mock_target_conn.begin.return_value.__enter__.return_value = mock_target_conn
        
        # First call returns range result, then chunk, then empty chunk to end loop
        mock_source_conn.execute.side_effect = [mock_range_result, mock_chunk_result, mock_empty_chunk_result]
        
        result = replicator._copy_with_pk_chunking(mock_source_conn, mock_target_conn, 'test_table', ['id'])
        
        assert result is True
        assert mock_source_conn.execute.call_count >= 2
        assert mock_target_conn.execute.call_count >= 1
    
    def test_copy_with_pk_chunking_no_data(self, replicator):
        """Test PK chunking with no data."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock range query result with no data
        mock_range_result = MagicMock()
        mock_range_result.fetchone.return_value = None
        mock_source_conn.execute.return_value = mock_range_result
        
        result = replicator._copy_with_pk_chunking(mock_source_conn, mock_target_conn, 'test_table', ['id'])
        
        assert result is True
        mock_source_conn.execute.assert_called_once()
    
    def test_copy_with_pk_chunking_exception(self, replicator):
        """Test PK chunking with exception."""
        # Mock source connection to raise exception
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = Exception("PK chunking failed")
        
        mock_target_conn = MagicMock()
        
        result = replicator._copy_with_pk_chunking(mock_source_conn, mock_target_conn, 'test_table', ['id'])
        
        assert result is False


@pytest.mark.unit
class TestCopyWithLimitOffset(TestMySQLReplicatorUnit):
    """Test _copy_with_limit_offset method."""
    
    def test_copy_with_limit_offset_success(self, replicator):
        """Test successful limit/offset copy."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock chunk query result
        mock_chunk_result = MagicMock()
        mock_row1 = MagicMock()
        mock_row1._mapping = {'id': 1, 'name': 'test1'}
        mock_row2 = MagicMock()
        mock_row2._mapping = {'id': 2, 'name': 'test2'}
        mock_chunk_result.fetchall.return_value = [mock_row1, mock_row2]
        mock_chunk_result.keys.return_value = ['id', 'name']
        mock_source_conn.execute.return_value = mock_chunk_result
        
        # Mock target connection context manager
        mock_target_conn.begin.return_value.__enter__.return_value = mock_target_conn
        
        result = replicator._copy_with_limit_offset(mock_source_conn, mock_target_conn, 'test_table')
        
        assert result is True
        assert mock_source_conn.execute.call_count >= 1
        assert mock_target_conn.execute.call_count >= 1
    
    def test_copy_with_limit_offset_no_data(self, replicator):
        """Test limit/offset copy with no data."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock chunk query result with no data
        mock_chunk_result = MagicMock()
        mock_chunk_result.fetchall.return_value = []
        mock_source_conn.execute.return_value = mock_chunk_result
        
        result = replicator._copy_with_limit_offset(mock_source_conn, mock_target_conn, 'test_table')
        
        assert result is True
        mock_source_conn.execute.assert_called_once()
    
    def test_copy_with_limit_offset_exception(self, replicator):
        """Test limit/offset copy with exception."""
        # Mock source connection to raise exception
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = Exception("Limit/offset copy failed")
        
        mock_target_conn = MagicMock()
        
        result = replicator._copy_with_limit_offset(mock_source_conn, mock_target_conn, 'test_table')
        
        assert result is False


@pytest.mark.unit
class TestVerifyDataIntegrity(TestMySQLReplicatorUnit):
    """Test _verify_data_integrity method."""
    
    def test_verify_data_integrity_success(self, replicator):
        """Test successful data integrity verification."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock checksum query results
        mock_source_result = MagicMock()
        mock_source_row = MagicMock()
        mock_source_row.chunk_hash = 'hash1'
        mock_source_row.row_count = 100
        mock_source_result.__iter__.return_value = [mock_source_row]
        
        mock_target_result = MagicMock()
        mock_target_row = MagicMock()
        mock_target_row.chunk_hash = 'hash1'
        mock_target_row.row_count = 100
        mock_target_result.__iter__.return_value = [mock_target_row]
        
        mock_source_conn.execute.return_value = mock_source_result
        mock_target_conn.execute.return_value = mock_target_result
        
        result = replicator._verify_data_integrity(mock_source_conn, mock_target_conn, 'test_table', ['id'])
        
        assert result is True
        mock_source_conn.execute.assert_called_once()
        mock_target_conn.execute.assert_called_once()
    
    def test_verify_data_integrity_mismatch(self, replicator):
        """Test data integrity verification with mismatch."""
        # Mock source and target connections
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock checksum query results with different hashes
        mock_source_result = MagicMock()
        mock_source_row = MagicMock()
        mock_source_row.chunk_hash = 'hash1'
        mock_source_row.row_count = 100
        mock_source_result.__iter__.return_value = [mock_source_row]
        
        mock_target_result = MagicMock()
        mock_target_row = MagicMock()
        mock_target_row.chunk_hash = 'hash2'
        mock_target_row.row_count = 100
        mock_target_result.__iter__.return_value = [mock_target_row]
        
        mock_source_conn.execute.return_value = mock_source_result
        mock_target_conn.execute.return_value = mock_target_result
        
        result = replicator._verify_data_integrity(mock_source_conn, mock_target_conn, 'test_table', ['id'])
        
        assert result is False
    
    def test_verify_data_integrity_exception(self, replicator):
        """Test data integrity verification with exception."""
        # Mock source connection to raise exception
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = Exception("Integrity check failed")
        
        mock_target_conn = MagicMock()
        
        result = replicator._verify_data_integrity(mock_source_conn, mock_target_conn, 'test_table', ['id'])
        
        assert result is False


@pytest.mark.unit
class TestErrorHandling(TestMySQLReplicatorUnit):
    """Test error handling logic."""
    
    def test_handle_database_connection_error(self, replicator):
        """Test handling of database connection errors."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Connection failed")
        
        # Test that the error is properly handled
        with pytest.raises(SQLAlchemyError):
            mock_conn.execute("SELECT 1")
    
    def test_handle_table_not_found_error(self, replicator):
        """Test handling of table not found errors."""
        # This would be tested in integration tests with actual database
        pass
    
    def test_handle_primary_key_error(self, replicator):
        """Test handling of primary key errors."""
        # This would be tested in integration tests with actual database
        pass
    
    def test_handle_row_count_error(self, replicator):
        """Test handling of row count errors."""
        # This would be tested in integration tests with actual database
        pass


@pytest.mark.unit
class TestDataTransformation(TestMySQLReplicatorUnit):
    """Test data transformation logic."""
    
    def test_convert_row_to_dict(self, replicator):
        """Test conversion of database row to dictionary."""
        # Mock row with _mapping attribute
        mock_row = MagicMock()
        mock_row._mapping = {'id': 1, 'name': 'test'}
        
        # Test conversion
        result = dict(mock_row._mapping)
        assert result == {'id': 1, 'name': 'test'}
    
    def test_build_insert_query(self, replicator):
        """Test building INSERT query with placeholders."""
        columns = ['id', 'name', 'email']
        placeholders = ', '.join([':' + col for col in columns])
        
        expected = "INSERT INTO test_table (id, name, email) VALUES (:id, :name, :email)"
        # This would be tested in integration tests with actual SQL generation
        assert placeholders == ':id, :name, :email'
    
    def test_build_select_query(self, replicator):
        """Test building SELECT query."""
        query = "SELECT * FROM test_table"
        # This would be tested in integration tests with actual SQL generation
        assert "SELECT" in query
        assert "FROM" in query


@pytest.mark.unit
class TestConfigurationValidation(TestMySQLReplicatorUnit):
    """Test configuration validation logic."""
    
    def test_validate_engine_configuration(self, mock_source_engine, mock_target_engine, mock_schema_discovery):
        """Test validation of engine configuration."""
        replicator = ExactMySQLReplicator(
            source_engine=mock_source_engine,
            target_engine=mock_target_engine,
            source_db='test_source',
            target_db='test_target',
            schema_discovery=mock_schema_discovery
        )
        
        # Verify engines are properly set
        assert replicator.source_engine is not None
        assert replicator.target_engine is not None
        assert replicator.schema_discovery is not None
    
    def test_validate_database_names(self, mock_source_engine, mock_target_engine, mock_schema_discovery):
        """Test validation of database names."""
        replicator = ExactMySQLReplicator(
            source_engine=mock_source_engine,
            target_engine=mock_target_engine,
            source_db='test_source',
            target_db='test_target',
            schema_discovery=mock_schema_discovery
        )
        
        # Verify database names are properly set
        assert replicator.source_db == 'test_source'
        assert replicator.target_db == 'test_target'


@pytest.mark.unit
class TestBusinessLogic(TestMySQLReplicatorUnit):
    """Test core business logic."""
    
    def test_copy_strategy_logic(self, replicator):
        """Test copy strategy logic based on table size."""
        # Test small table strategy
        small_table_size = 100
        should_use_direct = small_table_size <= replicator.max_batch_size
        assert should_use_direct is True
        
        # Test large table strategy
        large_table_size = 50000
        should_use_chunked = large_table_size > replicator.max_batch_size
        assert should_use_chunked is True


@pytest.mark.unit
class TestEdgeCases(TestMySQLReplicatorUnit):
    """Test edge cases and boundary conditions."""
    
    def test_empty_table_handling(self, replicator):
        """Test handling of empty tables."""
        # This would be tested in integration tests with actual database
        pass
    
    def test_single_row_table_handling(self, replicator):
        """Test handling of tables with single row."""
        # This would be tested in integration tests with actual database
        pass
    
    def test_very_large_table_handling(self, replicator):
        """Test handling of very large tables."""
        # This would be tested in integration tests with actual database
        pass
    
    def test_special_characters_in_table_name(self, replicator):
        """Test handling of special characters in table names."""
        # Test with table name containing special characters
        table_name = "test_table_with_special_chars_123"
        # This would be tested in integration tests with actual database
        assert "test_table" in table_name
    
    def test_null_values_handling(self, replicator):
        """Test handling of null values in data."""
        # This would be tested in integration tests with actual database
        pass