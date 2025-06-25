"""
Comprehensive tests for MySQL replicator focused on exact table replication and schema management.

This test suite addresses critical testing gaps identified in TESTING_PLAN.md:
- Current coverage: 11% → Target: 85%
- Critical for production deployment
- Tests exact schema replication, data copying, and validation

Testing Strategy:
- Unit tests with mocked database connections
- Integration tests with real test databases
- Performance tests with large datasets
- Error handling and edge case validation
- Schema change detection and handling
"""

# NOTE: Shared fixtures (mock_source_engine, mock_target_engine, replicator, sample_create_statement, sample_table_data) are now provided by conftest.py and should not be redefined here.

import pytest
import hashlib
from unittest.mock import MagicMock, patch, call
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging

# Import the component under test
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator


@pytest.mark.unit
class TestExactMySQLReplicator:
    """Unit tests for ExactMySQLReplicator class with comprehensive mocking."""
    
    @pytest.fixture
    def mock_source_engine(self):
        """Mock source database engine."""
        engine = MagicMock(spec=Engine)
        engine.name = 'mysql'
        return engine
    
    @pytest.fixture
    def mock_target_engine(self):
        """Mock target database engine."""
        engine = MagicMock(spec=Engine)
        engine.name = 'mysql'
        return engine
    
    @pytest.fixture
    def replicator(self, mock_source_engine, mock_target_engine):
        """Create ExactMySQLReplicator instance with mocked engines."""
        with patch('etl_pipeline.core.mysql_replicator.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspect.return_value = mock_inspector
            
            replicator = ExactMySQLReplicator(
                source_engine=mock_source_engine,
                target_engine=mock_target_engine,
                source_db='test_source',
                target_db='test_target'
            )
            replicator.inspector = mock_inspector
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
class TestInitialization:
    """Test replicator initialization."""
    
    def test_initialization_with_valid_engines(self, mock_source_engine, mock_target_engine):
        """Test successful initialization with valid database engines."""
        with patch('etl_pipeline.core.mysql_replicator.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspect.return_value = mock_inspector
            
            replicator = ExactMySQLReplicator(
                source_engine=mock_source_engine,
                target_engine=mock_target_engine,
                source_db='test_source',
                target_db='test_target'
            )
            
            assert replicator.source_engine == mock_source_engine
            assert replicator.target_engine == mock_target_engine
            assert replicator.source_db == 'test_source'
            assert replicator.target_db == 'test_target'
            assert replicator.inspector == mock_inspector
            assert replicator.query_timeout == 300
            assert replicator.max_batch_size == 10000
            mock_inspect.assert_called_once_with(mock_source_engine)
    
    def test_initialization_with_custom_settings(self, mock_source_engine, mock_target_engine):
        """Test initialization with custom timeout and batch size settings."""
        with patch('etl_pipeline.core.mysql_replicator.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspect.return_value = mock_inspector
            
            replicator = ExactMySQLReplicator(
                source_engine=mock_source_engine,
                target_engine=mock_target_engine,
                source_db='test_source',
                target_db='test_target'
            )
            
            # Test default values
            assert replicator.query_timeout == 300  # 5 minutes
            assert replicator.max_batch_size == 10000
            
            # Test that schema cache is initialized
            assert replicator._schema_cache == {}


@pytest.mark.unit
class TestSchemaOperations:
    """Test schema-related operations."""
    
    def test_get_exact_table_schema_success(self, replicator, sample_create_statement):
        """Test successful retrieval of table schema."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_row = ('patient', sample_create_statement)
        
        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        schema = replicator.get_exact_table_schema('patient', replicator.source_engine)
        
        assert schema is not None
        assert schema['table_name'] == 'patient'
        assert schema['create_statement'] == sample_create_statement
        assert 'normalized_schema' in schema
        assert 'schema_hash' in schema
        assert len(schema['schema_hash']) == 32  # MD5 hash length
        
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args[0][0]
        assert 'SHOW CREATE TABLE' in str(call_args)
        assert '`patient`' in str(call_args)
    
    def test_get_exact_table_schema_table_not_found(self, replicator):
        """Test schema retrieval when table doesn't exist."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        
        mock_conn.execute.return_value = mock_result
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        schema = replicator.get_exact_table_schema('nonexistent_table', replicator.source_engine)
        
        assert schema is None
    
    def test_get_exact_table_schema_database_error(self, replicator):
        """Test schema retrieval with database error."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Database connection failed")
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        schema = replicator.get_exact_table_schema('patient', replicator.source_engine)
        
        assert schema is None
    
    def test_normalize_create_statement(self, replicator, sample_create_statement):
        """Test CREATE statement normalization."""
        normalized = replicator._normalize_create_statement(sample_create_statement)
        
        # Verify normalization removes engine, charset, etc.
        assert 'ENGINE=' not in normalized
        assert 'DEFAULT CHARSET=' not in normalized
        assert 'COLLATE ' not in normalized
        assert 'AUTO_INCREMENT=' not in normalized
        
        # Verify integer display widths are removed
        assert 'int(11)' not in normalized
        assert 'int' in normalized
        
        # Verify quotes are removed
        assert '`' not in normalized
    
    def test_schema_hash_consistency(self, replicator, sample_create_statement):
        """Test that identical schemas produce identical hashes."""
        # Mock the database calls to return the same schema
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_row = ('patient', sample_create_statement)
        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        schema1 = replicator.get_exact_table_schema('patient', replicator.source_engine)
        schema2 = replicator.get_exact_table_schema('patient', replicator.source_engine)
        
        assert schema1['schema_hash'] == schema2['schema_hash']
    
    def test_adapt_create_statement_for_target(self, replicator, sample_create_statement):
        """Test adaptation of CREATE statement for target database."""
        adapted = replicator._adapt_create_statement_for_target(sample_create_statement, 'patient')
        
        # Verify proper table name quoting
        assert 'CREATE TABLE `patient`' in adapted
        
        # Verify engine standardization
        assert 'ENGINE=InnoDB' in adapted
        
        # Verify charset standardization
        assert 'DEFAULT CHARSET=utf8mb4' in adapted
        
        # Verify integer display widths are removed
        assert 'int(11)' not in adapted
        assert 'int' in adapted


@pytest.mark.unit
class TestTableReplication:
    """Test table replication operations."""
    
    def test_create_exact_replica_success(self, replicator, sample_create_statement):
        """Test successful creation of exact table replica."""
        # Mock source schema retrieval
        mock_schema = {
            'table_name': 'patient',
            'create_statement': sample_create_statement,
            'normalized_schema': 'normalized_schema',
            'schema_hash': 'abc123'
        }
        
        with patch.object(replicator, 'get_exact_table_schema', return_value=mock_schema):
            with patch.object(replicator, '_adapt_create_statement_for_target', return_value='CREATE TABLE `patient` (...)'):
                mock_conn = MagicMock()
                replicator.target_engine.connect.return_value.__enter__.return_value = mock_conn
                
                result = replicator.create_exact_replica('patient')
                
                assert result is True
                assert mock_conn.execute.call_count == 2  # DROP and CREATE
                
                # Verify DROP TABLE call
                drop_call = mock_conn.execute.call_args_list[0][0][0]
                assert 'DROP TABLE IF EXISTS' in str(drop_call)
                assert '`patient`' in str(drop_call)
                
                # Verify CREATE TABLE call
                create_call = mock_conn.execute.call_args_list[1][0][0]
                assert 'CREATE TABLE' in str(create_call)
    
    def test_create_exact_replica_source_schema_failure(self, replicator):
        """Test replica creation when source schema retrieval fails."""
        with patch.object(replicator, 'get_exact_table_schema', return_value=None):
            result = replicator.create_exact_replica('patient')
            assert result is False
    
    def test_create_exact_replica_target_creation_failure(self, replicator, sample_create_statement):
        """Test replica creation when target table creation fails."""
        mock_schema = {
            'table_name': 'patient',
            'create_statement': sample_create_statement,
            'normalized_schema': 'normalized_schema',
            'schema_hash': 'abc123'
        }
        
        with patch.object(replicator, 'get_exact_table_schema', return_value=mock_schema):
            with patch.object(replicator, '_adapt_create_statement_for_target', return_value='CREATE TABLE `patient` (...)'):
                mock_conn = MagicMock()
                mock_conn.execute.side_effect = SQLAlchemyError("Table creation failed")
                replicator.target_engine.connect.return_value.__enter__.return_value = mock_conn
                
                result = replicator.create_exact_replica('patient')
                assert result is False


@pytest.mark.unit
class TestDataCopying:
    """Test data copying operations."""
    
    def test_copy_table_data_small_table_success(self, replicator, sample_table_data):
        """Test successful data copying for small tables."""
        # Mock row count
        mock_source_conn = MagicMock()
        mock_source_conn.execute.return_value.scalar.return_value = len(sample_table_data)
        
        # Mock target truncate
        mock_target_conn = MagicMock()
        
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_source_conn
        replicator.target_engine.connect.return_value.__enter__.return_value = mock_target_conn
        
        with patch.object(replicator, '_copy_direct', return_value=True) as mock_copy_direct:
            result = replicator.copy_table_data('patient')
            
            assert result is True
            mock_copy_direct.assert_called_once_with('patient', len(sample_table_data))
            
            # Verify row count query
            count_call = mock_source_conn.execute.call_args_list[0][0][0]
            assert 'SELECT COUNT(*)' in str(count_call)
            assert '`patient`' in str(count_call)
            
            # Verify truncate query
            truncate_call = mock_target_conn.execute.call_args_list[0][0][0]
            assert 'TRUNCATE TABLE' in str(truncate_call)
            assert '`patient`' in str(truncate_call)
    
    def test_copy_table_data_large_table_success(self, replicator):
        """Test successful data copying for large tables."""
        large_row_count = 50000  # Exceeds max_batch_size
        
        mock_source_conn = MagicMock()
        mock_source_conn.execute.return_value.scalar.return_value = large_row_count
        
        mock_target_conn = MagicMock()
        
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_source_conn
        replicator.target_engine.connect.return_value.__enter__.return_value = mock_target_conn
        
        with patch.object(replicator, '_copy_chunked', return_value=True) as mock_copy_chunked:
            result = replicator.copy_table_data('patient')
            
            assert result is True
            mock_copy_chunked.assert_called_once_with('patient', large_row_count)
    
    def test_copy_table_data_database_error(self, replicator):
        """Test data copying with database error."""
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = SQLAlchemyError("Connection failed")
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_source_conn
        
        result = replicator.copy_table_data('patient')
        assert result is False
    
    def test_copy_direct_success(self, replicator, sample_table_data):
        """Test direct copy method for small tables."""
        # Mock source connection and result
        mock_source_conn = MagicMock()
        mock_result = MagicMock()
        
        # Create mock rows with _mapping attribute
        mock_rows = []
        for data in sample_table_data:
            mock_row = MagicMock()
            mock_row._mapping = data
            mock_rows.append(mock_row)
        
        mock_result.fetchall.return_value = mock_rows
        mock_result.keys.return_value = list(sample_table_data[0].keys())
        mock_source_conn.execute.return_value = mock_result
        
        # Mock target connection
        mock_target_conn = MagicMock()
        mock_target_transaction = MagicMock()
        mock_target_conn.begin.return_value.__enter__.return_value = mock_target_transaction
        
        # Set up the engine mocks
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_source_conn
        replicator.target_engine.connect.return_value.__enter__.return_value = mock_target_conn
        
        result = replicator._copy_direct('patient', len(sample_table_data))
        
        assert result is True
    
    def test_copy_direct_no_data(self, replicator):
        """Test direct copy method with no data."""
        mock_source_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_source_conn.execute.return_value = mock_result
        
        mock_target_conn = MagicMock()
        
        # Set up the engine mocks
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_source_conn
        replicator.target_engine.connect.return_value.__enter__.return_value = mock_target_conn
        
        result = replicator._copy_direct('patient', 0)
        
        assert result is True  # No data is still success
    
    def test_copy_direct_database_error(self, replicator):
        """Test direct copy method with database error."""
        mock_source_conn = MagicMock()
        mock_source_conn.execute.side_effect = SQLAlchemyError("Query failed")
        
        mock_target_conn = MagicMock()
        
        # Set up the engine mocks
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_source_conn
        replicator.target_engine.connect.return_value.__enter__.return_value = mock_target_conn
        
        result = replicator._copy_direct('patient', 100)
        assert result is False
    
    def test_copy_chunked_with_primary_key(self, replicator):
        """Test chunked copy with primary key."""
        with patch.object(replicator, '_get_primary_key_columns', return_value=['PatNum']):
            with patch.object(replicator, '_copy_with_pk_chunking', return_value=True) as mock_pk_copy:
                result = replicator._copy_chunked('patient', 50000)
                
                assert result is True
                # Verify that _copy_with_pk_chunking is called with the correct parameters
                mock_pk_copy.assert_called_once()
                # The method should be called with source_conn, target_conn, table_name
                # but we can't easily mock the connection context managers in this test
    
    def test_copy_chunked_without_primary_key(self, replicator):
        """Test chunked copy without primary key."""
        with patch.object(replicator, '_get_primary_key_columns', return_value=[]):
            with patch.object(replicator, '_copy_with_limit_offset', return_value=True) as mock_offset_copy:
                result = replicator._copy_chunked('patient', 50000)
                
                assert result is True
                # Verify that _copy_with_limit_offset is called
                mock_offset_copy.assert_called_once()
    
    def test_copy_chunked_database_error(self, replicator):
        """Test chunked copy with database error."""
        with patch.object(replicator, '_get_primary_key_columns', side_effect=SQLAlchemyError("Connection failed")):
            result = replicator._copy_chunked('patient', 50000)
            assert result is False


@pytest.mark.unit
class TestPrimaryKeyOperations:
    """Test primary key related operations."""
    
    def test_get_primary_key_columns_success(self, replicator):
        """Test successful primary key column retrieval."""
        mock_pk_constraint = {'constrained_columns': ['PatNum', 'ClinicNum']}
        replicator.inspector.get_pk_constraint.return_value = mock_pk_constraint
        
        pk_columns = replicator._get_primary_key_columns('patient')
        
        assert pk_columns == ['PatNum', 'ClinicNum']
        replicator.inspector.get_pk_constraint.assert_called_once_with('patient')
    
    def test_get_primary_key_columns_no_primary_key(self, replicator):
        """Test primary key retrieval when table has no primary key."""
        replicator.inspector.get_pk_constraint.side_effect = Exception("No primary key")
        
        pk_columns = replicator._get_primary_key_columns('patient')
        
        assert pk_columns == []
    
    def test_copy_with_pk_chunking_success(self, replicator, sample_mysql_replicator_table_data):
        """Test successful primary key chunking."""
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        with patch.object(replicator, '_get_primary_key_columns', return_value=['PatNum']):
            # Mock range query result
            mock_range_result = MagicMock()
            mock_range_row = MagicMock()
            mock_range_row.min_pk = 1
            mock_range_row.max_pk = 3
            mock_range_result.fetchone.return_value = mock_range_row
            
            # Mock chunk query results
            mock_chunk_result = MagicMock()
            mock_chunk_rows = []
            for data in sample_mysql_replicator_table_data:
                mock_row = MagicMock()
                mock_row._mapping = data
                mock_row.PatNum = data['PatNum']  # For primary key access
                mock_chunk_rows.append(mock_row)
            
            mock_chunk_result.fetchall.side_effect = [mock_chunk_rows, []]  # First chunk has data, second is empty
            mock_chunk_result.keys.return_value = list(sample_mysql_replicator_table_data[0].keys())
            
            # Configure mock connection execute calls
            mock_source_conn.execute.side_effect = [mock_range_result, mock_chunk_result]
            
            # Mock target transaction
            mock_target_transaction = MagicMock()
            mock_target_conn.begin.return_value.__enter__.return_value = mock_target_transaction
            
            result = replicator._copy_with_pk_chunking(mock_source_conn, mock_target_conn, 'patient')
            
            assert result is True
    
    def test_copy_with_pk_chunking_no_primary_key(self, replicator):
        """Test primary key chunking when no primary key exists."""
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        with patch.object(replicator, '_get_primary_key_columns', return_value=[]):
            result = replicator._copy_with_pk_chunking(mock_source_conn, mock_target_conn, 'patient')
            assert result is False
    
    def test_copy_with_pk_chunking_no_data(self, replicator):
        """Test primary key chunking with no data."""
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        with patch.object(replicator, '_get_primary_key_columns', return_value=['PatNum']):
            mock_range_result = MagicMock()
            mock_range_result.fetchone.return_value = None
            mock_source_conn.execute.return_value = mock_range_result
            
            result = replicator._copy_with_pk_chunking(mock_source_conn, mock_target_conn, 'patient')
            assert result is True  # No data is still success


@pytest.mark.unit
class TestLimitOffsetCopy:
    """Test LIMIT/OFFSET copy operations."""
    
    def test_copy_with_limit_offset_success(self, replicator, sample_table_data):
        """Test successful LIMIT/OFFSET copy."""
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        # Mock chunk query results
        mock_chunk_result = MagicMock()
        mock_chunk_rows = []
        for data in sample_table_data:
            mock_row = MagicMock()
            mock_row._mapping = data
            mock_chunk_rows.append(mock_row)
        
        # First call returns data, second call returns empty (end of data)
        mock_chunk_result.fetchall.side_effect = [mock_chunk_rows, []]
        mock_chunk_result.keys.return_value = list(sample_table_data[0].keys())
        mock_source_conn.execute.return_value = mock_chunk_result
        
        # Mock target transaction
        mock_target_transaction = MagicMock()
        mock_target_conn.begin.return_value.__enter__.return_value = mock_target_transaction
        
        result = replicator._copy_with_limit_offset(mock_source_conn, mock_target_conn, 'patient')
        
        assert result is True
        assert mock_source_conn.execute.call_count == 1  # One chunk (all data fits)
    
    def test_copy_with_limit_offset_no_data(self, replicator):
        """Test LIMIT/OFFSET copy with no data."""
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        mock_chunk_result = MagicMock()
        mock_chunk_result.fetchall.return_value = []
        mock_source_conn.execute.return_value = mock_chunk_result
        
        result = replicator._copy_with_limit_offset(mock_source_conn, mock_target_conn, 'patient')
        assert result is True
    
    def test_copy_with_limit_offset_database_error(self, replicator):
        """Test LIMIT/OFFSET copy with database error."""
        mock_source_conn = MagicMock()
        mock_target_conn = MagicMock()
        
        mock_source_conn.execute.side_effect = SQLAlchemyError("Query failed")
        
        result = replicator._copy_with_limit_offset(mock_source_conn, mock_target_conn, 'patient')
        assert result is False


@pytest.mark.unit
class TestReplicaVerification:
    """Test replica verification operations."""
    
    def test_verify_exact_replica_success(self, replicator):
        """Test successful replica verification."""
        mock_schema = {
            'table_name': 'patient',
            'create_statement': 'CREATE TABLE patient (...)',
            'normalized_schema': 'normalized_schema',
            'schema_hash': 'abc123'
        }
        
        with patch.object(replicator, 'get_exact_table_schema', return_value=mock_schema):
            with patch.object(replicator, '_get_row_count', side_effect=[100, 100]):  # Same count for both
                result = replicator.verify_exact_replica('patient')
                
                assert result is True
    
    def test_verify_exact_replica_schema_mismatch(self, replicator):
        """Test replica verification with schema mismatch."""
        source_schema = {
            'table_name': 'patient',
            'create_statement': 'CREATE TABLE patient (...)',
            'normalized_schema': 'normalized_schema',
            'schema_hash': 'abc123'
        }
        
        target_schema = {
            'table_name': 'patient',
            'create_statement': 'CREATE TABLE patient (...)',
            'normalized_schema': 'different_normalized_schema',
            'schema_hash': 'def456'  # Different hash
        }
        
        with patch.object(replicator, 'get_exact_table_schema', side_effect=[source_schema, target_schema]):
            result = replicator.verify_exact_replica('patient')
            assert result is False
    
    def test_verify_exact_replica_row_count_mismatch(self, replicator):
        """Test replica verification with row count mismatch."""
        mock_schema = {
            'table_name': 'patient',
            'create_statement': 'CREATE TABLE patient (...)',
            'normalized_schema': 'normalized_schema',
            'schema_hash': 'abc123'
        }
        
        with patch.object(replicator, 'get_exact_table_schema', return_value=mock_schema):
            with patch.object(replicator, '_get_row_count', side_effect=[100, 95]):  # Different counts
                result = replicator.verify_exact_replica('patient')
                assert result is False
    
    def test_verify_exact_replica_source_schema_failure(self, replicator):
        """Test replica verification when source schema retrieval fails."""
        with patch.object(replicator, 'get_exact_table_schema', return_value=None):
            result = replicator.verify_exact_replica('patient')
            assert result is False
    
    def test_verify_exact_replica_target_schema_failure(self, replicator):
        """Test replica verification when target schema retrieval fails."""
        source_schema = {
            'table_name': 'patient',
            'create_statement': 'CREATE TABLE patient (...)',
            'normalized_schema': 'normalized_schema',
            'schema_hash': 'abc123'
        }
        
        with patch.object(replicator, 'get_exact_table_schema', side_effect=[source_schema, None]):
            result = replicator.verify_exact_replica('patient')
            assert result is False
    
    def test_verify_exact_replica_database_error(self, replicator):
        """Test replica verification with database error."""
        with patch.object(replicator, 'get_exact_table_schema', side_effect=SQLAlchemyError("Connection failed")):
            result = replicator.verify_exact_replica('patient')
            assert result is False


@pytest.mark.unit
class TestUtilityMethods:
    """Test utility methods."""
    
    def test_get_row_count_success(self, replicator):
        """Test successful row count retrieval."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 150
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        count = replicator._get_row_count('patient', replicator.source_engine)
        
        assert count == 150
        count_call = mock_conn.execute.call_args[0][0]
        assert 'SELECT COUNT(*)' in str(count_call)
        assert '`patient`' in str(count_call)
    
    def test_get_row_count_no_data(self, replicator):
        """Test row count retrieval with no data."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = None
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        count = replicator._get_row_count('patient', replicator.source_engine)
        
        assert count == 0
    
    def test_get_row_count_database_error(self, replicator):
        """Test row count retrieval with database error."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Query failed")
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # The actual implementation has error handling that returns 0
        # So we should test that it returns 0 instead of raising an exception
        count = replicator._get_row_count('patient', replicator.source_engine)
        assert count == 0
    
    def test_calculate_schema_hash(self, replicator, sample_create_statement):
        """Test schema hash calculation."""
        hash1 = replicator._calculate_schema_hash(sample_create_statement)
        hash2 = replicator._calculate_schema_hash(sample_create_statement)
        
        # Same input should produce same hash
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hash length
        
        # Different input should produce different hash
        different_statement = sample_create_statement.replace('PatNum', 'PatientNum')
        hash3 = replicator._calculate_schema_hash(different_statement)
        assert hash1 != hash3


@pytest.mark.integration
class TestMySQLReplicatorIntegration:
    """Integration tests with real database connections."""
    
    def test_full_table_replication_workflow(self):
        """Test complete table replication workflow with real databases."""
        pytest.skip("Integration tests require real database setup")
        
        # This would test:
        # 1. Create exact replica
        # 2. Copy table data  
        # 3. Verify exact replica
        # 4. Validate data integrity
    
    def test_large_table_replication_performance(self):
        """Test replication performance with large tables."""
        pytest.skip("Integration tests require real database setup")
        
        # This would test:
        # 1. Replication of tables > 10k rows
        # 2. Memory usage monitoring
        # 3. Processing time validation
        # 4. Connection pool behavior
    
    def test_schema_change_detection(self):
        """Test schema change detection between source and target."""
        pytest.skip("Integration tests require real database setup")
        
        # This would test:
        # 1. Initial replication
        # 2. Schema modification in source
        # 3. Change detection
        # 4. Schema synchronization


@pytest.mark.performance
class TestMySQLReplicatorPerformance:
    """Performance tests for MySQL replicator."""
    
    def test_small_table_replication_performance(self, replicator):
        """Test performance with small tables (< 1000 rows)."""
        pytest.skip("Performance tests require test data setup")
        
        # This would test:
        # 1. Replication time < 30 seconds
        # 2. Memory usage < 100MB
        # 3. Connection efficiency
    
    def test_medium_table_replication_performance(self, replicator):
        """Test performance with medium tables (1k-10k rows)."""
        pytest.skip("Performance tests require test data setup")
        
        # This would test:
        # 1. Replication time < 2 minutes
        # 2. Memory usage < 500MB
        # 3. Chunking efficiency
    
    def test_large_table_replication_performance(self, replicator):
        """Test performance with large tables (> 10k rows)."""
        pytest.skip("Performance tests require test data setup")
        
        # This would test:
        # 1. Replication time < 10 minutes
        # 2. Memory usage < 1GB
        # 3. Chunking strategy effectiveness
    
    def test_memory_usage_optimization(self, replicator):
        """Test memory usage during replication."""
        pytest.skip("Performance tests require memory monitoring setup")
        
        # This would test:
        # 1. Memory usage remains constant
        # 2. No memory leaks
        # 3. Garbage collection efficiency


@pytest.mark.idempotency 
class TestMySQLReplicatorIdempotency:
    """Idempotency tests for MySQL replicator."""
    
    def test_multiple_replica_creation_idempotent(self, replicator):
        """Test that creating replica multiple times produces same result."""
        pytest.skip("Idempotency tests require database setup")
        
        # This would test:
        # 1. Create replica
        # 2. Create replica again
        # 3. Verify identical results
        # 4. No duplicate data
    
    def test_data_copying_idempotent(self, replicator):
        """Test that copying data multiple times produces same result."""
        pytest.skip("Idempotency tests require database setup")
        
        # This would test:
        # 1. Copy data
        # 2. Copy data again
        # 3. Verify no duplicates
        # 4. Verify data integrity
    
    def test_verification_consistency(self, replicator):
        """Test that verification produces consistent results."""
        pytest.skip("Idempotency tests require database setup")
        
        # This would test:
        # 1. Multiple verification runs
        # 2. Consistent results
        # 3. No side effects


if __name__ == "__main__":
    pytest.main([__file__, "-v"])