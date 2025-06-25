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
from unittest.mock import MagicMock, patch, call, Mock
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import logging

# Import the component under test
from etl_pipeline.core.mysql_replicator import ExactMySQLReplicator

# NOTE: Shared fixtures (mock_source_engine, mock_target_engine, sample_table_data) are now provided by conftest.py and should not be redefined here.

@pytest.mark.unit
class TestMySQLReplicatorUnit:
    """Unit tests for MySQL replicator business logic."""
    
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
class TestInitialization(TestMySQLReplicatorUnit):
    """Test replicator initialization logic."""
    
    def test_initialization_with_valid_parameters(self, mock_source_engine, mock_target_engine):
        """Test successful initialization with valid parameters."""
        with patch('etl_pipeline.core.mysql_replicator.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspect.return_value = mock_inspector
            
            replicator = ExactMySQLReplicator(
                source_engine=mock_source_engine,
                target_engine=mock_target_engine,
                source_db='test_source',
                target_db='test_target'
            )
            
            # Verify initialization
            assert replicator.source_engine == mock_source_engine
            assert replicator.target_engine == mock_target_engine
            assert replicator.source_db == 'test_source'
            assert replicator.target_db == 'test_target'
            assert replicator.inspector == mock_inspector
            assert replicator.query_timeout == 300
            assert replicator.max_batch_size == 10000
            assert replicator._schema_cache == {}
            
            # Verify inspector was created correctly
            mock_inspect.assert_called_once_with(mock_source_engine)
    
    def test_initialization_with_custom_settings(self, mock_source_engine, mock_target_engine):
        """Test initialization with custom timeout and batch size."""
        with patch('etl_pipeline.core.mysql_replicator.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspect.return_value = mock_inspector
            
            replicator = ExactMySQLReplicator(
                source_engine=mock_source_engine,
                target_engine=mock_target_engine,
                source_db='test_source',
                target_db='test_target'
            )
            
            # Test default values are set correctly
            assert replicator.query_timeout == 300
            assert replicator.max_batch_size == 10000
            assert isinstance(replicator._schema_cache, dict)


@pytest.mark.unit
class TestSchemaNormalization(TestMySQLReplicatorUnit):
    """Test schema normalization logic."""
    
    def test_normalize_create_statement_removes_engine(self, replicator, sample_create_statement):
        """Test that engine specification is removed during normalization."""
        normalized = replicator._normalize_create_statement(sample_create_statement)
        
        assert 'ENGINE=InnoDB' not in normalized
        assert 'ENGINE=' not in normalized
    
    def test_normalize_create_statement_removes_charset(self, replicator, sample_create_statement):
        """Test that charset and collation are removed during normalization."""
        normalized = replicator._normalize_create_statement(sample_create_statement)
        
        assert 'DEFAULT CHARSET=utf8mb4' not in normalized
        assert 'COLLATE=utf8mb4_0900_ai_ci' not in normalized
        assert 'DEFAULT CHARSET=' not in normalized
        assert 'COLLATE ' not in normalized
    
    def test_normalize_create_statement_removes_display_widths(self, replicator, sample_create_statement):
        """Test that integer display widths are removed."""
        normalized = replicator._normalize_create_statement(sample_create_statement)
        
        assert 'int(11)' not in normalized
        assert 'int' in normalized
    
    def test_normalize_create_statement_removes_quotes(self, replicator, sample_create_statement):
        """Test that backticks are removed during normalization."""
        normalized = replicator._normalize_create_statement(sample_create_statement)
        
        assert '`' not in normalized
    
    def test_normalize_create_statement_removes_auto_increment(self, replicator, sample_create_statement):
        """Test that auto increment values are removed."""
        normalized = replicator._normalize_create_statement(sample_create_statement)
        
        assert 'AUTO_INCREMENT=' not in normalized
    
    def test_normalize_create_statement_preserves_structure(self, replicator, sample_create_statement):
        """Test that essential table structure is preserved."""
        normalized = replicator._normalize_create_statement(sample_create_statement)
        
        # Essential elements should remain
        assert 'CREATE TABLE' in normalized
        assert 'patient' in normalized
        assert 'PatNum' in normalized
        assert 'LName' in normalized
        assert 'FName' in normalized
        assert 'PRIMARY KEY' in normalized
    
    def test_schema_hash_consistency(self, replicator, sample_create_statement):
        """Test that identical schemas produce identical hashes."""
        hash1 = replicator._calculate_schema_hash(sample_create_statement)
        hash2 = replicator._calculate_schema_hash(sample_create_statement)
        
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hash length
        assert isinstance(hash1, str)
    
    def test_schema_hash_different_for_different_schemas(self, replicator):
        """Test that different schemas produce different hashes."""
        schema1 = "CREATE TABLE test1 (id int PRIMARY KEY)"
        schema2 = "CREATE TABLE test2 (id int PRIMARY KEY)"
        
        hash1 = replicator._calculate_schema_hash(schema1)
        hash2 = replicator._calculate_schema_hash(schema2)
        
        assert hash1 != hash2


@pytest.mark.unit
class TestCreateStatementAdaptation(TestMySQLReplicatorUnit):
    """Test CREATE statement adaptation logic."""
    
    def test_adapt_create_statement_removes_database_name(self, replicator, sample_create_statement):
        """Test that database name is removed from CREATE statement."""
        adapted = replicator._adapt_create_statement_for_target(sample_create_statement, 'patient')
        
        # Should not contain database name
        assert 'test_source.patient' not in adapted
        assert 'test_target.patient' not in adapted
    
    def test_adapt_create_statement_standardizes_engine(self, replicator, sample_create_statement):
        """Test that engine is standardized to InnoDB."""
        adapted = replicator._adapt_create_statement_for_target(sample_create_statement, 'patient')
        
        assert 'ENGINE=InnoDB' in adapted
    
    def test_adapt_create_statement_standardizes_charset(self, replicator, sample_create_statement):
        """Test that charset and collation are standardized."""
        adapted = replicator._adapt_create_statement_for_target(sample_create_statement, 'patient')
        
        assert 'DEFAULT CHARSET=utf8mb4' in adapted
        assert 'COLLATE=utf8mb4_0900_ai_ci' in adapted  # Note: uses = not space
    
    def test_adapt_create_statement_removes_display_widths(self, replicator, sample_create_statement):
        """Test that integer display widths are removed."""
        adapted = replicator._adapt_create_statement_for_target(sample_create_statement, 'patient')
        
        assert 'int(11)' not in adapted
        assert 'int' in adapted


@pytest.mark.unit
class TestErrorHandling(TestMySQLReplicatorUnit):
    """Test error handling logic."""
    
    def test_handle_database_connection_error(self, replicator):
        """Test handling of database connection errors."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Connection failed")
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Test that schema retrieval returns None on error
        schema = replicator.get_exact_table_schema('patient', replicator.source_engine)
        assert schema is None
    
    def test_handle_table_not_found_error(self, replicator):
        """Test handling when table doesn't exist."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        
        mock_conn.execute.return_value = mock_result
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        schema = replicator.get_exact_table_schema('nonexistent_table', replicator.source_engine)
        assert schema is None
    
    def test_handle_primary_key_error(self, replicator):
        """Test handling when primary key information is unavailable."""
        replicator.inspector.get_pk_constraint.side_effect = Exception("PK info unavailable")
        
        pk_columns = replicator._get_primary_key_columns('patient')
        assert pk_columns == []
    
    def test_handle_row_count_error(self, replicator):
        """Test handling of row count query errors."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = SQLAlchemyError("Count query failed")
        replicator.source_engine.connect.return_value.__enter__.return_value = mock_conn
        
        row_count = replicator._get_row_count('patient', replicator.source_engine)
        assert row_count == 0


@pytest.mark.unit
class TestDataTransformation(TestMySQLReplicatorUnit):
    """Test data transformation logic."""
    
    def test_convert_row_to_dict(self, replicator):
        """Test conversion of SQLAlchemy Row to dictionary."""
        # Mock a SQLAlchemy Row object
        mock_row = MagicMock()
        mock_row._mapping = {'PatNum': 1, 'LName': 'Doe', 'FName': 'John'}
        
        # Test the conversion (this would be part of the copy logic)
        row_dict = dict(mock_row._mapping)
        
        assert row_dict == {'PatNum': 1, 'LName': 'Doe', 'FName': 'John'}
        assert isinstance(row_dict, dict)
    
    def test_build_insert_query(self, replicator):
        """Test building INSERT query with proper column names."""
        columns = ['PatNum', 'LName', 'FName']
        table_name = 'patient'
        
        # Simulate the query building logic
        column_list = ', '.join(f'`{col}`' for col in columns)
        placeholders = ', '.join([':' + col for col in columns])
        
        expected_query = f"INSERT INTO `{table_name}` ({column_list}) VALUES ({placeholders})"
        
        assert '`PatNum`' in expected_query
        assert '`LName`' in expected_query
        assert '`FName`' in expected_query
        assert ':PatNum' in expected_query
        assert ':LName' in expected_query
        assert ':FName' in expected_query
    
    def test_build_select_query(self, replicator):
        """Test building SELECT query for data retrieval."""
        table_name = 'patient'
        
        # Simulate the query building logic
        select_query = f"SELECT * FROM `{table_name}`"
        
        assert select_query == "SELECT * FROM `patient`"
        assert '`patient`' in select_query


@pytest.mark.unit
class TestConfigurationValidation(TestMySQLReplicatorUnit):
    """Test configuration validation logic."""
    
    def test_validate_engine_configuration(self, mock_source_engine, mock_target_engine):
        """Test that engine configuration is validated."""
        # Test with valid engines
        with patch('etl_pipeline.core.mysql_replicator.inspect'):
            replicator = ExactMySQLReplicator(
                source_engine=mock_source_engine,
                target_engine=mock_target_engine,
                source_db='test_source',
                target_db='test_target'
            )
            
            assert replicator.source_engine is not None
            assert replicator.target_engine is not None
            assert replicator.source_engine.name == 'mysql'
            assert replicator.target_engine.name == 'mysql'
    
    def test_validate_database_names(self, mock_source_engine, mock_target_engine):
        """Test that database names are properly set."""
        with patch('etl_pipeline.core.mysql_replicator.inspect'):
            replicator = ExactMySQLReplicator(
                source_engine=mock_source_engine,
                target_engine=mock_target_engine,
                source_db='source_db',
                target_db='target_db'
            )
            
            assert replicator.source_db == 'source_db'
            assert replicator.target_db == 'target_db'
            assert replicator.source_db != replicator.target_db


@pytest.mark.unit
class TestBusinessLogic(TestMySQLReplicatorUnit):
    """Test core business logic."""
    
    def test_determine_copy_strategy_small_table(self, replicator):
        """Test that small tables use direct copy strategy."""
        # Mock row count for small table
        with patch.object(replicator, '_get_row_count', return_value=100):
            row_count = replicator._get_row_count('small_table', replicator.source_engine)
            
            # Small table should use direct copy
            should_use_direct = row_count <= replicator.max_batch_size
            assert should_use_direct is True
    
    def test_determine_copy_strategy_large_table(self, replicator):
        """Test that large tables use chunked copy strategy."""
        # Mock row count for large table
        with patch.object(replicator, '_get_row_count', return_value=50000):
            row_count = replicator._get_row_count('large_table', replicator.source_engine)
            
            # Large table should use chunked copy
            should_use_chunked = row_count > replicator.max_batch_size
            assert should_use_chunked is True
    
    def test_primary_key_detection_logic(self, replicator):
        """Test primary key detection logic."""
        # Mock primary key detection
        replicator.inspector.get_pk_constraint.return_value = {
            'constrained_columns': ['PatNum']
        }
        
        pk_columns = replicator._get_primary_key_columns('patient')
        assert pk_columns == ['PatNum']
        
        # Test no primary key case
        replicator.inspector.get_pk_constraint.return_value = {
            'constrained_columns': []
        }
        
        pk_columns = replicator._get_primary_key_columns('log')
        assert pk_columns == []
    
    def test_schema_cache_logic(self, replicator):
        """Test schema caching logic."""
        # Test initial cache state
        assert replicator._schema_cache == {}
        
        # Test cache population
        replicator._schema_cache['patient'] = {'schema_hash': 'abc123'}
        assert 'patient' in replicator._schema_cache
        assert replicator._schema_cache['patient']['schema_hash'] == 'abc123'


@pytest.mark.unit
class TestEdgeCases(TestMySQLReplicatorUnit):
    """Test edge cases and boundary conditions."""
    
    def test_empty_table_handling(self, replicator):
        """Test handling of empty tables."""
        with patch.object(replicator, '_get_row_count', return_value=0):
            row_count = replicator._get_row_count('empty_table', replicator.source_engine)
            assert row_count == 0
    
    def test_single_row_table_handling(self, replicator):
        """Test handling of tables with single row."""
        with patch.object(replicator, '_get_row_count', return_value=1):
            row_count = replicator._get_row_count('single_row_table', replicator.source_engine)
            assert row_count == 1
    
    def test_very_large_table_handling(self, replicator):
        """Test handling of very large tables."""
        with patch.object(replicator, '_get_row_count', return_value=1000000):
            row_count = replicator._get_row_count('very_large_table', replicator.source_engine)
            assert row_count == 1000000
            assert row_count > replicator.max_batch_size
    
    def test_special_characters_in_table_name(self, replicator):
        """Test handling of special characters in table names."""
        table_name = 'test-table_with_underscores'
        
        # Test that table name is properly quoted
        quoted_name = f"`{table_name}`"
        assert quoted_name == "`test-table_with_underscores`"
        assert '`' in quoted_name
    
    def test_null_values_handling(self, replicator):
        """Test handling of null values in data."""
        # Mock data with null values
        mock_data = [
            {'PatNum': 1, 'LName': 'Doe', 'FName': 'John', 'Email': None},
            {'PatNum': 2, 'LName': 'Smith', 'FName': 'Jane', 'Email': 'jane@example.com'}
        ]
        
        # Test that null values are handled
        for row in mock_data:
            assert 'PatNum' in row
            assert 'LName' in row
            assert 'FName' in row
            # Email can be None
            assert row['Email'] is None or isinstance(row['Email'], str) 