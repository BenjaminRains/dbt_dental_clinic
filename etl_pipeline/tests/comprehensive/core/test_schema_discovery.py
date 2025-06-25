"""Comprehensive tests for schema discovery functionality.

PURPOSE: Full functionality testing with mocked dependencies
SCOPE: Complete component behavior, error handling
COVERAGE: 90%+ target coverage (main test suite)
MARKERS: @pytest.mark.unit (default)

This file provides comprehensive testing of SchemaDiscovery functionality
with mocked dependencies for complete coverage while maintaining fast execution.
"""
import pytest
import logging
from unittest.mock import MagicMock, patch, Mock
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from etl_pipeline.core.schema_discovery import SchemaDiscovery
from etl_pipeline.core.connections import ConnectionFactory

# Set up logging for comprehensive tests
logger = logging.getLogger(__name__)

@pytest.fixture
def schema_discovery_mocked(mock_database_engines):
    """Create a SchemaDiscovery instance with mocked engines for testing.
    
    Following Section 4 of pytest_debugging_notes.md for SQLAlchemy inspection.
    """
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
    # Mock the URL attributes properly (Section 4.1 of debugging notes)
    source_engine.url = Mock()
    target_engine.url = Mock()
    source_engine.url.database = 'opendental'
    target_engine.url.database = 'opendental_replication'
    
    # Mock the inspect function to avoid SQLAlchemy inspection errors
    mock_source_inspector = Mock()
    mock_target_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.side_effect = lambda engine: mock_source_inspector if engine == source_engine else mock_target_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            target_engine=target_engine,
            source_db='opendental',
            target_db='opendental_replication'
        )
        
        logger.info("Created SchemaDiscovery instance with mocked engines")
        return schema_discovery

def test_get_table_schema_comprehensive(schema_discovery_mocked):
    """Test comprehensive schema retrieval with full mocking.
    
    Following Section 23 of pytest_debugging_notes.md for context manager protocol.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing comprehensive schema retrieval")
    
    # Mock connection and execute with proper context manager protocol
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    mock_result = Mock()
    mock_row = Mock()
    mock_row.__getitem__ = lambda self, key: 'CREATE TABLE definition...' if key == 1 else 'definition'
    
    mock_result.fetchone.return_value = mock_row
    mock_conn.execute.return_value = mock_result
    schema_discovery.source_engine.connect.return_value = mock_conn
    
    # Mock the private methods with realistic data
    with patch.object(schema_discovery, '_get_table_metadata') as mock_metadata, \
         patch.object(schema_discovery, '_get_table_indexes') as mock_indexes, \
         patch.object(schema_discovery, '_get_foreign_keys') as mock_fks, \
         patch.object(schema_discovery, '_get_detailed_columns') as mock_columns, \
         patch.object(schema_discovery, '_calculate_schema_hash') as mock_hash:
        
        mock_metadata.return_value = {
            'engine': 'InnoDB',
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_general_ci',
            'auto_increment': 100,
            'row_count': 500
        }
        mock_indexes.return_value = [
            {'name': 'PRIMARY', 'columns': ['id'], 'is_unique': True, 'type': 'BTREE'},
            {'name': 'idx_name', 'columns': ['name'], 'is_unique': False, 'type': 'BTREE'}
        ]
        mock_fks.return_value = [
            {'name': 'fk_patient', 'column': 'patient_id', 'referenced_table': 'patient', 'referenced_column': 'id'}
        ]
        mock_columns.return_value = [
            {'name': 'id', 'type': 'int', 'is_nullable': False, 'default': None, 'extra': 'auto_increment', 'comment': 'Primary key', 'key_type': 'PRI'},
            {'name': 'name', 'type': 'varchar', 'is_nullable': True, 'default': None, 'extra': '', 'comment': 'Definition name', 'key_type': ''},
            {'name': 'description', 'type': 'text', 'is_nullable': True, 'default': None, 'extra': '', 'comment': 'Definition description', 'key_type': ''}
        ]
        mock_hash.return_value = 'abc123def456'
        
        schema = schema_discovery.get_table_schema('definition')
        
        # Comprehensive assertions
        assert schema is not None
        assert schema['table_name'] == 'definition'
        assert 'create_statement' in schema
        assert 'metadata' in schema
        assert 'indexes' in schema
        assert 'foreign_keys' in schema
        assert 'columns' in schema
        assert 'schema_hash' in schema
        
        # Check metadata
        assert schema['metadata']['engine'] == 'InnoDB'
        assert schema['metadata']['charset'] == 'utf8mb4'
        assert schema['metadata']['row_count'] == 500
        
        # Check indexes
        assert len(schema['indexes']) == 2
        assert schema['indexes'][0]['name'] == 'PRIMARY'
        assert schema['indexes'][1]['name'] == 'idx_name'
        
        # Check foreign keys
        assert len(schema['foreign_keys']) == 1
        assert schema['foreign_keys'][0]['name'] == 'fk_patient'
        
        # Check columns
        assert len(schema['columns']) == 3
        assert schema['columns'][0]['name'] == 'id'
        assert schema['columns'][0]['key_type'] == 'PRI'
        assert schema['columns'][1]['name'] == 'name'
        assert schema['columns'][2]['name'] == 'description'
        
        # Check schema hash
        assert schema['schema_hash'] == 'abc123def456'
        
        logger.info("Comprehensive schema retrieval test completed")

def test_schema_hash_calculation_comprehensive():
    """Test comprehensive schema hash calculation scenarios."""
    source_engine = Mock(spec=Engine)
    target_engine = Mock(spec=Engine)
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    mock_target_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.side_effect = lambda engine: mock_source_inspector if engine == source_engine else mock_target_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            target_engine=target_engine,
            source_db='opendental',
            target_db='opendental_replication'
        )
        
        # Test various CREATE statements
        statements = [
            "CREATE TABLE test (id INT AUTO_INCREMENT=1, name VARCHAR(50))",
            "CREATE TABLE test (id INT AUTO_INCREMENT=999, name VARCHAR(50))",
            "CREATE TABLE test (id INT AUTO_INCREMENT=12345, name VARCHAR(50))",
            "CREATE TABLE test (id INT, name VARCHAR(50))",
            "CREATE TABLE test (id INT, name VARCHAR(100))",
            "CREATE TABLE test (id INT, name VARCHAR(50), description TEXT)"
        ]
        
        hashes = [schema_discovery._calculate_schema_hash(stmt) for stmt in statements]
        
        # Same structure with different AUTO_INCREMENT should have same hash
        assert hashes[0] == hashes[1] == hashes[2]
        
        # Different structures should have different hashes
        assert hashes[0] != hashes[3]  # Different structure
        assert hashes[3] != hashes[4]  # Different VARCHAR length
        assert hashes[4] != hashes[5]  # Different number of columns
        
        logger.info("Schema hash calculation test completed")

def test_discover_all_tables_comprehensive(schema_discovery_mocked):
    """Test comprehensive table discovery with mocked data.
    
    Following Section 23 of pytest_debugging_notes.md for context manager protocol.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing comprehensive table discovery")
    
    # Mock connection with realistic table list and proper context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    mock_result = Mock()
    tables_data = [
        ['definition'], ['patient'], ['appointment'], ['procedurelog'], 
        ['payment'], ['claim'], ['insplan'], ['carrier'], ['fee'], ['employee']
    ]
    mock_result.__iter__ = lambda self: iter(tables_data)
    mock_conn.execute.return_value = mock_result
    schema_discovery.source_engine.connect.return_value = mock_conn
    
    tables = schema_discovery.discover_all_tables()
    
    assert isinstance(tables, list)
    assert len(tables) == 10
    assert 'definition' in tables
    assert 'patient' in tables
    assert 'appointment' in tables
    assert 'procedurelog' in tables
    assert 'payment' in tables
    assert 'claim' in tables
    assert 'insplan' in tables
    assert 'carrier' in tables
    assert 'fee' in tables
    assert 'employee' in tables
    
    logger.info(f"Discovered {len(tables)} tables: {tables}")

def test_get_table_size_info_comprehensive(schema_discovery_mocked):
    """Test comprehensive table size information retrieval.
    
    Following Section 23 of pytest_debugging_notes.md for context manager protocol.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing comprehensive table size information retrieval")
    
    # Mock connection with realistic size data and proper context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    mock_result = Mock()
    mock_row = Mock()
    mock_row.row_count = 15000
    mock_row.data_size_bytes = 52428800  # 50MB
    mock_row.index_size_bytes = 10485760  # 10MB
    mock_row.total_size_bytes = 62914560  # 60MB
    
    mock_result.fetchone.return_value = mock_row
    mock_conn.execute.return_value = mock_result
    schema_discovery.source_engine.connect.return_value = mock_conn
    
    size_info = schema_discovery.get_table_size_info('patient')
    
    assert size_info['row_count'] == 15000
    assert size_info['data_size_bytes'] == 52428800
    assert size_info['index_size_bytes'] == 10485760
    assert size_info['total_size_bytes'] == 62914560
    assert size_info['data_size_mb'] == 50.0
    assert size_info['index_size_mb'] == 10.0
    assert size_info['total_size_mb'] == 60.0
    
    logger.info(f"Table size info: {size_info}")

def test_get_incremental_columns_comprehensive(schema_discovery_mocked):
    """Test comprehensive incremental columns retrieval.
    
    Following Section 19 of pytest_debugging_notes.md for data transformation testing.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing comprehensive incremental columns retrieval")
    
    # Mock the _get_detailed_columns method with realistic column data
    with patch.object(schema_discovery, '_get_detailed_columns') as mock_get_columns:
        mock_get_columns.return_value = [
            {
                'name': 'PatNum',
                'type': 'bigint',
                'is_nullable': False,
                'default': None,
                'extra': 'auto_increment',
                'comment': 'Primary key - patient number',
                'key_type': 'PRI'
            },
            {
                'name': 'LName',
                'type': 'varchar',
                'is_nullable': True,
                'default': None,
                'extra': '',
                'comment': 'Last name',
                'key_type': ''
            },
            {
                'name': 'FName',
                'type': 'varchar',
                'is_nullable': True,
                'default': None,
                'extra': '',
                'comment': 'First name',
                'key_type': ''
            },
            {
                'name': 'Birthdate',
                'type': 'date',
                'is_nullable': True,
                'default': None,
                'extra': '',
                'comment': 'Date of birth',
                'key_type': ''
            },
            {
                'name': 'DateCreated',
                'type': 'datetime',
                'is_nullable': True,
                'default': 'CURRENT_TIMESTAMP',
                'extra': '',
                'comment': 'Record creation date',
                'key_type': ''
            }
        ]
        
        columns = schema_discovery.get_incremental_columns('patient')
        
        assert len(columns) == 5
        
        # Check first column (primary key)
        assert columns[0]['column_name'] == 'PatNum'
        assert columns[0]['data_type'] == 'bigint'
        assert columns[0]['default'] is None
        assert columns[0]['extra'] == 'auto_increment'
        assert columns[0]['comment'] == 'Primary key - patient number'
        assert columns[0]['priority'] == 1
        
        # Check other columns
        assert columns[1]['column_name'] == 'LName'
        assert columns[1]['data_type'] == 'varchar'
        assert columns[2]['column_name'] == 'FName'
        assert columns[3]['column_name'] == 'Birthdate'
        assert columns[4]['column_name'] == 'DateCreated'
        assert columns[4]['default'] == 'CURRENT_TIMESTAMP'
        
        logger.info(f"Retrieved {len(columns)} incremental columns")

def test_replicate_schema_comprehensive(schema_discovery_mocked):
    """Test comprehensive schema replication with mocked database.
    
    Following Section 18 of pytest_debugging_notes.md for mock chain verification.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing comprehensive schema replication")
    
    # Mock the get_table_schema method
    with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
        mock_get_schema.return_value = {
            'create_statement': "CREATE TABLE `definition` (`id` int NOT NULL AUTO_INCREMENT, `name` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        }
        
        # Mock target database operations with proper context manager
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        
        mock_verify_result = Mock()
        mock_verify_result.fetchone.return_value = ['test_replica']  # Table exists
        
        mock_conn.execute.return_value = mock_verify_result
        schema_discovery.target_engine.begin.return_value.__enter__.return_value = mock_conn
        schema_discovery.target_engine.connect.return_value.__enter__.return_value = mock_conn
        
        success = schema_discovery.replicate_schema('definition', target_table='test_replica')
        
        assert success is True
        
        # Verify the correct SQL was executed (Section 18 - Mock Chain Verification)
        expected_calls = [
            'USE opendental_replication',
            'DROP TABLE IF EXISTS `test_replica`',
            'CREATE TABLE `test_replica` (`id` int NOT NULL AUTO_INCREMENT, `name` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4',
            "SHOW TABLES LIKE 'test_replica'"
        ]
        
        # Check that all expected operations were called
        actual_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
        for expected_call in expected_calls:
            assert any(expected_call in str(call) for call in actual_calls), f"Expected call '{expected_call}' not found in actual calls"
        
        logger.info("Schema replication test completed successfully")

def test_replicate_schema_failure_handling(schema_discovery_mocked):
    """Test schema replication failure scenarios.
    
    Following Section 16 of pytest_debugging_notes.md for error handling.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing schema replication failure scenarios")
    
    # Mock the get_table_schema method to raise an exception
    with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
        mock_get_schema.side_effect = Exception("Schema retrieval failed")
        
        success = schema_discovery.replicate_schema('definition', target_table='test_replica')
        assert success is False
        
        logger.info("Schema replication failure test completed")

def test_has_schema_changed_comprehensive(schema_discovery_mocked):
    """Test comprehensive schema change detection scenarios.
    
    Following Section 16 of pytest_debugging_notes.md for error handling.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing comprehensive schema change detection")
    
    # Test scenario 1: Schema has changed
    with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
        mock_get_schema.return_value = {'schema_hash': 'new_hash_123'}
        
        has_changed = schema_discovery.has_schema_changed('patient', 'old_hash_456')
        assert has_changed is True
    
    # Test scenario 2: Schema hasn't changed
    with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
        mock_get_schema.return_value = {'schema_hash': 'same_hash_789'}
        
        has_changed = schema_discovery.has_schema_changed('patient', 'same_hash_789')
        assert has_changed is False
    
    # Test scenario 3: Exception during schema retrieval
    with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
        mock_get_schema.side_effect = Exception("Database connection failed")
        
        # Should assume changed when exception occurs
        has_changed = schema_discovery.has_schema_changed('patient', 'old_hash_456')
        assert has_changed is True
        
        logger.info("Schema change detection test completed")

def test_adapt_create_statement_comprehensive():
    """Test comprehensive CREATE statement adaptation scenarios.
    
    Following Section 24 of pytest_debugging_notes.md for f-string syntax.
    """
    source_engine = Mock(spec=Engine)
    target_engine = Mock(spec=Engine)
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    mock_target_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.side_effect = lambda engine: mock_source_inspector if engine == source_engine else mock_target_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            target_engine=target_engine,
            source_db='opendental',
            target_db='opendental_replication'
        )
        
        # Test various CREATE statement formats
        test_cases = [
            {
                'input': "CREATE TABLE `source_table` (id INT PRIMARY KEY, name VARCHAR(50))",
                'target': 'target_table',
                'expected': "CREATE TABLE `target_table` (id INT PRIMARY KEY, name VARCHAR(50))"
            },
            {
                'input': "CREATE TABLE source_table (id INT AUTO_INCREMENT, name VARCHAR(50)) ENGINE=InnoDB",
                'target': 'new_table',
                'expected': "CREATE TABLE `new_table` (id INT AUTO_INCREMENT, name VARCHAR(50)) ENGINE=InnoDB"
            },
            {
                'input': "CREATE TABLE `complex_table` (`id` int NOT NULL AUTO_INCREMENT, `name` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
                'target': 'replica_table',
                'expected': "CREATE TABLE `replica_table` (`id` int NOT NULL AUTO_INCREMENT, `name` varchar(50) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            }
        ]
        
        for i, test_case in enumerate(test_cases):
            logger.debug(f"Testing CREATE statement adaptation case {i+1}")
            result = schema_discovery._adapt_create_statement_for_target(
                test_case['input'], test_case['target']
            )
            assert result == test_case['expected'], f"Case {i+1} failed: expected '{test_case['expected']}', got '{result}'"
        
        logger.info("CREATE statement adaptation test completed")

def test_schema_cache_functionality(schema_discovery_mocked):
    """Test schema caching functionality.
    
    Following Section 18 of pytest_debugging_notes.md for mock chain verification.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing schema caching functionality")
    
    # Mock the database operations with proper context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    mock_result = Mock()
    mock_row = Mock()
    mock_row.__getitem__ = lambda self, key: 'CREATE TABLE definition...' if key == 1 else 'definition'
    
    mock_result.fetchone.return_value = mock_row
    mock_conn.execute.return_value = mock_result
    schema_discovery.source_engine.connect.return_value = mock_conn
    
    # Mock the private methods
    with patch.object(schema_discovery, '_get_table_metadata') as mock_metadata, \
         patch.object(schema_discovery, '_get_table_indexes') as mock_indexes, \
         patch.object(schema_discovery, '_get_foreign_keys') as mock_fks, \
         patch.object(schema_discovery, '_get_detailed_columns') as mock_columns, \
         patch.object(schema_discovery, '_calculate_schema_hash') as mock_hash:
        
        mock_metadata.return_value = {'engine': 'InnoDB'}
        mock_indexes.return_value = []
        mock_fks.return_value = []
        mock_columns.return_value = [{'name': 'id', 'type': 'int'}]
        mock_hash.return_value = 'cached_hash'
        
        # First call should populate cache
        schema1 = schema_discovery.get_table_schema('definition')
        assert 'definition' in schema_discovery._schema_cache
        assert schema_discovery._schema_cache['definition']['schema_hash'] == 'cached_hash'
        
        # Get the call count after first call
        first_call_count = mock_conn.execute.call_count
        logger.info(f"Database queries after first call: {first_call_count}")
        
        # Second call should use cache (verify by checking call count)
        schema2 = schema_discovery.get_table_schema('definition')
        assert schema1 == schema2
        
        # Get the call count after second call
        second_call_count = mock_conn.execute.call_count
        logger.info(f"Database queries after second call: {second_call_count}")
        
        # Verify the database was not queried again (second call should use cache)
        assert second_call_count == first_call_count, f"Database was queried again. Expected {first_call_count} calls, got {second_call_count} calls"
        
        logger.info("Schema caching test completed")

def test_error_handling_comprehensive(schema_discovery_mocked):
    """Test comprehensive error handling scenarios.
    
    Following Section 16 of pytest_debugging_notes.md for error handling.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing comprehensive error handling")
    
    # Test database connection errors
    with patch.object(schema_discovery.source_engine, 'connect') as mock_connect:
        mock_connect.side_effect = Exception("Database connection failed")
        
        # Should handle connection errors gracefully
        tables = schema_discovery.discover_all_tables()
        assert tables == []
        
        size_info = schema_discovery.get_table_size_info('test_table')
        assert size_info['row_count'] == 0
    
    # Test query execution errors
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_conn.execute.side_effect = Exception("Query execution failed")
    schema_discovery.source_engine.connect.return_value = mock_conn
    
    # Should handle query errors gracefully
    tables = schema_discovery.discover_all_tables()
    assert tables == []
    
    logger.info("Comprehensive error handling test completed")

def test_performance_comprehensive(schema_discovery_mocked):
    """Test performance with comprehensive mocking.
    
    Following Section 21 of pytest_debugging_notes.md for performance testing.
    """
    schema_discovery = schema_discovery_mocked
    
    logger.info("Testing performance with comprehensive mocking")
    
    import time
    
    # Mock all database operations for performance testing
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    mock_result = Mock()
    mock_row = Mock()
    mock_row.__getitem__ = lambda self, key: 'CREATE TABLE definition...' if key == 1 else 'definition'
    mock_result.fetchone.return_value = mock_row
    mock_conn.execute.return_value = mock_result
    schema_discovery.source_engine.connect.return_value = mock_conn
    
    with patch.object(schema_discovery, '_get_table_metadata') as mock_metadata, \
         patch.object(schema_discovery, '_get_table_indexes') as mock_indexes, \
         patch.object(schema_discovery, '_get_foreign_keys') as mock_fks, \
         patch.object(schema_discovery, '_get_detailed_columns') as mock_columns, \
         patch.object(schema_discovery, '_calculate_schema_hash') as mock_hash:
        
        mock_metadata.return_value = {'engine': 'InnoDB'}
        mock_indexes.return_value = []
        mock_fks.return_value = []
        mock_columns.return_value = [{'name': 'id', 'type': 'int'}]
        mock_hash.return_value = 'test_hash'
        
        # Test performance of schema retrieval
        start_time = time.time()
        schema = schema_discovery.get_table_schema('definition')
        schema_time = time.time() - start_time
        
        # Test performance of table discovery
        start_time = time.time()
        tables = schema_discovery.discover_all_tables()
        discovery_time = time.time() - start_time
        
        # Verify results
        assert schema is not None
        assert isinstance(tables, list)
        
        # Performance assertions (should be very fast with mocking)
        assert schema_time < 0.1, f"Schema retrieval took {schema_time:.3f}s"
        assert discovery_time < 0.1, f"Table discovery took {discovery_time:.3f}s"
        
        logger.info(f"Performance test completed - Schema: {schema_time:.3f}s, Discovery: {discovery_time:.3f}s") 