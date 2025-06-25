"""Unit tests for schema discovery functionality.

PURPOSE: Fast execution (< 1 second), isolated component behavior
SCOPE: Core logic and edge cases with comprehensive mocking
COVERAGE: Method validation, error handling, schema hash calculation
MARKERS: @pytest.mark.unit

This file focuses on testing the core logic of SchemaDiscovery without any
real database connections, using comprehensive mocking for fast execution.
"""
import pytest
from unittest.mock import MagicMock, patch, Mock
from sqlalchemy.engine import Engine
from sqlalchemy import text
from etl_pipeline.core.schema_discovery import SchemaDiscovery


@pytest.mark.unit
def test_schema_discovery_initialization(mock_database_engines):
    """Test SchemaDiscovery initialization with mocked engines."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
    # Mock the URL attributes properly
    source_engine.url = Mock()
    source_engine.url.database = 'opendental'
    target_engine.url = Mock()
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
        
        assert schema_discovery.source_engine == source_engine
        assert schema_discovery.target_engine == target_engine
        assert schema_discovery.source_db == 'opendental'
        assert schema_discovery.target_db == 'opendental_replication'
        assert schema_discovery._schema_cache == {}


@pytest.mark.unit
def test_get_table_schema_success(mock_database_engines):
    """Test successful schema retrieval with mocked database."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        # Mock connection and execute with proper context manager protocol
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        
        mock_result = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = lambda self, key: 'CREATE TABLE definition...' if key == 1 else 'definition'
        
        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result
        source_engine.connect.return_value = mock_conn
        
        # Mock the private methods
        with patch.object(schema_discovery, '_get_table_metadata') as mock_metadata, \
             patch.object(schema_discovery, '_get_table_indexes') as mock_indexes, \
             patch.object(schema_discovery, '_get_foreign_keys') as mock_fks, \
             patch.object(schema_discovery, '_get_detailed_columns') as mock_columns, \
             patch.object(schema_discovery, '_calculate_schema_hash') as mock_hash:
            
            mock_metadata.return_value = {'engine': 'InnoDB', 'charset': 'utf8mb4'}
            mock_indexes.return_value = [{'name': 'PRIMARY', 'columns': ['id']}]
            mock_fks.return_value = []
            mock_columns.return_value = [{'name': 'id', 'type': 'int', 'is_nullable': False}]
            mock_hash.return_value = 'abc123'
            
            schema = schema_discovery.get_table_schema('definition')
            
            assert schema is not None
            assert schema['table_name'] == 'definition'
            assert 'columns' in schema
            assert 'indexes' in schema
            assert 'foreign_keys' in schema
            assert 'schema_hash' in schema
            assert schema['schema_hash'] == 'abc123'


@pytest.mark.unit
def test_get_table_schema_table_not_found(mock_database_engines):
    """Test schema retrieval when table doesn't exist."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        # Mock connection that returns no results with proper context manager
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result
        source_engine.connect.return_value = mock_conn
        
        with pytest.raises(ValueError, match="Table non_existent not found"):
            schema_discovery.get_table_schema('non_existent')


@pytest.mark.unit
def test_schema_hash_calculation():
    """Test schema hash calculation logic."""
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
        
        # Test hash calculation
        create_statement1 = "CREATE TABLE test (id INT AUTO_INCREMENT=1, name VARCHAR(50))"
        create_statement2 = "CREATE TABLE test (id INT AUTO_INCREMENT=999, name VARCHAR(50))"
        create_statement3 = "CREATE TABLE test (id INT, name VARCHAR(50))"
        
        hash1 = schema_discovery._calculate_schema_hash(create_statement1)
        hash2 = schema_discovery._calculate_schema_hash(create_statement2)
        hash3 = schema_discovery._calculate_schema_hash(create_statement3)
        
        # Same table structure should have same hash (ignoring AUTO_INCREMENT)
        assert hash1 == hash2
        # Different structure should have different hash
        assert hash1 != hash3


@pytest.mark.unit
def test_has_schema_changed_true(mock_database_engines):
    """Test schema change detection when schema has changed."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
            mock_get_schema.return_value = {'schema_hash': 'new_hash'}
            
            has_changed = schema_discovery.has_schema_changed('test_table', 'old_hash')
            assert has_changed is True


@pytest.mark.unit
def test_has_schema_changed_false(mock_database_engines):
    """Test schema change detection when schema hasn't changed."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
            mock_get_schema.return_value = {'schema_hash': 'same_hash'}
            
            has_changed = schema_discovery.has_schema_changed('test_table', 'same_hash')
            assert has_changed is False


@pytest.mark.unit
def test_has_schema_changed_exception_handling(mock_database_engines):
    """Test schema change detection when exception occurs."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
            mock_get_schema.side_effect = Exception("Database error")
            
            # Should return True (assume changed) when exception occurs
            has_changed = schema_discovery.has_schema_changed('test_table', 'old_hash')
            assert has_changed is True


@pytest.mark.unit
def test_adapt_create_statement_for_target():
    """Test CREATE statement adaptation for target database."""
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
        
        original_statement = "CREATE TABLE `source_table` (id INT PRIMARY KEY, name VARCHAR(50))"
        target_statement = schema_discovery._adapt_create_statement_for_target(
            original_statement, 'target_table'
        )
        
        expected = "CREATE TABLE `target_table` (id INT PRIMARY KEY, name VARCHAR(50))"
        assert target_statement == expected


@pytest.mark.unit
def test_adapt_create_statement_invalid_input():
    """Test CREATE statement adaptation with invalid input."""
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
        
        with pytest.raises(ValueError, match="Invalid CREATE TABLE statement"):
            schema_discovery._adapt_create_statement_for_target("INVALID STATEMENT", 'target_table')


@pytest.mark.unit
def test_get_incremental_columns_success(mock_database_engines):
    """Test getting incremental columns with mocked data."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        # Mock the _get_detailed_columns method
        with patch.object(schema_discovery, '_get_detailed_columns') as mock_get_columns:
            mock_get_columns.return_value = [
                {
                    'name': 'id',
                    'type': 'int',
                    'is_nullable': False,
                    'default': None,
                    'extra': 'auto_increment',
                    'comment': 'Primary key',
                    'key_type': 'PRI'
                },
                {
                    'name': 'name',
                    'type': 'varchar',
                    'is_nullable': True,
                    'default': None,
                    'extra': '',
                    'comment': 'Patient name',
                    'key_type': ''
                }
            ]
            
            columns = schema_discovery.get_incremental_columns('patient')
            
            assert len(columns) == 2
            assert columns[0]['column_name'] == 'id'
            assert columns[0]['data_type'] == 'int'
            assert columns[0]['priority'] == 1
            assert columns[1]['column_name'] == 'name'
            assert columns[1]['data_type'] == 'varchar'


@pytest.mark.unit
def test_get_incremental_columns_exception_handling(mock_database_engines):
    """Test getting incremental columns when exception occurs."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        with patch.object(schema_discovery, '_get_detailed_columns') as mock_get_columns:
            mock_get_columns.side_effect = Exception("Database error")
            
            # Should return empty list when exception occurs
            columns = schema_discovery.get_incremental_columns('patient')
            assert columns == []


@pytest.mark.unit
def test_discover_all_tables_success(mock_database_engines):
    """Test discovering all tables with mocked data."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        # Mock connection and execute with proper context manager
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        
        mock_result = Mock()
        mock_result.__iter__ = lambda self: iter([['definition'], ['patient'], ['appointment']])
        mock_conn.execute.return_value = mock_result
        source_engine.connect.return_value = mock_conn
        
        tables = schema_discovery.discover_all_tables()
        
        assert isinstance(tables, list)
        assert len(tables) == 3
        assert 'definition' in tables
        assert 'patient' in tables
        assert 'appointment' in tables


@pytest.mark.unit
def test_discover_all_tables_exception_handling(mock_database_engines):
    """Test discovering all tables when exception occurs."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        # Mock connection that raises exception with proper context manager
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_conn.execute.side_effect = Exception("Database error")
        source_engine.connect.return_value = mock_conn
        
        # Should return empty list when exception occurs
        tables = schema_discovery.discover_all_tables()
        assert tables == []


@pytest.mark.unit
def test_get_table_size_info_success(mock_database_engines):
    """Test getting table size information with mocked data."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        # Mock connection and execute with proper context manager
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        
        mock_result = Mock()
        mock_row = Mock()
        mock_row.row_count = 1000
        mock_row.data_size_bytes = 1048576  # Exactly 1 MB (1024^2)
        mock_row.index_size_bytes = 524288   # Exactly 0.5 MB (1024^2 / 2)
        mock_row.total_size_bytes = 1572864  # Exactly 1.5 MB (1024^2 * 1.5)
        
        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result
        source_engine.connect.return_value = mock_conn
        
        size_info = schema_discovery.get_table_size_info('definition')
        
        assert size_info['row_count'] == 1000
        assert size_info['data_size_bytes'] == 1048576
        assert size_info['index_size_bytes'] == 524288
        assert size_info['total_size_bytes'] == 1572864
        assert size_info['data_size_mb'] == 1.0
        assert size_info['index_size_mb'] == 0.5
        assert size_info['total_size_mb'] == 1.5


@pytest.mark.unit
def test_get_table_size_info_no_data(mock_database_engines):
    """Test getting table size information when no data exists."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        # Mock connection that returns no results with proper context manager
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result
        source_engine.connect.return_value = mock_conn
        
        size_info = schema_discovery.get_table_size_info('empty_table')
        
        assert size_info['row_count'] == 0
        assert size_info['data_size_bytes'] == 0
        assert size_info['index_size_bytes'] == 0
        assert size_info['total_size_bytes'] == 0
        assert size_info['data_size_mb'] == 0
        assert size_info['index_size_mb'] == 0
        assert size_info['total_size_mb'] == 0


@pytest.mark.unit
def test_get_table_size_info_exception_handling(mock_database_engines):
    """Test getting table size information when exception occurs."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['replication']
    
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
        
        # Mock connection that raises exception with proper context manager
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_conn.execute.side_effect = Exception("Database error")
        source_engine.connect.return_value = mock_conn
        
        size_info = schema_discovery.get_table_size_info('error_table')
        
        # Should return zero values when exception occurs
        assert size_info['row_count'] == 0
        assert size_info['data_size_bytes'] == 0
        assert size_info['index_size_mb'] == 0 