"""Unit tests for schema discovery functionality.

PURPOSE: Fast execution (< 1 second), isolated component behavior
SCOPE: Core logic and edge cases with comprehensive mocking
COVERAGE: Method validation, error handling, schema hash calculation
MARKERS: @pytest.mark.unit

This file focuses on testing the core logic of SchemaDiscovery without any
real database connections, using comprehensive mocking for fast execution.

ARCHITECTURAL NOTES:
- Uses new configuration system with dependency injection
- Imports fixtures from modular test fixtures
- Follows enum-based database type patterns
- Proper test isolation with reset_global_settings
"""
import pytest
from unittest.mock import MagicMock, patch, Mock
from sqlalchemy.engine import Engine
from sqlalchemy import text

# Import ETL pipeline components
from etl_pipeline.core.schema_discovery import SchemaDiscovery

# Import new configuration system
try:
    from etl_pipeline.config import DatabaseType, PostgresSchema
    NEW_CONFIG_AVAILABLE = True
except ImportError:
    NEW_CONFIG_AVAILABLE = False

# Import modular test fixtures
from tests.fixtures.connection_fixtures import mock_database_engines, mock_source_engine
from tests.fixtures.config_fixtures import test_pipeline_config, test_tables_config
from tests.fixtures.env_fixtures import test_env_vars, test_settings, reset_global_settings


@pytest.mark.unit
def test_schema_discovery_initialization(mock_database_engines, test_settings):
    """Test SchemaDiscovery initialization with mocked engines and new config system."""
    source_engine = mock_database_engines['source']
    
    # Mock the URL attributes properly
    source_engine.url = Mock()
    source_engine.url.database = 'opendental'
    
    # Mock the inspect function to avoid SQLAlchemy inspection errors
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        assert schema_discovery.source_engine == source_engine
        assert schema_discovery.source_db == 'opendental'
        assert schema_discovery._schema_cache == {}
        assert schema_discovery.source_inspector == mock_source_inspector
        # Note: SchemaDiscovery creates its own settings instance via get_settings()
        # so we can't assert they are the same object, but we can verify settings exist
        assert schema_discovery.settings is not None
        assert schema_discovery.connection_manager is not None


@pytest.mark.unit
def test_get_table_schema_success(mock_database_engines, test_settings):
    """Test successful schema retrieval with mocked database and new config system."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager and its context manager
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn_mgr.get_connection.return_value = mock_conn
        
        mock_result = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = lambda self, key: 'CREATE TABLE definition...' if key == 1 else 'definition'
        
        mock_result.fetchone.return_value = mock_row
        mock_conn_mgr.execute_with_retry.return_value = mock_result
        schema_discovery.connection_manager = mock_conn_mgr
        
        # Mock the private methods
        with patch.object(schema_discovery, '_get_table_metadata_with_conn') as mock_metadata, \
             patch.object(schema_discovery, '_get_table_indexes_with_conn') as mock_indexes, \
             patch.object(schema_discovery, '_get_foreign_keys_with_conn') as mock_fks, \
             patch.object(schema_discovery, '_get_detailed_columns_with_conn') as mock_columns, \
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
def test_get_table_schema_table_not_found(mock_database_engines, test_settings):
    """Test schema retrieval when table doesn't exist."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager that returns no results
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn_mgr.get_connection.return_value = mock_conn
        
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_conn_mgr.execute_with_retry.return_value = mock_result
        schema_discovery.connection_manager = mock_conn_mgr
        
        with pytest.raises(Exception, match="Table non_existent not found"):
            schema_discovery.get_table_schema('non_existent')


@pytest.mark.unit
def test_schema_hash_calculation(test_settings):
    """Test schema hash calculation logic."""
    source_engine = Mock(spec=Engine)
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
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
def test_has_schema_changed_true(mock_database_engines, test_settings):
    """Test schema change detection when schema has changed."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
            mock_get_schema.return_value = {'schema_hash': 'new_hash'}
            
            has_changed = schema_discovery.has_schema_changed('test_table', 'old_hash')
            assert has_changed is True


@pytest.mark.unit
def test_has_schema_changed_false(mock_database_engines, test_settings):
    """Test schema change detection when schema hasn't changed."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
            mock_get_schema.return_value = {'schema_hash': 'same_hash'}
            
            has_changed = schema_discovery.has_schema_changed('test_table', 'same_hash')
            assert has_changed is False


@pytest.mark.unit
def test_has_schema_changed_exception_handling(mock_database_engines, test_settings):
    """Test schema change detection when exception occurs."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema:
            mock_get_schema.side_effect = Exception("Database error")
            
            # Should return True (assume changed) when exception occurs
            has_changed = schema_discovery.has_schema_changed('test_table', 'old_hash')
            assert has_changed is True


@pytest.mark.unit
def test_adapt_create_statement_for_target(test_settings):
    """Test CREATE statement adaptation for target database."""
    source_engine = Mock(spec=Engine)
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        original_statement = "CREATE TABLE `source_table` (id INT PRIMARY KEY, name VARCHAR(50))"
        target_statement = schema_discovery._adapt_create_statement_for_target(
            original_statement, 'target_table'
        )
        
        expected = "CREATE TABLE `target_table` (id INT PRIMARY KEY, name VARCHAR(50))"
        assert target_statement == expected


@pytest.mark.unit
def test_adapt_create_statement_invalid_input(test_settings):
    """Test CREATE statement adaptation with invalid input."""
    source_engine = Mock(spec=Engine)
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        with pytest.raises(ValueError, match="Invalid CREATE TABLE statement"):
            schema_discovery._adapt_create_statement_for_target("INVALID STATEMENT", 'target_table')


@pytest.mark.unit
def test_get_incremental_columns_success(mock_database_engines, test_settings):
    """Test getting incremental columns with mocked data."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock the _get_detailed_columns_with_conn method
        with patch.object(schema_discovery, '_get_detailed_columns_with_conn') as mock_get_columns:
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
def test_get_incremental_columns_exception_handling(mock_database_engines, test_settings):
    """Test getting incremental columns when exception occurs."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        with patch.object(schema_discovery, '_get_detailed_columns_with_conn') as mock_get_columns:
            mock_get_columns.side_effect = Exception("Database error")
            
            # Should return empty list when exception occurs
            columns = schema_discovery.get_incremental_columns('patient')
            assert columns == []


@pytest.mark.unit
def test_discover_all_tables_success(mock_database_engines, test_settings):
    """Test discovering all tables with mocked data."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager and execute with proper context manager
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn_mgr.get_connection.return_value = mock_conn
        
        # Create a proper mock result that behaves like a SQLAlchemy result
        mock_result = Mock()
        # The result should be iterable and return rows
        mock_result.__iter__ = lambda self: iter([['definition'], ['patient'], ['appointment']])
        mock_result.fetchall = lambda: [['definition'], ['patient'], ['appointment']]
        mock_result.fetchone = lambda: ['definition']
        
        # Mock the connection and its execute method
        mock_conn = Mock()
        mock_conn.execute.return_value = mock_result
        mock_conn_mgr.get_connection.return_value = mock_conn
        schema_discovery.connection_manager = mock_conn_mgr
        
        tables = schema_discovery.discover_all_tables()
        
        assert isinstance(tables, list)
        assert len(tables) == 3
        assert 'definition' in tables
        assert 'patient' in tables
        assert 'appointment' in tables


@pytest.mark.unit
def test_discover_all_tables_exception_handling(mock_database_engines, test_settings):
    """Test discovering all tables when exception occurs."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager that raises exception
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn.execute.side_effect = Exception("Database error")
        schema_discovery.connection_manager = mock_conn_mgr
        
        # Should return empty list when exception occurs
        tables = schema_discovery.discover_all_tables()
        assert tables == []


@pytest.mark.unit
def test_get_table_size_info_success(mock_database_engines, test_settings):
    """Test getting table size information with mocked data."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager and execute with proper context manager
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn_mgr.get_connection.return_value = mock_conn
        
        # Mock the count query result
        mock_count_result = Mock()
        mock_count_result.scalar.return_value = 1000
        
        # Mock the size query result
        mock_size_result = Mock()
        mock_size_row = Mock()
        # Create a proper mapping object that behaves like a dictionary
        mock_mapping = {
            'data_size_bytes': 1048576,  # Exactly 1 MB (1024^2)
            'index_size_bytes': 524288,   # Exactly 0.5 MB (1024^2 / 2)
            'total_size_bytes': 1572864   # Exactly 1.5 MB (1024^2 * 1.5)
        }
        # Make the mock row properly behave like a dictionary
        mock_size_row._mapping = mock_mapping
        mock_size_row.keys = lambda: mock_mapping.keys()
        mock_size_row.__getitem__ = lambda self, key: mock_mapping[key]
        mock_size_row.__contains__ = lambda self, key: key in mock_mapping
        mock_size_row.get = lambda key, default=None: mock_mapping.get(key, default)
        mock_size_result.fetchone.return_value = mock_size_row
        
        # Set up the execute method to return different results based on the query
        def execute_side_effect(query):
            if 'COUNT(*)' in str(query):
                return mock_count_result
            else:
                return mock_size_result
        
        mock_conn.execute.side_effect = execute_side_effect
        schema_discovery.connection_manager = mock_conn_mgr
        
        size_info = schema_discovery.get_table_size_info('definition')
        
        assert size_info['row_count'] == 1000
        assert size_info['data_size_bytes'] == 1048576
        assert size_info['index_size_bytes'] == 524288
        assert size_info['total_size_bytes'] == 1572864
        assert size_info['data_size_mb'] == 1.0
        assert size_info['index_size_mb'] == 0.5
        assert size_info['total_size_mb'] == 1.5


@pytest.mark.unit
def test_get_table_size_info_no_data(mock_database_engines, test_settings):
    """Test getting table size information when no data exists."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager that returns no results
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn_mgr.get_connection.return_value = mock_conn
        
        # Mock the count query result
        mock_count_result = Mock()
        mock_count_result.scalar.return_value = 0
        
        # Mock the size query result - no data found
        mock_size_result = Mock()
        mock_size_result.fetchone.return_value = None
        
        # Set up the execute method to return different results based on the query
        def execute_side_effect(query):
            if 'COUNT(*)' in str(query):
                return mock_count_result
            else:
                return mock_size_result
        
        mock_conn.execute.side_effect = execute_side_effect
        schema_discovery.connection_manager = mock_conn_mgr
        
        size_info = schema_discovery.get_table_size_info('empty_table')
        
        assert size_info['row_count'] == 0
        assert size_info['data_size_bytes'] == 0
        assert size_info['index_size_bytes'] == 0
        assert size_info['total_size_bytes'] == 0
        assert size_info['data_size_mb'] == 0
        assert size_info['index_size_mb'] == 0
        assert size_info['total_size_mb'] == 0


@pytest.mark.unit
def test_get_table_size_info_exception_handling(mock_database_engines, test_settings):
    """Test getting table size information when exception occurs."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager that raises exception
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn.execute.side_effect = Exception("Database error")
        schema_discovery.connection_manager = mock_conn_mgr
        
        size_info = schema_discovery.get_table_size_info('error_table')
        
        # Should return zero values when exception occurs
        assert size_info['row_count'] == 0
        assert size_info['data_size_bytes'] == 0
        assert size_info['index_size_bytes'] == 0
        assert size_info['total_size_bytes'] == 0
        assert size_info['data_size_mb'] == 0
        assert size_info['index_size_mb'] == 0
        assert size_info['total_size_mb'] == 0


# ============================================================================
# TESTS FOR NEW ENHANCED FEATURES
# ============================================================================

@pytest.mark.unit
def test_cache_operations(mock_database_engines, test_settings):
    """Test cache operations for the three-tier caching system."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Test schema caching
        schema_data = {'table_name': 'test', 'columns': []}
        schema_discovery._cache_schema('test_table', schema_data)
        cached_schema = schema_discovery._get_cached_schema('test_table')
        assert cached_schema == schema_data
        
        # Test config caching
        config_data = {'batch_size': 1000, 'incremental': True}
        schema_discovery._cache_config('test_table', config_data)
        cached_config = schema_discovery._get_cached_config('test_table')
        assert cached_config == config_data
        
        # Test analysis caching
        analysis_data = {'relationships': [], 'usage_patterns': {}}
        schema_discovery._cache_analysis('relationships', analysis_data)
        cached_analysis = schema_discovery._get_cached_analysis('relationships')
        assert cached_analysis == analysis_data
        
        # Test cache clearing
        schema_discovery.clear_cache('schema')
        assert schema_discovery._get_cached_schema('test_table') is None
        assert schema_discovery._get_cached_config('test_table') == config_data  # Should still exist
        
        schema_discovery.clear_cache()  # Clear all
        assert schema_discovery._get_cached_config('test_table') is None
        assert schema_discovery._get_cached_analysis('relationships') is None


@pytest.mark.unit
def test_get_pipeline_configuration(mock_database_engines, test_settings):
    """Test pipeline configuration generation."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock the required methods
        with patch.object(schema_discovery, 'get_table_schema') as mock_get_schema, \
             patch.object(schema_discovery, 'get_table_size_info') as mock_get_size, \
             patch.object(schema_discovery, '_get_table_schema_with_conn') as mock_get_schema_with_conn:
            
            mock_get_schema.return_value = {
                'table_name': 'patient',
                'create_statement': 'CREATE TABLE `patient` (id INT PRIMARY KEY, name VARCHAR(50))',
                'columns': [{'name': 'id', 'type': 'int', 'is_nullable': False}],
                'indexes': [{'name': 'PRIMARY', 'columns': ['id']}],
                'foreign_keys': [],
                'schema_hash': 'abc123',
                'analysis_timestamp': '2023-01-01T00:00:00',
                'analysis_version': '3.0'
            }
            
            mock_get_size.return_value = {
                'row_count': 1000,
                'data_size_mb': 10.5,
                'total_size_mb': 12.0
            }
            
            # Mock the internal schema method that's called by other methods
            mock_get_schema_with_conn.return_value = {
                'table_name': 'patient',
                'create_statement': 'CREATE TABLE `patient` (id INT PRIMARY KEY, name VARCHAR(50))',
                'columns': [{'name': 'id', 'type': 'int', 'is_nullable': False}],
                'indexes': [{'name': 'PRIMARY', 'columns': ['id']}],
                'foreign_keys': [],
                'schema_hash': 'abc123',
                'analysis_timestamp': '2023-01-01T00:00:00',
                'analysis_version': '3.0'
            }
            
            config = schema_discovery.get_pipeline_configuration('patient')
            
            # The config doesn't include table_name, it's passed as parameter
            assert 'incremental_column' in config
            assert 'batch_size' in config
            assert 'extraction_strategy' in config
            assert 'table_importance' in config
            assert 'estimated_size_mb' in config
            assert 'estimated_rows' in config
            assert 'dependencies' in config
            assert 'is_modeled' in config
            assert 'dbt_model_types' in config
            assert 'column_overrides' in config
            assert 'monitoring' in config


@pytest.mark.unit
def test_analyze_complete_schema(mock_database_engines, test_settings):
    """Test complete schema analysis."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock the required methods
        with patch.object(schema_discovery, 'discover_all_tables') as mock_discover, \
             patch.object(schema_discovery, 'analyze_table_relationships') as mock_relationships, \
             patch.object(schema_discovery, 'analyze_table_usage_patterns') as mock_usage, \
             patch.object(schema_discovery, 'determine_table_importance') as mock_importance:
            
            mock_discover.return_value = ['patient', 'appointment']
            mock_relationships.return_value = {'patient': {}, 'appointment': {}}
            mock_usage.return_value = {'patient': {}, 'appointment': {}}
            mock_importance.return_value = {'patient': 'critical', 'appointment': 'high'}
            
            analysis = schema_discovery.analyze_complete_schema()
            
            assert 'tables' in analysis
            assert 'relationships' in analysis
            assert 'usage_patterns' in analysis
            assert 'importance_scores' in analysis
            assert 'summary_statistics' in analysis
            assert analysis['tables'] == ['patient', 'appointment'] 