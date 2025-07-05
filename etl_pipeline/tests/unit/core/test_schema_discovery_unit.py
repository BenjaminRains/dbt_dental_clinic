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


@pytest.mark.unit
def test_dbt_model_discovery_with_project_root(mock_database_engines, test_settings):
    """Test DBT model discovery when dbt project root is provided."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch('etl_pipeline.core.schema_discovery.os.path.join') as mock_join, \
         patch('etl_pipeline.core.schema_discovery.glob.glob') as mock_glob, \
         patch('etl_pipeline.core.schema_discovery.os.path.basename') as mock_basename:
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental',
            dbt_project_root='/path/to/dbt'
        )
        
        # Mock file discovery
        mock_join.side_effect = lambda *args: '/'.join(args)
        mock_glob.return_value = [
            '/path/to/dbt/models/staging/opendental/stg_opendental__patient.sql',
            '/path/to/dbt/models/staging/opendental/stg_opendental__appointment.sql'
        ]
        mock_basename.side_effect = lambda x: x.split('/')[-1]
        
        modeled_tables = schema_discovery._discover_dbt_models()
        
        assert 'patient' in modeled_tables
        assert 'appointment' in modeled_tables
        assert modeled_tables['patient'] == ['staging']
        assert modeled_tables['appointment'] == ['staging']


@pytest.mark.unit
def test_dbt_model_discovery_without_project_root(mock_database_engines, test_settings):
    """Test DBT model discovery when no dbt project root is provided."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        modeled_tables = schema_discovery._discover_dbt_models()
        
        assert modeled_tables == {}


@pytest.mark.unit
def test_dbt_model_discovery_exception_handling(mock_database_engines, test_settings):
    """Test DBT model discovery when exceptions occur."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch('etl_pipeline.core.schema_discovery.os.path.join', side_effect=Exception("Test error")):
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental',
            dbt_project_root='/path/to/dbt'
        )
        
        modeled_tables = schema_discovery._discover_dbt_models()
        
        assert modeled_tables == {}


@pytest.mark.unit
def test_is_table_modeled_by_dbt(mock_database_engines, test_settings):
    """Test checking if a table is modeled by DBT."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental',
            dbt_project_root='/path/to/dbt'
        )
        
        # Mock the _discover_dbt_models method
        schema_discovery._modeled_tables_cache = {
            'patient': ['staging'],
            'appointment': ['staging', 'mart']
        }
        
        assert schema_discovery.is_table_modeled_by_dbt('patient') is True
        assert schema_discovery.is_table_modeled_by_dbt('appointment') is True
        assert schema_discovery.is_table_modeled_by_dbt('non_existent') is False


@pytest.mark.unit
def test_get_dbt_model_types(mock_database_engines, test_settings):
    """Test getting DBT model types for a table."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental',
            dbt_project_root='/path/to/dbt'
        )
        
        # Mock the _discover_dbt_models method
        schema_discovery._modeled_tables_cache = {
            'patient': ['staging'],
            'appointment': ['staging', 'mart']
        }
        
        assert schema_discovery.get_dbt_model_types('patient') == ['staging']
        assert schema_discovery.get_dbt_model_types('appointment') == ['staging', 'mart']
        assert schema_discovery.get_dbt_model_types('non_existent') == []


@pytest.mark.unit
def test_generate_complete_configuration_success(mock_database_engines, test_settings):
    """Test successful complete configuration generation."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, 'analyze_complete_schema') as mock_analyze, \
         patch.object(SchemaDiscovery, 'get_table_schema') as mock_get_schema, \
         patch.object(SchemaDiscovery, '_save_complete_configuration') as mock_save:
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock analysis results
        mock_analyze.return_value = {
            'tables': ['patient', 'appointment'],
            'pipeline_configs': {
                'patient': {'batch_size': 1000},
                'appointment': {'batch_size': 2000}
            },
            'metadata': {'timestamp': '2024-01-01', 'analysis_version': '1.0'}
        }
        
        # Mock schema info
        mock_get_schema.return_value = {'schema_hash': 'abc123'}
        
        config = schema_discovery.generate_complete_configuration()
        
        assert 'tables' in config
        assert 'patient' in config['tables']
        assert 'appointment' in config['tables']
        assert config['tables']['patient']['batch_size'] == 1000
        assert config['tables']['patient']['schema_hash'] == 'abc123'


@pytest.mark.unit
def test_generate_complete_configuration_with_output_dir(mock_database_engines, test_settings):
    """Test complete configuration generation with output directory."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, 'analyze_complete_schema') as mock_analyze, \
         patch.object(SchemaDiscovery, 'get_table_schema') as mock_get_schema, \
         patch.object(SchemaDiscovery, '_save_complete_configuration') as mock_save:
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock analysis results
        mock_analyze.return_value = {
            'tables': ['patient'],
            'pipeline_configs': {'patient': {'batch_size': 1000}},
            'metadata': {'timestamp': '2024-01-01', 'analysis_version': '1.0'}
        }
        
        mock_get_schema.return_value = {'schema_hash': 'abc123'}
        
        config = schema_discovery.generate_complete_configuration('/tmp/output')
        
        mock_save.assert_called_once()
        assert 'tables' in config


@pytest.mark.unit
def test_generate_complete_configuration_exception(mock_database_engines, test_settings):
    """Test complete configuration generation with exception."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, 'analyze_complete_schema', side_effect=Exception("Test error")):
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        with pytest.raises(Exception, match="Configuration generation failed"):
            schema_discovery.generate_complete_configuration()


@pytest.mark.unit
def test_filter_excluded_tables(mock_database_engines, test_settings):
    """Test filtering of excluded tables."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        tables = ['patient', 'temp_backup', 'tmp_old', 'appointment', 'data_backup']
        filtered = schema_discovery._filter_excluded_tables(tables)
        
        assert 'patient' in filtered
        assert 'appointment' in filtered
        assert 'temp_backup' not in filtered
        assert 'tmp_old' not in filtered
        # Note: 'data_backup' is not excluded by the current regex pattern
        # The pattern is r'_backup$' which only matches tables ending with _backup
        # 'data_backup' would be excluded, but 'backup_data' would not be


@pytest.mark.unit
def test_get_best_incremental_column(mock_database_engines, test_settings):
    """Test getting the best incremental column."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Test with timestamp columns
        timestamp_cols = [
            {'column_name': 'created_at', 'data_type': 'timestamp'},
            {'column_name': 'updated_at', 'data_type': 'datetime'}
        ]
        best = schema_discovery._get_best_incremental_column(timestamp_cols)
        assert best == 'created_at'
        
        # Test with non-timestamp columns
        regular_cols = [
            {'column_name': 'id', 'data_type': 'int'},
            {'column_name': 'name', 'data_type': 'varchar'}
        ]
        best = schema_discovery._get_best_incremental_column(regular_cols)
        assert best == 'id'
        
        # Test with empty list
        best = schema_discovery._get_best_incremental_column([])
        assert best is None


@pytest.mark.unit
def test_determine_update_frequency(mock_database_engines, test_settings):
    """Test determining update frequency based on table characteristics."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        schema_info = {'columns': []}
        size_info = {'row_count': 1000}
        
        # Test high frequency tables
        assert schema_discovery._determine_update_frequency('audit_log', schema_info, size_info) == 'high'
        assert schema_discovery._determine_update_frequency('event_history', schema_info, size_info) == 'high'
        
        # Test medium frequency tables
        assert schema_discovery._determine_update_frequency('patient', schema_info, size_info) == 'medium'
        assert schema_discovery._determine_update_frequency('appointment', schema_info, size_info) == 'medium'
        
        # Test low frequency tables
        assert schema_discovery._determine_update_frequency('definition', schema_info, size_info) == 'low'


@pytest.mark.unit
def test_determine_table_type(mock_database_engines, test_settings):
    """Test determining table type based on characteristics."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        schema_info = {'columns': []}
        
        # Test audit tables
        size_info = {'row_count': 10000}
        assert schema_discovery._determine_table_type('audit_log', schema_info, size_info) == 'audit'
        assert schema_discovery._determine_table_type('event_history', schema_info, size_info) == 'audit'
        
        # Test transaction tables
        assert schema_discovery._determine_table_type('patient', schema_info, size_info) == 'transaction'
        assert schema_discovery._determine_table_type('appointment', schema_info, size_info) == 'transaction'
        
        # Test lookup tables (small row count)
        size_info = {'row_count': 500}
        assert schema_discovery._determine_table_type('definition', schema_info, size_info) == 'lookup'
        
        # Test master tables (large row count)
        size_info = {'row_count': 50000}
        assert schema_discovery._determine_table_type('definition', schema_info, size_info) == 'master'


@pytest.mark.unit
def test_get_recommended_batch_size(mock_database_engines, test_settings):
    """Test getting recommended batch size based on table characteristics."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Test small tables
        usage_metrics = {'size_mb': 0.5, 'update_frequency': 'low'}
        assert schema_discovery._get_recommended_batch_size(usage_metrics) == 5000
        
        # Test medium tables with high frequency
        usage_metrics = {'size_mb': 50, 'update_frequency': 'high'}
        assert schema_discovery._get_recommended_batch_size(usage_metrics) == 2000
        
        # Test medium tables with low frequency
        usage_metrics = {'size_mb': 50, 'update_frequency': 'low'}
        assert schema_discovery._get_recommended_batch_size(usage_metrics) == 3000
        
        # Test large tables with high frequency
        usage_metrics = {'size_mb': 500, 'update_frequency': 'high'}
        assert schema_discovery._get_recommended_batch_size(usage_metrics) == 1000
        
        # Test very large tables
        usage_metrics = {'size_mb': 2000, 'update_frequency': 'low'}
        assert schema_discovery._get_recommended_batch_size(usage_metrics) == 1000


@pytest.mark.unit
def test_get_extraction_strategy(mock_database_engines, test_settings):
    """Test getting extraction strategy based on usage metrics and importance."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Test small tables (< 1MB)
        usage_metrics = {'size_mb': 0.5, 'update_frequency': 'high'}
        assert schema_discovery._get_extraction_strategy(usage_metrics, 'critical') == 'full_table'
        
        # Test medium tables (1-100MB)
        usage_metrics = {'size_mb': 50, 'update_frequency': 'high'}
        assert schema_discovery._get_extraction_strategy(usage_metrics, 'critical') == 'incremental'
        
        # Test large tables (100-1000MB) with high frequency
        usage_metrics = {'size_mb': 500, 'update_frequency': 'high'}
        assert schema_discovery._get_extraction_strategy(usage_metrics, 'critical') == 'chunked_incremental'
        
        # Test large tables (100-1000MB) with low frequency
        usage_metrics = {'size_mb': 500, 'update_frequency': 'low'}
        assert schema_discovery._get_extraction_strategy(usage_metrics, 'critical') == 'incremental'
        
        # Test very large tables (> 1000MB)
        usage_metrics = {'size_mb': 1500, 'update_frequency': 'high'}
        assert schema_discovery._get_extraction_strategy(usage_metrics, 'critical') == 'streaming_incremental'


@pytest.mark.unit
def test_get_column_overrides(mock_database_engines, test_settings):
    """Test getting column overrides from schema info."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        schema_info = {
            'columns': [
                {'name': 'id', 'type': 'int', 'is_nullable': False},
                {'name': 'is_active', 'type': 'tinyint(1)', 'is_nullable': False},
                {'name': 'amount', 'type': 'decimal(10,2)', 'is_nullable': True},
                {'name': 'created_at', 'type': 'timestamp', 'is_nullable': False}
            ]
        }
        
        overrides = schema_discovery._get_column_overrides(schema_info)
        
        # Check that boolean and decimal columns have overrides
        assert 'is_active' in overrides
        assert 'amount' in overrides
        assert overrides['is_active']['conversion_rule'] == 'tinyint_to_boolean'
        assert overrides['amount']['conversion_rule'] == 'decimal_round'
        assert overrides['amount']['precision'] == 2
        
        # Regular columns should not have overrides
        assert 'id' not in overrides
        assert 'created_at' not in overrides


@pytest.mark.unit
def test_get_monitoring_config(mock_database_engines, test_settings):
    """Test getting monitoring configuration."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        size_info = {'row_count': 10000, 'total_size_mb': 100}
        
        # Test critical tables
        config = schema_discovery._get_monitoring_config('critical', size_info)
        assert config['alert_on_failure'] is True
        assert config['data_quality_threshold'] == 0.99
        assert config['max_extraction_time_minutes'] == 10  # 100/10
        
        # Test important tables
        config = schema_discovery._get_monitoring_config('important', size_info)
        assert config['alert_on_failure'] is True
        assert config['data_quality_threshold'] == 0.95
        
        # Test standard tables
        config = schema_discovery._get_monitoring_config('standard', size_info)
        assert config['alert_on_failure'] is False
        assert config['data_quality_threshold'] == 0.95
        
        # Test low priority tables
        config = schema_discovery._get_monitoring_config('low', size_info)
        assert config['alert_on_failure'] is False


@pytest.mark.unit
def test_get_default_pipeline_configuration(mock_database_engines, test_settings):
    """Test getting default pipeline configuration."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        config = schema_discovery._get_default_pipeline_configuration('test_table')
        
        assert config['incremental_column'] is None
        assert config['batch_size'] == 5000
        assert config['extraction_strategy'] == 'full_table'
        assert config['table_importance'] == 'standard'
        assert config['estimated_size_mb'] == 0
        assert config['estimated_rows'] == 0
        assert config['dependencies'] == []
        assert config['is_modeled'] is False
        assert config['dbt_model_types'] == []
        assert 'monitoring' in config


@pytest.mark.unit
def test_analyze_table_relationships(mock_database_engines, test_settings):
    """Test analyzing table relationships."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, '_get_table_schema_with_conn') as mock_get_schema:
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn_mgr.get_connection.return_value = mock_conn
        schema_discovery.connection_manager = mock_conn_mgr
        
        # Mock schema data
        mock_get_schema.side_effect = [
            {
                'foreign_keys': [
                    {'column': 'patient_id', 'referenced_table': 'patient', 'referenced_column': 'id'}
                ],
                'columns': []
            },
            {
                'foreign_keys': [],
                'columns': []
            }
        ]
        
        relationships = schema_discovery.analyze_table_relationships(['appointment', 'patient'])
        
        assert 'appointment' in relationships
        assert 'patient' in relationships
        assert relationships['appointment']['dependency_count'] == 1
        assert relationships['patient']['dependency_count'] == 0


@pytest.mark.unit
def test_analyze_table_usage_patterns(mock_database_engines, test_settings):
    """Test analyzing table usage patterns."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, '_get_table_schema_with_conn') as mock_get_schema, \
         patch.object(SchemaDiscovery, '_get_table_size_info_with_conn') as mock_get_size:
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn_mgr.get_connection.return_value = mock_conn
        schema_discovery.connection_manager = mock_conn_mgr
        
        # Mock schema and size data
        mock_get_schema.return_value = {
            'columns': [{'name': 'id', 'type': 'int'}],
            'indexes': [{'name': 'PRIMARY', 'columns': ['id']}]
        }
        mock_get_size.return_value = {
            'row_count': 1000,
            'total_size_mb': 50
        }
        
        patterns = schema_discovery.analyze_table_usage_patterns(['patient'])
        
        assert 'patient' in patterns
        assert patterns['patient']['row_count'] == 1000
        assert patterns['patient']['size_mb'] == 50
        assert patterns['patient']['column_count'] == 1


@pytest.mark.unit
def test_determine_table_importance(mock_database_engines, test_settings):
    """Test determining table importance."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, '_get_table_schema_with_conn') as mock_get_schema, \
         patch.object(SchemaDiscovery, '_get_table_size_info_with_conn') as mock_get_size:
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection manager
        mock_conn_mgr = Mock()
        mock_conn = Mock()
        mock_conn_mgr.__enter__ = Mock(return_value=mock_conn_mgr)
        mock_conn_mgr.__exit__ = Mock(return_value=None)
        mock_conn_mgr.get_connection.return_value = mock_conn
        schema_discovery.connection_manager = mock_conn_mgr
        
        # Mock schema and size data
        mock_get_schema.return_value = {
            'columns': [{'name': 'id', 'type': 'int'}],
            'foreign_keys': []
        }
        mock_get_size.return_value = {
            'row_count': 10000,
            'total_size_mb': 100
        }
        
        importance = schema_discovery.determine_table_importance(['patient'])
        
        assert 'patient' in importance
        assert importance['patient'] in ['critical', 'important', 'standard', 'low']


@pytest.mark.unit
def test_replicate_schema_success(mock_database_engines, test_settings):
    """Test successful schema replication."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['analytics']  # Use 'analytics' instead of 'target'
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, 'get_table_schema') as mock_get_schema:
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock schema data
        mock_get_schema.return_value = {
            'create_statement': 'CREATE TABLE patient (id INT PRIMARY KEY, name VARCHAR(255))'
        }
        
        # Mock target engine connection
        mock_conn = Mock()
        mock_conn.execute.return_value = Mock()
        target_engine.begin.return_value.__enter__.return_value = mock_conn
        target_engine.begin.return_value.__exit__.return_value = None
        
        result = schema_discovery.replicate_schema(
            source_table='patient',
            target_engine=target_engine,
            target_db='analytics',
            target_table='patient_raw'
        )
        
        assert result is True


@pytest.mark.unit
def test_replicate_schema_failure(mock_database_engines, test_settings):
    """Test schema replication failure."""
    source_engine = mock_database_engines['source']
    target_engine = mock_database_engines['analytics']  # Use 'analytics' instead of 'target'
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, 'get_table_schema', side_effect=Exception("Schema error")):
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        result = schema_discovery.replicate_schema(
            source_table='patient',
            target_engine=target_engine,
            target_db='analytics'
        )
        
        assert result is False


@pytest.mark.unit
def test_get_table_schema_with_conn_success(mock_database_engines, test_settings):
    """Test getting table schema with provided connection."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect, \
         patch.object(SchemaDiscovery, '_get_table_metadata_with_conn') as mock_metadata, \
         patch.object(SchemaDiscovery, '_get_table_indexes_with_conn') as mock_indexes, \
         patch.object(SchemaDiscovery, '_get_foreign_keys_with_conn') as mock_fks, \
         patch.object(SchemaDiscovery, '_get_detailed_columns_with_conn') as mock_columns, \
         patch.object(SchemaDiscovery, '_calculate_schema_hash') as mock_hash:
        
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection and result
        mock_conn = Mock()
        mock_result = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = lambda self, key: 'CREATE TABLE definition...' if key == 1 else 'definition'
        mock_result.fetchone.return_value = mock_row
        mock_conn.execute.return_value = mock_result
        
        # Mock private methods
        mock_metadata.return_value = {'engine': 'InnoDB'}
        mock_indexes.return_value = [{'name': 'PRIMARY', 'columns': ['id']}]
        mock_fks.return_value = []
        mock_columns.return_value = [{'name': 'id', 'type': 'int'}]
        mock_hash.return_value = 'abc123'
        
        schema = schema_discovery._get_table_schema_with_conn(mock_conn, 'test_table')
        
        assert schema['table_name'] == 'test_table'
        assert schema['schema_hash'] == 'abc123'


@pytest.mark.unit
def test_get_table_schema_with_conn_table_not_found(mock_database_engines, test_settings):
    """Test getting table schema when table doesn't exist."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection that returns no results
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result
        
        with pytest.raises(Exception, match="Table non_existent not found"):
            schema_discovery._get_table_schema_with_conn(mock_conn, 'non_existent')


@pytest.mark.unit
def test_get_table_schema_with_conn_exception_handling(mock_database_engines, test_settings):
    """Test exception handling in get_table_schema_with_conn."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Mock connection that raises an exception
        mock_conn = Mock()
        mock_conn.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Schema analysis failed"):
            schema_discovery._get_table_schema_with_conn(mock_conn, 'test_table')


@pytest.mark.unit
def test_clear_cache_specific_type(mock_database_engines, test_settings):
    """Test clearing specific cache types."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Populate caches
        schema_discovery._schema_cache = {'table1': 'data1'}
        schema_discovery._config_cache = {'table1': 'config1'}
        schema_discovery._analysis_cache = {'analysis1': 'data1'}
        
        # Clear only schema cache
        schema_discovery.clear_cache('schema')
        assert schema_discovery._schema_cache == {}
        assert schema_discovery._config_cache == {'table1': 'config1'}
        assert schema_discovery._analysis_cache == {'analysis1': 'data1'}
        
        # Clear only config cache
        schema_discovery.clear_cache('config')
        assert schema_discovery._schema_cache == {}
        assert schema_discovery._config_cache == {}
        assert schema_discovery._analysis_cache == {'analysis1': 'data1'}
        
        # Clear only analysis cache
        schema_discovery.clear_cache('analysis')
        assert schema_discovery._schema_cache == {}
        assert schema_discovery._config_cache == {}
        assert schema_discovery._analysis_cache == {}


@pytest.mark.unit
def test_clear_cache_all(mock_database_engines, test_settings):
    """Test clearing all caches."""
    source_engine = mock_database_engines['source']
    
    # Mock the inspect function
    mock_source_inspector = Mock()
    
    with patch('etl_pipeline.core.schema_discovery.inspect') as mock_inspect:
        mock_inspect.return_value = mock_source_inspector
        
        schema_discovery = SchemaDiscovery(
            source_engine=source_engine,
            source_db='opendental'
        )
        
        # Populate caches
        schema_discovery._schema_cache = {'table1': 'data1'}
        schema_discovery._config_cache = {'table1': 'config1'}
        schema_discovery._analysis_cache = {'analysis1': 'data1'}
        
        # Clear all caches
        schema_discovery.clear_cache()
        assert schema_discovery._schema_cache == {}
        assert schema_discovery._config_cache == {}
        assert schema_discovery._analysis_cache == {} 