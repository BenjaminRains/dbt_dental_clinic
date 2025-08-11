"""
Unit tests for PostgresLoader schema cache functionality.

This module contains tests for:
    - Schema cache operations
    - Cache invalidation
    - Cache statistics
    - Cached schema retrieval
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Import ETL pipeline components
from etl_pipeline.exceptions.configuration import ConfigurationError
from etl_pipeline.config import DatabaseType

# Import PostgresLoader for testing
try:
    from etl_pipeline.loaders.postgres_loader import PostgresLoader, SchemaCache
    POSTGRES_LOADER_AVAILABLE = True
except ImportError:
    POSTGRES_LOADER_AVAILABLE = False
    PostgresLoader = None
    SchemaCache = None

# Import fixtures
try:
    from tests.fixtures.loader_fixtures import sample_table_data
    from tests.fixtures.env_fixtures import test_env_vars, load_test_environment_file
    FIXTURES_AVAILABLE = True
except ImportError:
    FIXTURES_AVAILABLE = False
    sample_table_data = None
    test_env_vars = None
    load_test_environment_file = None


@pytest.fixture
def schema_cache_instance():
    """Create a SchemaCache instance for testing."""
    if not POSTGRES_LOADER_AVAILABLE or SchemaCache is None:
        pytest.skip("SchemaCache not available")
    
    return SchemaCache()


@pytest.fixture
def mock_postgres_loader_instance():
    """Create a fully mocked PostgresLoader instance for unit testing."""
    # Create a mock PostgresLoader instance - remove spec to allow custom attributes
    mock_loader = MagicMock()
    
    # Set up basic attributes
    mock_loader.analytics_schema = 'raw'
    mock_loader.settings = MagicMock()  # Add missing settings attribute
    mock_loader.table_configs = {
        'patient': {'incremental_columns': ['DateModified']},
        'appointment': {'batch_size': 500, 'incremental_columns': ['DateCreated', 'DateModified']},
        'procedurelog': {'primary_key': 'ProcNum'},
        'periomeasure': {
            'incremental_columns': ['DateEntered'],
            'estimated_size_mb': 57,
            'performance_strategy': 'streaming',
            'bulk_insert_buffer': 50000
        },
        'large_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 500
        },
        'empty_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 100
        },
        'test_table': {
            'incremental_columns': ['DateModified'],
            'estimated_size_mb': 50
        }
    }
    
    # Mock the schema adapter
    mock_schema_adapter = MagicMock()
    mock_schema_adapter.get_table_schema_from_mysql.return_value = {'columns': []}
    mock_schema_adapter.ensure_table_exists.return_value = True
    mock_schema_adapter.convert_row_data_types.return_value = {'id': 1, 'data': 'test'}
    mock_loader.schema_adapter = mock_schema_adapter
    
    # Mock database engines
    mock_replication_engine = MagicMock()
    mock_analytics_engine = MagicMock()
    mock_loader.replication_engine = mock_replication_engine
    mock_loader.analytics_engine = mock_analytics_engine
    
    # Mock methods that are called internally
    # Configure get_table_config to return actual table configs
    def get_table_config_side_effect(table_name):
        return mock_loader.table_configs.get(table_name, {})
    
    mock_loader.get_table_config = MagicMock(side_effect=get_table_config_side_effect)
    mock_loader._build_load_query = MagicMock(return_value="SELECT * FROM test_table")
    mock_loader._update_load_status = MagicMock(return_value=True)
    mock_loader._get_loaded_at_time_max = MagicMock(return_value=datetime(2024, 1, 1, 10, 0, 0))
    mock_loader._ensure_tracking_record_exists = MagicMock(return_value=True)
    mock_loader.stream_mysql_data = MagicMock(return_value=[[{'id': 1, 'data': 'test'}]])
    mock_loader.load_table_copy_csv = MagicMock(return_value=True)
    mock_loader.load_table_standard = MagicMock(return_value=True)
    mock_loader.load_table_streaming = MagicMock(return_value=True)
    mock_loader.load_table_chunked = MagicMock(return_value=True)
    
    return mock_loader


@pytest.mark.unit
class TestSchemaCache:
    """
    Unit tests for SchemaCache functionality.
    
    Test Strategy:
        - Pure unit tests with mocked dependencies
        - Focus on cache operations and performance
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_schema_cache_initialization(self, schema_cache_instance):
        """Test SchemaCache initialization."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("SchemaCache not available")
        
        cache = schema_cache_instance
        
        # Assert
        assert hasattr(cache, '_cache')
        assert hasattr(cache, '_cache_timestamps')
        assert isinstance(cache._cache, dict)
        assert isinstance(cache._cache_timestamps, dict)
    
    def test_cache_schema_success(self, schema_cache_instance):
        """Test successful schema caching."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("SchemaCache not available")
        
        cache = schema_cache_instance
        table_name = 'patient'
        schema = {'columns': [{'name': 'id', 'type': 'int'}]}
        
        # Act
        cache.cache_schema(table_name, schema)
        
        # Assert
        assert table_name in cache._cache
        assert cache._cache[table_name] == schema
        assert table_name in cache._cache_timestamps
    
    def test_get_cached_schema_hit(self, schema_cache_instance):
        """Test successful cache hit."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("SchemaCache not available")
        
        cache = schema_cache_instance
        table_name = 'patient'
        schema = {'columns': [{'name': 'id', 'type': 'int'}]}
        
        # Arrange - cache the schema
        cache.cache_schema(table_name, schema)
        
        # Act
        result = cache.get_cached_schema(table_name)
        
        # Assert
        assert result == schema
    
    def test_get_cached_schema_miss(self, schema_cache_instance):
        """Test cache miss for uncached table."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("SchemaCache not available")
        
        cache = schema_cache_instance
        table_name = 'nonexistent'
        
        # Act
        result = cache.get_cached_schema(table_name)
        
        # Assert
        assert result is None
    
    def test_invalidate_cache_specific_table(self, schema_cache_instance):
        """Test invalidating cache for specific table."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("SchemaCache not available")
        
        cache = schema_cache_instance
        table_name = 'patient'
        schema = {'columns': [{'name': 'id', 'type': 'int'}]}
        
        # Arrange - cache the schema
        cache.cache_schema(table_name, schema)
        assert table_name in cache._cache
        
        # Act
        cache.invalidate_cache(table_name)
        
        # Assert
        assert table_name not in cache._cache
        assert table_name not in cache._cache_timestamps
    
    def test_invalidate_cache_all_tables(self, schema_cache_instance):
        """Test invalidating entire cache."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("SchemaCache not available")
        
        cache = schema_cache_instance
        
        # Arrange - cache multiple schemas
        cache.cache_schema('patient', {'columns': []})
        cache.cache_schema('appointment', {'columns': []})
        assert len(cache._cache) == 2
        
        # Act
        cache.invalidate_cache()
        
        # Assert
        assert len(cache._cache) == 0
        assert len(cache._cache_timestamps) == 0
    
    def test_get_cache_stats(self, schema_cache_instance):
        """Test cache statistics retrieval."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("SchemaCache not available")
        
        cache = schema_cache_instance
        
        # Arrange - cache some schemas
        cache.cache_schema('patient', {'columns': []})
        cache.cache_schema('appointment', {'columns': []})
        
        # Act
        stats = cache.get_cache_stats()
        
        # Assert
        assert 'cache_size' in stats
        assert 'cached_tables' in stats
        assert 'oldest_entry' in stats
        assert 'newest_entry' in stats
        assert stats['cache_size'] == 2
        assert 'patient' in stats['cached_tables']
        assert 'appointment' in stats['cached_tables']
    
    def test_get_cache_stats_empty_cache(self, schema_cache_instance):
        """Test cache statistics for empty cache."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("SchemaCache not available")
        
        cache = schema_cache_instance
        
        # Act
        stats = cache.get_cache_stats()
        
        # Assert
        assert stats['cache_size'] == 0
        assert len(stats['cached_tables']) == 0
        assert stats['oldest_entry'] is None
        assert stats['newest_entry'] is None


@pytest.mark.unit
class TestPostgresLoaderSchemaCache:
    """
    Unit tests for PostgresLoader schema cache integration.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on schema cache integration with PostgresLoader
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_get_cached_schema_integration(self, mock_postgres_loader_instance):
        """Test PostgresLoader schema cache integration."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the schema cache
        mock_schema = {'columns': [{'name': 'id', 'type': 'int'}]}
        loader.schema_cache = MagicMock()
        loader.schema_cache.get_cached_schema.return_value = mock_schema
        
        # Act
        result = loader._get_cached_schema(table_name)
        
        # Assert
        assert result == mock_schema
        loader.schema_cache.get_cached_schema.assert_called_once_with(table_name)
    
    def test_get_cached_schema_miss_integration(self, mock_postgres_loader_instance):
        """Test PostgresLoader schema cache miss."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the schema cache to return None (cache miss)
        loader.schema_cache = MagicMock()
        loader.schema_cache.get_cached_schema.return_value = None
        
        # Act
        result = loader._get_cached_schema(table_name)
        
        # Assert
        assert result is None
        loader.schema_cache.get_cached_schema.assert_called_once_with(table_name)
    
    def test_schema_cache_performance_optimization(self, mock_postgres_loader_instance):
        """Test that schema cache improves performance by avoiding repeated schema lookups."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the schema adapter to track calls
        loader.schema_adapter.get_table_schema_from_mysql = MagicMock(return_value={'columns': []})
        
        # Mock the schema cache
        mock_schema = {'columns': [{'name': 'id', 'type': 'int'}]}
        loader.schema_cache = MagicMock()
        loader.schema_cache.get_cached_schema.return_value = mock_schema
        
        # Act - call _get_cached_schema multiple times
        result1 = loader._get_cached_schema(table_name)
        result2 = loader._get_cached_schema(table_name)
        result3 = loader._get_cached_schema(table_name)
        
        # Assert - should use cache, not call schema adapter
        assert result1 == mock_schema
        assert result2 == mock_schema
        assert result3 == mock_schema
        
        # Verify schema adapter was not called (cache was used)
        loader.schema_adapter.get_table_schema_from_mysql.assert_not_called()
        
        # Verify cache was consulted 3 times
        assert loader.schema_cache.get_cached_schema.call_count == 3 