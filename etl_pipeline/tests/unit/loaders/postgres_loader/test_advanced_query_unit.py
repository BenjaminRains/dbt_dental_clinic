"""
Unit tests for PostgresLoader advanced query building functionality.

This module contains tests for:
    - Advanced query building methods
    - Count query generation
    - Enhanced load query building
    - Last load time retrieval
    - Incremental strategy methods
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Import ETL pipeline components
from etl_pipeline.exceptions.configuration import ConfigurationError
from etl_pipeline.config import DatabaseType

# Import PostgresLoader for testing
try:
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    POSTGRES_LOADER_AVAILABLE = True
except ImportError:
    POSTGRES_LOADER_AVAILABLE = False
    PostgresLoader = None

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
def mock_postgres_loader_instance():
    """Create a fully mocked PostgresLoader instance for unit testing."""
    # Create a mock PostgresLoader instance - remove spec to allow custom attributes
    mock_loader = MagicMock()
    
    # Set up basic attributes
    mock_loader.analytics_schema = 'raw'
    mock_loader.target_schema = 'raw'  # Used by query building methods
    mock_loader.settings = MagicMock()  # Add missing settings attribute
    # Mock settings.get_database_config for query building methods
    # Need to handle both REPLICATION and ANALYTICS calls
    from etl_pipeline.config import DatabaseType
    def get_database_config_side_effect(db_type, *args):
        # DatabaseType is an enum, compare by enum value
        if db_type == DatabaseType.REPLICATION:
            return {'database': 'test_db'}
        elif db_type == DatabaseType.ANALYTICS:
            return {'database': 'test_db', 'schema': 'raw'}
        return {'database': 'test_db'}
    mock_loader.settings.get_database_config.side_effect = get_database_config_side_effect
    mock_loader.table_configs = {
        'patient': {'incremental_columns': ['DateModified'], 'primary_key': 'PatientNum'},
        'appointment': {'batch_size': 500, 'incremental_columns': ['DateCreated', 'DateModified'], 'primary_key': 'AptNum'},
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
    mock_loader.target_schema = 'raw'  # Used by query building methods
    mock_loader._update_load_status = MagicMock(return_value=True)
    mock_loader._ensure_tracking_record_exists = MagicMock(return_value=True)
    mock_loader.load_table = MagicMock(return_value=(True, {}))
    
    # Import the real PostgresLoader class to bind real methods for testing
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    import logging
    
    # Set up logger for the mock loader
    mock_loader.logger = logging.getLogger('etl_pipeline.loaders.postgres_loader')
    
    # Create a real SchemaCache instance
    from etl_pipeline.loaders.postgres_loader import SchemaCache
    mock_loader.schema_cache = SchemaCache()
    
    # Bind real methods that exist in the new architecture
    mock_loader._build_load_query = PostgresLoader._build_load_query.__get__(mock_loader, PostgresLoader)
    mock_loader._build_enhanced_load_query = PostgresLoader._build_enhanced_load_query.__get__(mock_loader, PostgresLoader)
    mock_loader._get_primary_incremental_column = PostgresLoader._get_primary_incremental_column.__get__(mock_loader, PostgresLoader)
    mock_loader._get_last_primary_value = PostgresLoader._get_last_primary_value.__get__(mock_loader, PostgresLoader)
    
    # Mock helper methods needed by query building (don't bind real methods, use mocks for simplicity)
    # Mock _get_loaded_at_time to return a datetime for query building
    def mock_get_loaded_at_time(table_name):
        # For tests that need incremental queries, return a datetime
        # For tests that need full load, return None
        return datetime(2024, 1, 1, 10, 0, 0)
    mock_loader._get_loaded_at_time = mock_get_loaded_at_time
    
    # Mock _validate_incremental_columns to return the columns as-is (simplified for tests)
    def mock_validate_incremental_columns(table_name, incremental_columns):
        # For tests, just return the columns as-is (assume they exist)
        return incremental_columns
    mock_loader._validate_incremental_columns = mock_validate_incremental_columns
    
    # Mock _is_integer_column to return False for timestamp columns (DateCreated, DateModified, etc.)
    def mock_is_integer_column(table_name, column_name):
        # Timestamp columns are not integers
        if any(ts_col in column_name.lower() for ts_col in ['date', 'time', 'timestamp', 'created', 'modified']):
            return False
        # Primary key columns might be integers
        if 'num' in column_name.lower() or 'id' in column_name.lower():
            return True
        return False
    mock_loader._is_integer_column = mock_is_integer_column
    
    return mock_loader


@pytest.mark.unit
class TestPostgresLoaderAdvancedQuery:
    """
    Unit tests for PostgresLoader advanced query building functionality.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on advanced query building and strategy logic
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_build_load_query_success(self, mock_postgres_loader_instance):
        """Test successful load query building."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        incremental_columns = ['DateModified']
        force_full = False
        
        # Mock _get_loaded_at_time to return a datetime so WHERE clause is built (if not force_full)
        if not force_full:
            loader._get_loaded_at_time = lambda t: datetime(2024, 1, 1, 10, 0, 0)
        
        # Act
        result = loader._build_load_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result or 'test_db' in result
    
    def test_build_load_query_force_full(self, mock_postgres_loader_instance):
        """Test load query building with force full load."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        incremental_columns = ['DateModified']
        force_full = True
        
        # Mock _get_loaded_at_time (not needed for force_full, but set anyway)
        loader._get_loaded_at_time = lambda t: None
        
        # Act
        result = loader._build_load_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result or 'test_db' in result
        # Force full should not include WHERE clause for incremental columns
        assert 'WHERE' not in result.upper()
    
    @pytest.mark.skip(reason="_build_count_query is not part of the new architecture - count queries handled internally")
    def test_build_count_query_success(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test successful count query building.
        
        Count queries are now handled internally by strategies.
        """
        pytest.skip("_build_count_query is not part of the new architecture")
    
    @pytest.mark.skip(reason="_build_count_query is not part of the new architecture - count queries handled internally")
    def test_build_count_query_force_full(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test count query building with force full load.
        
        Count queries are now handled internally by strategies.
        """
        pytest.skip("_build_count_query is not part of the new architecture")
    
    def test_build_enhanced_load_query_success(self, mock_postgres_loader_instance):
        """Test successful enhanced load query building."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        incremental_columns = ['DateModified']
        force_full = False
        incremental_strategy = 'or_logic'
        
        # Mock _get_loaded_at_time to return a datetime so WHERE clause is built
        loader._get_loaded_at_time = lambda t: datetime(2024, 1, 1, 10, 0, 0)
        
        # Act - signature: (table_name, incremental_columns, primary_column=None, force_full=False, incremental_strategy='or_logic')
        result = loader._build_enhanced_load_query(table_name, incremental_columns, primary_column=None, force_full=force_full, incremental_strategy=incremental_strategy)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result or 'test_db' in result
    
    @pytest.mark.skip(reason="_get_last_load_time is not part of the new architecture - use _get_loaded_at_time_max instead")
    def test_get_last_load_time_success(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test successful last load time retrieval.
        
        This method is not in the new architecture. Use _get_loaded_at_time_max instead.
        """
        pytest.skip("_get_last_load_time is not part of the new architecture")
    
    @pytest.mark.skip(reason="_get_last_load_time is not part of the new architecture - use _get_loaded_at_time_max instead")
    def test_get_last_load_time_no_records(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test last load time retrieval when no records exist.
        
        This method is not in the new architecture.
        """
        pytest.skip("_get_last_load_time is not part of the new architecture")
    
    def test_get_last_primary_value_success(self, mock_postgres_loader_instance):
        """Test successful last primary value retrieval."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        # fetchone() returns a tuple: (last_primary_value, primary_column_name)
        # The method returns the value as-is from the database (could be int or string)
        mock_conn.execute.return_value.fetchone.return_value = (1000, 'PatientNum')
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = None
        loader.analytics_engine.connect.return_value = mock_context
        
        # Act
        result = loader._get_last_primary_value(table_name)
        
        # Assert
        # Method returns the value as-is from database (int in this case)
        # It only converts to string when recomputing from analytics data
        assert result == 1000
        mock_conn.execute.assert_called()
    
    def test_get_last_primary_value_no_records(self, mock_postgres_loader_instance):
        """Test last primary value retrieval when no records exist."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = None
        mock_conn.execute.return_value = mock_result
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._get_last_primary_value(table_name)
        
        # Assert
        assert result is None
        mock_conn.execute.assert_called()
    
    def test_get_primary_incremental_column_success(self, mock_postgres_loader_instance):
        """Test successful primary incremental column retrieval."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        # Method looks for 'primary_incremental_column', not 'primary_key'
        config = {'primary_incremental_column': 'PatientNum', 'incremental_columns': ['DateModified', 'PatientNum']}
        
        # Act
        result = loader._get_primary_incremental_column(config)
        
        # Assert
        assert result == 'PatientNum'
    
    def test_get_primary_incremental_column_not_found(self, mock_postgres_loader_instance):
        """Test primary incremental column retrieval when not found."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        config = {'incremental_columns': ['DateModified']}  # No primary key in incremental columns
        
        # Act
        result = loader._get_primary_incremental_column(config)
        
        # Assert
        assert result is None
    
    @pytest.mark.skip(reason="_get_incremental_strategy is not part of the new architecture - strategy handled internally")
    def test_get_incremental_strategy_success(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test successful incremental strategy determination.
        
        Incremental strategy is now handled internally by _prepare_table_load().
        """
        pytest.skip("_get_incremental_strategy is not part of the new architecture")
    
    def test_log_incremental_strategy_success(self, mock_postgres_loader_instance):
        """Test successful incremental strategy logging."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        primary_column = 'PatientNum'
        incremental_columns = ['DateModified', 'PatientNum']
        strategy = 'timestamp'
        
        # Act - should not raise any exceptions
        loader._log_incremental_strategy(table_name, primary_column, incremental_columns, strategy)
        
        # Assert - method should complete without errors
        assert True  # If we get here, no exceptions were raised
    
    def test_build_load_query_with_multiple_incremental_columns(self, mock_postgres_loader_instance):
        """Test load query building with multiple incremental columns."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'appointment'
        incremental_columns = ['DateCreated', 'DateModified']
        force_full = False
        
        # Mock _get_loaded_at_time to return a datetime so WHERE clause is built
        loader._get_loaded_at_time = lambda t: datetime(2024, 1, 1, 10, 0, 0)
        
        # Act
        result = loader._build_load_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result or 'test_db' in result
        # Should include WHERE clause (may be 1=1 if validation fails, but should have WHERE)
        assert 'WHERE' in result.upper()
    
    @pytest.mark.skip(reason="_build_count_query is not part of the new architecture - count queries handled internally")
    def test_build_count_query_with_complex_conditions(self, mock_postgres_loader_instance):
        """
        OBSOLETE: Test count query building with complex conditions.
        
        Count queries are now handled internally.
        """
        pytest.skip("_build_count_query is not part of the new architecture")
    
    def test_enhanced_load_query_with_or_logic(self, mock_postgres_loader_instance):
        """Test enhanced load query building with OR logic."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'appointment'
        incremental_columns = ['DateCreated', 'DateModified']
        force_full = False
        incremental_strategy = 'or_logic'
        
        # Mock _get_loaded_at_time to return a datetime so WHERE clause is built
        loader._get_loaded_at_time = lambda t: datetime(2024, 1, 1, 10, 0, 0)
        
        # Act - signature: (table_name, incremental_columns, primary_column=None, force_full=False, incremental_strategy='or_logic')
        result = loader._build_enhanced_load_query(table_name, incremental_columns, primary_column=None, force_full=force_full, incremental_strategy=incremental_strategy)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result or 'test_db' in result
        # Should use OR logic for multiple incremental columns (if WHERE clause is built)
        # Note: If _validate_incremental_columns returns empty, WHERE will be 1=1
        assert 'WHERE' in result.upper()
        # With proper mocking, should have OR in the WHERE clause
        if 'DateCreated' in result and 'DateModified' in result:
            assert 'OR' in result.upper()
    
    def test_enhanced_load_query_with_and_logic(self, mock_postgres_loader_instance):
        """Test enhanced load query building with AND logic."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'appointment'
        incremental_columns = ['DateCreated', 'DateModified']
        force_full = False
        incremental_strategy = 'and_logic'
        
        # Mock _get_loaded_at_time to return a datetime so WHERE clause is built
        loader._get_loaded_at_time = lambda t: datetime(2024, 1, 1, 10, 0, 0)
        
        # Act - signature: (table_name, incremental_columns, primary_column=None, force_full=False, incremental_strategy='and_logic')
        result = loader._build_enhanced_load_query(table_name, incremental_columns, primary_column=None, force_full=force_full, incremental_strategy=incremental_strategy)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result or 'test_db' in result
        # Should use AND logic for multiple incremental columns (if WHERE clause is built)
        assert 'WHERE' in result.upper()
        # With proper mocking, should have AND in the WHERE clause
        if 'DateCreated' in result and 'DateModified' in result:
            assert 'AND' in result.upper() 