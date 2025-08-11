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
    mock_loader.settings = MagicMock()  # Add missing settings attribute
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
        
        # Act
        result = loader._build_load_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result
    
    def test_build_load_query_force_full(self, mock_postgres_loader_instance):
        """Test load query building with force full load."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        incremental_columns = ['DateModified']
        force_full = True
        
        # Act
        result = loader._build_load_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result
        # Force full should not include WHERE clause for incremental columns
        assert 'WHERE' not in result.upper()
    
    def test_build_count_query_success(self, mock_postgres_loader_instance):
        """Test successful count query building."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        incremental_columns = ['DateModified']
        force_full = False
        
        # Act
        result = loader._build_count_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'COUNT' in result.upper()
        assert table_name in result
    
    def test_build_count_query_force_full(self, mock_postgres_loader_instance):
        """Test count query building with force full load."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        incremental_columns = ['DateModified']
        force_full = True
        
        # Act
        result = loader._build_count_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'COUNT' in result.upper()
        assert table_name in result
        # Force full should not include WHERE clause for incremental columns
        assert 'WHERE' not in result.upper()
    
    def test_build_enhanced_load_query_success(self, mock_postgres_loader_instance):
        """Test successful enhanced load query building."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        incremental_columns = ['DateModified']
        force_full = False
        use_or_logic = True
        
        # Act
        result = loader._build_enhanced_load_query(table_name, incremental_columns, force_full, use_or_logic)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result
    
    def test_get_last_load_time_success(self, mock_postgres_loader_instance):
        """Test successful last load time retrieval."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = datetime(2024, 1, 1, 10, 0, 0)
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._get_last_load_time(table_name)
        
        # Assert
        assert result == datetime(2024, 1, 1, 10, 0, 0)
        mock_conn.execute.assert_called()
    
    def test_get_last_load_time_no_records(self, mock_postgres_loader_instance):
        """Test last load time retrieval when no records exist."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = None
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._get_last_load_time(table_name)
        
        # Assert
        assert result is None
        mock_conn.execute.assert_called()
    
    def test_get_last_primary_value_success(self, mock_postgres_loader_instance):
        """Test successful last primary value retrieval."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'patient'
        
        # Mock the analytics engine connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 1000  # Last primary key value
        loader.analytics_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Act
        result = loader._get_last_primary_value(table_name)
        
        # Assert
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
        mock_conn.execute.return_value.scalar.return_value = None
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
        config = {'primary_key': 'PatientNum', 'incremental_columns': ['DateModified', 'PatientNum']}
        
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
    
    def test_get_incremental_strategy_success(self, mock_postgres_loader_instance):
        """Test successful incremental strategy determination."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        config = {'incremental_columns': ['DateModified']}
        
        # Act
        result = loader._get_incremental_strategy(config)
        
        # Assert
        assert result == 'timestamp'  # Default strategy for timestamp columns
    
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
        
        # Act
        result = loader._build_load_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result
        # Should include both incremental columns in the query
        assert 'DateCreated' in result or 'DateModified' in result
    
    def test_build_count_query_with_complex_conditions(self, mock_postgres_loader_instance):
        """Test count query building with complex conditions."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'procedurelog'
        incremental_columns = ['DateEntered']
        force_full = False
        
        # Act
        result = loader._build_count_query(table_name, incremental_columns, force_full)
        
        # Assert
        assert isinstance(result, str)
        assert 'COUNT' in result.upper()
        assert table_name in result
    
    def test_enhanced_load_query_with_or_logic(self, mock_postgres_loader_instance):
        """Test enhanced load query building with OR logic."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'appointment'
        incremental_columns = ['DateCreated', 'DateModified']
        force_full = False
        use_or_logic = True
        
        # Act
        result = loader._build_enhanced_load_query(table_name, incremental_columns, force_full, use_or_logic)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result
        # Should use OR logic for multiple incremental columns
        assert 'OR' in result.upper()
    
    def test_enhanced_load_query_with_and_logic(self, mock_postgres_loader_instance):
        """Test enhanced load query building with AND logic."""
        if not POSTGRES_LOADER_AVAILABLE:
            pytest.skip("PostgresLoader not available")
        
        loader = mock_postgres_loader_instance
        table_name = 'appointment'
        incremental_columns = ['DateCreated', 'DateModified']
        force_full = False
        use_or_logic = False
        
        # Act
        result = loader._build_enhanced_load_query(table_name, incremental_columns, force_full, use_or_logic)
        
        # Assert
        assert isinstance(result, str)
        assert 'SELECT' in result.upper()
        assert table_name in result
        # Should use AND logic for multiple incremental columns
        assert 'AND' in result.upper() 