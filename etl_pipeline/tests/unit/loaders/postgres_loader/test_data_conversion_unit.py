"""
Unit tests for PostgresLoader data type conversion and handling.

This module contains tests for:
    - SQLAlchemy row to dictionary conversion
    - Data type conversion for test environment
    - Different row object types handling
    - Data conversion edge cases
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from collections import namedtuple

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
def real_postgres_loader_instance():
    """Create a PostgresLoader instance with mocked dependencies for testing real methods."""
    if not POSTGRES_LOADER_AVAILABLE:
        pytest.skip("PostgresLoader not available")
    
    # Create a mock PostgresLoader instance that allows testing real methods
    mock_loader = MagicMock()
    
    # Set up basic attributes
    mock_loader.analytics_schema = 'raw'
    mock_loader.settings = MagicMock()
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
    
    # Configure get_table_config to return actual table configs
    def get_table_config_side_effect(table_name):
        return mock_loader.table_configs.get(table_name, {})
    
    mock_loader.get_table_config = MagicMock(side_effect=get_table_config_side_effect)
    
    # Mock methods that are called internally but don't need testing
    mock_loader._build_load_query = MagicMock(return_value="SELECT * FROM test_table")
    mock_loader._update_load_status = MagicMock(return_value=True)
    mock_loader._get_loaded_at_time_max = MagicMock(return_value=datetime(2024, 1, 1, 10, 0, 0))
    mock_loader._ensure_tracking_record_exists = MagicMock(return_value=True)
    mock_loader.stream_mysql_data = MagicMock(return_value=[[{'id': 1, 'data': 'test'}]])
    mock_loader.load_table_copy_csv = MagicMock(return_value=True)
    mock_loader.load_table_standard = MagicMock(return_value=True)
    mock_loader.load_table_streaming = MagicMock(return_value=True)
    mock_loader.load_table_chunked = MagicMock(return_value=True)
    
    # Import the real PostgresLoader class to get access to its methods
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    import logging
    
    # Set up logger for the mock loader
    mock_loader.logger = logging.getLogger('etl_pipeline.loaders.postgres_loader')
    
    # Bind real methods to the mock loader for testing
    mock_loader._filter_valid_incremental_columns = PostgresLoader._filter_valid_incremental_columns.__get__(mock_loader, PostgresLoader)
    mock_loader._build_upsert_sql = PostgresLoader._build_upsert_sql.__get__(mock_loader, PostgresLoader)
    mock_loader._validate_incremental_integrity = PostgresLoader._validate_incremental_integrity.__get__(mock_loader, PostgresLoader)
    mock_loader._convert_sqlalchemy_row_to_dict = PostgresLoader._convert_sqlalchemy_row_to_dict.__get__(mock_loader, PostgresLoader)
    
    return mock_loader


@pytest.mark.unit
class TestPostgresLoaderDataConversion:
    """
    Unit tests for PostgresLoader data type conversion and handling.
    
    Test Strategy:
        - Pure unit tests with mocked database connections
        - Focus on data type conversion and handling logic
        - AAA pattern for all test methods
        - No real database connections, full mocking
    """
    
    def test_convert_sqlalchemy_row_to_dict(self, real_postgres_loader_instance):
        """Test the SQLAlchemy row to dictionary conversion method."""
        loader = real_postgres_loader_instance
        
        # Test with modern SQLAlchemy Row object (has _mapping)
        class MockRow:
            def __init__(self, data):
                self._mapping = data
        
        modern_row = MockRow({'id': 1, 'name': 'test'})
        result = loader._convert_sqlalchemy_row_to_dict(modern_row, ['id', 'name'])
        assert result == {'id': 1, 'name': 'test'}
        
        # Test with named tuple (has _asdict)
        NamedRow = namedtuple('NamedRow', ['id', 'name'])
        named_row = NamedRow(1, 'test')
        result = loader._convert_sqlalchemy_row_to_dict(named_row, ['id', 'name'])
        assert result == {'id': 1, 'name': 'test'}
        
        # Test with older SQLAlchemy Row object (has keys method)
        class OldRow:
            def __init__(self, data):
                self._data = data
            
            def keys(self):
                return self._data.keys()
            
            def __getitem__(self, index):
                if isinstance(index, int):
                    return list(self._data.values())[index]
                else:
                    return self._data[index]
            
            def __iter__(self):
                return iter(self._data.values())
            
            def __len__(self):
                return len(self._data)
        
        old_row = OldRow({'id': 1, 'name': 'test'})
        result = loader._convert_sqlalchemy_row_to_dict(old_row, ['id', 'name'])
        assert result == {'id': 1, 'name': 'test'}
        
        # Test with dictionary (should return as-is)
        dict_row = {'id': 1, 'name': 'test'}
        result = loader._convert_sqlalchemy_row_to_dict(dict_row, ['id', 'name'])
        assert result == {'id': 1, 'name': 'test'}
        
        # Test with tuple/list (fallback)
        tuple_row = (1, 'test')
        result = loader._convert_sqlalchemy_row_to_dict(tuple_row, ['id', 'name'])
        assert result == {'id': 1, 'name': 'test'}
        
        # Test with invalid row (should return empty dict)
        invalid_row = None
        result = loader._convert_sqlalchemy_row_to_dict(invalid_row, ['id', 'name'])
        assert result == {}
    
    def test_convert_data_types_for_test_environment(self, real_postgres_loader_instance):
        """Test that data type conversion works correctly for test environment."""
        loader = real_postgres_loader_instance
        
        # Test data with integer boolean values
        test_row = {
            'PatNum': 1,
            'PatStatus': 1,  # Should be converted to True
            'Gender': 0,      # Should be converted to False
            'Position': 1,    # Should be converted to True
            'Name': 'Test Patient',
            'AskToArriveEarly': 1  # Should remain as integer
        }
        
        # Convert the data
        converted_row = loader._convert_data_types_for_test_environment('patient', test_row)
        
        # Verify boolean fields were converted
        assert converted_row['PatStatus'] is True
        assert converted_row['Gender'] is False
        assert converted_row['Position'] is True
        
        # Verify non-boolean fields were not converted
        assert converted_row['PatNum'] == 1
        assert converted_row['Name'] == 'Test Patient'
        assert converted_row['AskToArriveEarly'] == 1
        
        # Test with appointment data
        appointment_row = {
            'AptNum': 1,
            'AptStatus': 1,  # Should be converted to True
            'AptDateTime': '2023-01-01 10:00:00'
        }
        
        converted_appointment = loader._convert_data_types_for_test_environment('appointment', appointment_row)
        
        # Verify appointment boolean field was converted
        assert converted_appointment['AptStatus'] is True
        assert converted_appointment['AptNum'] == 1
        assert converted_appointment['AptDateTime'] == '2023-01-01 10:00:00'
    
    def test_convert_data_types_with_mixed_data(self, real_postgres_loader_instance):
        """Test data type conversion with mixed data types."""
        loader = real_postgres_loader_instance
        
        # Test with mixed data types
        mixed_row = {
            'id': 1,
            'name': 'Test',
            'is_active': 1,      # Should convert to True
            'is_deleted': 0,     # Should convert to False
            'created_date': '2023-01-01',
            'updated_date': '2023-01-02',
            'score': 95.5,       # Should remain float
            'count': 42          # Should remain int
        }
        
        converted_row = loader._convert_data_types_for_test_environment('test_table', mixed_row)
        
        # Verify boolean conversions
        assert converted_row['is_active'] is True
        assert converted_row['is_deleted'] is False
        
        # Verify other types remain unchanged
        assert converted_row['id'] == 1
        assert converted_row['name'] == 'Test'
        assert converted_row['created_date'] == '2023-01-01'
        assert converted_row['updated_date'] == '2023-01-02'
        assert converted_row['score'] == 95.5
        assert converted_row['count'] == 42
    
    def test_convert_data_types_with_null_values(self, real_postgres_loader_instance):
        """Test data type conversion with null values."""
        loader = real_postgres_loader_instance
        
        # Test with null values
        null_row = {
            'id': 1,
            'name': None,
            'is_active': None,   # Should remain None
            'is_deleted': 0,     # Should convert to False
            'created_date': None
        }
        
        converted_row = loader._convert_data_types_for_test_environment('test_table', null_row)
        
        # Verify null values are handled correctly
        assert converted_row['id'] == 1
        assert converted_row['name'] is None
        assert converted_row['is_active'] is None  # Should remain None
        assert converted_row['is_deleted'] is False
        assert converted_row['created_date'] is None
    
    def test_convert_data_types_with_string_booleans(self, real_postgres_loader_instance):
        """Test data type conversion with string boolean values."""
        loader = real_postgres_loader_instance
        
        # Test with string boolean values
        string_bool_row = {
            'id': 1,
            'name': 'Test',
            'is_active': '1',    # Should convert to True
            'is_deleted': '0',   # Should convert to False
            'is_verified': 'true',  # Should convert to True
            'is_archived': 'false'  # Should convert to False
        }
        
        converted_row = loader._convert_data_types_for_test_environment('test_table', string_bool_row)
        
        # Verify string boolean conversions
        assert converted_row['is_active'] is True
        assert converted_row['is_deleted'] is False
        assert converted_row['is_verified'] is True
        assert converted_row['is_archived'] is False
        
        # Verify other fields remain unchanged
        assert converted_row['id'] == 1
        assert converted_row['name'] == 'Test'
    
    def test_convert_sqlalchemy_row_with_missing_columns(self, real_postgres_loader_instance):
        """Test SQLAlchemy row conversion with missing columns."""
        loader = real_postgres_loader_instance
        
        # Test with row that has fewer columns than expected
        class MockRow:
            def __init__(self, data):
                self._mapping = data
        
        row = MockRow({'id': 1, 'name': 'test'})
        # Request more columns than available
        result = loader._convert_sqlalchemy_row_to_dict(row, ['id', 'name', 'missing_col'])
        
        # Should only return available columns
        assert result == {'id': 1, 'name': 'test'}
        assert 'missing_col' not in result
    
    def test_convert_sqlalchemy_row_with_extra_columns(self, real_postgres_loader_instance):
        """Test SQLAlchemy row conversion with extra columns."""
        loader = real_postgres_loader_instance
        
        # Test with row that has more columns than requested
        class MockRow:
            def __init__(self, data):
                self._mapping = data
        
        row = MockRow({'id': 1, 'name': 'test', 'extra_col': 'extra_value'})
        # Request fewer columns than available
        result = loader._convert_sqlalchemy_row_to_dict(row, ['id', 'name'])
        
        # Should only return requested columns
        assert result == {'id': 1, 'name': 'test'}
        assert 'extra_col' not in result
    
    def test_convert_sqlalchemy_row_with_empty_data(self, real_postgres_loader_instance):
        """Test SQLAlchemy row conversion with empty data."""
        loader = real_postgres_loader_instance
        
        # Test with empty row
        class MockRow:
            def __init__(self, data):
                self._mapping = data
        
        empty_row = MockRow({})
        result = loader._convert_sqlalchemy_row_to_dict(empty_row, ['id', 'name'])
        
        # Should return empty dict
        assert result == {}
    
    def test_convert_data_types_with_unknown_table(self, real_postgres_loader_instance):
        """Test data type conversion with unknown table (should not convert booleans)."""
        loader = real_postgres_loader_instance
        
        # Test with unknown table
        test_row = {
            'id': 1,
            'name': 'Test',
            'is_active': 1,  # Should NOT convert since table is unknown
            'is_deleted': 0   # Should NOT convert since table is unknown
        }
        
        converted_row = loader._convert_data_types_for_test_environment('unknown_table', test_row)
        
        # Verify boolean fields were NOT converted for unknown table
        assert converted_row['is_active'] == 1
        assert converted_row['is_deleted'] == 0
        
        # Verify other fields remain unchanged
        assert converted_row['id'] == 1
        assert converted_row['name'] == 'Test' 