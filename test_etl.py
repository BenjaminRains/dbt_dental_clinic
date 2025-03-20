import os
import pytest
import pandas as pd
import numpy as np
from sqlalchemy import quoted_name, text
from unittest.mock import Mock, patch, MagicMock, call
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.exc import SQLAlchemyError
from mariadb_postgre_pipe import (
    sanitize_identifier,
    map_type,
    convert_data_types,
    ETLConfig,
    validate_schema,
    verify_data_quality,
    sync_table_directly,
    ensure_tracking_table_exists
)

# Helper function to create mock SQLAlchemy result
def create_mock_result(data=None, scalar_value=None):
    mock_result = MagicMock(spec=CursorResult)
    if data is not None:
        mock_result.fetchall.return_value = data
        mock_result.scalar.return_value = data[0][0] if data else None
    if scalar_value is not None:
        mock_result.scalar.return_value = scalar_value
    return mock_result

@pytest.fixture
def mock_engines():
    with patch('mariadb_postgre_pipe.mariadb_engine') as mock_maria, \
         patch('mariadb_postgre_pipe.pg_engine') as mock_pg:
        yield mock_maria, mock_pg

@pytest.fixture
def reset_etl_config():
    """Reset ETLConfig singleton between tests"""
    ETLConfig._instance = None
    yield
    ETLConfig._instance = None

@pytest.fixture
def test_config(reset_etl_config):
    """Provide a test configuration with controlled values"""
    with patch.dict(os.environ, {
        'ETL_CHUNK_SIZE': '1000',
        'ETL_SUB_CHUNK_SIZE': '100',
        'ETL_MAX_RETRIES': '3',
        'ETL_TOLERANCE': '0.001',
        'ETL_SMALL_TABLE_THRESHOLD': '500'
    }):
        config = ETLConfig()
        yield config

def test_sanitize_identifier():
    # Test basic identifiers
    result = sanitize_identifier("valid_name")
    assert isinstance(result, quoted_name)
    assert result.quote is True
    assert str(result) == 'valid_name'
    
    # Test numbers in identifiers
    result = sanitize_identifier("valid123")
    assert isinstance(result, quoted_name)
    assert str(result) == 'valid123'
    
    # Test invalid inputs
    with pytest.raises(ValueError):
        sanitize_identifier("")
    with pytest.raises(ValueError):
        sanitize_identifier(None)
    
    # Test handling of special characters
    result = sanitize_identifier("column-name")
    assert str(result) == 'columnname'
    result = sanitize_identifier("table.name")
    assert str(result) == 'tablename'

def test_map_type():
    assert map_type("int") == "INTEGER"
    assert map_type("varchar(255)") == "VARCHAR(255)"
    assert map_type("unknown_type") == "TEXT"
    assert "NUMERIC" in map_type("decimal(10,2)")
    assert map_type("tinyint(1)") == "BOOLEAN"
    # Test edge cases
    assert map_type("varchar") == "VARCHAR(255)"  # Default when no length specified
    assert map_type("decimal") == "NUMERIC(10,2)"  # Default when no precision specified

def test_convert_data_types():
    df = pd.DataFrame({
        'int_col': [1, 2, None],
        'str_col': ['a', 'None', None],
        'date_col': pd.date_range('2021-01-01', periods=3),
        'float_col': [1.1, None, 3.3],
        'bool_col': [True, False, None]
    })
    
    # Test without table_name parameter
    converted = convert_data_types(df)
    
    assert pd.api.types.is_numeric_dtype(converted['int_col'])
    assert converted['str_col'].isna().sum() == 2  # Both None and 'None' converted
    assert pd.api.types.is_datetime64_dtype(converted['date_col'])
    assert pd.api.types.is_float_dtype(converted['float_col'])
    assert converted['float_col'].isna().sum() == 1  # None preserved
    
    # Instead of checking dtype, check that values are actual boolean values
    bool_values = converted['bool_col'].dropna().unique()
    assert all(isinstance(v, bool) for v in bool_values)
    assert converted['bool_col'].isna().sum() == 1  # None preserved
    
    # Test with table_name parameter
    with patch('mariadb_postgre_pipe.get_not_null_columns') as mock_get_not_null_cols:
        mock_get_not_null_cols.return_value = ['int_col', 'date_col']
        
        # Create a new DataFrame with some NULLs to test NOT NULL handling
        df_with_nulls = pd.DataFrame({
            'int_col': [1, None, 3],
            'date_col': [pd.Timestamp('2021-01-01'), None, pd.Timestamp('2021-01-03')]
        })
        
        converted_with_table = convert_data_types(df_with_nulls, 'test_table')
        
        # Check that NULLs were filled for NOT NULL columns
        assert converted_with_table['int_col'].isna().sum() == 0
        assert converted_with_table['date_col'].isna().sum() == 0
        
        # Check that proper default values were used
        assert converted_with_table.loc[1, 'int_col'] == 0
        assert converted_with_table.loc[1, 'date_col'] == pd.Timestamp('0001-01-01')

def test_etl_config_singleton():
    with patch.dict(os.environ, {
        'ETL_CHUNK_SIZE': '50000',
        'ETL_SUB_CHUNK_SIZE': '5000',
        'ETL_MAX_RETRIES': '5',
        'ETL_TOLERANCE': '0.002',
        'ETL_SMALL_TABLE_THRESHOLD': '5000'
    }):
        config1 = ETLConfig()
        config2 = ETLConfig()
    
        # Test singleton pattern
        assert config1 is config2
    
        # Test configuration values
        assert config1.chunk_size == 50000
        assert config1.sub_chunk_size == 5000
        assert config1.max_retries == 5
        assert config1.tolerance == 0.002
        assert config1.small_table_threshold == 5000

@patch('mariadb_postgre_pipe.pg_engine')
def test_validate_schema(mock_pg_engine):
    # Setup mock connection and cursor
    mock_conn = MagicMock()
    mock_pg_engine.connect.return_value.__enter__.return_value = mock_conn
    
    # Mock the column query result
    mock_result = create_mock_result([('id', 'integer'), ('name', 'varchar')])
    mock_conn.execute.return_value = mock_result
    
    # Test data
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['a', 'b', 'c'],
        'new_col': [1, 2, 3]
    })
    
    result = validate_schema('test_table', df)
    assert result is True
    
    # Verify proper SQL execution
    mock_conn.execute.assert_called()
    
    # Test error handling
    mock_conn.execute.side_effect = SQLAlchemyError("Database error")
    result = validate_schema('test_table', df)
    assert result is False

@patch('mariadb_postgre_pipe.pg_engine')
@patch('mariadb_postgre_pipe.mariadb_engine')
def test_verify_data_quality_with_pk_check(mock_mariadb_engine, mock_pg_engine):
    # Setup mock connections
    mock_maria_conn = MagicMock()
    mock_pg_conn = MagicMock()
    mock_mariadb_engine.connect.return_value.__enter__.return_value = mock_maria_conn
    mock_pg_engine.connect.return_value.__enter__.return_value = mock_pg_conn
    
    # Mock row counts for a small table
    mock_maria_conn.execute.return_value = create_mock_result(scalar_value=100)
    mock_pg_conn.execute.return_value = create_mock_result(scalar_value=100)
    
    # Mock primary key check
    pk_result = create_mock_result([('id',)])
    null_check_result = create_mock_result(scalar_value=0)
    
    mock_maria_conn.execute.side_effect = [
        create_mock_result(scalar_value=100),  # Row count
        pk_result,  # PK query
    ]
    mock_pg_conn.execute.side_effect = [
        create_mock_result(scalar_value=100),  # Row count
        null_check_result  # NULL check
    ]
    
    with patch.dict(os.environ, {'ETL_SMALL_TABLE_THRESHOLD': '1000'}):
        result = verify_data_quality('test_table')
        assert result is True

    # Test NULL primary keys
    mock_maria_conn.execute.side_effect = [
        create_mock_result(scalar_value=100),
        pk_result
    ]
    mock_pg_conn.execute.side_effect = [
        create_mock_result(scalar_value=100),
        create_mock_result(scalar_value=5)  # 5 NULL values in PK
    ]
    
    result = verify_data_quality('test_table')
    assert result is False

@patch('mariadb_postgre_pipe.pg_engine')
@patch('mariadb_postgre_pipe.mariadb_engine')
@patch('mariadb_postgre_pipe.fetch_last_sync')
@patch('mariadb_postgre_pipe.validate_schema')
@patch('mariadb_postgre_pipe.convert_data_types')
@patch('mariadb_postgre_pipe.update_sync_timestamp')
@patch('logging.getLogger')
def test_sync_table_directly(mock_logger, mock_update_sync, mock_convert_types, mock_validate_schema, mock_fetch_last_sync, mock_mariadb_engine, mock_pg_engine, test_config):
    """Test sync_table_directly with proper mocking of all dependencies"""
    # Setup logger mock
    mock_logger_instance = MagicMock()
    mock_logger.return_value = mock_logger_instance
    
    # Setup mock connections and cursors
    mock_maria_conn = MagicMock()
    mock_pg_conn = MagicMock()
    mock_mariadb_engine.connect.return_value.__enter__.return_value = mock_maria_conn
    mock_pg_engine.connect.return_value.__enter__.return_value = mock_pg_conn
    
    # Setup transaction context manager
    mock_transaction = MagicMock()
    mock_transaction.__enter__.return_value = mock_pg_conn
    mock_transaction.__exit__.return_value = None
    mock_pg_engine.begin.return_value = mock_transaction
    
    # Mock fetch_last_sync
    mock_fetch_last_sync.return_value = '1970-01-01 00:00:00'
    
    # Mock validate_schema and convert_data_types
    mock_validate_schema.return_value = True
    mock_convert_types.return_value = pd.DataFrame({
        'id': range(10),
        'name': [f'name_{i}' for i in range(10)]
    })
    mock_update_sync.return_value = True
    
    # Create test data that's smaller and simpler
    test_data = pd.DataFrame({
        'id': range(10),
        'name': [f'name_{i}' for i in range(10)]
    })
    
    # Mock the database queries
    mock_maria_conn.execute.side_effect = [
        create_mock_result(scalar_value=1),  # last_modified column check
        create_mock_result(scalar_value=10),  # row count check
        create_mock_result([('id',)]),        # primary key query result
    ]
    
    # Create a real DataFrame for to_sql patching
    df_for_to_sql = test_data.copy()
    
    # Create better mock for read_sql
    def mock_read_sql_func(query, con, params=None, chunksize=None, **kwargs):
        return [test_data]
    
    # Create better mock for to_sql
    def mock_to_sql_func(self, name, con, if_exists='fail', index=False, chunksize=None, **kwargs):
        return None
    
    # Mock pandas read_sql and DataFrame.to_sql
    with patch('pandas.read_sql', side_effect=mock_read_sql_func) as mock_read_sql, \
         patch.object(pd.DataFrame, 'to_sql', mock_to_sql_func):
        
        result = sync_table_directly('test_table')
        
        # Debug if we get a failure
        if not result:
            print("sync_table_directly returned False")
            print(f"Logger calls: {mock_logger_instance.error.call_args_list}")
        
        assert result is True
        
        # Verify read_sql was called
        mock_read_sql.assert_called()
        
        # Verify validate_schema was called
        mock_validate_schema.assert_called()
        
        # Verify convert_data_types was called
        mock_convert_types.assert_called()
        
        # Verify convert_data_types was called with the correct table name
        mock_convert_types.assert_called_with(test_data, 'test_table')
        
        # Verify update_sync_timestamp was called
        mock_update_sync.assert_called()
    
    # Test error handling
    mock_maria_conn.execute.side_effect = SQLAlchemyError("Database error")
    result = sync_table_directly('test_table')
    assert result is False

@patch('mariadb_postgre_pipe.pg_engine')
@patch('mariadb_postgre_pipe.mariadb_engine')
@patch('mariadb_postgre_pipe.fetch_last_sync')
def test_sync_table_directly_with_chunks(mock_fetch_last_sync, mock_mariadb_engine, mock_pg_engine, test_config):
    """Test sync_table_directly with large dataset to verify chunking behavior"""
    # Setup mock connections
    mock_maria_conn = MagicMock()
    mock_pg_conn = MagicMock()
    mock_mariadb_engine.connect.return_value.__enter__.return_value = mock_maria_conn
    mock_pg_engine.connect.return_value.__enter__.return_value = mock_pg_conn
    
    # Mock fetch_last_sync
    mock_fetch_last_sync.return_value = '1970-01-01 00:00:00'
    
    # Create large test dataset (10,000 rows)
    total_rows = 10000
    chunk_size = test_config.chunk_size  # 1000 from fixture
    num_chunks = total_rows // chunk_size
    
    test_data = pd.DataFrame({
        'id': range(total_rows),
        'name': [f'name_{i}' for i in range(total_rows)],
        'value': np.random.rand(total_rows)
    })
    
    # Mock the database queries
    mock_maria_conn.execute.side_effect = [
        create_mock_result(scalar_value=0),  # last_modified column check
        create_mock_result(scalar_value=total_rows),  # row count check
        create_mock_result([('id',)]),  # primary key check
    ]
    
    # Create chunks for testing
    chunks = [test_data[i:i+chunk_size] for i in range(0, total_rows, chunk_size)]
    
    # Mock pandas read_sql to return chunks
    with patch('pandas.read_sql') as mock_read_sql:
        # Create a mock iterator that yields our chunks
        mock_iterator = MagicMock()
        mock_iterator.__iter__.return_value = iter(chunks)
        mock_read_sql.return_value = mock_iterator
        
        result = sync_table_directly('test_table')
        assert result is True
        
        # Verify chunk processing
        mock_read_sql.assert_called_once()
        
        # Verify the exact calls made
        expected_calls = [
            call(text("SELECT COUNT(*) FROM \"test_table\"")).scalar(),
            call(text("SELECT MAX(\"id\") FROM \"test_table\"")).scalar()
        ]
        mock_pg_conn.execute.assert_has_calls(expected_calls, any_order=True)

@patch('mariadb_postgre_pipe.pg_engine')
@patch('mariadb_postgre_pipe.mariadb_engine')
def test_verify_data_quality_call_counts(mock_mariadb_engine, mock_pg_engine, test_config):
    """Test verify_data_quality with explicit call count verification"""
    mock_maria_conn = MagicMock()
    mock_pg_conn = MagicMock()
    mock_mariadb_engine.connect.return_value.__enter__.return_value = mock_maria_conn
    mock_pg_engine.connect.return_value.__enter__.return_value = mock_pg_conn
    
    # Setup return values
    mock_maria_conn.execute.side_effect = [
        create_mock_result(scalar_value=1000),  # row count
        create_mock_result([('id',)])  # primary key query
    ]
    
    mock_pg_conn.execute.side_effect = [
        create_mock_result(scalar_value=1000),  # row count
        create_mock_result(scalar_value=0)  # null check
    ]
    
    result = verify_data_quality('test_table')
    assert result is True
    
    # Verify exact number of database calls
    assert mock_maria_conn.execute.call_count == 2  # row count + pk query
    assert mock_pg_conn.execute.call_count == 2    # row count + null check
    
    # Get the actual calls made
    actual_calls = mock_maria_conn.execute.call_args_list
    
    # Verify the calls were made with the correct SQL
    # Check the first call argument's text property
    first_call_arg = actual_calls[0][0][0]
    second_call_arg = actual_calls[1][0][0]
    
    if hasattr(first_call_arg, 'text'):  # For TextClause objects
        assert 'COUNT(*)' in str(first_call_arg.text)
    else:
        assert 'COUNT(*)' in str(first_call_arg)
        
    if hasattr(second_call_arg, 'text'):  # For TextClause objects
        assert 'COLUMN_NAME' in str(second_call_arg.text)
        assert 'information_schema.KEY_COLUMN_USAGE' in str(second_call_arg.text)
    else:
        assert 'COLUMN_NAME' in str(second_call_arg)
        assert 'information_schema.KEY_COLUMN_USAGE' in str(second_call_arg)

@patch('mariadb_postgre_pipe.pg_engine')
@patch('mariadb_postgre_pipe.fetch_table_names')
def test_ensure_tracking_table_exists(mock_fetch_tables, mock_pg_engine):
    # Setup mock connection
    mock_conn = MagicMock()
    mock_pg_engine.begin.return_value.__enter__.return_value = mock_conn
    
    # Mock fetch_table_names to return tables
    mock_fetch_tables.return_value = ['table1', 'table2']
    
    # Test table doesn't exist
    mock_conn.execute.side_effect = [
        create_mock_result(scalar_value=False),  # Table exists check
        None,  # CREATE TABLE
        None, None  # Two insert operations for the tables
    ]
    
    result = ensure_tracking_table_exists()
    assert result is True
    
    # Verify proper transaction handling
    mock_pg_engine.begin.assert_called_once()
    
    # Test error handling
    mock_pg_engine.begin.side_effect = SQLAlchemyError("Transaction error")
    result = ensure_tracking_table_exists()
    assert result is False

@patch('mariadb_postgre_pipe.pg_engine')
@patch('mariadb_postgre_pipe.fetch_table_names')
def test_ensure_tracking_table_exists_calls(mock_fetch_tables, mock_pg_engine):
    """Test ensure_tracking_table_exists with explicit call count verification"""
    mock_conn = MagicMock()
    mock_pg_engine.begin.return_value.__enter__.return_value = mock_conn
    
    # Mock fetch_table_names to return tables
    mock_fetch_tables.return_value = ['table1', 'table2']
    
    # Mock sequence of operations
    mock_conn.execute.side_effect = [
        create_mock_result(scalar_value=False),  # Table check
        None,  # Create table
        None, None  # Two insert operations for the tables
    ]
    
    result = ensure_tracking_table_exists()
    assert result is True
    
    # Verify transaction handling
    assert mock_pg_engine.begin.call_count == 1
    
    # Verify sequence of operations
    assert mock_conn.execute.call_count == 4  # check + create + 2 inserts
    
    # Get the actual calls made
    actual_calls = mock_conn.execute.call_args_list
    
    # Extract the first two SQL statements from the actual calls
    first_sql = str(actual_calls[0][0][0].text).strip() if hasattr(actual_calls[0][0][0], 'text') else str(actual_calls[0][0][0]).strip()
    second_sql = str(actual_calls[1][0][0].text).strip() if hasattr(actual_calls[1][0][0], 'text') else str(actual_calls[1][0][0]).strip()
    
    # Verify the SQL content matches
    assert "SELECT EXISTS" in first_sql
    assert "table_name = 'etl_sync_status'" in first_sql
    assert "CREATE TABLE etl_sync_status" in second_sql
    assert "id SERIAL PRIMARY KEY" in second_sql

def test_transaction_rollback():
    """Test that transactions are properly rolled back on error."""
    with patch('mariadb_postgre_pipe.pg_engine') as mock_pg_engine:
        mock_conn = MagicMock()
        mock_pg_engine.begin.return_value.__enter__.return_value = mock_conn
        
        # Simulate an error during execution
        mock_conn.execute.side_effect = SQLAlchemyError("Database error")
        
        result = ensure_tracking_table_exists()
        assert result is False
        
        # Verify that rollback was called
        mock_pg_engine.begin.return_value.__exit__.assert_called()

def test_etl_config_isolation(reset_etl_config):
    """Test that ETLConfig is properly isolated between tests"""
    with patch.dict(os.environ, {'ETL_CHUNK_SIZE': '1000'}):
        config1 = ETLConfig()
        assert config1.chunk_size == 1000
    
    # Reset singleton
    ETLConfig._instance = None
    
    with patch.dict(os.environ, {'ETL_CHUNK_SIZE': '2000'}):
        config2 = ETLConfig()
        assert config2.chunk_size == 2000
        assert config1 is not config2

if __name__ == '__main__':
    pytest.main([__file__])