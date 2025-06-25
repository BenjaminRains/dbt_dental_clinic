"""
Unit Tests for RawToPublicTransformer

HYBRID TESTING APPROACH - UNIT TESTS
====================================
Purpose: Pure unit tests with comprehensive mocking
Scope: Fast execution, isolated component behavior
Coverage: Core logic and edge cases
Execution: < 1 second per component
Markers: @pytest.mark.unit

This file focuses on testing the core transformation logic in isolation
with comprehensive mocking of all external dependencies.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import Engine, inspect, text
from datetime import datetime
import logging

from etl_pipeline.transformers.raw_to_public import RawToPublicTransformer

logger = logging.getLogger(__name__)

@pytest.fixture
def mock_engines():
    """Create mock database engines for unit tests."""
    source_engine = Mock(spec=Engine)
    target_engine = Mock(spec=Engine)
    
    # Mock engine properties
    source_engine.url = Mock()
    source_engine.url.drivername = 'postgresql'
    target_engine.url = Mock()
    target_engine.url.drivername = 'postgresql'
    
    return source_engine, target_engine

@pytest.fixture
def transformer(mock_engines):
    """Create a real RawToPublicTransformer instance with mocked engines."""
    source_engine, target_engine = mock_engines
    
    # Mock the inspect function to return a mock inspector
    mock_inspector = Mock()
    with patch('etl_pipeline.transformers.raw_to_public.inspect') as mock_inspect:
        mock_inspect.return_value = mock_inspector
        return RawToPublicTransformer(source_engine, target_engine)

@pytest.fixture
def sample_raw_data():
    """Sample data from raw schema with PostgreSQL field names."""
    return pd.DataFrame({
        'PatNum': [1, 2, 3],  # Remove quotes - these are actual column names
        'LName': ['Doe', 'Smith', 'Johnson'],
        'FName': ['John', 'Jane', 'Bob'],
        'BirthDate': ['1990-01-01', '1985-05-15', '1978-12-25'],
        'Gender': ['M', 'F', 'M'],
        'SSN': ['123-45-6789', '987-65-4321', '555-12-3456']
    })

@pytest.fixture
def sample_transformed_data():
    """Sample data after transformation to public schema."""
    return pd.DataFrame({
        'patnum': [1, 2, 3],
        'lname': ['Doe', 'Smith', 'Johnson'],
        'fname': ['John', 'Jane', 'Bob'],
        'birthdate': ['1990-01-01', '1985-05-15', '1978-12-25'],
        'gender': ['M', 'F', 'M'],
        'ssn': ['123-45-6789', '987-65-4321', '555-12-3456']
    })

@pytest.mark.unit
def test_transformer_initialization(transformer, mock_engines):
    """Test transformer initialization with proper schema configuration."""
    source_engine, target_engine = mock_engines
    
    assert transformer.source_engine == source_engine
    assert transformer.target_engine == target_engine
    assert transformer.source_schema == 'raw'
    assert transformer.target_schema == 'public'
    assert transformer.inspector is not None

@pytest.mark.unit
def test_get_last_transformed_success(transformer):
    """Test successful retrieval of last transformation timestamp."""
    # Mock the database connection and query
    mock_conn = Mock()
    mock_result = Mock()
    mock_result.scalar.return_value = datetime(2024, 1, 1, 12, 0, 0)
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_result
        
        result = transformer.get_last_transformed('patient')
        
        assert result == datetime(2024, 1, 1, 12, 0, 0)
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_not_called()  # No commit for SELECT

@pytest.mark.unit
def test_get_last_transformed_no_data(transformer):
    """Test retrieval when no transformation history exists."""
    mock_conn = Mock()
    mock_result = Mock()
    mock_result.scalar.return_value = None
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_result
        
        result = transformer.get_last_transformed('patient')
        
        assert result is None

@pytest.mark.unit
def test_get_last_transformed_error(transformer):
    """Test error handling when database query fails."""
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.side_effect = Exception("Database error")
        
        result = transformer.get_last_transformed('patient')
        
        assert result is None

@pytest.mark.unit
def test_update_transform_status_success(transformer):
    """Test successful update of transformation status."""
    mock_conn = Mock()
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        transformer.update_transform_status('patient', 100, 'success')
        
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

@pytest.mark.unit
def test_update_transform_status_error(transformer):
    """Test error handling when status update fails."""
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.side_effect = Exception("Database error")
        
        # Should not raise exception, just log error
        transformer.update_transform_status('patient', 100, 'success')

@pytest.mark.unit
def test_verify_transform_success(transformer):
    """Test successful transformation verification."""
    with patch.object(transformer, 'get_table_row_count') as mock_count:
        mock_count.side_effect = [100, 100]  # Source and target have same count
        
        result = transformer.verify_transform('patient')
        
        assert result is True
        assert mock_count.call_count == 2

@pytest.mark.unit
def test_verify_transform_mismatch(transformer):
    """Test transformation verification when counts don't match."""
    with patch.object(transformer, 'get_table_row_count') as mock_count:
        mock_count.side_effect = [100, 95]  # Different counts
        
        result = transformer.verify_transform('patient')
        
        assert result is False

@pytest.mark.unit
def test_verify_transform_error(transformer):
    """Test error handling during transformation verification."""
    with patch.object(transformer, 'get_table_row_count') as mock_count:
        mock_count.side_effect = Exception("Database error")
        
        result = transformer.verify_transform('patient')
        
        assert result is False

@pytest.mark.unit
def test_get_table_schema_success(transformer):
    """Test successful retrieval of table schema."""
    mock_schema = {
        'columns': [{'name': 'id', 'type': 'integer'}],
        'primary_key': ['id'],
        'foreign_keys': [],
        'indexes': [],
        'constraints': []
    }
    
    with patch.object(transformer, 'get_table_columns') as mock_columns, \
         patch.object(transformer, 'get_table_primary_key') as mock_pk, \
         patch.object(transformer, 'get_table_foreign_keys') as mock_fk, \
         patch.object(transformer, 'get_table_indexes') as mock_idx, \
         patch.object(transformer, 'get_table_constraints') as mock_constraints:
        
        mock_columns.return_value = mock_schema['columns']
        mock_pk.return_value = mock_schema['primary_key']
        mock_fk.return_value = mock_schema['foreign_keys']
        mock_idx.return_value = mock_schema['indexes']
        mock_constraints.return_value = mock_schema['constraints']
        
        result = transformer.get_table_schema('patient')
        
        assert result == mock_schema

@pytest.mark.unit
def test_get_table_schema_error(transformer):
    """Test error handling when schema retrieval fails."""
    with patch.object(transformer, 'get_table_columns') as mock_columns:
        mock_columns.side_effect = Exception("Database error")
        
        result = transformer.get_table_schema('patient')
        
        assert result == {}

@pytest.mark.unit
def test_has_schema_changed_true(transformer):
    """Test schema change detection when schema has changed."""
    with patch.object(transformer, 'get_table_schema') as mock_schema:
        mock_schema.return_value = {'columns': [{'name': 'new_column'}]}
        
        result = transformer.has_schema_changed('patient', 'old_hash')
        
        assert result is True

@pytest.mark.unit
def test_has_schema_changed_false(transformer):
    """Test schema change detection when schema hasn't changed."""
    test_schema = {'columns': [{'name': 'id'}]}
    schema_hash = str(hash(str(test_schema)))
    
    with patch.object(transformer, 'get_table_schema') as mock_schema:
        mock_schema.return_value = test_schema
        
        result = transformer.has_schema_changed('patient', schema_hash)
        
        assert result is False

@pytest.mark.unit
def test_get_table_metadata_success(transformer):
    """Test successful retrieval of table metadata."""
    mock_metadata = {
        'row_count': 100,
        'size': 1024,
        'last_transformed': datetime.now(),
        'schema': {'columns': []}
    }
    
    with patch.object(transformer, 'get_table_row_count') as mock_count, \
         patch.object(transformer, 'get_table_size') as mock_size, \
         patch.object(transformer, 'get_last_transformed') as mock_last, \
         patch.object(transformer, 'get_table_schema') as mock_schema:
        
        mock_count.return_value = mock_metadata['row_count']
        mock_size.return_value = mock_metadata['size']
        mock_last.return_value = mock_metadata['last_transformed']
        mock_schema.return_value = mock_metadata['schema']
        
        result = transformer.get_table_metadata('patient')
        
        assert result == mock_metadata

@pytest.mark.unit
def test_get_table_row_count_success(transformer):
    """Test successful row count retrieval."""
    mock_conn = Mock()
    mock_result = Mock()
    mock_result.scalar.return_value = 100
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_result
        
        result = transformer.get_table_row_count('patient')
        
        assert result == 100

@pytest.mark.unit
def test_get_table_row_count_error(transformer):
    """Test error handling when row count retrieval fails."""
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.side_effect = Exception("Database error")
        
        result = transformer.get_table_row_count('patient')
        
        assert result == 0

@pytest.mark.unit
def test_apply_transformations_column_lowercasing(transformer, sample_raw_data):
    """Test that column names are converted to lowercase."""
    result = transformer._apply_transformations(sample_raw_data, 'patient')
    
    # Check that column names are lowercase
    expected_columns = ['patnum', 'lname', 'fname', 'birthdate', 'gender', 'ssn']
    assert list(result.columns) == expected_columns

@pytest.mark.unit
def test_apply_transformations_data_preservation(transformer, sample_raw_data):
    """Test that data values are preserved during transformation."""
    result = transformer._apply_transformations(sample_raw_data, 'patient')
    
    # Check that data values are preserved
    assert result.iloc[0]['patnum'] == 1
    assert result.iloc[0]['lname'] == 'Doe'
    assert result.iloc[0]['fname'] == 'John'

@pytest.mark.unit
def test_apply_transformations_empty_dataframe(transformer):
    """Test transformation with empty dataframe."""
    empty_df = pd.DataFrame()
    result = transformer._apply_transformations(empty_df, 'patient')
    
    assert result.empty
    assert isinstance(result, pd.DataFrame)

@pytest.mark.unit
def test_convert_data_types_basic_conversion(transformer):
    """Test basic data type conversion."""
    df = pd.DataFrame({
        'id': ['1', '2', '3'],
        'name': ['John', 'Jane', 'Bob'],
        'active': ['true', 'false', 'true']
    })
    
    result = transformer._convert_data_types(df, 'patient')
    
    # Should preserve the dataframe structure
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert 'id' in result.columns
    assert 'name' in result.columns
    assert 'active' in result.columns

@pytest.mark.unit
def test_apply_table_specific_transformations(transformer, sample_transformed_data):
    """Test table-specific transformation application."""
    result = transformer._apply_table_specific_transformations(sample_transformed_data, 'patient')
    
    # Should preserve the dataframe structure
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(sample_transformed_data)
    assert list(result.columns) == list(sample_transformed_data.columns)

@pytest.mark.unit
def test_transform_table_success_flow(transformer, sample_raw_data):
    """Test successful table transformation flow."""
    with patch.object(transformer, '_read_from_raw') as mock_read, \
         patch.object(transformer, '_write_to_public') as mock_write, \
         patch.object(transformer, '_update_transformation_status') as mock_update:
        
        mock_read.return_value = sample_raw_data
        mock_write.return_value = True
        
        result = transformer.transform_table('patient')
        
        assert result is True
        mock_read.assert_called_once_with('patient', False)
        mock_write.assert_called_once()
        mock_update.assert_called_once_with('patient', 3)  # 3 rows

@pytest.mark.unit
def test_transform_table_empty_data(transformer):
    """Test transformation with empty data."""
    with patch.object(transformer, '_read_from_raw') as mock_read:
        mock_read.return_value = pd.DataFrame()
        
        result = transformer.transform_table('patient')
        
        # The transformer returns True for empty data (not an error)
        assert result is True

@pytest.mark.unit
def test_transform_table_read_error(transformer):
    """Test transformation when read operation fails."""
    with patch.object(transformer, '_read_from_raw') as mock_read:
        mock_read.side_effect = Exception("Read error")
        
        result = transformer.transform_table('patient')
        
        assert result is False

@pytest.mark.unit
def test_transform_table_write_error(transformer, sample_raw_data):
    """Test transformation when write operation fails."""
    with patch.object(transformer, '_read_from_raw') as mock_read, \
         patch.object(transformer, '_write_to_public') as mock_write:
        
        mock_read.return_value = sample_raw_data
        mock_write.return_value = False
        
        result = transformer.transform_table('patient')
        
        assert result is False

@pytest.mark.unit
def test_read_from_raw_success(transformer, sample_raw_data):
    """Test successful read from raw schema."""
    # Mock the engine.connect() to return a proper context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    with patch.object(transformer.source_engine, 'connect') as mock_connect, \
         patch('pandas.read_sql') as mock_read_sql:
        
        mock_connect.return_value = mock_conn
        mock_read_sql.return_value = sample_raw_data
        
        result = transformer._read_from_raw('patient', False)
        
        assert result.equals(sample_raw_data)
        mock_read_sql.assert_called_once()

@pytest.mark.unit
def test_read_from_raw_incremental(transformer, sample_raw_data):
    """Test incremental read from raw schema."""
    # Mock the engine.connect() to return a proper context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    with patch.object(transformer.source_engine, 'connect') as mock_connect, \
         patch('pandas.read_sql') as mock_read_sql:
        
        mock_connect.return_value = mock_conn
        mock_read_sql.return_value = sample_raw_data
        
        result = transformer._read_from_raw('patient', True)
        
        assert result.equals(sample_raw_data)
        # Should use different query for incremental
        mock_read_sql.assert_called_once()

@pytest.mark.unit
def test_write_to_public_success(transformer, sample_transformed_data):
    """Test successful write to public schema."""
    # Mock the engine.connect() to return a proper context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect, \
         patch.object(transformer, '_ensure_table_exists') as mock_ensure, \
         patch.object(sample_transformed_data, 'to_sql') as mock_to_sql:
        
        mock_connect.return_value = mock_conn
        mock_to_sql.return_value = None
        
        result = transformer._write_to_public('patient', sample_transformed_data, False)
        
        assert result is True
        mock_ensure.assert_called_once_with('patient', sample_transformed_data)
        mock_to_sql.assert_called_once()

@pytest.mark.unit
def test_write_to_public_error(transformer, sample_transformed_data):
    """Test error handling during write to public."""
    with patch.object(transformer, '_ensure_table_exists') as mock_ensure:
        mock_ensure.side_effect = Exception("Table creation error")
        
        result = transformer._write_to_public('patient', sample_transformed_data, False)
        
        assert result is False

@pytest.mark.unit
def test_ensure_table_exists_success(transformer, sample_transformed_data):
    """Test successful table existence check."""
    mock_conn = Mock()
    mock_result = Mock()
    mock_result.fetchone.return_value = ['patient']  # Table exists
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_result
        
        transformer._ensure_table_exists('patient', sample_transformed_data)
        
        # Should not create table if it exists
        mock_conn.execute.assert_called()

@pytest.mark.unit
def test_ensure_table_exists_creates_table(transformer, sample_transformed_data):
    """Test table creation when table doesn't exist."""
    # Mock the engine.connect() to return a proper context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    mock_result = Mock()
    mock_result.scalar.return_value = False  # Table doesn't exist (False)
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect, \
         patch.object(transformer, '_generate_create_table_sql') as mock_sql:
        
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value = mock_result
        mock_sql.return_value = "CREATE TABLE patient (...)"
        
        transformer._ensure_table_exists('patient', sample_transformed_data)
        
        # Should create table if it doesn't exist
        # First call: check if table exists, Second call: create table
        assert mock_conn.execute.call_count == 2  # Check + create

@pytest.mark.unit
def test_update_transformation_status_internal(transformer):
    """Test internal transformation status update."""
    # Mock the engine.connect() to return a proper context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value = mock_conn
        
        transformer._update_transformation_status('patient', 100)
        
        # Verify the SQL was executed
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once() 