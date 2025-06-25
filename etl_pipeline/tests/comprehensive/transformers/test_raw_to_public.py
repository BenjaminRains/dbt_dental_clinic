"""
Comprehensive Tests for RawToPublicTransformer

HYBRID TESTING APPROACH - COMPREHENSIVE TESTS
=============================================
Purpose: Full functionality testing with mocked dependencies
Scope: Complete component behavior, error handling
Coverage: 90%+ target coverage (main test suite)
Execution: < 5 seconds per component
Markers: @pytest.mark.unit (default)

This file focuses on testing the complete RawToPublicTransformer functionality
with comprehensive mocking of external dependencies but testing real transformation logic.
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
    """Create mock database engines for comprehensive tests."""
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
        '"PatNum"': [1, 2, 3, 4, 5],
        '"LName"': ['Doe', 'Smith', 'Johnson', 'Brown', 'Wilson'],
        '"FName"': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
        '"BirthDate"': ['1990-01-01', '1985-05-15', '1978-12-25', '1992-03-10', '1988-07-20'],
        '"Gender"': ['M', 'F', 'M', 'F', 'M'],
        '"SSN"': ['123-45-6789', '987-65-4321', '555-12-3456', '111-22-3333', '444-55-6666'],
        '"Address"': ['123 Main St', '456 Oak Ave', '789 Pine Rd', '321 Elm St', '654 Maple Dr'],
        '"City"': ['Anytown', 'Somewhere', 'Elsewhere', 'Nowhere', 'Everywhere'],
        '"State"': ['CA', 'NY', 'TX', 'FL', 'WA'],
        '"Zip"': ['12345', '67890', '11111', '22222', '33333']
    })

@pytest.fixture
def sample_transformed_data():
    """Sample data after transformation to public schema."""
    return pd.DataFrame({
        'patnum': [1, 2, 3, 4, 5],
        'lname': ['Doe', 'Smith', 'Johnson', 'Brown', 'Wilson'],
        'fname': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
        'birthdate': ['1990-01-01', '1985-05-15', '1978-12-25', '1992-03-10', '1988-07-20'],
        'gender': ['M', 'F', 'M', 'F', 'M'],
        'ssn': ['123-45-6789', '987-65-4321', '555-12-3456', '111-22-3333', '444-55-6666'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd', '321 Elm St', '654 Maple Dr'],
        'city': ['Anytown', 'Somewhere', 'Elsewhere', 'Nowhere', 'Everywhere'],
        'state': ['CA', 'NY', 'TX', 'FL', 'WA'],
        'zip': ['12345', '67890', '11111', '22222', '33333']
    })

@pytest.fixture
def large_test_dataset():
    """Generate large dataset for performance testing."""
    def _generate_large_dataset(count: int = 1000):
        data = {
            '"PatNum"': list(range(1, count + 1)),
            '"LName"': [f'LastName{i}' for i in range(1, count + 1)],
            '"FName"': [f'FirstName{i}' for i in range(1, count + 1)],
            '"BirthDate"': ['1990-01-01'] * count,
            '"Gender"': ['M' if i % 2 == 0 else 'F' for i in range(1, count + 1)],
            '"SSN"': [f'{i:03d}-{i:02d}-{i:04d}' for i in range(1, count + 1)]
        }
        return pd.DataFrame(data)
    return _generate_large_dataset

def test_transformer_initialization_comprehensive(transformer, mock_engines):
    """Test comprehensive transformer initialization."""
    source_engine, target_engine = mock_engines
    
    assert transformer.source_engine == source_engine
    assert transformer.target_engine == target_engine
    assert transformer.source_schema == 'raw'
    assert transformer.target_schema == 'public'
    assert transformer.inspector is not None
    assert hasattr(transformer, 'source_engine')
    assert hasattr(transformer, 'target_engine')
    assert hasattr(transformer, 'source_schema')
    assert hasattr(transformer, 'target_schema')

def test_get_last_transformed_comprehensive(transformer):
    """Test comprehensive last transformation timestamp retrieval."""
    # Test successful retrieval
    mock_conn = Mock()
    mock_result = Mock()
    mock_result.scalar.return_value = datetime(2024, 1, 1, 12, 0, 0)
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value = mock_result
        
        result = transformer.get_last_transformed('patient')
        
        assert result == datetime(2024, 1, 1, 12, 0, 0)
        mock_conn.execute.assert_called_once()
        
        # Verify the SQL query structure
        call_args = mock_conn.execute.call_args
        assert 'etl_transform_status' in str(call_args)
        assert 'transform_type' in str(call_args)
        assert 'raw_to_public' in str(call_args)

def test_update_transform_status_comprehensive(transformer):
    """Test comprehensive transformation status update."""
    mock_conn = Mock()
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Test success case
        transformer.update_transform_status('patient', 100, 'success')
        
        mock_conn.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        
        # Verify the SQL query structure
        call_args = mock_conn.execute.call_args
        assert 'etl_transform_status' in str(call_args)
        assert 'INSERT INTO' in str(call_args)
        assert 'transform_type' in str(call_args)
        assert 'raw_to_public' in str(call_args)

def test_verify_transform_comprehensive(transformer):
    """Test comprehensive transformation verification."""
    with patch.object(transformer, 'get_table_row_count') as mock_count:
        # Test successful verification
        mock_count.side_effect = [100, 100]  # Source and target have same count
        
        result = transformer.verify_transform('patient')
        
        assert result is True
        assert mock_count.call_count == 2
        
        # Test verification failure
        mock_count.side_effect = [100, 95]  # Different counts
        
        result = transformer.verify_transform('patient')
        
        assert result is False

def test_get_table_schema_comprehensive(transformer):
    """Test comprehensive table schema retrieval."""
    mock_schema = {
        'columns': [
            {'name': 'id', 'type': 'integer', 'nullable': False},
            {'name': 'name', 'type': 'varchar', 'nullable': True},
            {'name': 'created_at', 'type': 'timestamp', 'nullable': False}
        ],
        'primary_key': ['id'],
        'foreign_keys': [
            {'name': 'fk_user_id', 'column': 'user_id', 'referenced_table': 'users'}
        ],
        'indexes': [
            {'name': 'idx_name', 'columns': ['name']}
        ],
        'constraints': [
            {'name': 'chk_status', 'type': 'CHECK', 'definition': 'status IN (active, inactive)'}
        ]
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
        assert 'columns' in result
        assert 'primary_key' in result
        assert 'foreign_keys' in result
        assert 'indexes' in result
        assert 'constraints' in result

def test_has_schema_changed_comprehensive(transformer):
    """Test comprehensive schema change detection."""
    test_schema = {
        'columns': [
            {'name': 'id', 'type': 'integer'},
            {'name': 'name', 'type': 'varchar'}
        ]
    }
    schema_hash = str(hash(str(test_schema)))
    
    with patch.object(transformer, 'get_table_schema') as mock_schema:
        # Test no change
        mock_schema.return_value = test_schema
        
        result = transformer.has_schema_changed('patient', schema_hash)
        
        assert result is False
        
        # Test schema change
        changed_schema = {
            'columns': [
                {'name': 'id', 'type': 'integer'},
                {'name': 'name', 'type': 'varchar'},
                {'name': 'new_column', 'type': 'text'}  # New column
            ]
        }
        mock_schema.return_value = changed_schema
        
        result = transformer.has_schema_changed('patient', schema_hash)
        
        assert result is True

def test_get_table_metadata_comprehensive(transformer):
    """Test comprehensive table metadata retrieval."""
    mock_metadata = {
        'row_count': 1000,
        'size': 1024000,  # 1MB
        'last_transformed': datetime(2024, 1, 1, 12, 0, 0),
        'schema': {
            'columns': [{'name': 'id', 'type': 'integer'}],
            'primary_key': ['id'],
            'foreign_keys': [],
            'indexes': [],
            'constraints': []
        }
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
        assert result['row_count'] == 1000
        assert result['size'] == 1024000
        assert isinstance(result['last_transformed'], datetime)
        assert 'schema' in result

def test_apply_transformations_comprehensive(transformer, sample_raw_data):
    """Test comprehensive data transformations."""
    result = transformer._apply_transformations(sample_raw_data, 'patient')
    
    # Test column name transformation
    expected_columns = [
        'patnum', 'lname', 'fname', 'birthdate', 'gender', 'ssn',
        'address', 'city', 'state', 'zip'
    ]
    assert list(result.columns) == expected_columns
    
    # Test data preservation
    assert result.iloc[0]['patnum'] == 1
    assert result.iloc[0]['lname'] == 'Doe'
    assert result.iloc[0]['fname'] == 'John'
    assert result.iloc[0]['gender'] == 'M'
    
    # Test all rows preserved
    assert len(result) == len(sample_raw_data)
    
    # Test data types
    assert isinstance(result, pd.DataFrame)
    assert not result.empty

def test_apply_transformations_edge_cases(transformer):
    """Test edge cases in data transformations."""
    # Test empty dataframe
    empty_df = pd.DataFrame()
    result = transformer._apply_transformations(empty_df, 'patient')
    assert result.empty
    assert isinstance(result, pd.DataFrame)
    
    # Test single column dataframe
    single_col_df = pd.DataFrame({'"TestCol"': [1, 2, 3]})
    result = transformer._apply_transformations(single_col_df, 'patient')
    assert list(result.columns) == ['testcol']
    assert len(result) == 3
    
    # Test dataframe with special characters in column names
    special_col_df = pd.DataFrame({
        '"Test-Col"': [1],
        '"Test_Col"': [2],
        '"Test Col"': [3]
    })
    result = transformer._apply_transformations(special_col_df, 'patient')
    expected_columns = ['test-col', 'test_col', 'test col']
    assert list(result.columns) == expected_columns

def test_convert_data_types_comprehensive(transformer):
    """Test comprehensive data type conversion."""
    df = pd.DataFrame({
        'id': ['1', '2', '3', 'invalid'],
        'name': ['John', 'Jane', 'Bob', 'Alice'],
        'active': ['true', 'false', 'true', 'invalid'],
        'score': ['1.5', '2.7', '3.0', 'invalid'],
        'date': ['2024-01-01', '2024-02-15', '2024-03-20', 'invalid-date']
    })
    
    result = transformer._convert_data_types(df, 'patient')
    
    # Should preserve the dataframe structure
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 4
    assert 'id' in result.columns
    assert 'name' in result.columns
    assert 'active' in result.columns
    assert 'score' in result.columns
    assert 'date' in result.columns
    
    # Should preserve data values
    assert result.iloc[0]['id'] == '1'
    assert result.iloc[0]['name'] == 'John'
    assert result.iloc[0]['active'] == 'true'

def test_apply_table_specific_transformations_comprehensive(transformer, sample_transformed_data):
    """Test comprehensive table-specific transformations."""
    result = transformer._apply_table_specific_transformations(sample_transformed_data, 'patient')
    
    # Should preserve the dataframe structure
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(sample_transformed_data)
    assert list(result.columns) == list(sample_transformed_data.columns)
    
    # Should preserve data values
    assert result.iloc[0]['patnum'] == 1
    assert result.iloc[0]['lname'] == 'Doe'
    assert result.iloc[0]['fname'] == 'John'

def test_transform_table_comprehensive_success(transformer, sample_raw_data):
    """Test comprehensive successful table transformation."""
    with patch.object(transformer, '_read_from_raw') as mock_read, \
         patch.object(transformer, '_write_to_public') as mock_write, \
         patch.object(transformer, '_update_transformation_status') as mock_update:
        
        mock_read.return_value = sample_raw_data
        mock_write.return_value = True
        
        result = transformer.transform_table('patient')
        
        assert result is True
        mock_read.assert_called_once_with('patient', False)
        mock_write.assert_called_once()
        mock_update.assert_called_once_with('patient', 5)  # 5 rows

def test_transform_table_incremental(transformer, sample_raw_data):
    """Test incremental table transformation."""
    with patch.object(transformer, '_read_from_raw') as mock_read, \
         patch.object(transformer, '_write_to_public') as mock_write, \
         patch.object(transformer, '_update_transformation_status') as mock_update:
        
        mock_read.return_value = sample_raw_data
        mock_write.return_value = True
        
        result = transformer.transform_table('patient', is_incremental=True)
        
        assert result is True
        mock_read.assert_called_once_with('patient', True)
        mock_write.assert_called_once()
        mock_update.assert_called_once_with('patient', 5)

def test_transform_table_error_scenarios(transformer):
    """Test various error scenarios in table transformation."""
    # Test read error
    with patch.object(transformer, '_read_from_raw') as mock_read:
        mock_read.side_effect = Exception("Database connection failed")
        
        result = transformer.transform_table('patient')
        
        assert result is False
    
    # Test empty data
    with patch.object(transformer, '_read_from_raw') as mock_read:
        mock_read.return_value = pd.DataFrame()
        
        result = transformer.transform_table('patient')
        
        assert result is False
    
    # Test write error
    with patch.object(transformer, '_read_from_raw') as mock_read, \
         patch.object(transformer, '_write_to_public') as mock_write:
        
        mock_read.return_value = pd.DataFrame({'test': [1, 2, 3]})
        mock_write.return_value = False
        
        result = transformer.transform_table('patient')
        
        assert result is False

def test_read_from_raw_comprehensive(transformer, sample_raw_data):
    """Test comprehensive read from raw schema."""
    with patch('pandas.read_sql') as mock_read_sql:
        mock_read_sql.return_value = sample_raw_data
        
        # Test full load
        result = transformer._read_from_raw('patient', False)
        
        assert result.equals(sample_raw_data)
        mock_read_sql.assert_called_once()
        
        # Verify SQL query structure for full load
        call_args = mock_read_sql.call_args
        assert 'raw.patient' in str(call_args)

def test_read_from_raw_incremental_comprehensive(transformer, sample_raw_data):
    """Test comprehensive incremental read from raw schema."""
    with patch('pandas.read_sql') as mock_read_sql:
        mock_read_sql.return_value = sample_raw_data
        
        # Test incremental load
        result = transformer._read_from_raw('patient', True)
        
        assert result.equals(sample_raw_data)
        mock_read_sql.assert_called_once()
        
        # Verify SQL query structure for incremental load
        call_args = mock_read_sql.call_args
        assert 'raw.patient' in str(call_args)

def test_write_to_public_comprehensive(transformer, sample_transformed_data):
    """Test comprehensive write to public schema."""
    with patch.object(transformer, '_ensure_table_exists') as mock_ensure, \
         patch.object(sample_transformed_data, 'to_sql') as mock_to_sql:
        
        mock_to_sql.return_value = None
        
        # Test full load
        result = transformer._write_to_public('patient', sample_transformed_data, False)
        
        assert result is True
        mock_ensure.assert_called_once_with('patient', sample_transformed_data)
        mock_to_sql.assert_called_once()
        
        # Verify to_sql parameters
        call_args = mock_to_sql.call_args
        assert call_args[0][0] == 'patient'  # table name
        assert call_args[1]['schema'] == 'public'
        assert call_args[1]['if_exists'] == 'replace'

def test_write_to_public_incremental(transformer, sample_transformed_data):
    """Test incremental write to public schema."""
    with patch.object(transformer, '_ensure_table_exists') as mock_ensure, \
         patch.object(sample_transformed_data, 'to_sql') as mock_to_sql:
        
        mock_to_sql.return_value = None
        
        # Test incremental load
        result = transformer._write_to_public('patient', sample_transformed_data, True)
        
        assert result is True
        mock_ensure.assert_called_once_with('patient', sample_transformed_data)
        mock_to_sql.assert_called_once()
        
        # Verify to_sql parameters for incremental
        call_args = mock_to_sql.call_args
        assert call_args[0][0] == 'patient'  # table name
        assert call_args[1]['schema'] == 'public'
        assert call_args[1]['if_exists'] == 'append'

def test_ensure_table_exists_comprehensive(transformer, sample_transformed_data):
    """Test comprehensive table existence check and creation."""
    mock_conn = Mock()
    mock_result = Mock()
    
    with patch.object(transformer.target_engine, 'connect') as mock_connect, \
         patch.object(transformer, '_generate_create_table_sql') as mock_sql:
        
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Test table exists
        mock_result.fetchone.return_value = ['patient']
        mock_conn.execute.return_value = mock_result
        
        transformer._ensure_table_exists('patient', sample_transformed_data)
        
        # Should not create table if it exists
        mock_conn.execute.assert_called()
        mock_sql.assert_not_called()
        
        # Test table doesn't exist
        mock_result.fetchone.return_value = None
        mock_sql.return_value = "CREATE TABLE patient (...)"
        
        transformer._ensure_table_exists('patient', sample_transformed_data)
        
        # Should create table if it doesn't exist
        assert mock_conn.execute.call_count >= 2  # Check + create
        mock_sql.assert_called_once_with('patient', sample_transformed_data)

def test_performance_with_large_dataset(transformer, large_test_dataset):
    """Test performance with large dataset."""
    large_data = large_test_dataset(1000)
    
    with patch.object(transformer, '_read_from_raw') as mock_read, \
         patch.object(transformer, '_write_to_public') as mock_write, \
         patch.object(transformer, '_update_transformation_status') as mock_update:
        
        mock_read.return_value = large_data
        mock_write.return_value = True
        
        # Test transformation with large dataset
        result = transformer.transform_table('patient')
        
        assert result is True
        mock_read.assert_called_once_with('patient', False)
        mock_write.assert_called_once()
        mock_update.assert_called_once_with('patient', 1000)

def test_error_handling_comprehensive(transformer):
    """Test comprehensive error handling across all methods."""
    # Test database connection errors
    with patch.object(transformer.target_engine, 'connect') as mock_connect:
        mock_connect.return_value.__enter__.side_effect = Exception("Connection failed")
        
        # These should handle errors gracefully
        result = transformer.get_last_transformed('patient')
        assert result is None
        
        transformer.update_transform_status('patient', 100, 'success')  # Should not raise
        
        result = transformer.get_table_row_count('patient')
        assert result == 0
    
    # Test pandas read_sql errors
    with patch('pandas.read_sql') as mock_read_sql:
        mock_read_sql.side_effect = Exception("SQL execution failed")
        
        result = transformer._read_from_raw('patient', False)
        assert result is None
    
    # Test to_sql errors
    with patch.object(transformer, '_ensure_table_exists') as mock_ensure:
        mock_ensure.side_effect = Exception("Table creation failed")
        
        result = transformer._write_to_public('patient', pd.DataFrame({'test': [1]}), False)
        assert result is False

def test_data_integrity_comprehensive(transformer, sample_raw_data):
    """Test comprehensive data integrity during transformation."""
    # Test that transformation preserves data integrity
    result = transformer._apply_transformations(sample_raw_data, 'patient')
    
    # Check row count preservation
    assert len(result) == len(sample_raw_data)
    
    # Check data value preservation
    for i in range(len(sample_raw_data)):
        assert result.iloc[i]['patnum'] == sample_raw_data.iloc[i]['"PatNum"']
        assert result.iloc[i]['lname'] == sample_raw_data.iloc[i]['"LName"']
        assert result.iloc[i]['fname'] == sample_raw_data.iloc[i]['"FName"']
    
    # Check no data corruption
    assert not result.isnull().all().all()  # No completely null columns
    assert not result.empty  # Not empty after transformation

def test_schema_handling_comprehensive(transformer):
    """Test comprehensive schema handling."""
    # Test various schema retrieval methods
    with patch.object(transformer, 'get_table_columns') as mock_columns, \
         patch.object(transformer, 'get_table_primary_key') as mock_pk, \
         patch.object(transformer, 'get_table_foreign_keys') as mock_fk, \
         patch.object(transformer, 'get_table_indexes') as mock_idx, \
         patch.object(transformer, 'get_table_constraints') as mock_constraints:
        
        mock_columns.return_value = [{'name': 'id', 'type': 'integer'}]
        mock_pk.return_value = ['id']
        mock_fk.return_value = []
        mock_idx.return_value = []
        mock_constraints.return_value = []
        
        schema = transformer.get_table_schema('patient')
        
        assert 'columns' in schema
        assert 'primary_key' in schema
        assert 'foreign_keys' in schema
        assert 'indexes' in schema
        assert 'constraints' in schema
        
        # Test schema change detection
        schema_hash = str(hash(str(schema)))
        result = transformer.has_schema_changed('patient', schema_hash)
        assert result is False 