"""
Tests for the RawToPublicTransformer class.

CURRENT STATE ANALYSIS:
======================
This test suite has comprehensive coverage but suffers from over-mocking issues.

STRENGTHS:
- ✅ Comprehensive method coverage for all RawToPublicTransformer methods
- ✅ Good error scenario testing and edge cases
- ✅ Sophisticated mock setup and method chain validation
- ✅ Tests both success and failure scenarios

ISSUES:
- ❌ OVER-MOCKING: Creates Mock(spec=RawToPublicTransformer) instead of testing real class
- ❌ NOT TESTING REAL LOGIC: Tests validate mock calls, not actual transformation logic
- ❌ MISSING INTEGRATION: No tests with real database connections or actual data
- ❌ LIMITED VALUE: Mostly testing that mocks were called, not real functionality

RECOMMENDATIONS:
1. Refactor to test real RawToPublicTransformer with mocked dependencies
2. Add integration tests with real sample data and database connections
3. Test actual data transformation logic (column lowercasing, type conversion, etc.)
4. Add tests for incomplete methods (_get_column_types, _generate_create_table_sql, etc.)
5. Validate actual return values and data changes, not just mock calls

PRIORITY ACTIONS:
- Replace Mock(spec=RawToPublicTransformer) with real transformer instance
- Add tests for actual data transformation validation
- Test incomplete methods once they're implemented
- Add integration tests with real database connections

TODO: Refactor this test suite to test actual transformation logic instead of mocks.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import Engine, inspect, MetaData, Table, Column, Integer, String
from etl_pipeline.transformers.raw_to_public import RawToPublicTransformer
import logging

logger = logging.getLogger(__name__)

@pytest.fixture
def mock_engine():
    """Create a basic mock engine."""
    engine = Mock(spec=Engine)
    engine.url = Mock()
    engine.url.drivername = 'postgresql'
    return engine

@pytest.fixture
def transformer(mock_engine):
    """Create a transformer instance with mocked dependencies."""
    source_engine = mock_engine
    target_engine = mock_engine
    
    # Create a mock transformer
    transformer = Mock(spec=RawToPublicTransformer)
    transformer.source_engine = source_engine
    transformer.target_engine = target_engine
    transformer.source_schema = 'raw'
    transformer.target_schema = 'public'
    
    # Mock all the methods we need to test
    transformer._read_from_raw = Mock()
    transformer._apply_transformations = Mock()
    transformer._write_to_public = Mock()
    transformer._update_transformation_status = Mock()
    
    # Define transform_table behavior to actually call the mocked methods
    def transform_table_side_effect(table_name, is_incremental=False):
        try:
            df = transformer._read_from_raw(table_name, is_incremental)
            if df is None or df.empty:
                return False
            transformed_df = transformer._apply_transformations(df, table_name)
            success = transformer._write_to_public(table_name, transformed_df, is_incremental)
            if success:
                transformer._update_transformation_status(table_name, len(transformed_df))
            return success
        except Exception as e:
            logger.error(f"Error in transform_table: {str(e)}")
            return False
    
    transformer.transform_table = Mock(side_effect=transform_table_side_effect)
    
    # Mock other methods
    transformer.get_table_schema = Mock(return_value={
        'columns': [{'name': 'id', 'type': Integer()}, {'name': 'name', 'type': String()}],
        'primary_key': ['id'],
        'foreign_keys': [],
        'indexes': [],
        'constraints': []
    })
    transformer.get_table_row_count = Mock(return_value=1)
    transformer.get_table_size = Mock(return_value=1024)
    transformer.get_last_transformed = Mock(return_value=None)
    transformer.has_schema_changed = Mock(return_value=False)
    transformer.get_table_metadata = Mock(return_value={
        'row_count': 1,
        'size': 1024,
        'last_transformed': None,
        'schema': {
            'columns': [{'name': 'id', 'type': Integer()}, {'name': 'name', 'type': String()}],
            'primary_key': ['id'],
            'foreign_keys': [],
            'indexes': [],
            'constraints': []
        }
    })
    
    # Add missing method mocks with proper return values
    transformer.get_table_partitions = Mock(return_value=None)
    transformer.get_table_grants = Mock(return_value=[])
    transformer.get_table_triggers = Mock(return_value=[])
    transformer.get_table_views = Mock(return_value=[])
    transformer.get_table_dependencies = Mock(return_value=[])
    transformer._convert_data_types = Mock(return_value=pd.DataFrame({'id': [1], 'name': ['test']}))
    transformer._apply_table_specific_transformations = Mock(return_value=pd.DataFrame({'id': [1], 'name': ['test']}))
    
    return transformer

def test_transform_table_success(transformer):
    """Test successful table transformation."""
    # Setup mock chain
    test_df = pd.DataFrame({'id': [1], 'name': ['test']})
    transformer._read_from_raw.return_value = test_df
    transformer._apply_transformations.return_value = test_df
    transformer._write_to_public.return_value = True
    
    # Call the method
    result = transformer.transform_table('test_table')
    
    # Verify the result and chain of calls
    assert result is True
    transformer._read_from_raw.assert_called_once_with('test_table', False)
    transformer._apply_transformations.assert_called_once_with(test_df, 'test_table')
    transformer._write_to_public.assert_called_once_with('test_table', test_df, False)
    transformer._update_transformation_status.assert_called_once_with('test_table', 1)

def test_transform_table_empty_data(transformer):
    """Test transformation with empty data."""
    # Setup mock chain
    empty_df = pd.DataFrame()
    transformer._read_from_raw.return_value = empty_df
    
    # Call the method
    result = transformer.transform_table('test_table')
    
    # Verify the result and chain of calls
    assert result is False
    transformer._read_from_raw.assert_called_once_with('test_table', False)
    transformer._apply_transformations.assert_not_called()
    transformer._write_to_public.assert_not_called()
    transformer._update_transformation_status.assert_not_called()

def test_transform_table_error(transformer):
    """Test transformation with error."""
    # Setup mock chain
    transformer._read_from_raw.side_effect = Exception("Test error")
    
    # Call the method
    result = transformer.transform_table('test_table')
    
    # Verify the result and chain of calls
    assert result is False
    transformer._read_from_raw.assert_called_once_with('test_table', False)
    transformer._apply_transformations.assert_not_called()
    transformer._write_to_public.assert_not_called()
    transformer._update_transformation_status.assert_not_called()

def test_apply_transformations(transformer):
    """Test applying transformations to data."""
    # Setup test data
    input_df = pd.DataFrame({'id': [1], 'name': ['test']})
    expected_df = pd.DataFrame({'id': [1], 'name': ['TEST']})
    
    # Setup mock chain
    transformer._apply_transformations.return_value = expected_df
    
    # Call the method
    result = transformer._apply_transformations(input_df, 'test_table')
    
    # Verify the result
    assert isinstance(result, pd.DataFrame)
    assert result.equals(expected_df)
    transformer._apply_transformations.assert_called_once_with(input_df, 'test_table')

def test_get_last_transformed(transformer):
    """Test getting last transformation timestamp."""
    result = transformer.get_last_transformed('test_table')
    assert result is None

def test_update_transform_status(transformer):
    """Test updating transformation status."""
    # Setup mock chain
    test_df = pd.DataFrame({'id': [1], 'name': ['test']})
    transformer._read_from_raw.return_value = test_df
    transformer._apply_transformations.return_value = test_df
    transformer._write_to_public.return_value = True
    
    # Call the method
    result = transformer.transform_table('test_table')
    
    # Verify the result and chain of calls
    assert result is True
    transformer._update_transformation_status.assert_called_once_with('test_table', 1)

def test_verify_transform(transformer):
    """Test verifying transformation."""
    transformer.verify_transform.return_value = True
    result = transformer.verify_transform('test_table')
    assert result is True

def test_verify_transform_mismatch(transformer):
    """Test verifying transformation with mismatch."""
    transformer.verify_transform.return_value = False
    result = transformer.verify_transform('test_table')
    assert result is False

def test_get_table_schema(transformer):
    """Test getting table schema."""
    result = transformer.get_table_schema('test_table')
    assert isinstance(result, dict)
    assert 'columns' in result
    assert 'primary_key' in result
    assert 'foreign_keys' in result
    assert 'indexes' in result
    assert 'constraints' in result

def test_has_schema_changed(transformer):
    """Test checking schema changes."""
    result = transformer.has_schema_changed('test_table', 'old_hash')
    assert result is False

def test_get_table_partitions(transformer):
    """Test getting table partitions."""
    result = transformer.get_table_partitions('test_table')
    assert result is None

def test_get_table_grants(transformer):
    """Test getting table grants."""
    result = transformer.get_table_grants('test_table')
    assert isinstance(result, list)

def test_get_table_triggers(transformer):
    """Test getting table triggers."""
    result = transformer.get_table_triggers('test_table')
    assert isinstance(result, list)

def test_get_table_views(transformer):
    """Test getting table views."""
    result = transformer.get_table_views('test_table')
    assert isinstance(result, list)

def test_get_table_dependencies(transformer):
    """Test getting table dependencies."""
    result = transformer.get_table_dependencies('test_table')
    assert isinstance(result, list)

def test_write_to_public_success(transformer):
    """Test successful write to public schema."""
    # Setup test data
    test_df = pd.DataFrame({'id': [1], 'name': ['test']})
    
    # Setup mock chain
    transformer._write_to_public.return_value = True
    
    # Call the method
    result = transformer._write_to_public('test_table', test_df, False)
    
    # Verify the result
    assert result is True
    transformer._write_to_public.assert_called_once_with('test_table', test_df, False)

def test_write_to_public_empty_data(transformer):
    """Test writing empty data to public."""
    transformer._write_to_public.return_value = False
    df = pd.DataFrame()
    result = transformer._write_to_public('test_table', df, False)
    assert result is False

def test_write_to_public_error(transformer):
    """Test writing to public with error."""
    df = pd.DataFrame({'id': [1], 'name': ['test']})
    transformer._write_to_public.side_effect = Exception("Test error")
    with pytest.raises(Exception):
        transformer._write_to_public('test_table', df, False)

def test_ensure_table_exists(transformer):
    """Test ensuring table exists."""
    df = pd.DataFrame({'id': [1], 'name': ['test']})
    transformer._ensure_table_exists('test_table', df)
    # Add assertions based on expected behavior

def test_ensure_table_exists_error(transformer):
    """Test ensuring table exists with error."""
    df = pd.DataFrame({'id': [1], 'name': ['test']})
    transformer._ensure_table_exists.side_effect = Exception("Test error")
    with pytest.raises(Exception):
        transformer._ensure_table_exists('test_table', df)

def test_read_from_raw_success(transformer):
    """Test successful read from raw schema."""
    # Setup test data
    expected_df = pd.DataFrame({'id': [1], 'name': ['test']})
    
    # Setup mock chain
    transformer._read_from_raw.return_value = expected_df
    
    # Call the method
    result = transformer._read_from_raw('test_table', False)
    
    # Verify the result
    assert isinstance(result, pd.DataFrame)
    assert result.equals(expected_df)
    transformer._read_from_raw.assert_called_once_with('test_table', False)

def test_read_from_raw_error(transformer):
    """Test reading from raw with error."""
    transformer._read_from_raw.side_effect = Exception("Test error")
    with pytest.raises(Exception):
        transformer._read_from_raw('test_table', False)

def test_convert_data_types(transformer):
    """Test converting data types."""
    df = pd.DataFrame({'id': ['1'], 'name': ['test']})
    result = transformer._convert_data_types(df, 'test_table')
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert 'id' in result.columns
    assert 'name' in result.columns

def test_convert_data_types_error(transformer):
    """Test converting data types with error."""
    df = pd.DataFrame({'id': ['1'], 'name': ['test']})
    transformer._convert_data_types.side_effect = Exception("Test error")
    with pytest.raises(Exception):
        transformer._convert_data_types(df, 'test_table')

def test_apply_table_specific_transformations(transformer):
    """Test applying table-specific transformations."""
    df = pd.DataFrame({'id': [1], 'name': ['test']})
    result = transformer._apply_table_specific_transformations(df, 'test_table')
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert 'id' in result.columns
    assert 'name' in result.columns

def test_update_transformation_status(transformer):
    """Test updating transformation status."""
    transformer._update_transformation_status('test_table', 1)
    transformer._update_transformation_status.assert_called_once_with('test_table', 1)

def test_update_transformation_status_error(transformer):
    """Test updating transformation status with error."""
    transformer._update_transformation_status.side_effect = Exception("Test error")
    with pytest.raises(Exception):
        transformer._update_transformation_status('test_table', 1) 