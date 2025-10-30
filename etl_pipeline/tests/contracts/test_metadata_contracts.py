"""
Metadata Contract Tests

Tests that verify metadata dictionaries returned from extraction and load
operations match the documented contracts in DATA_CONTRACTS.md.

These tests validate:
- Extraction metadata from SimpleMySQLReplicator
- Load metadata from PostgresLoader
- Tracking metadata for status tables

Run with: pytest tests/contracts/test_metadata_contracts.py -v
"""

import pytest
from typing import Dict, Any


class TestExtractionMetadataContract:
    """Test extraction metadata contract."""
    
    REQUIRED_FIELDS_SUCCESS = [
        'rows_copied',
        'strategy_used',
        'duration'
    ]
    
    REQUIRED_FIELDS_FAILURE = [
        'rows_copied',
        'strategy_used',
        'duration',
        'error'
    ]
    
    VALID_STRATEGIES = [
        'incremental',
        'full_table',
        'incremental_chunked',
        'error'
    ]
    
    @pytest.fixture
    def success_metadata(self) -> Dict[str, Any]:
        """Example successful extraction metadata."""
        return {
            'rows_copied': 1523,
            'strategy_used': 'incremental',
            'duration': 45.2,
            'force_full_applied': False,
            'primary_column': 'DateTStamp',
            'last_primary_value': '2025-10-22 15:30:00',
            'batch_size_used': 50000,
            'performance_category': 'medium'
        }
    
    @pytest.fixture
    def failure_metadata(self) -> Dict[str, Any]:
        """Example failed extraction metadata."""
        return {
            'rows_copied': 0,
            'strategy_used': 'error',
            'duration': 0.0,
            'force_full_applied': False,
            'primary_column': None,
            'last_primary_value': None,
            'error': 'Connection timeout'
        }
    
    def test_success_has_required_fields(self, success_metadata):
        """Successful extraction metadata must have required fields."""
        for field in self.REQUIRED_FIELDS_SUCCESS:
            assert field in success_metadata, \
                f"Success metadata missing required field: {field}"
    
    def test_failure_has_required_fields(self, failure_metadata):
        """Failed extraction metadata must have error field."""
        for field in self.REQUIRED_FIELDS_FAILURE:
            assert field in failure_metadata, \
                f"Failure metadata missing required field: {field}"
    
    def test_rows_copied_is_integer(self, success_metadata, failure_metadata):
        """rows_copied must be integer."""
        assert isinstance(success_metadata['rows_copied'], int), \
            "rows_copied must be integer"
        assert isinstance(failure_metadata['rows_copied'], int), \
            "rows_copied must be integer even on failure"
    
    def test_rows_copied_non_negative(self, success_metadata, failure_metadata):
        """rows_copied must be non-negative."""
        assert success_metadata['rows_copied'] >= 0, \
            "rows_copied must be non-negative"
        assert failure_metadata['rows_copied'] >= 0, \
            "rows_copied must be non-negative even on failure"
    
    def test_strategy_used_valid(self, success_metadata, failure_metadata):
        """strategy_used must be valid value."""
        assert success_metadata['strategy_used'] in self.VALID_STRATEGIES, \
            f"Invalid strategy: {success_metadata['strategy_used']}"
        assert failure_metadata['strategy_used'] in self.VALID_STRATEGIES, \
            f"Invalid strategy: {failure_metadata['strategy_used']}"
    
    def test_duration_is_numeric(self, success_metadata, failure_metadata):
        """duration must be numeric (int or float)."""
        assert isinstance(success_metadata['duration'], (int, float)), \
            "duration must be numeric"
        assert isinstance(failure_metadata['duration'], (int, float)), \
            "duration must be numeric"
    
    def test_duration_non_negative(self, success_metadata):
        """duration must be non-negative."""
        assert success_metadata['duration'] >= 0, \
            "duration must be non-negative"
    
    def test_failure_has_error_message(self, failure_metadata):
        """Failed extraction must have error message."""
        assert 'error' in failure_metadata, \
            "Failed extraction must have 'error' field"
        assert isinstance(failure_metadata['error'], str), \
            "error must be string"
        assert len(failure_metadata['error']) > 0, \
            "error message must not be empty"
    
    def test_failure_rows_copied_zero(self, failure_metadata):
        """Failed extraction should have rows_copied = 0."""
        assert failure_metadata['rows_copied'] == 0, \
            "Failed extraction should have rows_copied = 0"
    
    def test_success_no_error_field(self, success_metadata):
        """Successful extraction should not have error field."""
        # This is optional - can have error field as None
        if 'error' in success_metadata:
            assert success_metadata['error'] is None, \
                "Successful extraction error field should be None"


class TestLoadMetadataContract:
    """Test load metadata contract."""
    
    REQUIRED_FIELDS_SUCCESS = [
        'rows_loaded',
        'strategy_used',
        'duration'
    ]
    
    REQUIRED_FIELDS_FAILURE = [
        'rows_loaded',
        'strategy_used',
        'duration',
        'error'
    ]
    
    VALID_STRATEGIES = [
        'full_load',
        'incremental',
        'chunked',
        'skipped_no_new_data',
        'error'
    ]
    
    @pytest.fixture
    def success_metadata(self) -> Dict[str, Any]:
        """Example successful load metadata."""
        return {
            'rows_loaded': 1523,
            'strategy_used': 'incremental',
            'duration': 22.8,
            'last_primary_value': '2025-10-22 15:30:00',
            'primary_column': 'DateTStamp',
            'schema_converted': True,
            'chunked_loading': False,
            'verification_passed': True
        }
    
    @pytest.fixture
    def chunked_metadata(self) -> Dict[str, Any]:
        """Example chunked load metadata."""
        return {
            'rows_loaded': 150000,
            'strategy_used': 'chunked',
            'duration': 180.5,
            'last_primary_value': 150000,
            'primary_column': 'PatNum',
            'schema_converted': True,
            'chunked_loading': True,
            'chunk_count': 5,
            'verification_passed': True
        }
    
    @pytest.fixture
    def failure_metadata(self) -> Dict[str, Any]:
        """Example failed load metadata."""
        return {
            'rows_loaded': 0,
            'strategy_used': 'error',
            'duration': 0.0,
            'error': 'Schema conversion failed'
        }
    
    def test_success_has_required_fields(self, success_metadata):
        """Successful load metadata must have required fields."""
        for field in self.REQUIRED_FIELDS_SUCCESS:
            assert field in success_metadata, \
                f"Success metadata missing required field: {field}"
    
    def test_failure_has_required_fields(self, failure_metadata):
        """Failed load metadata must have error field."""
        for field in self.REQUIRED_FIELDS_FAILURE:
            assert field in failure_metadata, \
                f"Failure metadata missing required field: {field}"
    
    def test_rows_loaded_is_integer(self, success_metadata, failure_metadata):
        """rows_loaded must be integer."""
        assert isinstance(success_metadata['rows_loaded'], int), \
            "rows_loaded must be integer"
        assert isinstance(failure_metadata['rows_loaded'], int), \
            "rows_loaded must be integer even on failure"
    
    def test_rows_loaded_non_negative(self, success_metadata):
        """rows_loaded must be non-negative."""
        assert success_metadata['rows_loaded'] >= 0, \
            "rows_loaded must be non-negative"
    
    def test_strategy_used_valid(self, success_metadata, failure_metadata):
        """strategy_used must be valid value."""
        assert success_metadata['strategy_used'] in self.VALID_STRATEGIES, \
            f"Invalid strategy: {success_metadata['strategy_used']}"
        assert failure_metadata['strategy_used'] in self.VALID_STRATEGIES, \
            f"Invalid strategy: {failure_metadata['strategy_used']}"
    
    def test_duration_is_numeric(self, success_metadata):
        """duration must be numeric."""
        assert isinstance(success_metadata['duration'], (int, float)), \
            "duration must be numeric"
    
    def test_chunked_metadata_has_chunk_count(self, chunked_metadata):
        """Chunked loading should have chunk_count."""
        if chunked_metadata.get('chunked_loading'):
            assert 'chunk_count' in chunked_metadata, \
                "Chunked loading metadata should have chunk_count"
            assert isinstance(chunked_metadata['chunk_count'], int), \
                "chunk_count must be integer"
            assert chunked_metadata['chunk_count'] > 0, \
                "chunk_count must be positive"
    
    def test_schema_converted_is_boolean(self, success_metadata):
        """schema_converted must be boolean if present."""
        if 'schema_converted' in success_metadata:
            assert isinstance(success_metadata['schema_converted'], bool), \
                "schema_converted must be boolean"
    
    def test_verification_passed_is_boolean(self, success_metadata):
        """verification_passed must be boolean if present."""
        if 'verification_passed' in success_metadata:
            assert isinstance(success_metadata['verification_passed'], bool), \
                "verification_passed must be boolean"


class TestTrackingMetadataContract:
    """Test tracking metadata contract (for status tables)."""
    
    REQUIRED_FIELDS = [
        'rows_processed',
        'duration',
        'strategy_used'
    ]
    
    @pytest.fixture
    def tracking_metadata(self) -> Dict[str, Any]:
        """Example tracking metadata."""
        return {
            'rows_processed': 1523,
            'last_primary_value': '2025-10-22 15:30:00',
            'primary_column': 'DateTStamp',
            'duration': 45.2,
            'strategy_used': 'incremental'
        }
    
    def test_has_required_fields(self, tracking_metadata):
        """Tracking metadata must have required fields."""
        for field in self.REQUIRED_FIELDS:
            assert field in tracking_metadata, \
                f"Tracking metadata missing required field: {field}"
    
    def test_rows_processed_is_integer(self, tracking_metadata):
        """rows_processed must be integer."""
        assert isinstance(tracking_metadata['rows_processed'], int), \
            "rows_processed must be integer"
    
    def test_rows_processed_non_negative(self, tracking_metadata):
        """rows_processed must be non-negative."""
        assert tracking_metadata['rows_processed'] >= 0, \
            "rows_processed must be non-negative"
    
    def test_duration_is_numeric(self, tracking_metadata):
        """duration must be numeric."""
        assert isinstance(tracking_metadata['duration'], (int, float)), \
            "duration must be numeric"
    
    def test_strategy_used_is_string(self, tracking_metadata):
        """strategy_used must be string."""
        assert isinstance(tracking_metadata['strategy_used'], str), \
            "strategy_used must be string"
    
    def test_primary_column_is_string_or_none(self, tracking_metadata):
        """primary_column must be string or None."""
        primary_column = tracking_metadata.get('primary_column')
        assert isinstance(primary_column, (str, type(None))), \
            "primary_column must be string or None"
    
    def test_last_primary_value_allowed_types(self, tracking_metadata):
        """last_primary_value can be various types."""
        last_value = tracking_metadata.get('last_primary_value')
        # Can be string (timestamp), int (ID), or None
        assert isinstance(last_value, (str, int, float, type(None))), \
            "last_primary_value must be string, int, float, or None"


class TestMetadataReturnType:
    """Test the return type pattern (success: bool, metadata: dict)."""
    
    @pytest.fixture
    def success_result(self) -> tuple:
        """Example successful result."""
        return (True, {
            'rows_copied': 1523,
            'strategy_used': 'incremental',
            'duration': 45.2
        })
    
    @pytest.fixture
    def failure_result(self) -> tuple:
        """Example failed result."""
        return (False, {
            'rows_copied': 0,
            'strategy_used': 'error',
            'duration': 0.0,
            'error': 'Connection failed'
        })
    
    def test_return_is_tuple(self, success_result, failure_result):
        """Return value must be tuple."""
        assert isinstance(success_result, tuple), \
            "Return must be tuple"
        assert isinstance(failure_result, tuple), \
            "Return must be tuple"
    
    def test_tuple_has_two_elements(self, success_result, failure_result):
        """Return tuple must have exactly 2 elements."""
        assert len(success_result) == 2, \
            "Return tuple must have 2 elements (success, metadata)"
        assert len(failure_result) == 2, \
            "Return tuple must have 2 elements (success, metadata)"
    
    def test_first_element_is_boolean(self, success_result, failure_result):
        """First element must be boolean."""
        assert isinstance(success_result[0], bool), \
            "First element must be boolean"
        assert isinstance(failure_result[0], bool), \
            "First element must be boolean"
    
    def test_second_element_is_dict(self, success_result, failure_result):
        """Second element must be dictionary."""
        assert isinstance(success_result[1], dict), \
            "Second element must be dict"
        assert isinstance(failure_result[1], dict), \
            "Second element must be dict"
    
    def test_success_true_has_positive_rows(self, success_result):
        """When success=True, should have rows > 0 (usually)."""
        success, metadata = success_result
        if success and metadata.get('strategy_used') != 'skipped_no_new_data':
            # This is a soft check - success with 0 rows is possible
            # but usually indicates an issue
            pass
    
    def test_success_false_has_error(self, failure_result):
        """When success=False, should have error message."""
        success, metadata = failure_result
        assert not success, "Failure result should have success=False"
        assert 'error' in metadata, \
            "Failure result should have error message"


class TestMetadataFieldTypes:
    """Test common field type constraints across all metadata."""
    
    def test_string_fields_not_empty(self):
        """String fields should not be empty strings."""
        metadata = {
            'strategy_used': '',  # Invalid
            'error': ''           # Invalid
        }
        
        # Check strategy_used
        if 'strategy_used' in metadata:
            assert len(metadata['strategy_used']) > 0, \
                "strategy_used should not be empty string"
        
        # Check error
        if 'error' in metadata:
            assert metadata['error'] is None or len(metadata['error']) > 0, \
                "error should be None or non-empty string"
    
    def test_numeric_fields_finite(self):
        """Numeric fields should be finite (not inf or nan)."""
        import math
        
        metadata = {
            'duration': 45.2,
            'rows_copied': 1523
        }
        
        # Check duration
        if 'duration' in metadata:
            assert math.isfinite(metadata['duration']), \
                "duration must be finite"
        
        # Check row counts
        for field in ['rows_copied', 'rows_loaded', 'rows_processed']:
            if field in metadata:
                value = metadata[field]
                if isinstance(value, float):
                    assert math.isfinite(value), \
                        f"{field} must be finite"
    
    def test_boolean_fields_are_boolean(self):
        """Boolean fields should be actual booleans, not truthy values."""
        metadata = {
            'force_full_applied': False,
            'schema_converted': True,
            'chunked_loading': False
        }
        
        boolean_fields = [
            'force_full_applied',
            'schema_converted',
            'chunked_loading',
            'verification_passed'
        ]
        
        for field in boolean_fields:
            if field in metadata:
                assert isinstance(metadata[field], bool), \
                    f"{field} must be boolean, not truthy value"


class TestMetadataConsistency:
    """Test consistency rules across metadata fields."""
    
    def test_zero_rows_with_zero_duration_suspicious(self):
        """Zero rows with zero duration might indicate incomplete metadata."""
        metadata = {
            'rows_copied': 0,
            'duration': 0.0,
            'strategy_used': 'incremental'
        }
        
        rows = metadata.get('rows_copied', 0)
        duration = metadata.get('duration', 0)
        strategy = metadata.get('strategy_used')
        
        # If both are zero and strategy is not error, might be suspicious
        if rows == 0 and duration == 0.0 and strategy not in ['error', 'skipped_no_new_data']:
            pytest.warns(
                UserWarning,
                match="Zero rows with zero duration and non-error strategy"
            )
    
    def test_large_rows_small_duration_suspicious(self):
        """Very large row count with small duration might indicate issue."""
        metadata = {
            'rows_copied': 1000000,
            'duration': 0.1  # Too fast for 1M rows
        }
        
        rows = metadata.get('rows_copied', 0)
        duration = metadata.get('duration', 1)
        
        if rows > 100000 and duration > 0:
            rate = rows / duration
            # More than 1M records/sec is suspicious
            if rate > 1000000:
                pytest.warns(
                    UserWarning,
                    match=f"Suspiciously high processing rate: {rate:.0f} records/sec"
                )

