"""
Transformer fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Data transformation components
- Table processors
- Transformation configurations
- Processing utilities
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any
from datetime import datetime, timedelta


@pytest.fixture
def mock_table_processor_engines():
    """Mock engines for table processor testing."""
    return (
        MagicMock(),  # source engine
        MagicMock(),  # replication engine  
        MagicMock()   # analytics engine
    )


@pytest.fixture
def table_processor_standard_config():
    """Standard configuration for table processor testing."""
    return {
        'table_name': 'patient',
        'primary_key': 'PatNum',
        'incremental_column': 'DateTStamp',
        'batch_size': 1000,
        'parallel_processing': False,
        'validation_enabled': True,
        'transformation_rules': [
            'clean_dates',
            'standardize_names',
            'validate_required_fields'
        ]
    }


@pytest.fixture
def table_processor_large_config():
    """Large table configuration for table processor testing."""
    return {
        'table_name': 'procedurelog',
        'primary_key': 'ProcNum',
        'incremental_column': 'ProcDate',
        'batch_size': 5000,
        'parallel_processing': True,
        'validation_enabled': True,
        'transformation_rules': [
            'clean_amounts',
            'standardize_codes',
            'validate_amounts'
        ],
        'estimated_rows': 1000001,  # > 1,000,000 to trigger chunked loading
        'estimated_size_mb': 101    # > 100 to trigger chunked loading
    }


@pytest.fixture
def table_processor_medium_large_config():
    """Medium-large table configuration for table processor testing."""
    return {
        'table_name': 'appointment',
        'primary_key': 'AptNum',
        'incremental_column': 'AptDateTime',
        'batch_size': 2500,
        'parallel_processing': True,
        'validation_enabled': True,
        'transformation_rules': [
            'clean_datetime',
            'timezone_convert',
            'validate_dates'
        ],
        'estimated_rows': 1000001,  # > 1,000,000 to trigger chunked loading
        'estimated_size_mb': 101    # > 100 to trigger chunked loading
    }


@pytest.fixture
def sample_transformation_data():
    """Sample data for transformation testing."""
    return {
        'raw_data': pd.DataFrame({
            'PatNum': [1, 2, 3, 4, 5],
            'LName': ['smith', 'JOHNSON', 'Williams', 'brown', 'JONES'],
            'FName': ['john', 'JANE', 'Bob', 'alice', 'CHARLIE'],
            'DateTStamp': [
                '2024-01-01 10:00:00',
                '2024-01-02 14:30:00',
                '2024-01-03 09:15:00',
                '2024-01-04 16:45:00',
                '2024-01-05 11:20:00'
            ],
            'Status': ['active', 'ACTIVE', 'inactive', 'ACTIVE', 'active']
        }),
        'expected_transformed': pd.DataFrame({
            'PatNum': [1, 2, 3, 4, 5],
            'LName': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones'],
            'FName': ['John', 'Jane', 'Bob', 'Alice', 'Charlie'],
            'DateTStamp': [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 2, 14, 30, 0),
                datetime(2024, 1, 3, 9, 15, 0),
                datetime(2024, 1, 4, 16, 45, 0),
                datetime(2024, 1, 5, 11, 20, 0)
            ],
            'Status': ['Active', 'Active', 'Inactive', 'Active', 'Active']
        })
    }


@pytest.fixture
def mock_transformation_rules():
    """Mock transformation rules for testing."""
    return {
        'clean_dates': {
            'description': 'Clean and standardize date fields',
            'fields': ['DateTStamp', 'ProcDate', 'AptDateTime'],
            'function': 'standardize_datetime'
        },
        'standardize_names': {
            'description': 'Standardize name formatting',
            'fields': ['LName', 'FName'],
            'function': 'title_case'
        },
        'clean_amounts': {
            'description': 'Clean and validate monetary amounts',
            'fields': ['ProcFee', 'PayAmt'],
            'function': 'standardize_currency'
        },
        'validate_required_fields': {
            'description': 'Validate required fields are not null',
            'fields': ['PatNum', 'LName'],
            'function': 'check_not_null'
        }
    }


@pytest.fixture
def mock_transformation_stats():
    """Mock transformation statistics for testing."""
    return {
        'table_name': 'patient',
        'rows_processed': 1000,
        'rows_transformed': 950,
        'rows_failed': 50,
        'start_time': datetime.now() - timedelta(minutes=5),
        'end_time': datetime.now(),
        'transformation_rules_applied': [
            'clean_dates',
            'standardize_names',
            'validate_required_fields'
        ],
        'errors': [
            {
                'row': 25,
                'field': 'DateTStamp',
                'error': 'Invalid date format',
                'value': '2024-13-45 25:70:00'
            }
        ]
    }


@pytest.fixture
def mock_transformer_config():
    """Mock transformer configuration for testing."""
    return {
        'enabled': True,
        'parallel_processing': True,
        'max_workers': 4,
        'chunk_size': 1000,
        'validation_enabled': True,
        'error_handling': 'continue',
        'logging_level': 'INFO'
    }


@pytest.fixture
def sample_validation_rules():
    """Sample validation rules for testing."""
    return {
        'not_null': {
            'description': 'Field must not be null',
            'fields': ['PatNum', 'LName'],
            'severity': 'error'
        },
        'valid_date': {
            'description': 'Field must be a valid date',
            'fields': ['DateTStamp', 'ProcDate'],
            'severity': 'warning'
        },
        'valid_amount': {
            'description': 'Field must be a valid monetary amount',
            'fields': ['ProcFee'],
            'severity': 'error'
        },
        'unique': {
            'description': 'Field must be unique',
            'fields': ['PatNum'],
            'severity': 'error'
        }
    }


@pytest.fixture
def mock_transformation_error():
    """Mock transformation error for testing error handling."""
    class MockTransformationError(Exception):
        def __init__(self, message="Transformation failed", field=None, value=None, rule=None):
            self.message = message
            self.field = field
            self.value = value
            self.rule = rule
            super().__init__(self.message)
    
    return MockTransformationError


@pytest.fixture
def mock_validation_result():
    """Mock validation result for testing."""
    return {
        'valid': True,
        'errors': [],
        'warnings': [],
        'total_checks': 10,
        'passed_checks': 10,
        'failed_checks': 0
    }


@pytest.fixture
def mock_validation_result_with_errors():
    """Mock validation result with errors for testing."""
    return {
        'valid': False,
        'errors': [
            {
                'field': 'DateTStamp',
                'value': '2024-13-45 25:70:00',
                'rule': 'valid_date',
                'message': 'Invalid date format'
            }
        ],
        'warnings': [
            {
                'field': 'LName',
                'value': '',
                'rule': 'not_null',
                'message': 'Field is empty'
            }
        ],
        'total_checks': 10,
        'passed_checks': 8,
        'failed_checks': 2
    } 