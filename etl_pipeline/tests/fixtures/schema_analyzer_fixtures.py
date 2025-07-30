"""
Schema Analyzer Test Data Fixtures

This module contains standardized test data fixtures for schema analyzer tests.
These fixtures provide consistent, reusable test data for OpenDentalSchemaAnalyzer testing.

Connection Architecture Compliance:
- ✅ Environment-specific test data following naming conventions
- ✅ Test data that supports Settings injection patterns
- ✅ Test data for provider pattern testing
- ✅ Test data for unified interface testing
- ✅ Test data for enum-based database type testing
"""

import pytest
from typing import Dict, List, Any


@pytest.fixture
def mock_schema_data():
    """
    Mock schema data for dental clinic tables.
    
    Provides realistic dental clinic table schemas with:
    - Standard OpenDental table structures
    - Common data types and constraints
    - Primary/foreign key relationships
    - Timestamp columns for incremental loading
    
    ETL Usage:
        - Testing table schema analysis with mocked data
        - Validating incremental column discovery
        - Testing table importance determination
        - Mocking database inspector responses
    
    Returns:
        Dict: Mock schema data for dental clinic tables
    """
    return {
        'patient': {
            'columns': {
                'PatNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'LName': {'type': 'varchar(100)', 'nullable': True, 'primary_key': False, 'default': None},
                'FName': {'type': 'varchar(100)', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'DateModified': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'SecDateTEdit': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['PatNum'],
            'foreign_keys': [
                {'constrained_columns': ['PatNum'], 'referred_table': 'appointment', 'referred_columns': ['PatNum']},
                {'constrained_columns': ['PatNum'], 'referred_table': 'procedurelog', 'referred_columns': ['PatNum']}
            ],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['PatNum']},
                {'name': 'idx_patient_name', 'column_names': ['LName', 'FName']}
            ]
        },
        'appointment': {
            'columns': {
                'AptNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'PatNum': {'type': 'int(11)', 'nullable': True, 'primary_key': False, 'default': None},
                'AptDateTime': {'type': 'datetime', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'SecDateTEdit': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['AptNum'],
            'foreign_keys': [
                {'constrained_columns': ['PatNum'], 'referred_table': 'patient', 'referred_columns': ['PatNum']}
            ],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['AptNum']},
                {'name': 'idx_appointment_patnum', 'column_names': ['PatNum']}
            ]
        },
        'procedurelog': {
            'columns': {
                'ProcNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'PatNum': {'type': 'int(11)', 'nullable': True, 'primary_key': False, 'default': None},
                'ProcDate': {'type': 'date', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'SecDateTEdit': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['ProcNum'],
            'foreign_keys': [
                {'constrained_columns': ['PatNum'], 'referred_table': 'patient', 'referred_columns': ['PatNum']}
            ],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['ProcNum']},
                {'name': 'idx_procedure_patnum', 'column_names': ['PatNum']}
            ]
        },
        'insplan': {
            'columns': {
                'PlanNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'Carrier': {'type': 'varchar(100)', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['PlanNum'],
            'foreign_keys': [],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['PlanNum']}
            ]
        },
        'definition': {
            'columns': {
                'DefNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'DefName': {'type': 'varchar(100)', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['DefNum'],
            'foreign_keys': [],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['DefNum']}
            ]
        },
        'securitylog': {
            'columns': {
                'LogNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'LogMessage': {'type': 'text', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['LogNum'],
            'foreign_keys': [],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['LogNum']}
            ]
        },
        'claim': {
            'columns': {
                'ClaimNum': {'type': 'int(11)', 'nullable': False, 'primary_key': True, 'default': None},
                'PatNum': {'type': 'int(11)', 'nullable': True, 'primary_key': False, 'default': None},
                'ClaimDate': {'type': 'date', 'nullable': True, 'primary_key': False, 'default': None},
                'ClaimAmt': {'type': 'decimal(10,2)', 'nullable': True, 'primary_key': False, 'default': None},
                'DateTStamp': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'},
                'SecDateTEdit': {'type': 'timestamp', 'nullable': True, 'primary_key': False, 'default': 'CURRENT_TIMESTAMP'}
            },
            'primary_keys': ['ClaimNum'],
            'foreign_keys': [
                {'constrained_columns': ['PatNum'], 'referred_table': 'patient', 'referred_columns': ['PatNum']}
            ],
            'indexes': [
                {'name': 'PRIMARY', 'column_names': ['ClaimNum']},
                {'name': 'idx_claim_patnum', 'column_names': ['PatNum']}
            ]
        }
    }


@pytest.fixture
def mock_size_data():
    """
    Mock size data for dental clinic tables.
    
    Provides realistic table size information with:
    - Estimated row counts for different table types
    - Size estimates in MB
    - Source information for tracking
    
    ETL Usage:
        - Testing table size analysis with mocked data
        - Validating extraction strategy determination
        - Testing table importance based on size
        - Mocking database size queries
    
    Returns:
        Dict: Mock size data for dental clinic tables
    """
    return {
        'patient': {'estimated_row_count': 50000, 'size_mb': 25.5, 'source': 'information_schema_estimate'},
        'appointment': {'estimated_row_count': 150000, 'size_mb': 45.2, 'source': 'information_schema_estimate'},
        'procedurelog': {'estimated_row_count': 200000, 'size_mb': 78.9, 'source': 'information_schema_estimate'},
        'insplan': {'estimated_row_count': 500, 'size_mb': 2.1, 'source': 'information_schema_estimate'},
        'definition': {'estimated_row_count': 100, 'size_mb': 0.5, 'source': 'information_schema_estimate'},
        'securitylog': {'estimated_row_count': 5000000, 'size_mb': 1250.0, 'source': 'information_schema_estimate'},
        'claim': {'estimated_row_count': 2000000, 'size_mb': 800.0, 'source': 'information_schema_estimate'}
    }


@pytest.fixture
def mock_dbt_models():
    """
    Mock DBT models for dental clinic project.
    
    Provides realistic DBT model structure with:
    - Staging models for source tables
    - Mart models for business entities
    - Intermediate models for transformations
    
    ETL Usage:
        - Testing DBT model discovery with mocked project
        - Validating model type classification
        - Testing configuration generation with model info
    
    Returns:
        Dict: Mock DBT models structure
    """
    return {
        'staging': ['stg_opendental__patient', 'stg_opendental__appointment', 'stg_opendental__procedurelog'],
        'mart': ['dim_patient', 'dim_appointment', 'fact_procedures'],
        'intermediate': ['int_patient_appointments', 'int_procedure_summary']
    } 