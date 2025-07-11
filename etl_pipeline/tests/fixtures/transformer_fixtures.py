"""
Transformer fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Data transformation components with Settings injection
- Table processors using unified interface
- Transformation configurations following connection architecture patterns
- Processing utilities with enum-based database type specification

Connection Architecture Compliance:
- ✅ Uses Settings injection for environment-agnostic operation
- ✅ Uses unified ConnectionFactory API with Settings injection
- ✅ Uses enum-based database type specification
- ✅ Supports provider pattern for dependency injection
- ✅ Environment-agnostic (works for both production and test)
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any
from datetime import datetime, timedelta

# Import connection architecture components
from etl_pipeline.config import DatabaseType, PostgresSchema, Settings
from etl_pipeline.config.providers import DictConfigProvider


@pytest.fixture
def database_types():
    """Database type enums for testing."""
    return DatabaseType


@pytest.fixture
def postgres_schemas():
    """PostgreSQL schema enums for testing."""
    return PostgresSchema


@pytest.fixture
def test_transformer_env_vars():
    """Test transformer environment variables following connection architecture naming convention.
    
    This fixture provides test environment variables for transformer testing that conform to the connection architecture:
    - Uses TEST_ prefix for test environment variables
    - Follows the environment-specific variable naming convention
    - Matches the .env_test file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        # Environment declaration (required for fail-fast validation)
        'ETL_ENVIRONMENT': 'test',
        
        # OpenDental Source (Test) - following architecture naming
        'TEST_OPENDENTAL_SOURCE_HOST': 'client-server',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'your_test_password',
        
        # MySQL Replication (Test) - following architecture naming
        'TEST_MYSQL_REPLICATION_HOST': 'localhost',
        'TEST_MYSQL_REPLICATION_PORT': '3305',
        'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'TEST_MYSQL_REPLICATION_USER': 'replication_test_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'your_test_password',
        
        # PostgreSQL Analytics (Test) - following architecture naming
        'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'analytics_test_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'your_test_password'
    }


@pytest.fixture
def test_transformer_config_provider(test_transformer_env_vars):
    """Test transformer configuration provider following the provider pattern for dependency injection.
    
    This fixture implements the DictConfigProvider pattern as specified in the connection architecture:
    - Uses DictConfigProvider for testing (as recommended)
    - Provides injected configuration for clean test isolation
    - Supports dependency injection for easy configuration swapping
    - Follows the provider pattern for configuration loading
    """
    return DictConfigProvider(
        pipeline={'connections': {
            'source': {'pool_size': 5, 'connect_timeout': 30},
            'replication': {'pool_size': 3, 'connect_timeout': 30},
            'analytics': {'pool_size': 4, 'connect_timeout': 30}
        }},
        tables={'tables': {}},
        env=test_transformer_env_vars
    )


@pytest.fixture
def test_transformer_settings(test_transformer_config_provider):
    """Test transformer settings following connection architecture patterns.
    
    This fixture provides test settings for transformer testing that conform to the connection architecture:
    - Uses Settings injection for environment-agnostic operation
    - Uses provider pattern for dependency injection
    - Supports unified interface for connection creation
    - Uses enums for type safety
    """
    return Settings(environment='test', provider=test_transformer_config_provider)


@pytest.fixture
def mock_table_processor_engines():
    """Mock engines for table processor testing following connection architecture patterns."""
    source_engine = MagicMock()
    source_engine.name = 'mysql'
    source_engine.url.database = 'test_opendental'
    source_engine.url.host = 'client-server'
    source_engine.url.port = 3306
    
    replication_engine = MagicMock()
    replication_engine.name = 'mysql'
    replication_engine.url.database = 'test_opendental_replication'
    replication_engine.url.host = 'localhost'
    replication_engine.url.port = 3305
    
    analytics_engine = MagicMock()
    analytics_engine.name = 'postgresql'
    analytics_engine.url.database = 'test_opendental_analytics'
    analytics_engine.url.host = 'localhost'
    analytics_engine.url.port = 5432
    
    return (source_engine, replication_engine, analytics_engine)


@pytest.fixture
def table_processor_standard_config():
    """Standard configuration for table processor testing following connection architecture patterns."""
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
        ],
        # Connection architecture specific settings
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.ANALYTICS,
        'target_schema': PostgresSchema.RAW
    }


@pytest.fixture
def table_processor_large_config():
    """Large table configuration for table processor testing following connection architecture patterns."""
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
        'estimated_size_mb': 101,   # > 100 to trigger chunked loading
        # Connection architecture specific settings
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.ANALYTICS,
        'target_schema': PostgresSchema.RAW
    }


@pytest.fixture
def table_processor_medium_large_config():
    """Medium-large table configuration for table processor testing following connection architecture patterns."""
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
        'estimated_size_mb': 101,   # > 100 to trigger chunked loading
        # Connection architecture specific settings
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.ANALYTICS,
        'target_schema': PostgresSchema.RAW
    }


@pytest.fixture
def sample_transformation_data():
    """Sample data for transformation testing following connection architecture patterns."""
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
        }),
        # Connection architecture specific metadata
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.ANALYTICS,
        'target_schema': PostgresSchema.RAW
    }


@pytest.fixture
def mock_transformation_rules():
    """Mock transformation rules for testing following connection architecture patterns."""
    return {
        'clean_dates': {
            'description': 'Clean and standardize date fields',
            'fields': ['DateTStamp', 'ProcDate', 'AptDateTime'],
            'function': 'standardize_datetime',
            'database_types': [DatabaseType.SOURCE, DatabaseType.ANALYTICS]
        },
        'standardize_names': {
            'description': 'Standardize name formatting',
            'fields': ['LName', 'FName'],
            'function': 'title_case',
            'database_types': [DatabaseType.SOURCE, DatabaseType.ANALYTICS]
        },
        'clean_amounts': {
            'description': 'Clean and validate monetary amounts',
            'fields': ['ProcFee', 'PayAmt'],
            'function': 'standardize_currency',
            'database_types': [DatabaseType.SOURCE, DatabaseType.ANALYTICS]
        },
        'validate_required_fields': {
            'description': 'Validate required fields are not null',
            'fields': ['PatNum', 'LName'],
            'function': 'check_not_null',
            'database_types': [DatabaseType.SOURCE, DatabaseType.ANALYTICS]
        }
    }


@pytest.fixture
def mock_transformation_stats():
    """Mock transformation statistics for testing following connection architecture patterns."""
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
        ],
        # Connection architecture specific metadata
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.ANALYTICS,
        'target_schema': PostgresSchema.RAW
    }


@pytest.fixture
def mock_transformer_config():
    """Mock transformer configuration for testing following connection architecture patterns."""
    return {
        'enabled': True,
        'parallel_processing': True,
        'max_workers': 4,
        'chunk_size': 1000,
        'validation_enabled': True,
        'error_handling': 'continue',
        'logging_level': 'INFO',
        # Connection architecture specific settings
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.ANALYTICS,
        'target_schema': PostgresSchema.RAW
    }


@pytest.fixture
def sample_validation_rules():
    """Sample validation rules for testing following connection architecture patterns."""
    return {
        'not_null': {
            'description': 'Field must not be null',
            'fields': ['PatNum', 'LName'],
            'severity': 'error',
            'database_types': [DatabaseType.SOURCE, DatabaseType.ANALYTICS]
        },
        'valid_date': {
            'description': 'Field must be a valid date',
            'fields': ['DateTStamp', 'ProcDate'],
            'severity': 'warning',
            'database_types': [DatabaseType.SOURCE, DatabaseType.ANALYTICS]
        },
        'valid_amount': {
            'description': 'Field must be a valid monetary amount',
            'fields': ['ProcFee'],
            'severity': 'error',
            'database_types': [DatabaseType.SOURCE, DatabaseType.ANALYTICS]
        },
        'unique': {
            'description': 'Field must be unique',
            'fields': ['PatNum'],
            'severity': 'error',
            'database_types': [DatabaseType.SOURCE, DatabaseType.ANALYTICS]
        }
    }


@pytest.fixture
def mock_transformation_error():
    """Mock transformation error for testing error handling following connection architecture patterns."""
    class MockTransformationError(Exception):
        def __init__(self, message="Transformation failed", field=None, value=None, rule=None, database_type=None):
            self.message = message
            self.field = field
            self.value = value
            self.rule = rule
            self.database_type = database_type
            super().__init__(self.message)
    
    return MockTransformationError


@pytest.fixture
def mock_validation_result():
    """Mock validation result for testing following connection architecture patterns."""
    return {
        'valid': True,
        'errors': [],
        'warnings': [],
        'total_checks': 10,
        'passed_checks': 10,
        'failed_checks': 0,
        # Connection architecture specific metadata
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.ANALYTICS,
        'target_schema': PostgresSchema.RAW
    }


@pytest.fixture
def mock_validation_result_with_errors():
    """Mock validation result with errors for testing following connection architecture patterns."""
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
        'failed_checks': 2,
        # Connection architecture specific metadata
        'source_database_type': DatabaseType.SOURCE,
        'target_database_type': DatabaseType.ANALYTICS,
        'target_schema': PostgresSchema.RAW
    }


@pytest.fixture
def transformer_test_cases():
    """Test cases for transformer scenarios following connection architecture patterns."""
    return [
        {
            'name': 'patient_table_transformation',
            'source_table': 'patient',
            'target_table': 'patient',
            'source_db_type': DatabaseType.SOURCE,
            'target_db_type': DatabaseType.ANALYTICS,
            'target_schema': PostgresSchema.RAW,
            'transformation_rules': ['clean_dates', 'standardize_names'],
            'expected_rows': 1000
        },
        {
            'name': 'appointment_table_transformation',
            'source_table': 'appointment',
            'target_table': 'appointment',
            'source_db_type': DatabaseType.SOURCE,
            'target_db_type': DatabaseType.ANALYTICS,
            'target_schema': PostgresSchema.RAW,
            'transformation_rules': ['clean_datetime', 'timezone_convert'],
            'expected_rows': 500
        },
        {
            'name': 'procedurelog_table_transformation',
            'source_table': 'procedurelog',
            'target_table': 'procedurelog',
            'source_db_type': DatabaseType.SOURCE,
            'target_db_type': DatabaseType.ANALYTICS,
            'target_schema': PostgresSchema.RAW,
            'transformation_rules': ['clean_amounts', 'standardize_codes'],
            'expected_rows': 2000
        }
    ] 