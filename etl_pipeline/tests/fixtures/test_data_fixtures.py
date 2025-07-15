"""
Test Data Fixtures

This module contains standardized test data fixtures for integration tests.
These fixtures provide consistent, reusable test data across all test modules.

Connection Architecture Compliance:
- ✅ Environment-specific test data following naming conventions
- ✅ Test data that supports Settings injection patterns
- ✅ Test data for provider pattern testing
- ✅ Test data for unified interface testing
- ✅ Test data for enum-based database type testing
"""

import pytest
from datetime import datetime, timedelta
from typing import Dict, List, Any

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
def test_environment_vars():
    """Test environment variables following connection architecture naming convention.
    
    This fixture provides test environment variables that conform to the connection architecture:
    - Uses TEST_ prefix for test environment variables
    - Follows the environment-specific variable naming convention
    - Matches the .env_test file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        # Environment declaration (required for fail-fast validation)
        'ETL_ENVIRONMENT': 'test',
        
        # OpenDental Source (Test) - following architecture naming
        'TEST_OPENDENTAL_SOURCE_HOST': 'test-source-host',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
        
        # MySQL Replication (Test) - following architecture naming
        'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
        'TEST_MYSQL_REPLICATION_PORT': '3306',
        'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
        
        # PostgreSQL Analytics (Test) - following architecture naming
        'TEST_POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
    }


@pytest.fixture
def production_environment_vars():
    """Production environment variables following connection architecture naming convention.
    
    This fixture provides production environment variables that conform to the connection architecture:
    - Uses non-prefixed variables for production environment
    - Follows the environment-specific variable naming convention
    - Matches the .env_production file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        # Environment declaration (required for fail-fast validation)
        'ETL_ENVIRONMENT': 'production',
        
        # OpenDental Source (Production) - following architecture naming
        'OPENDENTAL_SOURCE_HOST': 'prod-source-host',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'opendental',
        'OPENDENTAL_SOURCE_USER': 'source_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'source_pass',
        
        # MySQL Replication (Production) - following architecture naming
        'MYSQL_REPLICATION_HOST': 'prod-repl-host',
        'MYSQL_REPLICATION_PORT': '3306',
        'MYSQL_REPLICATION_DB': 'opendental_replication',
        'MYSQL_REPLICATION_USER': 'repl_user',
        'MYSQL_REPLICATION_PASSWORD': 'repl_pass',
        
        # PostgreSQL Analytics (Production) - following architecture naming
        'POSTGRES_ANALYTICS_HOST': 'prod-analytics-host',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }


@pytest.fixture
def test_settings_provider(test_environment_vars):
    """Test settings provider following the provider pattern for dependency injection.
    
    This fixture implements the DictConfigProvider pattern as specified in the connection architecture:
    - Uses DictConfigProvider for testing (as recommended)
    - Provides injected configuration for clean test isolation
    - Supports dependency injection for easy configuration swapping
    - Follows the provider pattern for configuration loading
    """
    return DictConfigProvider(
        pipeline={
            'general': {
                'pipeline_name': 'dental_clinic_etl',
                'environment': 'test',
                'timezone': 'UTC',
                'max_retries': 3,
                'retry_delay_seconds': 300,
                'batch_size': 25000,
                'parallel_jobs': 6
            },
            'connections': {
                'source': {'pool_size': 5, 'connect_timeout': 30},
                'replication': {'pool_size': 3, 'connect_timeout': 30},
                'analytics': {'pool_size': 4, 'connect_timeout': 30}
            },
            'stages': {
                'extract': {
                    'enabled': True,
                    'timeout_minutes': 30,
                    'error_threshold': 0.01
                },
                'load': {
                    'enabled': True,
                    'timeout_minutes': 45,
                    'error_threshold': 0.01
                },
                'transform': {
                    'enabled': True,
                    'timeout_minutes': 60,
                    'error_threshold': 0.01
                }
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': {
                    'enabled': True,
                    'path': 'logs/pipeline.log',
                    'max_size_mb': 100,
                    'backup_count': 10
                },
                'console': {
                    'enabled': True,
                    'level': 'INFO'
                }
            },
            'error_handling': {
                'max_consecutive_failures': 3,
                'failure_notification_threshold': 2,
                'auto_retry': {
                    'enabled': True,
                    'max_attempts': 3,
                    'delay_minutes': 5
                }
            }
        },
        tables={'tables': {}},
        env=test_environment_vars
    )


@pytest.fixture
def test_settings(test_settings_provider):
    """Test settings following connection architecture patterns.
    
    This fixture provides test settings that conform to the connection architecture:
    - Uses Settings injection for environment-agnostic operation
    - Uses provider pattern for dependency injection
    - Supports unified interface for connection creation
    - Uses enums for type safety
    """
    return Settings(environment='test', provider=test_settings_provider)


@pytest.fixture
def standard_patient_test_data():
    """Standardized patient test data for all integration tests following connection architecture patterns."""
    return [
        {'PatNum': 1, 'LName': 'John Doe', 'FName': 'John', 'DateTStamp': '2023-01-01 10:00:00', 'PatStatus': 0},
        {'PatNum': 2, 'LName': 'Jane Smith', 'FName': 'Jane', 'DateTStamp': '2023-01-02 11:00:00', 'PatStatus': 0},
        {'PatNum': 3, 'LName': 'Bob Johnson', 'FName': 'Bob', 'DateTStamp': '2023-01-03 12:00:00', 'PatStatus': 0}
    ]


@pytest.fixture
def incremental_patient_test_data():
    """Test data for incremental loading tests following connection architecture patterns."""
    return [
        {'PatNum': 4, 'LName': 'New User', 'FName': 'Test', 'DateTStamp': '2023-01-04 13:00:00', 'PatStatus': 0}
    ]


@pytest.fixture
def partial_patient_test_data():
    """Test data for partial loading tests (2 patients instead of 3) following connection architecture patterns."""
    return [
        {'PatNum': 1, 'LName': 'John Doe', 'FName': 'John', 'DateTStamp': '2023-01-01 10:00:00', 'PatStatus': 0},
        {'PatNum': 2, 'LName': 'Jane Smith', 'FName': 'Jane', 'DateTStamp': '2023-01-02 11:00:00', 'PatStatus': 0}
    ]


@pytest.fixture
def etl_tracking_test_data():
    """Standardized ETL tracking test data following connection architecture patterns."""
    return [
        {
            'table_name': 'patient',
            'last_loaded': '2023-01-01 10:00:00',
            'load_status': 'success',
            'rows_loaded': 3,
            'database_type': DatabaseType.SOURCE.value,  # Using enum value
            'schema_name': None  # No schema for MySQL
        }
    ]


@pytest.fixture
def analytics_etl_tracking_test_data():
    """Analytics ETL tracking test data following connection architecture patterns."""
    return [
        {
            'table_name': 'patient',
            'last_loaded': '2023-01-01 10:00:00',
            'load_status': 'success',
            'rows_loaded': 3,
            'database_type': DatabaseType.ANALYTICS.value,  # Using enum value
            'schema_name': PostgresSchema.RAW.value  # Using enum value
        }
    ]


@pytest.fixture
def invalid_schema_test_data():
    """Test data for invalid schema tests."""
    return [
        {'id': 1, 'invalid_column': 'test data', 'created_at': '2023-01-01 10:00:00'}
    ]


@pytest.fixture
def composite_pk_test_data():
    """Test data for composite primary key tests."""
    return [
        {'id1': 0, 'id2': 0, 'value': 'val0'},
        {'id1': 0, 'id2': 1, 'value': 'val1'},
        {'id1': 1, 'id2': 0, 'value': 'val2'},
        {'id1': 1, 'id2': 1, 'value': 'val3'}
    ]


@pytest.fixture
def large_table_test_data():
    """Test data for large table tests (no primary key)."""
    return [
        {'id': i, 'value': f'val{i}'} for i in range(6)
    ]


@pytest.fixture
def simple_test_table_data():
    """Simple test data for basic table operations."""
    return [
        {'id': 1, 'name': 'test'},
        {'id': 2, 'name': 'test2'}
    ]


@pytest.fixture
def database_connection_test_data():
    """Test data for database connection testing following connection architecture patterns."""
    return {
        'source': {
            'database_type': DatabaseType.SOURCE,
            'host': 'test-source-host',
            'port': 3306,
            'database': 'test_opendental',
            'user': 'test_source_user',
            'password': 'test_source_pass'
        },
        'replication': {
            'database_type': DatabaseType.REPLICATION,
            'host': 'test-repl-host',
            'port': 3306,
            'database': 'test_opendental_replication',
            'user': 'test_repl_user',
            'password': 'test_repl_pass'
        },
        'analytics_raw': {
            'database_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'host': 'test-analytics-host',
            'port': 5432,
            'database': 'test_opendental_analytics',
            'user': 'test_analytics_user',
            'password': 'test_analytics_pass'
        }
    }


@pytest.fixture
def settings_injection_test_data():
    """Test data for Settings injection testing following connection architecture patterns."""
    return {
        'test_environment': {
            'environment': 'test',
            'database_types': [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS],
            'schemas': [PostgresSchema.RAW, PostgresSchema.STAGING, PostgresSchema.INTERMEDIATE, PostgresSchema.MARTS]
        },
        'production_environment': {
            'environment': 'production',
            'database_types': [DatabaseType.SOURCE, DatabaseType.REPLICATION, DatabaseType.ANALYTICS],
            'schemas': [PostgresSchema.RAW, PostgresSchema.STAGING, PostgresSchema.INTERMEDIATE, PostgresSchema.MARTS]
        }
    }


@pytest.fixture
def provider_pattern_test_data():
    """Test data for provider pattern testing following connection architecture patterns."""
    return {
        'test_provider': {
            'provider_type': 'DictConfigProvider',
            'environment': 'test',
            'config_sources': ['pipeline', 'tables', 'env'],
            'injected_config': True
        },
        'production_provider': {
            'provider_type': 'FileConfigProvider',
            'environment': 'production',
            'config_sources': ['pipeline.yml', 'tables.yml', '.env_production'],
            'injected_config': False
        }
    }


@pytest.fixture
def unified_interface_test_data():
    """Test data for unified interface testing following connection architecture patterns."""
    return {
        'connection_methods': [
            'get_source_connection(settings)',
            'get_replication_connection(settings)',
            'get_analytics_connection(settings, schema)',
            'get_analytics_raw_connection(settings)',
            'get_analytics_staging_connection(settings)',
            'get_analytics_intermediate_connection(settings)',
            'get_analytics_marts_connection(settings)'
        ],
        'settings_methods': [
            'get_database_config(DatabaseType.SOURCE)',
            'get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)',
            'get_source_connection_config()',
            'get_replication_connection_config()',
            'get_analytics_connection_config(schema)'
        ]
    }


@pytest.fixture
def patient_with_all_fields_test_data():
    """Patient test data with all fields populated for comprehensive testing following connection architecture patterns."""
    return [
        {
            'PatNum': 9999,
            'LName': 'Test',
            'FName': 'Type',
            # 'MiddleI': 'T',  # Removed - column doesn't exist in actual schema
            'Preferred': 'Test',
            'PatStatus': 0,
            'Gender': 0,
            'Position': 0,
            'Birthdate': '1990-01-01',
            'SSN': '123-45-6789',
            'Address': '123 Test St',
            'Address2': 'Apt 1',
            'City': 'Test City',
            'State': 'TS',
            'Zip': '12345',
            'HmPhone': '555-123-4567',
            'WkPhone': '555-987-6543',
            'WirelessPhone': '555-555-5555',
            'Guarantor': 0,
            'CreditType': 'C',
            'Email': 'test@example.com',
            'Salutation': 'Mr.',
            'EstBalance': 0.0,
            'PriProv': 0,
            'SecProv': 0,
            'FeeSched': 0,
            'BillingType': 0,
            'ImageFolder': '',
            'AddrNote': None,
            'FamFinUrgNote': None,
            'MedUrgNote': '',
            'ApptModNote': '',
            'StudentStatus': '',
            'SchoolName': '',
            'ChartNumber': '',
            'MedicaidID': '',
            'Bal_0_30': 0.0,
            'Bal_31_60': 0.0,
            'Bal_61_90': 0.0,
            'BalOver90': 0.0,
            'InsEst': 0.0,
            'BalTotal': 0.0,
            'EmployerNum': 0,
            'EmploymentNote': '',
            'County': '',
            'GradeLevel': 0,
            'Urgency': 0,
            'DateFirstVisit': '2023-01-01',
            'ClinicNum': 0,
            'HasIns': '',
            'TrophyFolder': '',
            'PlannedIsDone': 0,
            'Premed': 0,
            'Ward': '',
            'PreferConfirmMethod': 0,
            'PreferContactMethod': 0,
            'PreferRecallMethod': 0,
            'SchedBeforeTime': None,
            'SchedAfterTime': None,
            'SchedDayOfWeek': 0,
            'Language': '',
            'AdmitDate': '2023-01-01',
            'Title': None,
            'PayPlanDue': 0.0,
            'SiteNum': 0,
            'DateTStamp': '2023-01-01 10:00:00',
            'ResponsParty': 0,
            'CanadianEligibilityCode': 0,
            'AskToArriveEarly': 0,
            'PreferContactConfidential': 0,
            'SuperFamily': 0,
            'TxtMsgOk': 0,
            'SmokingSnoMed': '',
            'Country': '',
            'DateTimeDeceased': '0001-01-01 00:00:00',
            'BillingCycleDay': 1,
            'SecUserNumEntry': 0,
            'SecDateEntry': '2023-01-01',
            'HasSuperBilling': 0,
            'PatNumCloneFrom': 0,
            'DiscountPlanNum': 0,
            'HasSignedTil': 0,
            'ShortCodeOptIn': 0,
            'SecurityHash': ''
        }
    ] 