"""
Test Fixtures Package

This package contains all test fixtures organized by responsibility.
The fixtures follow the new architectural patterns and provide standardized
test data management for integration tests.

Modules:
- test_data_definitions: Standardized test data that matches real schemas
- test_data_manager: Manager class for test data operations
- integration_fixtures: Pytest fixtures for integration tests
"""

from .test_data_definitions import (
    get_test_patient_data,
    get_test_appointment_data,
    get_incremental_test_patient_data,
    get_test_data_for_table,
    STANDARD_TEST_PATIENTS,
    STANDARD_TEST_APPOINTMENTS,
    MINIMAL_TEST_PATIENTS,
    INCREMENTAL_TEST_PATIENTS
)

from .test_data_manager import IntegrationTestDataManager

from .integration_fixtures import (
    test_data_manager,
    populated_test_databases,
    minimal_test_databases,
    incremental_test_databases,
    test_database_engines,
    test_source_engine,
    test_replication_engine,
    test_analytics_engine,
    test_raw_engine,
    test_staging_engine,
    test_intermediate_engine,
    test_marts_engine,
    setup_patient_table,  # Legacy compatibility
    setup_etl_tracking    # Legacy compatibility
)

from .env_fixtures import test_settings

__all__ = [
    # Test data definitions
    'get_test_patient_data',
    'get_test_appointment_data',
    'get_incremental_test_patient_data',
    'get_test_data_for_table',
    'STANDARD_TEST_PATIENTS',
    'STANDARD_TEST_APPOINTMENTS',
    'MINIMAL_TEST_PATIENTS',
    'INCREMENTAL_TEST_PATIENTS',
    
    # Test data manager
    'IntegrationTestDataManager',
    
    # Integration fixtures
    'test_data_manager',
    'populated_test_databases',
    'minimal_test_databases',
    'incremental_test_databases',
    'test_database_engines',
    'test_source_engine',
    'test_replication_engine',
    'test_analytics_engine',
    'test_raw_engine',
    'test_staging_engine',
    'test_intermediate_engine',
    'test_marts_engine',
    'setup_patient_table',
    'setup_etl_tracking',
    
    # Environment fixtures
    'test_settings'
] 