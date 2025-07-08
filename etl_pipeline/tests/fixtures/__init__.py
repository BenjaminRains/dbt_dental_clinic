"""
Test Fixtures Package

This package contains all test fixtures organized by responsibility.
The fixtures follow the new architectural patterns and provide standardized
test data management for integration tests.

Modules:
- test_data_definitions: Standardized test data that matches real schemas
- test_data_manager: Manager class for test data operations
- integration_fixtures: Pytest fixtures for integration tests
- test_data_fixtures: Standardized test data fixtures for all tests
"""

from .test_data_definitions import (
    get_test_patient_data,
    get_test_appointment_data,
    get_test_data_for_table,
    STANDARD_TEST_PATIENTS,
    STANDARD_TEST_APPOINTMENTS
)

from .test_data_manager import IntegrationTestDataManager

from .integration_fixtures import (
    test_data_manager,
    populated_test_databases
)

from .env_fixtures import (
    test_env_vars,
    test_settings
)

from .config_fixtures import (
    test_pipeline_config,
    test_tables_config
)

from .cli_fixtures import (
    cli_runner,
    cli_test_config,
    cli_test_env_vars,
    cli_config_provider,
    cli_test_settings,
    cli_test_config_reader,
    cli_with_injected_config,
    temp_cli_config_file,
    temp_tables_config_file,
    mock_cli_database_connections,
    cli_expected_outputs,
    cli_error_cases,
    cli_performance_thresholds,
    cli_output_validators,
    cli_integration_test_data,
    cli_mock_orchestrator,
    cli_mock_metrics_collector
)

from .test_data_fixtures import (
    standard_patient_test_data,
    incremental_patient_test_data,
    partial_patient_test_data,
    etl_tracking_test_data,
    invalid_schema_test_data,
    composite_pk_test_data,
    large_table_test_data,
    simple_test_table_data,
    patient_with_all_fields_test_data
)

__all__ = [
    # Test data definitions
    'get_test_patient_data',
    'get_test_appointment_data', 
    'get_test_data_for_table',
    'STANDARD_TEST_PATIENTS',
    'STANDARD_TEST_APPOINTMENTS',
    
    # Test data manager
    'IntegrationTestDataManager',
    
    # Integration fixtures
    'test_data_manager',
    'populated_test_databases',
    
    # Environment fixtures
    'test_env_vars',
    'test_settings',
    
    # Config fixtures
    'test_pipeline_config',
    'test_tables_config',
    
    # CLI fixtures
    'cli_runner',
    'cli_test_config',
    'cli_test_env_vars',
    'cli_config_provider',
    'cli_test_settings',
    'cli_test_config_reader',
    'cli_with_injected_config',
    'temp_cli_config_file',
    'temp_tables_config_file',
    'mock_cli_database_connections',
    'cli_expected_outputs',
    'cli_error_cases',
    'cli_performance_thresholds',
    'cli_output_validators',
    'cli_integration_test_data',
    'cli_mock_orchestrator',
    'cli_mock_metrics_collector',
    
    # Test data fixtures
    'standard_patient_test_data',
    'incremental_patient_test_data',
    'partial_patient_test_data',
    'etl_tracking_test_data',
    'invalid_schema_test_data',
    'composite_pk_test_data',
    'large_table_test_data',
    'simple_test_table_data',
    'patient_with_all_fields_test_data'
] 