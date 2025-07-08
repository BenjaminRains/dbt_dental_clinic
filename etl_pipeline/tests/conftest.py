"""
Minimal conftest.py for ETL pipeline tests.

This file imports fixtures from the modular fixtures directory
to make them available to all tests in the test suite.
"""

# Import essential fixtures from modular fixtures directory
from tests.fixtures.env_fixtures import test_env_vars, production_env_vars, setup_test_environment, reset_global_settings, test_settings

# Import integration fixtures for integration tests
from tests.fixtures.integration_fixtures import (
    test_data_manager,
    populated_test_databases,
    test_database_engines,
    test_source_engine,
    test_replication_engine,
    test_analytics_engine,
    test_raw_engine,
    test_staging_engine,
    test_intermediate_engine,
    test_marts_engine,
    setup_patient_table,
    setup_etl_tracking
)

# Import config fixtures for settings
from tests.fixtures.config_fixtures import (
    test_pipeline_config,
    test_tables_config,
    complete_config_environment,
    valid_pipeline_config,
    minimal_pipeline_config,
    invalid_pipeline_config,
    complete_tables_config,
    mock_settings_environment
)

# Import metrics fixtures for monitoring tests
from tests.fixtures.metrics_fixtures import (
    mock_unified_metrics_connection,
    unified_metrics_collector_no_persistence,
    mock_metrics_data,
    mock_performance_metrics,
    mock_pipeline_metrics,
    mock_database_metrics,
    mock_metrics_collector,
    mock_metrics_storage,
    mock_metrics_aggregator,
    mock_metrics_alert,
    mock_metrics_dashboard,
    sample_metrics_query,
    mock_metrics_error,
    metrics_collector_with_settings,
    mock_analytics_engine_for_metrics
)

# Import test data fixtures for standardized test data
from tests.fixtures.test_data_fixtures import (
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

# Import test data definitions for reusable test data
from tests.fixtures.test_data_definitions import (
    get_test_patient_data,
    get_test_appointment_data,
    get_test_data_for_table,
    STANDARD_TEST_PATIENTS,
    STANDARD_TEST_APPOINTMENTS
) 