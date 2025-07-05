"""
Minimal conftest.py for ETL pipeline tests.

This file imports fixtures from the modular fixtures directory
to make them available to all tests in the test suite.
"""

# Import essential fixtures from modular fixtures directory
from tests.fixtures.env_fixtures import test_env_vars, production_env_vars, setup_test_environment, reset_global_settings, test_settings

# Import postgres schema fixtures for integration tests
from tests.fixtures.postgres_schema_fixtures import (
    postgres_schema_test_settings,
    real_postgres_schema_instance,
    real_schema_discovery_instance,
    postgres_schema_test_tables,
    postgres_schema_error_cases,
    mock_postgres_schema_engines,
    mock_schema_discovery,
    sample_mysql_schemas,
    expected_postgres_schemas,
    postgres_schema_test_data,
    mock_postgres_schema_instance
)

# Import schema discovery fixtures for integration tests
from tests.fixtures.schema_discovery_fixtures import (
    schema_discovery_test_settings,
    schema_discovery_instance,
    mock_schema_discovery,
    sample_table_schemas,
    sample_table_size_info
)

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