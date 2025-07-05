"""
Minimal conftest.py for ETL pipeline tests.

This file imports fixtures from the modular fixtures directory
to make them available to all tests in the test suite.
"""

# Import essential fixtures from modular fixtures directory
from tests.fixtures.env_fixtures import test_env_vars, production_env_vars, setup_test_environment, reset_global_settings

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