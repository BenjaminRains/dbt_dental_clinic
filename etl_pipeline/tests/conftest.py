"""
Minimal conftest.py for ETL pipeline tests.

This file imports fixtures from the modular fixtures directory
to make them available to all tests in the test suite.
"""

# Import essential fixtures from modular fixtures directory
from tests.fixtures.env_fixtures import test_env_vars, production_env_vars, setup_test_environment, reset_global_settings 