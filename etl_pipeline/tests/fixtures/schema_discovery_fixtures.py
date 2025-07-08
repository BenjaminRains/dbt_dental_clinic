"""
Schema Discovery fixtures for ETL pipeline tests.

DEPRECATED: This module contains deprecated fixtures for schema discovery testing.
These fixtures have been removed as part of the new architectural patterns.
The file is kept for debugging purposes only.

This module previously contained fixtures related to:
- Schema discovery testing
- Test data management for schema discovery
- Schema discovery configuration
- Real database schema discovery testing

All fixtures have been deprecated and removed. Use the new fixtures instead:
- Use config_fixtures.py for configuration testing
- Use connection_fixtures.py for connection testing
- Use env_fixtures.py for environment testing
- Use integration_fixtures.py for integration testing
"""

import pytest
import warnings
import logging
from unittest.mock import MagicMock, Mock
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def _deprecation_warning(fixture_name: str):
    """Show deprecation warning for removed fixtures."""
    warnings.warn(
        f"Fixture '{fixture_name}' has been deprecated and removed. "
        f"Use the new architectural fixtures instead. "
        f"See docs/etl_test_fixtures_reference.md for current fixtures.",
        DeprecationWarning,
        stacklevel=3
    )
    return MagicMock()


@pytest.fixture
def schema_discovery_test_settings():
    """DEPRECATED: Create test settings specifically for schema discovery testing."""
    _deprecation_warning("schema_discovery_test_settings")
    return _deprecation_warning("schema_discovery_test_settings")


@pytest.fixture
def schema_discovery_test_data_manager():
    """DEPRECATED: Manage test data in the real OpenDental database for schema discovery testing."""
    _deprecation_warning("schema_discovery_test_data_manager")
    return _deprecation_warning("schema_discovery_test_data_manager")


@pytest.fixture
def schema_discovery_instance():
    """DEPRECATED: Create real SchemaDiscovery instance with real MySQL connection using new configuration system."""
    _deprecation_warning("schema_discovery_instance")
    return _deprecation_warning("schema_discovery_instance")


@pytest.fixture
def mock_schema_discovery():
    """DEPRECATED: Mock SchemaDiscovery instance for unit testing."""
    _deprecation_warning("mock_schema_discovery")
    return _deprecation_warning("mock_schema_discovery")


@pytest.fixture
def sample_table_schemas():
    """DEPRECATED: Sample table schemas for testing."""
    _deprecation_warning("sample_table_schemas")
    return _deprecation_warning("sample_table_schemas")


@pytest.fixture
def sample_table_size_info():
    """DEPRECATED: Sample table size information for testing."""
    _deprecation_warning("sample_table_size_info")
    return _deprecation_warning("sample_table_size_info")


# Schema discovery fixture names that were previously available
# These are kept for reference and debugging purposes
SCHEMA_DISCOVERY_FIXTURE_NAMES = [
    'schema_discovery_test_settings',
    'schema_discovery_test_data_manager',
    'schema_discovery_instance',
    'mock_schema_discovery',
    'sample_table_schemas',
    'sample_table_size_info'
]


def get_schema_discovery_fixture_info():
    """Get information about deprecated schema discovery fixtures for debugging."""
    return {
        'deprecated_fixtures': SCHEMA_DISCOVERY_FIXTURE_NAMES,
        'replacement_guidance': {
            'schema_discovery_test_settings': 'Use test_settings() from env_fixtures.py',
            'schema_discovery_test_data_manager': 'Use test_data_manager() from integration_fixtures.py',
            'schema_discovery_instance': 'Use test_replication_engine() from integration_fixtures.py',
            'mock_schema_discovery': 'Use mock_components() from orchestrator_fixtures.py',
            'sample_table_schemas': 'Use sample_mysql_schema() from loader_fixtures.py',
            'sample_table_size_info': 'Use mock_loader_stats() from loader_fixtures.py'
        },
        'migration_notes': [
            'All schema discovery fixtures have been deprecated and removed',
            'Use the new architectural fixtures that follow enum-based type safety',
            'See docs/etl_test_fixtures_reference.md for complete fixture documentation',
            'New fixtures use DatabaseType and PostgresSchema enums',
            'New fixtures follow explicit environment separation (production vs test)',
            'New fixtures integrate with the Settings class and ConnectionFactory pattern',
            'Schema discovery functionality is now handled by the orchestrator components'
        ]
    } 