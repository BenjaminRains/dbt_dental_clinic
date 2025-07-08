"""
Legacy fixtures for ETL pipeline tests.

DEPRECATED: This module contains deprecated fixtures for backward compatibility.
These fixtures have been removed as part of the new architectural patterns.
The file is kept for debugging purposes only.

This module previously contained fixtures related to:
- Backward compatibility
- Legacy settings
- Legacy connection factory
- Migration utilities

All fixtures have been deprecated and removed. Use the new fixtures instead:
- Use config_fixtures.py for configuration testing
- Use connection_fixtures.py for connection testing
- Use env_fixtures.py for environment testing
- Use integration_fixtures.py for integration testing
"""

import pytest
import warnings
from unittest.mock import MagicMock, Mock
from typing import Dict, List, Any


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
def legacy_settings():
    """DEPRECATED: Legacy settings for backward compatibility testing."""
    _deprecation_warning("legacy_settings")
    return _deprecation_warning("legacy_settings")


@pytest.fixture
def legacy_connection_factory():
    """DEPRECATED: Legacy connection factory for backward compatibility testing."""
    _deprecation_warning("legacy_connection_factory")
    return _deprecation_warning("legacy_connection_factory")


@pytest.fixture
def legacy_config():
    """DEPRECATED: Legacy configuration for backward compatibility testing."""
    _deprecation_warning("legacy_config")
    return _deprecation_warning("legacy_config")


@pytest.fixture
def legacy_table_config():
    """DEPRECATED: Legacy table configuration for backward compatibility testing."""
    _deprecation_warning("legacy_table_config")
    return _deprecation_warning("legacy_table_config")


@pytest.fixture
def legacy_loader():
    """DEPRECATED: Legacy loader for backward compatibility testing."""
    _deprecation_warning("legacy_loader")
    return _deprecation_warning("legacy_loader")


@pytest.fixture
def legacy_transformer():
    """DEPRECATED: Legacy transformer for backward compatibility testing."""
    _deprecation_warning("legacy_transformer")
    return _deprecation_warning("legacy_transformer")


@pytest.fixture
def legacy_replicator():
    """DEPRECATED: Legacy replicator for backward compatibility testing."""
    _deprecation_warning("legacy_replicator")
    return _deprecation_warning("legacy_replicator")


@pytest.fixture
def legacy_orchestrator():
    """DEPRECATED: Legacy orchestrator for backward compatibility testing."""
    _deprecation_warning("legacy_orchestrator")
    return _deprecation_warning("legacy_orchestrator")


@pytest.fixture
def legacy_metrics():
    """DEPRECATED: Legacy metrics for backward compatibility testing."""
    _deprecation_warning("legacy_metrics")
    return _deprecation_warning("legacy_metrics")


@pytest.fixture
def legacy_validation():
    """DEPRECATED: Legacy validation for backward compatibility testing."""
    _deprecation_warning("legacy_validation")
    return _deprecation_warning("legacy_validation")


@pytest.fixture
def legacy_error_handler():
    """DEPRECATED: Legacy error handler for backward compatibility testing."""
    _deprecation_warning("legacy_error_handler")
    return _deprecation_warning("legacy_error_handler")


@pytest.fixture
def legacy_logger():
    """DEPRECATED: Legacy logger for backward compatibility testing."""
    _deprecation_warning("legacy_logger")
    return _deprecation_warning("legacy_logger")


@pytest.fixture
def legacy_config_parser():
    """DEPRECATED: Legacy config parser for backward compatibility testing."""
    _deprecation_warning("legacy_config_parser")
    return _deprecation_warning("legacy_config_parser")


@pytest.fixture
def legacy_migration_utils():
    """DEPRECATED: Legacy migration utilities for backward compatibility testing."""
    _deprecation_warning("legacy_migration_utils")
    return _deprecation_warning("legacy_migration_utils")


# Legacy fixture names that were previously available
# These are kept for reference and debugging purposes
LEGACY_FIXTURE_NAMES = [
    'legacy_settings',
    'legacy_connection_factory', 
    'legacy_config',
    'legacy_table_config',
    'legacy_loader',
    'legacy_transformer',
    'legacy_replicator',
    'legacy_orchestrator',
    'legacy_metrics',
    'legacy_validation',
    'legacy_error_handler',
    'legacy_logger',
    'legacy_config_parser',
    'legacy_migration_utils'
]


def get_legacy_fixture_info():
    """Get information about deprecated legacy fixtures for debugging."""
    return {
        'deprecated_fixtures': LEGACY_FIXTURE_NAMES,
        'replacement_guidance': {
            'legacy_settings': 'Use test_settings() from env_fixtures.py',
            'legacy_connection_factory': 'Use mock_connection_factory_methods() from connection_fixtures.py',
            'legacy_config': 'Use test_pipeline_config() from config_fixtures.py',
            'legacy_table_config': 'Use test_tables_config() from config_fixtures.py',
            'legacy_loader': 'Use postgres_loader() from loader_fixtures.py',
            'legacy_transformer': 'Use mock_table_processor_engines() from transformer_fixtures.py',
            'legacy_replicator': 'Use mock_replicator() from replicator_fixtures.py',
            'legacy_orchestrator': 'Use orchestrator() from orchestrator_fixtures.py',
            'legacy_metrics': 'Use mock_metrics_collector() from metrics_fixtures.py',
            'legacy_validation': 'Use sample_validation_rules() from transformer_fixtures.py',
            'legacy_error_handler': 'Use mock_connection_error() from connection_fixtures.py',
            'legacy_logger': 'Use mock_logger() from logging_fixtures.py',
            'legacy_config_parser': 'Use test_pipeline_config() from config_fixtures.py',
            'legacy_migration_utils': 'Use mock_schema_migration() from postgres_schema_fixtures.py'
        },
        'migration_notes': [
            'All legacy fixtures have been deprecated and removed',
            'Use the new architectural fixtures that follow enum-based type safety',
            'See docs/etl_test_fixtures_reference.md for complete fixture documentation',
            'New fixtures use DatabaseType and PostgresSchema enums',
            'New fixtures follow explicit environment separation (production vs test)',
            'New fixtures integrate with the Settings class and ConnectionFactory pattern'
        ]
    } 