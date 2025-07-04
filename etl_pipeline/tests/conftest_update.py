"""
Step 3.1: Update tests/conftest.py

BACKUP YOUR EXISTING FILE FIRST!
cp tests/conftest.py tests/conftest.py.backup

Then replace with this clean implementation.
"""

import pytest
import os
from unittest.mock import patch
from pathlib import Path
from etl_pipeline.config import reset_settings, create_test_settings, DatabaseType, PostgresSchema


@pytest.fixture(autouse=True)
def reset_global_settings():
    """Reset global settings before and after each test."""
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def test_env_vars():
    """Standard test environment variables."""
    return {
        'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
        'TEST_MYSQL_REPLICATION_HOST': 'localhost',
        'TEST_MYSQL_REPLICATION_PORT': '3306',
        'TEST_MYSQL_REPLICATION_DB': 'test_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_pass',
        'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_pass',
        'ETL_ENVIRONMENT': 'test'
    }


@pytest.fixture
def test_pipeline_config():
    """Standard test pipeline configuration."""
    return {
        'general': {
            'pipeline_name': 'test_pipeline',
            'environment': 'test',
            'batch_size': 1000,
            'parallel_jobs': 2
        },
        'connections': {
            'source': {
                'pool_size': 2,
                'connect_timeout': 30
            },
            'replication': {
                'pool_size': 2,
                'connect_timeout': 30
            },
            'analytics': {
                'pool_size': 2,
                'connect_timeout': 30
            }
        }
    }


@pytest.fixture
def test_tables_config():
    """Standard test tables configuration."""
    return {
        'tables': {
            'patient': {
                'primary_key': 'PatNum',
                'incremental_column': 'DateTStamp',
                'extraction_strategy': 'incremental',
                'table_importance': 'critical',
                'batch_size': 100
            },
            'appointment': {
                'primary_key': 'AptNum',
                'incremental_column': 'AptDateTime',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 50
            },
            'procedurelog': {
                'primary_key': 'ProcNum',
                'incremental_column': 'ProcDate',
                'extraction_strategy': 'incremental',
                'table_importance': 'important',
                'batch_size': 200
            }
        }
    }


@pytest.fixture
def test_settings(test_env_vars, test_pipeline_config, test_tables_config):
    """Create isolated test settings."""
    return create_test_settings(
        pipeline_config=test_pipeline_config,
        tables_config=test_tables_config,
        env_vars=test_env_vars
    )


@pytest.fixture
def mock_env_test_settings(test_env_vars):
    """Create test settings with mocked environment variables."""
    with patch.dict(os.environ, test_env_vars):
        from etl_pipeline.config import create_settings
        yield create_settings(environment='test')


@pytest.fixture
def production_env_vars():
    """Production environment variables for integration tests."""
    return {
        'OPENDENTAL_SOURCE_HOST': os.getenv('OPENDENTAL_SOURCE_HOST', 'localhost'),
        'OPENDENTAL_SOURCE_PORT': os.getenv('OPENDENTAL_SOURCE_PORT', '3306'),
        'OPENDENTAL_SOURCE_DB': os.getenv('OPENDENTAL_SOURCE_DB', 'opendental'),
        'OPENDENTAL_SOURCE_USER': os.getenv('OPENDENTAL_SOURCE_USER', 'user'),
        'OPENDENTAL_SOURCE_PASSWORD': os.getenv('OPENDENTAL_SOURCE_PASSWORD', 'password'),
        'MYSQL_REPLICATION_HOST': os.getenv('MYSQL_REPLICATION_HOST', 'localhost'),
        'MYSQL_REPLICATION_PORT': os.getenv('MYSQL_REPLICATION_PORT', '3306'),
        'MYSQL_REPLICATION_DB': os.getenv('MYSQL_REPLICATION_DB', 'opendental_replication'),
        'MYSQL_REPLICATION_USER': os.getenv('MYSQL_REPLICATION_USER', 'user'),
        'MYSQL_REPLICATION_PASSWORD': os.getenv('MYSQL_REPLICATION_PASSWORD', 'password'),
        'POSTGRES_ANALYTICS_HOST': os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
        'POSTGRES_ANALYTICS_PORT': os.getenv('POSTGRES_ANALYTICS_PORT', '5432'),
        'POSTGRES_ANALYTICS_DB': os.getenv('POSTGRES_ANALYTICS_DB', 'opendental_analytics'),
        'POSTGRES_ANALYTICS_SCHEMA': os.getenv('POSTGRES_ANALYTICS_SCHEMA', 'raw'),
        'POSTGRES_ANALYTICS_USER': os.getenv('POSTGRES_ANALYTICS_USER', 'user'),
        'POSTGRES_ANALYTICS_PASSWORD': os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'password'),
        'ETL_ENVIRONMENT': 'production'
    }


@pytest.fixture
def production_settings(production_env_vars):
    """Create production settings for integration tests."""
    with patch.dict(os.environ, production_env_vars):
        from etl_pipeline.config import create_settings
        yield create_settings(environment='production')


# Database type fixtures for easy access
@pytest.fixture
def database_types():
    """Provide DatabaseType enum for tests."""
    return DatabaseType


@pytest.fixture
def postgres_schemas():
    """Provide PostgresSchema enum for tests."""
    return PostgresSchema


# Test markers and configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: Unit tests (fast, isolated)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (require databases)"
    )
    config.addinivalue_line(
        "markers", "config: Configuration system tests"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take longer to run"
    )
    config.addinivalue_line(
        "markers", "postgres: Tests requiring PostgreSQL"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle markers."""
    for item in items:
        # Add unit marker to tests that don't have any marker
        if not any(item.iter_markers()):
            item.add_marker(pytest.mark.unit)