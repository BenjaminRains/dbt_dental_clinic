"""
Environment fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Environment variables
- Test environment setup
- Global settings management
- Pytest configuration
"""

import os
import pytest
import logging
from unittest.mock import patch
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
def load_test_environment():
    """Load environment variables from .env file for testing."""
    # Try to load from etl_pipeline/.env first
    etl_env_path = Path(__file__).parent.parent.parent / '.env'
    if etl_env_path.exists():
        load_dotenv(etl_env_path)
        print(f"Loaded environment from: {etl_env_path}")
    else:
        # Fall back to parent directory .env
        parent_env_path = Path(__file__).parent.parent.parent.parent / '.env'
        if parent_env_path.exists():
            load_dotenv(parent_env_path)
            print(f"Loaded environment from: {parent_env_path}")
        else:
            print("No .env file found")

# Load environment at module import time
load_test_environment()

# Import ETL pipeline components for testing
try:
    from etl_pipeline.config.settings import Settings
except ImportError:
    # Fallback for new configuration system
    from etl_pipeline.config import create_settings
    Settings = None

# Import new configuration system components
try:
    from etl_pipeline.config import reset_settings, create_test_settings, DatabaseType, PostgresSchema
    NEW_CONFIG_AVAILABLE = True
except ImportError:
    # Fallback for backward compatibility
    NEW_CONFIG_AVAILABLE = False
    DatabaseType = None
    PostgresSchema = None


@pytest.fixture(autouse=True)
def reset_global_settings():
    """Reset global settings before and after each test for proper isolation."""
    if NEW_CONFIG_AVAILABLE:
        reset_settings()
        yield
        reset_settings()
    else:
        # Fallback for old configuration system
        yield


@pytest.fixture
def test_env_vars():
    """Standard test environment variables loaded from actual .env file."""
    return {
        'TEST_OPENDENTAL_SOURCE_HOST': os.getenv('TEST_OPENDENTAL_SOURCE_HOST', 'localhost'),
        'TEST_OPENDENTAL_SOURCE_PORT': os.getenv('TEST_OPENDENTAL_SOURCE_PORT', '3306'),
        'TEST_OPENDENTAL_SOURCE_DB': os.getenv('TEST_OPENDENTAL_SOURCE_DB', 'test_opendental'),
        'TEST_OPENDENTAL_SOURCE_USER': os.getenv('TEST_OPENDENTAL_SOURCE_USER', 'test_user'),
        'TEST_OPENDENTAL_SOURCE_PASSWORD': os.getenv('TEST_OPENDENTAL_SOURCE_PASSWORD', 'test_pass'),
        'TEST_MYSQL_REPLICATION_HOST': os.getenv('TEST_MYSQL_REPLICATION_HOST', 'localhost'),
        'TEST_MYSQL_REPLICATION_PORT': os.getenv('TEST_MYSQL_REPLICATION_PORT', '3306'),
        'TEST_MYSQL_REPLICATION_DB': os.getenv('TEST_MYSQL_REPLICATION_DB', 'test_opendental_replication'),
        'TEST_MYSQL_REPLICATION_USER': os.getenv('TEST_MYSQL_REPLICATION_USER', 'test_user'),
        'TEST_MYSQL_REPLICATION_PASSWORD': os.getenv('TEST_MYSQL_REPLICATION_PASSWORD', 'test_pass'),
        'TEST_POSTGRES_ANALYTICS_HOST': os.getenv('TEST_POSTGRES_ANALYTICS_HOST', 'localhost'),
        'TEST_POSTGRES_ANALYTICS_PORT': os.getenv('TEST_POSTGRES_ANALYTICS_PORT', '5432'),
        'TEST_POSTGRES_ANALYTICS_DB': os.getenv('TEST_POSTGRES_ANALYTICS_DB', 'test_opendental_analytics'),
        'TEST_POSTGRES_ANALYTICS_SCHEMA': os.getenv('TEST_POSTGRES_ANALYTICS_SCHEMA', 'raw'),
        'TEST_POSTGRES_ANALYTICS_USER': os.getenv('TEST_POSTGRES_ANALYTICS_USER', 'test_user'),
        'TEST_POSTGRES_ANALYTICS_PASSWORD': os.getenv('TEST_POSTGRES_ANALYTICS_PASSWORD', 'test_pass'),
        'ETL_ENVIRONMENT': 'test'
    }


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
        'POSTGRES_ANALYTICS_USER': os.getenv('POSTGRES_ANALYTICS_USER', 'analytics_user'),
        'POSTGRES_ANALYTICS_PASSWORD': os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'password'),
        'ETL_ENVIRONMENT': 'production'
    }


@pytest.fixture
def test_settings(test_env_vars, test_pipeline_config, test_tables_config):
    """Create isolated test settings using new configuration system."""
    if NEW_CONFIG_AVAILABLE:
        return create_test_settings(
            pipeline_config=test_pipeline_config,
            tables_config=test_tables_config,
            env_vars=test_env_vars
        )
    else:
        # Fallback to old settings pattern
        with patch.dict(os.environ, test_env_vars):
            if Settings is not None:
                return Settings(environment='test')
            else:
                return create_settings(environment='test')


@pytest.fixture
def mock_env_test_settings(test_env_vars):
    """Create test settings with mocked environment variables."""
    if NEW_CONFIG_AVAILABLE:
        with patch.dict(os.environ, test_env_vars):
            from etl_pipeline.config import create_settings
            yield create_settings(environment='test')
    else:
        # Fallback to old settings pattern
        with patch.dict(os.environ, test_env_vars):
            if Settings is not None:
                yield Settings(environment='test')
            else:
                yield create_settings(environment='test')


@pytest.fixture
def production_settings(production_env_vars):
    """Create production settings for integration tests."""
    if NEW_CONFIG_AVAILABLE:
        return create_test_settings(
            env_vars=production_env_vars,
            environment='production'
        )
    else:
        # Fallback to old settings pattern
        with patch.dict(os.environ, production_env_vars):
            if Settings is not None:
                return Settings(environment='production')
            else:
                return create_settings(environment='production')


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch, request):
    """Set up test environment with proper isolation."""
    # Set test environment variables
    test_env = {
        'ETL_ENVIRONMENT': 'test',
        'PYTHONPATH': os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'TESTING': 'true'
    }
    
    # Apply test environment variables
    for key, value in test_env.items():
        monkeypatch.setenv(key, value)
    
    # Set up logging for tests
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Reset any global state
    if NEW_CONFIG_AVAILABLE:
        reset_settings()
    
    yield
    
    # Cleanup after test
    if NEW_CONFIG_AVAILABLE:
        reset_settings()


@pytest.fixture(scope="session")
def test_environment():
    """Session-scoped test environment setup."""
    # Create test directories if they don't exist
    test_dirs = [
        'logs',
        'temp',
        'test_data'
    ]
    
    for dir_name in test_dirs:
        os.makedirs(dir_name, exist_ok=True)
    
    # Set up session-level environment
    os.environ['ETL_ENVIRONMENT'] = 'test'
    os.environ['TESTING'] = 'true'
    
    yield
    
    # Cleanup session-level resources
    # (This could include cleaning up test databases, etc.)


def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    # Register custom markers
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as an end-to-end test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "database: mark test as requiring database access"
    )
    config.addinivalue_line(
        "markers", "external: mark test as requiring external services"
    )
    
    # Set up test configuration
    config.option.strict_markers = True
    config.option.addopts = [
        "--strict-markers",
        "--disable-warnings",
        "--tb=short"
    ]


def pytest_collection_modifyitems(config, items):
    """Modify test collection to apply markers based on test location."""
    for item in items:
        # Auto-mark tests based on directory structure
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "e2e" in str(item.fspath):
            item.add_marker(pytest.mark.e2e)
        elif "performance" in str(item.fspath):
            item.add_marker(pytest.mark.slow) 