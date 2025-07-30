"""
Environment fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Environment variables following connection architecture
- Test environment setup with provider pattern
- Global settings management with Settings injection
- Pytest configuration
- Database connection environment variables with environment-specific naming
"""

import os
import pytest
import logging
from unittest.mock import patch
from typing import Dict, Any
from pathlib import Path

# Import new configuration system components
from etl_pipeline.config import (
    reset_settings,
    Settings,
    DatabaseType,
    PostgresSchema
)
from etl_pipeline.config.providers import DictConfigProvider


@pytest.fixture(autouse=True)
def reset_global_settings():
    """Reset global settings before and after each test for proper isolation."""
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def test_env_vars():
    """Test environment variables following connection architecture naming convention.
    
    This fixture provides test environment variables that conform to the connection architecture:
    - Uses TEST_ prefix for test environment variables
    - Follows the environment-specific variable naming convention
    - Matches the .env_test file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        # Environment declaration (required for fail-fast validation)
        'ETL_ENVIRONMENT': 'test',
        
        # OpenDental Source (Test) - following architecture naming
        'TEST_OPENDENTAL_SOURCE_HOST': 'localhost',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_source_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_source_pass',
        
        # MySQL Replication (Test) - following architecture naming
        'TEST_MYSQL_REPLICATION_HOST': 'localhost',
        'TEST_MYSQL_REPLICATION_PORT': '3305',
        'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
        'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
        'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
        
        # PostgreSQL Analytics (Test) - following architecture naming
        'TEST_POSTGRES_ANALYTICS_HOST': 'localhost',
        'TEST_POSTGRES_ANALYTICS_PORT': '5432',
        'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
        'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
        'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
    }





@pytest.fixture
def test_env_provider(test_env_vars):
    """Test environment provider following the provider pattern.
    
    This fixture implements the DictConfigProvider pattern for test environment:
    - Uses DictConfigProvider for testing (as recommended)
    - Provides injected environment variables for clean test isolation
    - Supports dependency injection for easy environment swapping
    - Follows the provider pattern for configuration loading
    """
    return DictConfigProvider(
        pipeline={},
        tables={'tables': {}},
        env=test_env_vars
    )


@pytest.fixture
def production_env_provider(production_env_vars):
    """Production environment provider following the provider pattern.
    
    This fixture implements the DictConfigProvider pattern for production environment:
    - Uses DictConfigProvider with production environment variables
    - Provides injected environment variables for integration testing
    - Supports dependency injection for environment swapping
    """
    return DictConfigProvider(
        pipeline={},
        tables={'tables': {}},
        env=production_env_vars
    )


@pytest.fixture
def test_settings(test_env_provider):
    """Test settings with provider injection following connection architecture.
    
    This fixture implements the Settings injection pattern as specified in the architecture:
    - Uses Settings with provider injection for environment-agnostic operation
    - Uses DictConfigProvider for testing (as recommended)
    - Supports dependency injection for clean test isolation
    - Follows the unified interface pattern
    """
    return Settings(environment='test', provider=test_env_provider)


@pytest.fixture
def production_settings(production_env_provider):
    """Production settings with provider injection following connection architecture.
    
    This fixture implements the Settings injection pattern for production environment:
    - Uses Settings with provider injection for environment-agnostic operation
    - Uses DictConfigProvider with production environment variables
    - Supports dependency injection for integration testing
    """
    return Settings(environment='production', provider=production_env_provider)


@pytest.fixture
def test_settings_with_config(test_env_vars, test_pipeline_config, test_tables_config):
    """Create test settings with configuration using provider pattern.
    
    This fixture demonstrates the provider pattern for complete configuration:
    - Uses DictConfigProvider for testing (as recommended)
    - Provides injected configuration for clean test isolation
    - Supports dependency injection for easy configuration swapping
    - Follows the provider pattern for configuration loading
    """
    test_provider = DictConfigProvider(
        pipeline=test_pipeline_config,
        tables=test_tables_config,
        env=test_env_vars
    )
    return Settings(environment='test', provider=test_provider)


@pytest.fixture
def mock_env_test_settings(test_env_provider):
    """Create test settings with mocked environment variables using provider pattern.
    
    This fixture demonstrates Settings injection with provider pattern:
    - Uses Settings with provider injection for environment-agnostic operation
    - Uses DictConfigProvider for testing (as recommended)
    - Supports dependency injection for clean test isolation
    """
    return Settings(environment='test', provider=test_env_provider)


@pytest.fixture
def production_settings_with_config(production_env_vars, valid_pipeline_config, complete_tables_config):
    """Create production settings with configuration using provider pattern.
    
    This fixture demonstrates the provider pattern for production configuration:
    - Uses DictConfigProvider with production-like configuration
    - Provides injected configuration for integration testing
    - Supports dependency injection for configuration swapping
    """
    production_provider = DictConfigProvider(
        pipeline=valid_pipeline_config,
        tables=complete_tables_config,
        env=production_env_vars
    )
    return Settings(environment='production', provider=production_provider)


@pytest.fixture
def production_settings_with_file_provider(load_production_environment_file):
    """Production settings with FileConfigProvider using loaded .env_production file.
    
    This fixture creates production settings using FileConfigProvider with the actual
    .env_production file, which is what integration tests need for real production
    database connections.
    
    This is different from the production_settings fixture which uses DictConfigProvider
    with hardcoded values for unit testing.
    """
    from etl_pipeline.config.providers import FileConfigProvider
    from etl_pipeline.config.settings import Settings
    from pathlib import Path
    
    # Get the etl_pipeline root directory
    config_dir = Path(__file__).parent.parent.parent  # etl_pipeline directory
    
    # Create FileConfigProvider that will load from .env_production
    provider = FileConfigProvider(config_dir, environment='production')
    
    # Create Settings with FileConfigProvider for real production connections
    settings = Settings(environment='production', provider=provider)
    
    return settings


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch, request):
    """Set up test environment with proper isolation following connection architecture.
    
    This fixture sets up the test environment following the connection architecture:
    - Uses ETL_ENVIRONMENT for environment detection
    - Supports provider pattern for configuration loading
    - Maintains test isolation through proper cleanup
    """
    # Set test environment variables following architecture
    test_env = {
        'ETL_ENVIRONMENT': 'test',  # Required for fail-fast validation
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
    reset_settings()
    
    yield
    
    # Cleanup after test
    reset_settings()


@pytest.fixture
def load_test_environment_file():
    """Load environment variables from .env_test file for integration tests.
    
    This fixture loads the actual .env_test file and sets the environment variables
    in os.environ for integration tests that need real database connections.
    """
    from pathlib import Path
    from dotenv import load_dotenv
    import os
    
    # Find the .env_test file
    etl_pipeline_dir = Path(__file__).parent.parent.parent  # Go to etl_pipeline root
    env_test_path = etl_pipeline_dir / '.env_test'
    
    if env_test_path.exists():
        # Load environment variables from .env_test file
        load_dotenv(env_test_path, override=True)
        print(f"Loaded environment variables from: {env_test_path}")
        # Debug print for replication variables
        print("DEBUG: TEST_MYSQL_REPLICATION_HOST =", os.environ.get("TEST_MYSQL_REPLICATION_HOST"))
        print("DEBUG: TEST_MYSQL_REPLICATION_PORT =", os.environ.get("TEST_MYSQL_REPLICATION_PORT"))
        print("DEBUG: TEST_MYSQL_REPLICATION_DB =", os.environ.get("TEST_MYSQL_REPLICATION_DB"))
        print("DEBUG: TEST_MYSQL_REPLICATION_USER =", os.environ.get("TEST_MYSQL_REPLICATION_USER"))
        print("DEBUG: TEST_MYSQL_REPLICATION_PASSWORD =", os.environ.get("TEST_MYSQL_REPLICATION_PASSWORD"))
        # Verify that ETL_ENVIRONMENT is set to 'test'
        if os.environ.get('ETL_ENVIRONMENT') != 'test':
            os.environ['ETL_ENVIRONMENT'] = 'test'
            print("Set ETL_ENVIRONMENT=test for integration tests")
    else:
        print(f"Warning: .env_test file not found at {env_test_path}")
        print("Integration tests may fail due to missing environment variables")
    
    yield
    
    # No cleanup needed - environment variables persist for the test session


@pytest.fixture
def load_production_environment_file():
    """Load environment variables from .env_production file for production integration tests.
    
    This fixture loads the actual .env_production file and sets the environment variables
    in os.environ for integration tests that need real production database connections.
    """
    from pathlib import Path
    from dotenv import load_dotenv
    import os
    
    # Find the .env_production file
    etl_pipeline_dir = Path(__file__).parent.parent.parent  # Go to etl_pipeline root
    env_production_path = etl_pipeline_dir / '.env_production'
    
    if env_production_path.exists():
        # Load environment variables from .env_production file
        load_dotenv(env_production_path, override=True)
        print(f"Loaded production environment variables from: {env_production_path}")
        # Debug print for production variables
        print("DEBUG: OPENDENTAL_SOURCE_HOST =", os.environ.get("OPENDENTAL_SOURCE_HOST"))
        print("DEBUG: OPENDENTAL_SOURCE_PORT =", os.environ.get("OPENDENTAL_SOURCE_PORT"))
        print("DEBUG: OPENDENTAL_SOURCE_DB =", os.environ.get("OPENDENTAL_SOURCE_DB"))
        print("DEBUG: OPENDENTAL_SOURCE_USER =", os.environ.get("OPENDENTAL_SOURCE_USER"))
        print("DEBUG: OPENDENTAL_SOURCE_PASSWORD =", os.environ.get("OPENDENTAL_SOURCE_PASSWORD"))
        # Verify that ETL_ENVIRONMENT is set to 'production'
        if os.environ.get('ETL_ENVIRONMENT') != 'production':
            os.environ['ETL_ENVIRONMENT'] = 'production'
            print("Set ETL_ENVIRONMENT=production for production integration tests")
    else:
        print(f"Warning: .env_production file not found at {env_production_path}")
        print("Production integration tests may fail due to missing environment variables")
        print("Please copy .env_production.template to .env_production and configure it")
    
    yield
    
    # No cleanup needed - environment variables persist for the test session


@pytest.fixture(scope="session")
def test_environment():
    """Session-scoped test environment setup following connection architecture.
    
    This fixture sets up the session-level test environment following the architecture:
    - Uses ETL_ENVIRONMENT for environment detection
    - Supports provider pattern for configuration loading
    - Maintains proper test isolation
    """
    # Create test directories if they don't exist
    test_dirs = [
        'logs',
        'temp',
        'test_data'
    ]
    
    for dir_name in test_dirs:
        os.makedirs(dir_name, exist_ok=True)
    
    # Set up session-level environment following architecture
    os.environ['ETL_ENVIRONMENT'] = 'test'  # Required for fail-fast validation
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
        
        # Auto-mark tests based on test name
        if "slow" in item.name.lower():
            item.add_marker(pytest.mark.slow)
        if "database" in item.name.lower():
            item.add_marker(pytest.mark.database)
        if "external" in item.name.lower():
            item.add_marker(pytest.mark.external)


@pytest.fixture
def environment_detection_test_cases():
    """Test cases for environment detection following connection architecture.
    
    This fixture provides test cases that follow the connection architecture:
    - Uses ETL_ENVIRONMENT for environment detection
    - Follows fail-fast validation (no defaults to production)
    - Supports provider pattern for configuration loading
    """
    return [
        # Valid environment detection cases
        ({'ETL_ENVIRONMENT': 'test'}, 'test', 'ETL_ENVIRONMENT=test'),
        ({'ETL_ENVIRONMENT': 'production'}, 'production', 'ETL_ENVIRONMENT=production'),
        
        # Invalid environment detection cases (should fail fast)
        ({}, None, 'Missing ETL_ENVIRONMENT should fail fast'),
        ({'ETL_ENVIRONMENT': 'invalid'}, None, 'Invalid ETL_ENVIRONMENT should fail fast'),
        ({'ENVIRONMENT': 'production'}, None, 'Other env vars should not override ETL_ENVIRONMENT'),
        ({'APP_ENV': 'development'}, None, 'Other env vars should not override ETL_ENVIRONMENT'),
    ]





@pytest.fixture
def test_environment_prefixes():
    """Test environment prefixes following connection architecture naming convention.
    
    This fixture provides environment prefixes that follow the architecture:
    - Uses TEST_ prefix for test environment variables
    - Uses non-prefixed variables for production environment
    - Supports provider pattern for configuration loading
    """
    return {
        'test': 'TEST_',
        'production': '',
        'development': 'DEV_'
    }


@pytest.fixture
def schema_environment_variables():
    """Schema-specific environment variables for testing following connection architecture.
    
    This fixture provides schema-specific environment variables that follow the architecture:
    - Uses enums for type safety as specified in the architecture
    - Follows environment-specific variable naming convention
    - Supports provider pattern for configuration loading
    """
    return {
        PostgresSchema.RAW: {
            'schema': 'raw',
            'env_var': 'POSTGRES_ANALYTICS_SCHEMA'
        },
        PostgresSchema.STAGING: {
            'schema': 'staging',
            'env_var': 'POSTGRES_ANALYTICS_SCHEMA'
        },
        PostgresSchema.INTERMEDIATE: {
            'schema': 'intermediate',
            'env_var': 'POSTGRES_ANALYTICS_SCHEMA'
        },
        PostgresSchema.MARTS: {
            'schema': 'marts',
            'env_var': 'POSTGRES_ANALYTICS_SCHEMA'
        }
    } 


@pytest.fixture
def comprehensive_test_isolation():
    """Comprehensive test isolation fixture for complex test scenarios.
    
    This fixture provides enhanced isolation for comprehensive tests:
    - Cleans up mock objects and patches
    - Manages test resources and connections
    - Ensures proper teardown of complex test scenarios
    - Supports provider pattern for dependency injection
    - Follows the connection architecture for proper isolation
    """
    import gc
    from unittest.mock import patch
    
    # Track patches and mocks for cleanup
    patches = []
    mocks = []
    
    def add_patch(patch_obj):
        """Add a patch for cleanup."""
        patches.append(patch_obj)
        return patch_obj
    
    def add_mock(mock_obj):
        """Add a mock for cleanup."""
        mocks.append(mock_obj)
        return mock_obj
    
    # Provide the cleanup functions to the test
    yield {
        'add_patch': add_patch,
        'add_mock': add_mock,
        'patches': patches,
        'mocks': mocks
    }
    
    # Cleanup: Stop all patches
    for patch_obj in patches:
        try:
            patch_obj.stop()
        except Exception as e:
            logging.warning(f"Error stopping patch {patch_obj}: {e}")
    
    # Cleanup: Reset all mocks
    for mock_obj in mocks:
        try:
            mock_obj.reset_mock()
        except Exception as e:
            logging.warning(f"Error resetting mock {mock_obj}: {e}")
    
    # Force garbage collection to clean up any remaining references
    gc.collect()


@pytest.fixture
def mock_cleanup():
    """Mock cleanup fixture for comprehensive tests.
    
    This fixture provides automatic mock cleanup for comprehensive tests:
    - Automatically stops patches when they go out of scope
    - Resets mock state for clean test isolation
    - Supports context manager pattern for easy cleanup
    """
    from unittest.mock import patch
    
    class MockCleanup:
        def __init__(self):
            self.patches = []
            self.mocks = []
        
        def add_patch(self, patch_obj):
            """Add a patch for automatic cleanup."""
            self.patches.append(patch_obj)
            return patch_obj
        
        def add_mock(self, mock_obj):
            """Add a mock for automatic cleanup."""
            self.mocks.append(mock_obj)
            return mock_obj
        
        def cleanup(self):
            """Clean up all patches and mocks."""
            for patch_obj in self.patches:
                try:
                    patch_obj.stop()
                except Exception as e:
                    logging.warning(f"Error stopping patch {patch_obj}: {e}")
            
            for mock_obj in self.mocks:
                try:
                    mock_obj.reset_mock()
                except Exception as e:
                    logging.warning(f"Error resetting mock {mock_obj}: {e}")
    
    cleanup = MockCleanup()
    yield cleanup
    cleanup.cleanup() 