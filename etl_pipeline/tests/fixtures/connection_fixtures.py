"""
Connection fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Database connection mocks with Settings injection
- Connection factory mocks following unified interface
- Engine mocks for different database types
- Connection utilities with provider pattern integration
- ConnectionFactory method mocks with Settings injection
"""

import pytest
from unittest.mock import MagicMock, Mock
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text
from typing import Dict, Any

# Import new configuration system components
from etl_pipeline.config import DatabaseType, PostgresSchema, Settings
from etl_pipeline.config.providers import DictConfigProvider


@pytest.fixture
def database_types():
    """Database type enums for testing."""
    return DatabaseType


@pytest.fixture
def postgres_schemas():
    """PostgreSQL schema enums for testing."""
    return PostgresSchema


@pytest.fixture(scope="session")
def mock_database_engines():
    """Session-scoped mock database engines."""
    return {
        'source': MagicMock(spec=Engine),
        'replication': MagicMock(spec=Engine),
        'analytics': MagicMock(spec=Engine)
    }


@pytest.fixture
def mock_source_engine():
    """Mock source database engine."""
    engine = MagicMock(spec=Engine)
    engine.name = 'mysql'
    # Create a mock URL object
    mock_url = MagicMock()
    mock_url.database = 'test_opendental'
    mock_url.host = 'localhost'
    mock_url.port = 3306
    engine.url = mock_url
    return engine


@pytest.fixture
def mock_replication_engine():
    """Mock replication database engine."""
    engine = MagicMock(spec=Engine)
    engine.name = 'mysql'
    # Create a mock URL object
    mock_url = MagicMock()
    mock_url.database = 'test_opendental_replication'
    mock_url.host = 'localhost'
    mock_url.port = 3306
    engine.url = mock_url
    return engine


@pytest.fixture
def mock_analytics_engine():
    """Mock analytics database engine."""
    engine = MagicMock(spec=Engine)
    engine.name = 'postgresql'
    # Create a mock URL object
    mock_url = MagicMock()
    mock_url.database = 'test_opendental_analytics'
    mock_url.host = 'localhost'
    mock_url.port = 5432
    engine.url = mock_url
    return engine


@pytest.fixture
def test_connection_config():
    """Test connection configuration following connection architecture naming convention.
    
    This fixture provides test connection configuration that conforms to the connection architecture:
    - Uses TEST_ prefix for test environment variables
    - Follows the environment-specific variable naming convention
    - Matches the .env_test file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        'source': {
            'host': 'localhost',
            'port': 3306,
            'database': 'test_opendental',
            'user': 'test_source_user',
            'password': 'test_source_pass'
        },
        'replication': {
            'host': 'localhost',
            'port': 3305,
            'database': 'test_opendental_replication',
            'user': 'test_repl_user',
            'password': 'test_repl_pass',
            'pool_size': 3,
            'max_overflow': 5,
            'connect_timeout': 30
        },
        'analytics': {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_opendental_analytics',
            'user': 'test_analytics_user',
            'password': 'test_analytics_pass',
            'pool_size': 4,
            'max_overflow': 8,
            'connect_timeout': 30
        }
    }


@pytest.fixture
def production_connection_config():
    """Production connection configuration following connection architecture naming convention.
    
    This fixture provides production connection configuration that conforms to the connection architecture:
    - Uses non-prefixed variables for production environment
    - Follows the environment-specific variable naming convention
    - Matches the .env_production file structure
    - Supports the provider pattern for dependency injection
    """
    return {
        'source': {
            'host': 'prod-source-host',
            'port': 3306,
            'database': 'opendental',
            'user': 'source_user',
            'password': 'source_pass',
            'pool_size': 6,
            'max_overflow': 12,
            'connect_timeout': 60
        },
        'replication': {
            'host': 'prod-repl-host',
            'port': 3306,
            'database': 'opendental_replication',
            'user': 'repl_user',
            'password': 'repl_pass',
            'pool_size': 6,
            'max_overflow': 12,
            'connect_timeout': 60
        },
        'analytics': {
            'host': 'prod-analytics-host',
            'port': 5432,
            'database': 'opendental_analytics',
            'user': 'analytics_user',
            'password': 'analytics_pass',
            'pool_size': 6,
            'max_overflow': 12,
            'connect_timeout': 60
        }
    }


@pytest.fixture
def test_connection_provider(test_connection_config):
    """Test connection configuration provider following the provider pattern.
    
    This fixture implements the DictConfigProvider pattern for connection testing:
    - Uses DictConfigProvider for testing (as recommended)
    - Provides injected configuration for clean test isolation
    - Supports dependency injection for easy configuration swapping
    - Follows the provider pattern for configuration loading
    """
    return DictConfigProvider(
        pipeline={'connections': test_connection_config},
        tables={'tables': {}},
        env={'ETL_ENVIRONMENT': 'test'}
    )


@pytest.fixture
def production_connection_provider(production_connection_config):
    """Production connection configuration provider following the provider pattern.
    
    This fixture implements the DictConfigProvider pattern for production-like connection testing:
    - Uses DictConfigProvider with production-like configuration
    - Provides injected configuration for integration testing
    - Supports dependency injection for configuration swapping
    """
    return DictConfigProvider(
        pipeline={'connections': production_connection_config},
        tables={'tables': {}},
        env={'ETL_ENVIRONMENT': 'production'}
    )


@pytest.fixture
def test_connection_settings(test_connection_provider):
    """Test connection settings with provider injection following connection architecture.
    
    This fixture implements the Settings injection pattern as specified in the architecture:
    - Uses Settings with provider injection for environment-agnostic operation
    - Uses DictConfigProvider for testing (as recommended)
    - Supports dependency injection for clean test isolation
    - Follows the unified interface pattern
    """
    return Settings(environment='test', provider=test_connection_provider)


@pytest.fixture
def production_connection_settings(production_connection_provider):
    """Production connection settings with provider injection following connection architecture.
    
    This fixture implements the Settings injection pattern for production-like connection testing:
    - Uses Settings with provider injection for environment-agnostic operation
    - Uses DictConfigProvider with production-like configuration
    - Supports dependency injection for integration testing
    """
    return Settings(environment='production', provider=production_connection_provider)


@pytest.fixture
def mock_connection_factory_with_settings():
    """Mock connection factory with Settings injection following connection architecture.
    
    This fixture implements the unified interface pattern as specified in the architecture:
    - Uses Settings injection for all connection methods
    - Follows the unified interface pattern (same methods for all environments)
    - Supports environment-agnostic operation through Settings injection
    - Uses provider pattern for configuration loading
    """
    factory = MagicMock()
    
    # Unified interface methods with Settings injection
    def mock_get_source_connection(settings: Settings):
        """Mock source connection with Settings injection."""
        engine = MagicMock(spec=Engine)
        engine.name = 'mysql'
        mock_url = MagicMock()
        mock_url.database = settings.get_database_config(DatabaseType.SOURCE).get('database', 'test_opendental')
        mock_url.host = settings.get_database_config(DatabaseType.SOURCE).get('host', 'localhost')
        mock_url.port = settings.get_database_config(DatabaseType.SOURCE).get('port', 3306)
        engine.url = mock_url
        return engine
    
    def mock_get_replication_connection(settings: Settings):
        """Mock replication connection with Settings injection."""
        engine = MagicMock(spec=Engine)
        engine.name = 'mysql'
        mock_url = MagicMock()
        mock_url.database = settings.get_database_config(DatabaseType.REPLICATION).get('database', 'test_opendental_replication')
        mock_url.host = settings.get_database_config(DatabaseType.REPLICATION).get('host', 'localhost')
        mock_url.port = settings.get_database_config(DatabaseType.REPLICATION).get('port', 3306)
        engine.url = mock_url
        return engine
    
    def mock_get_analytics_connection(settings: Settings, schema: PostgresSchema = PostgresSchema.RAW):
        """Mock analytics connection with Settings injection."""
        engine = MagicMock(spec=Engine)
        engine.name = 'postgresql'
        mock_url = MagicMock()
        config = settings.get_database_config(DatabaseType.ANALYTICS, schema)
        mock_url.database = config.get('database', 'test_opendental_analytics')
        mock_url.host = config.get('host', 'localhost')
        mock_url.port = config.get('port', 5432)
        engine.url = mock_url
        return engine
    
    # Schema-specific convenience methods with Settings injection
    def mock_get_analytics_raw_connection(settings: Settings):
        """Mock analytics raw connection with Settings injection."""
        return mock_get_analytics_connection(settings, PostgresSchema.RAW)
    
    def mock_get_analytics_staging_connection(settings: Settings):
        """Mock analytics staging connection with Settings injection."""
        return mock_get_analytics_connection(settings, PostgresSchema.STAGING)
    
    def mock_get_analytics_intermediate_connection(settings: Settings):
        """Mock analytics intermediate connection with Settings injection."""
        return mock_get_analytics_connection(settings, PostgresSchema.INTERMEDIATE)
    
    def mock_get_analytics_marts_connection(settings: Settings):
        """Mock analytics marts connection with Settings injection."""
        return mock_get_analytics_connection(settings, PostgresSchema.MARTS)
    
    # Assign methods to factory
    factory.get_source_connection = mock_get_source_connection
    factory.get_replication_connection = mock_get_replication_connection
    factory.get_analytics_connection = mock_get_analytics_connection
    factory.get_analytics_raw_connection = mock_get_analytics_raw_connection
    factory.get_analytics_staging_connection = mock_get_analytics_staging_connection
    factory.get_analytics_intermediate_connection = mock_get_analytics_intermediate_connection
    factory.get_analytics_marts_connection = mock_get_analytics_marts_connection
    
    return factory


@pytest.fixture
def mock_connection_factory():
    """Mock connection factory for testing (legacy compatibility).
    
    This fixture provides backward compatibility while supporting the new architecture:
    - Maintains compatibility with existing tests
    - Supports both old and new connection patterns
    - Can be used with Settings injection when needed
    """
    factory = MagicMock()
    
    # Mock the get_connection method (legacy pattern)
    def mock_get_connection(db_type, **kwargs):
        mock_engine = MagicMock(spec=Engine)
        # Create a mock URL object
        mock_url = MagicMock()
        if db_type == 'source':
            mock_engine.name = 'mysql'
            mock_url.database = 'test_opendental'
        elif db_type == 'replication':
            mock_engine.name = 'mysql'
            mock_url.database = 'test_opendental_replication'
        elif db_type == 'analytics':
            mock_engine.name = 'postgresql'
            mock_url.database = 'test_opendental_analytics'
        mock_engine.url = mock_url
        return mock_engine
    
    factory.get_connection = mock_get_connection
    return factory


@pytest.fixture
def mock_connection_factory_methods():
    """Mock ConnectionFactory methods following new architecture with Settings injection.
    
    This fixture implements the unified interface pattern as specified in the architecture:
    - Uses Settings injection for all connection methods
    - Follows the unified interface pattern (same methods for all environments)
    - Supports environment-agnostic operation through Settings injection
    - Uses provider pattern for configuration loading
    """
    factory = MagicMock()
    
    # Unified interface methods with Settings injection
    factory.get_source_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_replication_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_analytics_connection = MagicMock(return_value=MagicMock(spec=Engine))
    
    # Schema-specific analytics connections with Settings injection
    factory.get_analytics_raw_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_analytics_staging_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_analytics_intermediate_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_analytics_marts_connection = MagicMock(return_value=MagicMock(spec=Engine))
    
    return factory


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection for testing."""
    connection = MagicMock()
    connection.execute.return_value = MagicMock()
    connection.close.return_value = None
    return connection


@pytest.fixture
def mock_mysql_connection():
    """Mock MySQL connection for testing."""
    connection = MagicMock()
    connection.execute.return_value = MagicMock()
    connection.close.return_value = None
    return connection


@pytest.fixture
def mock_engine_with_connection():
    """Mock engine that returns a mock connection."""
    def _mock_engine_with_connection(connection_mock=None):
        engine = MagicMock(spec=Engine)
        
        if connection_mock is None:
            connection_mock = MagicMock()
            connection_mock.execute.return_value = MagicMock()
            connection_mock.close.return_value = None
        
        engine.connect.return_value.__enter__.return_value = connection_mock
        engine.connect.return_value.__exit__.return_value = None
        
        return engine
    
    return _mock_engine_with_connection


@pytest.fixture
def mock_connection_pool():
    """Mock connection pool for testing."""
    pool = MagicMock()
    pool.connect.return_value = MagicMock()
    pool.dispose.return_value = None
    return pool


@pytest.fixture
def mock_database_urls():
    """Mock database URLs for testing following connection architecture naming."""
    return {
        'source': 'mysql://test_source_user:test_source_pass@localhost:3306/test_opendental',
        'replication': 'mysql://test_repl_user:test_repl_pass@localhost:3305/test_opendental_replication',
        'analytics': 'postgresql://test_analytics_user:test_analytics_pass@localhost:5432/test_opendental_analytics'
    }


@pytest.fixture
def mock_connection_config():
    """Mock connection configuration for testing (legacy compatibility).
    
    This fixture provides backward compatibility while supporting the new architecture:
    - Maintains compatibility with existing tests
    - Uses environment-specific naming where possible
    - Can be used with provider pattern when needed
    """
    return {
        'source': {
            'host': 'localhost',
            'port': 3306,
            'database': 'test_opendental',
            'username': 'test_user',
            'password': 'test_pass',
            'pool_size': 5,
            'max_overflow': 10,
            'connect_timeout': 30
        },
        'replication': {
            'host': 'localhost',
            'port': 3306,
            'database': 'test_opendental_replication',
            'username': 'test_user',
            'password': 'test_pass',
            'pool_size': 3,
            'max_overflow': 5,
            'connect_timeout': 30
        },
        'analytics': {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_opendental_analytics',
            'username': 'test_user',
            'password': 'test_pass',
            'pool_size': 4,
            'max_overflow': 8,
            'connect_timeout': 30
        }
    }


@pytest.fixture
def mock_connection_error():
    """Mock connection error for testing error handling."""
    class MockConnectionError(Exception):
        def __init__(self, message="Connection failed", code=None):
            self.message = message
            self.code = code
            super().__init__(self.message)
    
    return MockConnectionError


@pytest.fixture
def mock_connection_timeout():
    """Mock connection timeout for testing timeout handling."""
    class MockConnectionTimeout(Exception):
        def __init__(self, message="Connection timeout"):
            self.message = message
            super().__init__(self.message)
    
    return MockConnectionTimeout


@pytest.fixture
def connection_factory_test_cases():
    """Test cases for ConnectionFactory methods with Settings injection.
    
    This fixture provides test cases that demonstrate the unified interface pattern:
    - Uses Settings injection for all connection methods
    - Follows the unified interface pattern (same methods for all environments)
    - Supports environment-agnostic operation through Settings injection
    - Uses enums for type safety as specified in the connection architecture
    """
    return [
        # Test connection methods with Settings injection
        {
            'method': 'get_source_connection',
            'settings': 'test_settings',  # Settings with provider injection
            'db_type': DatabaseType.SOURCE,
            'schema': None,
            'expected_engine_type': 'mysql'
        },
        {
            'method': 'get_replication_connection',
            'settings': 'test_settings',  # Settings with provider injection
            'db_type': DatabaseType.REPLICATION,
            'schema': None,
            'expected_engine_type': 'mysql'
        },
        {
            'method': 'get_analytics_connection',
            'settings': 'test_settings',  # Settings with provider injection
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'expected_engine_type': 'postgresql'
        },
        # Schema-specific test analytics connections with Settings injection
        {
            'method': 'get_analytics_raw_connection',
            'settings': 'test_settings',  # Settings with provider injection
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_analytics_staging_connection',
            'settings': 'test_settings',  # Settings with provider injection
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.STAGING,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_analytics_intermediate_connection',
            'settings': 'test_settings',  # Settings with provider injection
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.INTERMEDIATE,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_analytics_marts_connection',
            'settings': 'test_settings',  # Settings with provider injection
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.MARTS,
            'expected_engine_type': 'postgresql'
        },
        # Production connection methods with Settings injection
        {
            'method': 'get_source_connection',
            'settings': 'production_settings',  # Settings with provider injection
            'db_type': DatabaseType.SOURCE,
            'schema': None,
            'expected_engine_type': 'mysql'
        },
        {
            'method': 'get_replication_connection',
            'settings': 'production_settings',  # Settings with provider injection
            'db_type': DatabaseType.REPLICATION,
            'schema': None,
            'expected_engine_type': 'mysql'
        },
        {
            'method': 'get_analytics_connection',
            'settings': 'production_settings',  # Settings with provider injection
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'expected_engine_type': 'postgresql'
        }
    ]


@pytest.fixture
def mock_connection_manager():
    """Mock ConnectionManager for testing.
    
    This fixture provides a mock ConnectionManager that supports the connection architecture:
    - Supports context manager pattern for resource management
    - Provides retry logic for robust connection handling
    - Supports Settings injection for configuration
    """
    manager = MagicMock()
    
    # Mock context manager methods
    manager.__enter__ = MagicMock(return_value=manager)
    manager.__exit__ = MagicMock(return_value=None)
    
    # Mock connection methods
    manager.get_connection = MagicMock(return_value=MagicMock())
    manager.close_connection = MagicMock()
    manager.execute_with_retry = MagicMock(return_value=MagicMock())
    
    return manager


@pytest.fixture
def connection_string_test_cases():
    """Test cases for connection string building with Settings injection.
    
    This fixture provides test cases that demonstrate connection string building
    with Settings injection as specified in the connection architecture.
    """
    return [
        # MySQL connection strings with Settings injection
        {
            'db_type': DatabaseType.SOURCE,
            'settings': 'test_settings',  # Settings with provider injection
            'config': {
                'host': 'test-source-host',
                'port': 3306,
                'database': 'test_opendental',
                'user': 'test_source_user',
                'password': 'test_source_pass',
                'connect_timeout': 10,
                'read_timeout': 30,
                'write_timeout': 30,
                'charset': 'utf8mb4'
            },
            'expected_prefix': 'mysql+pymysql://',
            'expected_params': ['connect_timeout=10', 'read_timeout=30', 'write_timeout=30', 'charset=utf8mb4']
        },
        # PostgreSQL connection strings with Settings injection
        {
            'db_type': DatabaseType.ANALYTICS,
            'settings': 'test_settings',  # Settings with provider injection
            'schema': PostgresSchema.RAW,
            'config': {
                'host': 'test-analytics-host',
                'port': 5432,
                'database': 'test_opendental_analytics',
                'user': 'test_analytics_user',
                'password': 'test_analytics_pass',
                'schema': 'raw',
                'connect_timeout': 10,
                'application_name': 'etl_pipeline'
            },
            'expected_prefix': 'postgresql+psycopg2://',
            'expected_params': ['connect_timeout=10', 'application_name=etl_pipeline', 'options=-csearch_path%3Draw']
        }
    ]


@pytest.fixture
def pool_config_test_cases():
    """Test cases for connection pool configuration with Settings injection.
    
    This fixture provides test cases that demonstrate connection pool configuration
    with Settings injection as specified in the connection architecture.
    """
    return [
        {
            'name': 'default_pool',
            'settings': 'test_settings',  # Settings with provider injection
            'pool_size': 5,
            'max_overflow': 10,
            'pool_timeout': 30,
            'pool_recycle': 1800,
            'expected_config': {
                'pool_size': 5,
                'max_overflow': 10,
                'pool_timeout': 30,
                'pool_recycle': 1800
            }
        },
        {
            'name': 'custom_pool',
            'settings': 'production_settings',  # Settings with provider injection
            'pool_size': 10,
            'max_overflow': 20,
            'pool_timeout': 60,
            'pool_recycle': 3600,
            'expected_config': {
                'pool_size': 10,
                'max_overflow': 20,
                'pool_timeout': 60,
                'pool_recycle': 3600
            }
        }
    ] 