"""
Connection fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Database connection mocks
- Connection factory mocks
- Engine mocks for different database types
- Connection utilities
- ConnectionFactory method mocks
"""

import pytest
from unittest.mock import MagicMock, Mock
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text
from typing import Dict, Any

# Import new configuration system components
from etl_pipeline.config import DatabaseType, PostgresSchema


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
    mock_url.database = 'test_replication'
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
    mock_url.database = 'test_analytics'
    mock_url.host = 'localhost'
    mock_url.port = 5432
    engine.url = mock_url
    return engine


@pytest.fixture
def mock_connection_factory():
    """Mock connection factory for testing."""
    factory = MagicMock()
    
    # Mock the get_connection method
    def mock_get_connection(db_type, **kwargs):
        mock_engine = MagicMock(spec=Engine)
        # Create a mock URL object
        mock_url = MagicMock()
        if db_type == 'source':
            mock_engine.name = 'mysql'
            mock_url.database = 'test_opendental'
        elif db_type == 'replication':
            mock_engine.name = 'mysql'
            mock_url.database = 'test_replication'
        elif db_type == 'analytics':
            mock_engine.name = 'postgresql'
            mock_url.database = 'test_analytics'
        mock_engine.url = mock_url
        return mock_engine
    
    factory.get_connection = mock_get_connection
    return factory


@pytest.fixture
def mock_connection_factory_methods():
    """Mock ConnectionFactory methods following new architecture."""
    factory = MagicMock()
    
    # Production connection methods
    factory.get_opendental_source_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_mysql_replication_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_postgres_analytics_connection = MagicMock(return_value=MagicMock(spec=Engine))
    
    # Schema-specific production analytics connections
    factory.get_opendental_analytics_raw_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_opendental_analytics_staging_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_opendental_analytics_intermediate_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_opendental_analytics_marts_connection = MagicMock(return_value=MagicMock(spec=Engine))
    
    # Test connection methods
    factory.get_opendental_source_test_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_mysql_replication_test_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_postgres_analytics_test_connection = MagicMock(return_value=MagicMock(spec=Engine))
    
    # Schema-specific test analytics connections
    factory.get_opendental_analytics_raw_test_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_opendental_analytics_staging_test_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_opendental_analytics_intermediate_test_connection = MagicMock(return_value=MagicMock(spec=Engine))
    factory.get_opendental_analytics_marts_test_connection = MagicMock(return_value=MagicMock(spec=Engine))
    
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
    """Mock database URLs for testing."""
    return {
        'source': 'mysql://test_user:test_pass@localhost:3306/test_opendental',
        'replication': 'mysql://test_user:test_pass@localhost:3306/test_replication',
        'analytics': 'postgresql://test_user:test_pass@localhost:5432/test_analytics'
    }


@pytest.fixture
def mock_connection_config():
    """Mock connection configuration for testing."""
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
            'database': 'test_replication',
            'username': 'test_user',
            'password': 'test_pass',
            'pool_size': 3,
            'max_overflow': 5,
            'connect_timeout': 30
        },
        'analytics': {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_analytics',
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
    """Test cases for ConnectionFactory methods."""
    return [
        # Production connection methods
        {
            'method': 'get_opendental_source_connection',
            'environment': 'production',
            'db_type': DatabaseType.SOURCE,
            'schema': None,
            'expected_engine_type': 'mysql'
        },
        {
            'method': 'get_mysql_replication_connection',
            'environment': 'production',
            'db_type': DatabaseType.REPLICATION,
            'schema': None,
            'expected_engine_type': 'mysql'
        },
        {
            'method': 'get_postgres_analytics_connection',
            'environment': 'production',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'expected_engine_type': 'postgresql'
        },
        # Schema-specific production analytics connections
        {
            'method': 'get_opendental_analytics_raw_connection',
            'environment': 'production',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_opendental_analytics_staging_connection',
            'environment': 'production',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.STAGING,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_opendental_analytics_intermediate_connection',
            'environment': 'production',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.INTERMEDIATE,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_opendental_analytics_marts_connection',
            'environment': 'production',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.MARTS,
            'expected_engine_type': 'postgresql'
        },
        # Test connection methods
        {
            'method': 'get_opendental_source_test_connection',
            'environment': 'test',
            'db_type': DatabaseType.SOURCE,
            'schema': None,
            'expected_engine_type': 'mysql'
        },
        {
            'method': 'get_mysql_replication_test_connection',
            'environment': 'test',
            'db_type': DatabaseType.REPLICATION,
            'schema': None,
            'expected_engine_type': 'mysql'
        },
        {
            'method': 'get_postgres_analytics_test_connection',
            'environment': 'test',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'expected_engine_type': 'postgresql'
        },
        # Schema-specific test analytics connections
        {
            'method': 'get_opendental_analytics_raw_test_connection',
            'environment': 'test',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.RAW,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_opendental_analytics_staging_test_connection',
            'environment': 'test',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.STAGING,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_opendental_analytics_intermediate_test_connection',
            'environment': 'test',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.INTERMEDIATE,
            'expected_engine_type': 'postgresql'
        },
        {
            'method': 'get_opendental_analytics_marts_test_connection',
            'environment': 'test',
            'db_type': DatabaseType.ANALYTICS,
            'schema': PostgresSchema.MARTS,
            'expected_engine_type': 'postgresql'
        }
    ]


@pytest.fixture
def mock_connection_manager():
    """Mock ConnectionManager for testing."""
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
    """Test cases for connection string building."""
    return [
        # MySQL connection strings
        {
            'db_type': DatabaseType.SOURCE,
            'config': {
                'host': 'localhost',
                'port': 3306,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_pass',
                'connect_timeout': 10,
                'read_timeout': 30,
                'write_timeout': 30,
                'charset': 'utf8mb4'
            },
            'expected_prefix': 'mysql+pymysql://',
            'expected_params': ['connect_timeout=10', 'read_timeout=30', 'write_timeout=30', 'charset=utf8mb4']
        },
        # PostgreSQL connection strings
        {
            'db_type': DatabaseType.ANALYTICS,
            'config': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_analytics',
                'user': 'test_user',
                'password': 'test_pass',
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
    """Test cases for connection pool configuration."""
    return [
        {
            'name': 'default_pool',
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