"""
Connection fixtures for ETL pipeline tests.

This module contains fixtures related to:
- Database connection mocks
- Connection factory mocks
- Engine mocks for different database types
- Connection utilities
"""

import pytest
from unittest.mock import MagicMock, Mock
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text
from typing import Dict, Any


@pytest.fixture
def database_types():
    """Database type enums for testing."""
    try:
        from etl_pipeline.config import DatabaseType
        return DatabaseType
    except ImportError:
        # Fallback mock for testing
        class MockDatabaseType:
            SOURCE = 'source'
            REPLICATION = 'replication'
            ANALYTICS = 'analytics'
        return MockDatabaseType


@pytest.fixture
def postgres_schemas():
    """PostgreSQL schema enums for testing."""
    try:
        from etl_pipeline.config import PostgresSchema
        return PostgresSchema
    except ImportError:
        # Fallback mock for testing
        class MockPostgresSchema:
            RAW = 'raw'
            STAGING = 'staging'
            INTERMEDIATE = 'intermediate'
            MARTS = 'marts'
        return MockPostgresSchema


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