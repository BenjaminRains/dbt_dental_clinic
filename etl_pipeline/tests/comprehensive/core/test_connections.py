"""
Tests for the connections module.
"""
import os
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from etl_pipeline.core.connections import ConnectionFactory

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing."""
    env_vars = {
        'OPENDENTAL_SOURCE_HOST': 'source_host',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'opendental',
        'OPENDENTAL_SOURCE_USER': 'source_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'source_pass',
        'MYSQL_REPLICATION_HOST': 'repl_host',
        'MYSQL_REPLICATION_PORT': '3306',
        'MYSQL_REPLICATION_DB': 'repl_db',
        'MYSQL_REPLICATION_USER': 'repl_user',
        'MYSQL_REPLICATION_PASSWORD': 'repl_pass',
        'POSTGRES_ANALYTICS_HOST': 'analytics_host',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'analytics_db',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars

@pytest.fixture
def mock_create_engine():
    """Mock SQLAlchemy create_engine function."""
    with patch('etl_pipeline.core.connections.create_engine') as mock:
        mock_engine = MagicMock(spec=Engine)
        mock.return_value = mock_engine
        yield mock

def test_validate_connection_params():
    """Test connection parameter validation."""
    # Test valid parameters
    valid_params = {
        'host': 'localhost',
        'port': '3306',
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }
    ConnectionFactory.validate_connection_params(valid_params, 'MySQL')  # Should not raise
    
    # Test missing parameters
    invalid_params = {
        'host': '',
        'port': '3306',
        'database': None,
        'user': 'test_user',
        'password': ''
    }
    with pytest.raises(ValueError) as exc_info:
        ConnectionFactory.validate_connection_params(invalid_params, 'MySQL')
    assert "Missing required MySQL connection parameters" in str(exc_info.value)
    assert "host" in str(exc_info.value)
    assert "database" in str(exc_info.value)
    assert "password" in str(exc_info.value)

def test_create_mysql_connection(mock_create_engine):
    """Test MySQL connection creation."""
    # Test successful connection
    engine = ConnectionFactory.create_mysql_connection(
        host='localhost',
        port='3306',
        database='test_db',
        user='test_user',
        password='test_pass'
    )
    
    assert isinstance(engine, Engine)
    mock_create_engine.assert_called_once()
    call_args = mock_create_engine.call_args[1]
    assert call_args['pool_pre_ping'] is True
    assert call_args['pool_size'] == ConnectionFactory.DEFAULT_POOL_SIZE
    assert call_args['max_overflow'] == ConnectionFactory.DEFAULT_MAX_OVERFLOW
    
    # Test with custom pool settings
    mock_create_engine.reset_mock()
    engine = ConnectionFactory.create_mysql_connection(
        host='localhost',
        port='3306',
        database='test_db',
        user='test_user',
        password='test_pass',
        pool_size=10,
        max_overflow=20,
        pool_timeout=60,
        pool_recycle=3600
    )
    
    call_args = mock_create_engine.call_args[1]
    assert call_args['pool_size'] == 10
    assert call_args['max_overflow'] == 20
    assert call_args['pool_timeout'] == 60
    assert call_args['pool_recycle'] == 3600

def test_create_mysql_connection_error(mock_create_engine):
    """Test MySQL connection error handling."""
    # Configure mock to raise an exception
    mock_create_engine.side_effect = Exception("Connection failed")
    
    # Test that the error is properly caught and re-raised
    with pytest.raises(Exception) as exc_info:
        ConnectionFactory.create_mysql_connection(
            host='localhost',
            port='3306',
            database='test_db',
            user='test_user',
            password='test_pass'
        )
    
    assert "Failed to create MySQL connection to test_db" in str(exc_info.value)

def test_create_postgres_connection(mock_create_engine):
    """Test PostgreSQL connection creation."""
    # Test successful connection with schema
    engine = ConnectionFactory.create_postgres_connection(
        host='localhost',
        port='5432',
        database='test_db',
        schema='test_schema',
        user='test_user',
        password='test_pass'
    )
    
    assert isinstance(engine, Engine)
    mock_create_engine.assert_called_once()
    call_args = mock_create_engine.call_args[1]
    assert call_args['pool_pre_ping'] is True
    assert call_args['connect_args']['options'] == '-csearch_path=test_schema'
    
    # Test with default schema
    mock_create_engine.reset_mock()
    engine = ConnectionFactory.create_postgres_connection(
        host='localhost',
        port='5432',
        database='test_db',
        schema='',
        user='test_user',
        password='test_pass'
    )
    
    call_args = mock_create_engine.call_args[1]
    assert call_args['connect_args']['options'] == '-csearch_path=raw'

def test_create_postgres_connection_error(mock_create_engine):
    """Test PostgreSQL connection error handling."""
    # Configure mock to raise an exception
    mock_create_engine.side_effect = Exception("Connection failed")
    
    # Test that the error is properly caught and re-raised
    with pytest.raises(Exception) as exc_info:
        ConnectionFactory.create_postgres_connection(
            host='localhost',
            port='5432',
            database='test_db',
            schema='test_schema',
            user='test_user',
            password='test_pass'
        )
    
    assert "Failed to create PostgreSQL connection to test_db.test_schema" in str(exc_info.value)

def test_get_opendental_source_connection(mock_create_engine):
    """Test OpenDental source connection creation."""
    with patch.dict('os.environ', {
        'OPENDENTAL_SOURCE_HOST': 'localhost',
        'OPENDENTAL_SOURCE_PORT': '3306',
        'OPENDENTAL_SOURCE_DB': 'opendental',
        'OPENDENTAL_SOURCE_USER': 'test_user',
        'OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_opendental_source_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[0][0]
        assert 'mysql+pymysql://test_user:test_pass@localhost:3306/opendental' in call_args

def test_get_mysql_replication_connection(mock_create_engine):
    """Test MySQL replication connection creation."""
    with patch.dict('os.environ', {
        'MYSQL_REPLICATION_HOST': 'localhost',
        'MYSQL_REPLICATION_PORT': '3306',
        'MYSQL_REPLICATION_DB': 'replication',
        'MYSQL_REPLICATION_USER': 'test_user',
        'MYSQL_REPLICATION_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_mysql_replication_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[0][0]
        assert 'mysql+pymysql://test_user:test_pass@localhost:3306/replication' in call_args

def test_get_postgres_analytics_connection(mock_create_engine):
    """Test PostgreSQL analytics connection creation."""
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'analytics',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'test_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_postgres_analytics_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['connect_args']['options'] == '-csearch_path=raw'

def test_get_opendental_analytics_raw_connection(mock_create_engine):
    """Test OpenDental analytics raw schema connection creation."""
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }):
        engine = ConnectionFactory.get_opendental_analytics_raw_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['connect_args']['options'] == '-csearch_path=raw'

def test_get_opendental_analytics_public_connection(mock_create_engine):
    """Test OpenDental analytics public schema connection creation."""
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }):
        engine = ConnectionFactory.get_opendental_analytics_public_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['connect_args']['options'] == '-csearch_path=public'

def test_get_opendental_analytics_staging_connection(mock_create_engine):
    """Test OpenDental analytics public_staging schema connection creation."""
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }):
        engine = ConnectionFactory.get_opendental_analytics_staging_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['connect_args']['options'] == '-csearch_path=public_staging'

def test_get_opendental_analytics_intermediate_connection(mock_create_engine):
    """Test OpenDental analytics public_intermediate schema connection creation."""
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }):
        engine = ConnectionFactory.get_opendental_analytics_intermediate_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['connect_args']['options'] == '-csearch_path=public_intermediate'

def test_get_opendental_analytics_marts_connection(mock_create_engine):
    """Test OpenDental analytics public_marts schema connection creation."""
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
        'POSTGRES_ANALYTICS_USER': 'analytics_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
    }):
        engine = ConnectionFactory.get_opendental_analytics_marts_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['connect_args']['options'] == '-csearch_path=public_marts'

def test_get_mysql_replication_test_connection(mock_create_engine):
    """Test MySQL replication test connection creation."""
    with patch.dict('os.environ', {
        'MYSQL_REPLICATION_TEST_HOST': 'localhost',
        'MYSQL_REPLICATION_TEST_PORT': '3305',
        'MYSQL_REPLICATION_TEST_DB': 'opendental_replication_test',
        'MYSQL_REPLICATION_TEST_USER': 'replication_test_user',
        'MYSQL_REPLICATION_TEST_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_mysql_replication_test_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[0][0]
        assert 'mysql+pymysql://replication_test_user:test_pass@localhost:3305/opendental_replication_test' in call_args

def test_get_opendental_source_test_connection(mock_create_engine):
    """Test OpenDental source test connection creation."""
    with patch.dict('os.environ', {
        'OPENDENTAL_SOURCE_TEST_HOST': 'client-server',
        'OPENDENTAL_SOURCE_TEST_PORT': '3306',
        'OPENDENTAL_SOURCE_TEST_DB': 'opendental_test',
        'OPENDENTAL_SOURCE_TEST_USER': 'test_user',
        'OPENDENTAL_SOURCE_TEST_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_opendental_source_test_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[0][0]
        assert 'mysql+pymysql://test_user:test_pass@client-server:3306/opendental_test' in call_args

def test_get_postgres_analytics_test_connection(mock_create_engine):
    """Test PostgreSQL analytics test connection creation."""
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_TEST_HOST': 'localhost',
        'POSTGRES_ANALYTICS_TEST_PORT': '5432',
        'POSTGRES_ANALYTICS_TEST_DB': 'opendental_analytics_test',
        'POSTGRES_ANALYTICS_TEST_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_TEST_USER': 'analytics_test_user',
        'POSTGRES_ANALYTICS_TEST_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_postgres_analytics_test_connection()
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['connect_args']['options'] == '-csearch_path=raw'

def test_postgres_connection_with_custom_pool_settings(mock_create_engine):
    """Test PostgreSQL connection with custom pool settings."""
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_HOST': 'localhost',
        'POSTGRES_ANALYTICS_PORT': '5432',
        'POSTGRES_ANALYTICS_DB': 'analytics',
        'POSTGRES_ANALYTICS_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_USER': 'test_user',
        'POSTGRES_ANALYTICS_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_postgres_analytics_connection(
            pool_size=15,
            max_overflow=25,
            pool_timeout=45,
            pool_recycle=2400
        )
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['pool_size'] == 15
        assert call_args['max_overflow'] == 25
        assert call_args['pool_timeout'] == 45
        assert call_args['pool_recycle'] == 2400

def test_test_connections_with_custom_pool_settings(mock_create_engine):
    """Test test database connections with custom pool settings."""
    # Test MySQL replication test connection with custom settings
    with patch.dict('os.environ', {
        'MYSQL_REPLICATION_TEST_HOST': 'localhost',
        'MYSQL_REPLICATION_TEST_PORT': '3305',
        'MYSQL_REPLICATION_TEST_DB': 'opendental_replication_test',
        'MYSQL_REPLICATION_TEST_USER': 'replication_test_user',
        'MYSQL_REPLICATION_TEST_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_mysql_replication_test_connection(
            pool_size=8,
            max_overflow=15,
            pool_timeout=40,
            pool_recycle=2000
        )
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['pool_size'] == 8
        assert call_args['max_overflow'] == 15
        assert call_args['pool_timeout'] == 40
        assert call_args['pool_recycle'] == 2000

    # Test OpenDental source test connection with custom settings
    mock_create_engine.reset_mock()
    with patch.dict('os.environ', {
        'OPENDENTAL_SOURCE_TEST_HOST': 'client-server',
        'OPENDENTAL_SOURCE_TEST_PORT': '3306',
        'OPENDENTAL_SOURCE_TEST_DB': 'opendental_test',
        'OPENDENTAL_SOURCE_TEST_USER': 'test_user',
        'OPENDENTAL_SOURCE_TEST_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_opendental_source_test_connection(
            pool_size=3,
            max_overflow=5,
            pool_timeout=20,
            pool_recycle=1000
        )
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['pool_size'] == 3
        assert call_args['max_overflow'] == 5
        assert call_args['pool_timeout'] == 20
        assert call_args['pool_recycle'] == 1000

    # Test PostgreSQL analytics test connection with custom settings
    mock_create_engine.reset_mock()
    with patch.dict('os.environ', {
        'POSTGRES_ANALYTICS_TEST_HOST': 'localhost',
        'POSTGRES_ANALYTICS_TEST_PORT': '5432',
        'POSTGRES_ANALYTICS_TEST_DB': 'opendental_analytics_test',
        'POSTGRES_ANALYTICS_TEST_SCHEMA': 'raw',
        'POSTGRES_ANALYTICS_TEST_USER': 'analytics_test_user',
        'POSTGRES_ANALYTICS_TEST_PASSWORD': 'test_pass'
    }):
        engine = ConnectionFactory.get_postgres_analytics_test_connection(
            pool_size=4,
            max_overflow=8,
            pool_timeout=25,
            pool_recycle=1200
        )
        assert isinstance(engine, Engine)
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[1]
        assert call_args['pool_size'] == 4
        assert call_args['max_overflow'] == 8
        assert call_args['pool_timeout'] == 25
        assert call_args['pool_recycle'] == 1200

@patch('etl_pipeline.core.connections.logger')
def test_postgres_default_schema(mock_logger, mock_create_engine):
    """Test PostgreSQL connection with default schema."""
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    
    engine = ConnectionFactory.create_postgres_connection(
        host='test_host',
        port='5432',
        database='test_db',
        schema='',
        user='test_user',
        password='test_pass'
    )
    
    mock_logger.warning.assert_called_once_with("No schema specified for PostgreSQL connection to test_db, using default: raw")
    mock_create_engine.assert_called_once_with(
        'postgresql+psycopg2://test_user:test_pass@test_host:5432/test_db',
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        connect_args={'options': '-csearch_path=raw'}
    )

def test_mysql_connection_with_sql_mode_setting(mock_create_engine):
    """Test MySQL connection sets proper SQL mode."""
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine
    mock_connection = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection
    
    engine = ConnectionFactory.create_mysql_connection(
        host='localhost',
        port='3306',
        database='test_db',
        user='test_user',
        password='test_pass'
    )
    
    # Verify SQL mode is set
    mock_connection.execute.assert_called_once()
    call_args = mock_connection.execute.call_args[0][0]
    assert "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'" in str(call_args)

def test_connection_factory_default_pool_settings():
    """Test that default pool settings are properly defined."""
    assert ConnectionFactory.DEFAULT_POOL_SIZE == 5
    assert ConnectionFactory.DEFAULT_MAX_OVERFLOW == 10
    assert ConnectionFactory.DEFAULT_POOL_TIMEOUT == 30
    assert ConnectionFactory.DEFAULT_POOL_RECYCLE == 1800

def test_environment_variable_missing_handling(mock_create_engine):
    """Test handling of missing environment variables."""
    # Test with missing environment variables
    with patch.dict('os.environ', {}, clear=True):
        with pytest.raises(ValueError) as exc_info:
            ConnectionFactory.get_opendental_source_connection()
        assert "Missing required MySQL connection parameters" in str(exc_info.value)
        
        with pytest.raises(ValueError) as exc_info:
            ConnectionFactory.get_postgres_analytics_connection()
        assert "Missing required PostgreSQL connection parameters" in str(exc_info.value)
        
        with pytest.raises(ValueError) as exc_info:
            ConnectionFactory.get_mysql_replication_test_connection()
        assert "Missing required MySQL connection parameters" in str(exc_info.value)
        
        with pytest.raises(ValueError) as exc_info:
            ConnectionFactory.get_opendental_source_test_connection()
        assert "Missing required MySQL connection parameters" in str(exc_info.value)
        
        with pytest.raises(ValueError) as exc_info:
            ConnectionFactory.get_postgres_analytics_test_connection()
        assert "Missing required PostgreSQL connection parameters" in str(exc_info.value)

def test_connection_string_formatting():
    """Test that connection strings are properly formatted."""
    # Test MySQL connection string format
    with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
        ConnectionFactory.create_mysql_connection(
            host='test-host',
            port='3306',
            database='test_db',
            user='test_user',
            password='test_pass'
        )
        
        call_args = mock_create_engine.call_args[0][0]
        expected_string = 'mysql+pymysql://test_user:test_pass@test-host:3306/test_db'
        assert call_args == expected_string
    
    # Test PostgreSQL connection string format
    with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
        ConnectionFactory.create_postgres_connection(
            host='test-host',
            port='5432',
            database='test_db',
            schema='test_schema',
            user='test_user',
            password='test_pass'
        )
        
        call_args = mock_create_engine.call_args[0][0]
        expected_string = 'postgresql+psycopg2://test_user:test_pass@test-host:5432/test_db'
        assert call_args == expected_string 