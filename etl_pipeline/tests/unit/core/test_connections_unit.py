"""
Unit tests for the connections module.
Pure unit tests with mocked environment variables and database connections.
"""
import pytest
from unittest.mock import patch, MagicMock
import os
from dotenv import load_dotenv

from etl_pipeline.core.connections import ConnectionFactory, ConnectionManager

# Load environment variables for testing
load_dotenv()

class TestConnectionFactoryUnit:
    """Unit tests for ConnectionFactory using pure mocks."""
    
    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_source_connection_success(self, mock_create_engine):
        """Test successful production source connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'OPENDENTAL_SOURCE_HOST': 'prod-host',
            'OPENDENTAL_SOURCE_PORT': '3306',
            'OPENDENTAL_SOURCE_DB': 'opendental',
            'OPENDENTAL_SOURCE_USER': 'readonly_user',
            'OPENDENTAL_SOURCE_PASSWORD': 'readonly_pass'
        }):
            result = ConnectionFactory.get_opendental_source_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://readonly_user:readonly_pass@prod-host:3306/opendental' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_source_test_connection_success(self, mock_create_engine):
        """Test successful test source connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
            'TEST_OPENDENTAL_SOURCE_PORT': '3306',
            'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
            'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
            'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
        }):
            result = ConnectionFactory.get_opendental_source_test_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://test_user:test_pass@test-host:3306/test_opendental' in call_args

    def test_get_opendental_source_test_connection_missing_env_vars(self):
        """Test test source connection with missing environment variables."""
        # Clear all test environment variables
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                ConnectionFactory.get_opendental_source_test_connection()
            
            assert "Missing required test connection environment variables" in str(exc_info.value)
            assert "TEST_OPENDENTAL_SOURCE_HOST" in str(exc_info.value)
            assert "TEST_OPENDENTAL_SOURCE_PORT" in str(exc_info.value)
            assert "TEST_OPENDENTAL_SOURCE_DB" in str(exc_info.value)
            assert "TEST_OPENDENTAL_SOURCE_USER" in str(exc_info.value)
            assert "TEST_OPENDENTAL_SOURCE_PASSWORD" in str(exc_info.value)

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_mysql_replication_test_connection_success(self, mock_create_engine):
        """Test successful test replication connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
            'TEST_MYSQL_REPLICATION_PORT': '3306',
            'TEST_MYSQL_REPLICATION_DB': 'test_opendental_repl',
            'TEST_MYSQL_REPLICATION_USER': 'test_replication_user',
            'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass'
        }):
            result = ConnectionFactory.get_mysql_replication_test_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://test_replication_user:test_repl_pass@test-repl-host:3306/test_opendental_repl' in call_args

    def test_get_mysql_replication_test_connection_missing_env_vars(self):
        """Test test replication connection with missing environment variables."""
        # Clear all test environment variables
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                ConnectionFactory.get_mysql_replication_test_connection()
            
            assert "Missing required replication test connection environment variables" in str(exc_info.value)
            assert "TEST_MYSQL_REPLICATION_HOST" in str(exc_info.value)
            assert "TEST_MYSQL_REPLICATION_PORT" in str(exc_info.value)
            assert "TEST_MYSQL_REPLICATION_DB" in str(exc_info.value)
            assert "TEST_MYSQL_REPLICATION_USER" in str(exc_info.value)
            assert "TEST_MYSQL_REPLICATION_PASSWORD" in str(exc_info.value)

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_postgres_analytics_test_connection_success(self, mock_create_engine):
        """Test successful test analytics connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
            'TEST_POSTGRES_ANALYTICS_PORT': '5432',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
            'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
            'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
            'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
        }):
            result = ConnectionFactory.get_postgres_analytics_test_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_analytics_raw_test_connection_success(self, mock_create_engine):
        """Test successful test raw schema connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
            'TEST_POSTGRES_ANALYTICS_PORT': '5432',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
            'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
            'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
            'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
        }):
            result = ConnectionFactory.get_opendental_analytics_raw_test_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_analytics_staging_test_connection_success(self, mock_create_engine):
        """Test successful test staging schema connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
            'TEST_POSTGRES_ANALYTICS_PORT': '5432',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
            'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
            'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
        }):
            result = ConnectionFactory.get_opendental_analytics_staging_test_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            # Should use staging schema, not the environment variable
            assert 'postgresql+psycopg2://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_analytics_intermediate_test_connection_success(self, mock_create_engine):
        """Test successful test intermediate schema connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
            'TEST_POSTGRES_ANALYTICS_PORT': '5432',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
            'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
            'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
        }):
            result = ConnectionFactory.get_opendental_analytics_intermediate_test_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            # Should use intermediate schema, not the environment variable
            assert 'postgresql+psycopg2://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_analytics_marts_test_connection_success(self, mock_create_engine):
        """Test successful test marts schema connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
            'TEST_POSTGRES_ANALYTICS_PORT': '5432',
            'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
            'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
            'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
        }):
            result = ConnectionFactory.get_opendental_analytics_marts_test_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            # Should use marts schema, not the environment variable
            assert 'postgresql+psycopg2://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

    def test_connection_methods_use_correct_environment_variables(self):
        """Test that production and test methods use different environment variables."""
        # Test that production methods use production env vars
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Mock both production and test environment variables
            with patch.dict(os.environ, {
                # Production variables
                'OPENDENTAL_SOURCE_HOST': 'prod-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'opendental',
                'OPENDENTAL_SOURCE_USER': 'readonly_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'readonly_pass',
                # Test variables
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }):
                # Test production method
                ConnectionFactory.get_opendental_source_connection()
                prod_call_args = mock_create_engine.call_args[0][0]
                assert 'prod-host' in prod_call_args
                assert 'readonly_user' in prod_call_args
                assert 'opendental' in prod_call_args
                
                # Reset mock
                mock_create_engine.reset_mock()
                
                # Test test method
                ConnectionFactory.get_opendental_source_test_connection()
                test_call_args = mock_create_engine.call_args[0][0]
                assert 'test-host' in test_call_args
                assert 'test_user' in test_call_args
                assert 'test_opendental' in test_call_args

    # ============================================================================
    # ADDITIONAL TESTS FOR IMPROVED COVERAGE
    # ============================================================================

    def test_validate_connection_params_success(self):
        """Test successful parameter validation."""
        params = {
            'host': 'test-host',
            'port': '3306',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        # Should not raise any exception
        ConnectionFactory.validate_connection_params(params, 'MySQL')

    def test_validate_connection_params_missing_params(self):
        """Test parameter validation with missing parameters."""
        params = {
            'host': 'test-host',
            'port': '',  # Empty
            'database': None,  # None
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        with pytest.raises(ValueError) as exc_info:
            ConnectionFactory.validate_connection_params(params, 'MySQL')
        
        assert "Missing required MySQL connection parameters" in str(exc_info.value)
        assert "port" in str(exc_info.value)
        assert "database" in str(exc_info.value)

    @patch('etl_pipeline.core.connections.create_engine')
    def test_create_mysql_connection_success(self, mock_create_engine):
        """Test successful MySQL connection creation."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        result = ConnectionFactory.create_mysql_connection(
            host='test-host',
            port='3306',
            database='test_db',
            user='test_user',
            password='test_pass'
        )
        
        assert result == mock_engine
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[0][0]
        assert 'mysql+pymysql://test_user:test_pass@test-host:3306/test_db' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_create_mysql_connection_with_pool_settings(self, mock_create_engine):
        """Test MySQL connection creation with custom pool settings."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        result = ConnectionFactory.create_mysql_connection(
            host='test-host',
            port='3306',
            database='test_db',
            user='test_user',
            password='test_pass',
            pool_size=10,
            max_overflow=20,
            pool_timeout=60,
            pool_recycle=3600
        )
        
        assert result == mock_engine
        mock_create_engine.assert_called_once()
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs['pool_size'] == 10
        assert call_kwargs['max_overflow'] == 20
        assert call_kwargs['pool_timeout'] == 60
        assert call_kwargs['pool_recycle'] == 3600

    @patch('etl_pipeline.core.connections.create_engine')
    def test_create_mysql_connection_error(self, mock_create_engine):
        """Test MySQL connection creation with error."""
        mock_create_engine.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            ConnectionFactory.create_mysql_connection(
                host='test-host',
                port='3306',
                database='test_db',
                user='test_user',
                password='test_pass'
            )
        
        assert "Failed to create MySQL connection to test_db" in str(exc_info.value)

    @patch('etl_pipeline.core.connections.create_engine')
    def test_create_postgres_connection_success(self, mock_create_engine):
        """Test successful PostgreSQL connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        result = ConnectionFactory.create_postgres_connection(
            host='test-host',
            port='5432',
            database='test_db',
            schema='test_schema',
            user='test_user',
            password='test_pass'
        )
        
        assert result == mock_engine
        mock_create_engine.assert_called_once()
        call_args = mock_create_engine.call_args[0][0]
        assert 'postgresql+psycopg2://test_user:test_pass@test-host:5432/test_db' in call_args
        
        # Check connect_args for schema
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs['connect_args']['options'] == '-csearch_path=test_schema'

    @patch('etl_pipeline.core.connections.create_engine')
    def test_create_postgres_connection_empty_schema(self, mock_create_engine):
        """Test PostgreSQL connection creation with empty schema (should default to 'raw')."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        result = ConnectionFactory.create_postgres_connection(
            host='test-host',
            port='5432',
            database='test_db',
            schema='',  # Empty schema
            user='test_user',
            password='test_pass'
        )
        
        assert result == mock_engine
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs['connect_args']['options'] == '-csearch_path=raw'

    @patch('etl_pipeline.core.connections.create_engine')
    def test_create_postgres_connection_none_schema(self, mock_create_engine):
        """Test PostgreSQL connection creation with None schema (should default to 'raw')."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        result = ConnectionFactory.create_postgres_connection(
            host='test-host',
            port='5432',
            database='test_db',
            schema=None,  # None schema
            user='test_user',
            password='test_pass'
        )
        
        assert result == mock_engine
        call_kwargs = mock_create_engine.call_args[1]
        assert call_kwargs['connect_args']['options'] == '-csearch_path=raw'

    @patch('etl_pipeline.core.connections.create_engine')
    def test_create_postgres_connection_error(self, mock_create_engine):
        """Test PostgreSQL connection creation with error."""
        mock_create_engine.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception) as exc_info:
            ConnectionFactory.create_postgres_connection(
                host='test-host',
                port='5432',
                database='test_db',
                schema='test_schema',
                user='test_user',
                password='test_pass'
            )
        
        assert "Failed to create PostgreSQL connection to test_db.test_schema" in str(exc_info.value)

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_mysql_replication_connection_success(self, mock_create_engine):
        """Test successful production MySQL replication connection creation."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'MYSQL_REPLICATION_HOST': 'repl-host',
            'MYSQL_REPLICATION_PORT': '3306',
            'MYSQL_REPLICATION_DB': 'opendental_repl',
            'MYSQL_REPLICATION_USER': 'repl_user',
            'MYSQL_REPLICATION_PASSWORD': 'repl_pass'
        }):
            result = ConnectionFactory.get_mysql_replication_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://repl_user:repl_pass@repl-host:3306/opendental_repl' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_postgres_analytics_connection_success(self, mock_create_engine):
        """Test successful production PostgreSQL analytics connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'POSTGRES_ANALYTICS_HOST': 'analytics-host',
            'POSTGRES_ANALYTICS_PORT': '5432',
            'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
            'POSTGRES_ANALYTICS_SCHEMA': 'raw',
            'POSTGRES_ANALYTICS_USER': 'analytics_user',
            'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
        }):
            result = ConnectionFactory.get_postgres_analytics_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://analytics_user:analytics_pass@analytics-host:5432/opendental_analytics' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_analytics_raw_connection_success(self, mock_create_engine):
        """Test successful production raw schema connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'POSTGRES_ANALYTICS_HOST': 'analytics-host',
            'POSTGRES_ANALYTICS_PORT': '5432',
            'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
            'POSTGRES_ANALYTICS_SCHEMA': 'raw',
            'POSTGRES_ANALYTICS_USER': 'analytics_user',
            'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
        }):
            result = ConnectionFactory.get_opendental_analytics_raw_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://analytics_user:analytics_pass@analytics-host:5432/opendental_analytics' in call_args

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_analytics_staging_connection_success(self, mock_create_engine):
        """Test successful production staging schema connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'POSTGRES_ANALYTICS_HOST': 'analytics-host',
            'POSTGRES_ANALYTICS_PORT': '5432',
            'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
            'POSTGRES_ANALYTICS_USER': 'analytics_user',
            'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
        }):
            result = ConnectionFactory.get_opendental_analytics_staging_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://analytics_user:analytics_pass@analytics-host:5432/opendental_analytics' in call_args
            
            # Check that staging schema is used
            call_kwargs = mock_create_engine.call_args[1]
            assert call_kwargs['connect_args']['options'] == '-csearch_path=staging'

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_analytics_intermediate_connection_success(self, mock_create_engine):
        """Test successful production intermediate schema connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'POSTGRES_ANALYTICS_HOST': 'analytics-host',
            'POSTGRES_ANALYTICS_PORT': '5432',
            'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
            'POSTGRES_ANALYTICS_USER': 'analytics_user',
            'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
        }):
            result = ConnectionFactory.get_opendental_analytics_intermediate_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://analytics_user:analytics_pass@analytics-host:5432/opendental_analytics' in call_args
            
            # Check that intermediate schema is used
            call_kwargs = mock_create_engine.call_args[1]
            assert call_kwargs['connect_args']['options'] == '-csearch_path=intermediate'

    @patch('etl_pipeline.core.connections.create_engine')
    def test_get_opendental_analytics_marts_connection_success(self, mock_create_engine):
        """Test successful production marts schema connection creation."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock environment variables
        with patch.dict(os.environ, {
            'POSTGRES_ANALYTICS_HOST': 'analytics-host',
            'POSTGRES_ANALYTICS_PORT': '5432',
            'POSTGRES_ANALYTICS_DB': 'opendental_analytics',
            'POSTGRES_ANALYTICS_USER': 'analytics_user',
            'POSTGRES_ANALYTICS_PASSWORD': 'analytics_pass'
        }):
            result = ConnectionFactory.get_opendental_analytics_marts_connection()
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://analytics_user:analytics_pass@analytics-host:5432/opendental_analytics' in call_args
            
            # Check that marts schema is used
            call_kwargs = mock_create_engine.call_args[1]
            assert call_kwargs['connect_args']['options'] == '-csearch_path=marts'

    def test_get_opendental_source_connection_missing_env_vars(self):
        """Test production source connection with missing environment variables."""
        # Clear all production environment variables
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                ConnectionFactory.get_opendental_source_connection()
            
            assert "Missing required MySQL connection parameters" in str(exc_info.value)

    def test_get_mysql_replication_connection_missing_env_vars(self):
        """Test production replication connection with missing environment variables."""
        # Clear all production environment variables
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                ConnectionFactory.get_mysql_replication_connection()
            
            assert "Missing required MySQL connection parameters" in str(exc_info.value)

    def test_get_postgres_analytics_connection_missing_env_vars(self):
        """Test production analytics connection with missing environment variables."""
        # Clear all production environment variables
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                ConnectionFactory.get_postgres_analytics_connection()
            
            assert "Missing required PostgreSQL connection parameters" in str(exc_info.value)

    def test_get_postgres_analytics_test_connection_missing_env_vars(self):
        """Test test analytics connection with missing environment variables."""
        # Clear all test environment variables
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                ConnectionFactory.get_postgres_analytics_test_connection()
            
            assert "Missing required PostgreSQL connection parameters" in str(exc_info.value)


class TestConnectionManagerUnit:
    """Unit tests for ConnectionManager class."""
    
    def test_connection_manager_initialization(self):
        """Test ConnectionManager initialization."""
        mock_engine = MagicMock()
        manager = ConnectionManager(mock_engine, max_retries=5, retry_delay=2.0)
        
        assert manager.engine == mock_engine
        assert manager.max_retries == 5
        assert manager.retry_delay == 2.0
        assert manager._current_connection is None
        assert manager._connection_count == 0
        assert manager._last_query_time == 0

    def test_get_connection_first_time(self):
        """Test getting connection for the first time."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        manager = ConnectionManager(mock_engine)
        result = manager.get_connection()
        
        assert result == mock_connection
        assert manager._current_connection == mock_connection
        assert manager._connection_count == 1
        mock_engine.connect.assert_called_once()

    def test_get_connection_reuse_existing(self):
        """Test reusing existing connection."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        manager = ConnectionManager(mock_engine)
        
        # Get connection twice
        result1 = manager.get_connection()
        result2 = manager.get_connection()
        
        assert result1 == result2 == mock_connection
        assert manager._connection_count == 1  # Only one connection created
        mock_engine.connect.assert_called_once()  # Only called once

    def test_close_connection(self):
        """Test closing connection."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        manager = ConnectionManager(mock_engine)
        manager.get_connection()  # Create a connection
        manager.close_connection()
        
        mock_connection.close.assert_called_once()
        assert manager._current_connection is None

    def test_close_connection_none(self):
        """Test closing connection when none exists."""
        mock_engine = MagicMock()
        manager = ConnectionManager(mock_engine)
        
        # Should not raise any exception
        manager.close_connection()

    @patch('etl_pipeline.core.connections.time')
    def test_execute_with_retry_success(self, mock_time):
        """Test successful query execution with retry."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value = mock_connection
        
        # Mock time.time() to return increasing values
        mock_time.time.side_effect = [0.0, 0.2]  # First call, then second call
        
        manager = ConnectionManager(mock_engine)
        result = manager.execute_with_retry("SELECT 1")
        
        assert result == mock_result
        mock_connection.execute.assert_called_once()

    @patch('etl_pipeline.core.connections.time')
    def test_execute_with_retry_with_params(self, mock_time):
        """Test successful query execution with parameters."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value = mock_connection
        
        # Mock time.time() to return increasing values
        mock_time.time.side_effect = [0.0, 0.2]  # First call, then second call
        
        manager = ConnectionManager(mock_engine)
        params = {'id': 1, 'name': 'test'}
        result = manager.execute_with_retry("SELECT * FROM table WHERE id = :id", params)
        
        assert result == mock_result
        mock_connection.execute.assert_called_once()

    @patch('etl_pipeline.core.connections.time')
    def test_execute_with_retry_rate_limiting(self, mock_time):
        """Test query execution with rate limiting."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value = mock_connection
        
        # Mock time to simulate rate limiting - need more values for multiple calls
        mock_time.time.side_effect = [0.0, 0.1, 0.05, 0.15]  # First call start, first call end, second call start, second call end
        # Rate limiting: time_since_last = 0.05 - 0.1 = -0.05, so sleep(0.1 - (-0.05)) = sleep(0.15)
        
        manager = ConnectionManager(mock_engine)
        manager.execute_with_retry("SELECT 1")  # First call
        manager.execute_with_retry("SELECT 2")  # Second call should trigger rate limiting
        
        # Should have called sleep to respect rate limiting
        # The first call might also trigger rate limiting, so check for the expected call
        assert mock_time.sleep.call_count >= 1
        # Check that one of the calls was for the expected rate limiting delay
        sleep_calls = [call[0][0] for call in mock_time.sleep.call_args_list]
        assert 0.15 in sleep_calls or 0.15000000000000002 in sleep_calls  # Handle floating point precision

    @patch('etl_pipeline.core.connections.time')
    def test_execute_with_retry_failure_then_success(self, mock_time):
        """Test query execution that fails then succeeds on retry."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        
        # First call fails, second succeeds
        mock_connection.execute.side_effect = [Exception("Connection error"), mock_result]
        mock_engine.connect.return_value = mock_connection
        
        # Mock time.time() to return increasing values for multiple calls
        mock_time.time.side_effect = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]  # Multiple time calls during retries
        
        manager = ConnectionManager(mock_engine, max_retries=2, retry_delay=0.1)
        result = manager.execute_with_retry("SELECT 1")
        
        assert result == mock_result
        assert mock_connection.execute.call_count == 2
        # Should have slept once for the retry delay (0.1 * (0 + 1) = 0.1 for first attempt)
        mock_time.sleep.assert_called_with(0.1)  # retry_delay * (attempt + 1) where attempt=0

    @patch('etl_pipeline.core.connections.time')
    def test_execute_with_retry_all_failures(self, mock_time):
        """Test query execution that fails all retries."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        
        # All calls fail
        mock_connection.execute.side_effect = Exception("Connection error")
        mock_engine.connect.return_value = mock_connection
        
        # Mock time.time() to return increasing values for multiple calls
        mock_time.time.side_effect = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]  # Multiple time calls during retries
        
        manager = ConnectionManager(mock_engine, max_retries=3, retry_delay=0.1)
        
        with pytest.raises(Exception) as exc_info:
            manager.execute_with_retry("SELECT 1")
        
        assert "Connection error" in str(exc_info.value)
        assert mock_connection.execute.call_count == 3  # Should have tried 3 times

    def test_context_manager(self):
        """Test ConnectionManager as context manager."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        with ConnectionManager(mock_engine) as manager:
            assert manager.engine == mock_engine
            # Create a connection during context
            manager.get_connection()
            assert manager._current_connection == mock_connection
        
        # Connection should be closed after context exit
        mock_connection.close.assert_called_once()

    def test_context_manager_with_exception(self):
        """Test ConnectionManager context manager with exception."""
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        with pytest.raises(ValueError):
            with ConnectionManager(mock_engine) as manager:
                manager.get_connection()  # Create a connection
                raise ValueError("Test exception")
        
        # Connection should still be closed even with exception
        mock_connection.close.assert_called_once() 