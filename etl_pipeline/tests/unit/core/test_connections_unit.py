"""
Unit tests for the connections module.
Pure unit tests with mocked environment variables and database connections.
"""
import pytest
from unittest.mock import patch, MagicMock
import os
from dotenv import load_dotenv

from etl_pipeline.core.connections import ConnectionFactory

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
            assert 'postgresql://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

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
            assert 'postgresql://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

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
            assert 'postgresql://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

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
            assert 'postgresql://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

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
            assert 'postgresql://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

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