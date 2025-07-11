"""
Comprehensive tests for the connections module.
Full functionality testing with mocked dependencies and provider pattern.

This module follows the three-tier testing approach with provider pattern dependency injection.
Tests the unified interface with Settings injection for environment-agnostic connections.
"""

import pytest
import os
import logging
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# Import modern architecture components
from etl_pipeline.core.connections import ConnectionFactory, ConnectionManager, create_connection_manager
from etl_pipeline.config import (
    create_test_settings, 
    DatabaseType, 
    PostgresSchema,
    reset_settings
)
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

# Load environment variables for testing
load_dotenv()

logger = logging.getLogger(__name__)


class TestConnectionFactoryComprehensive:
    """Comprehensive tests for ConnectionFactory using modern architecture with provider pattern."""
    
    def test_get_source_connection_with_provider_pattern(self):
        """Test source connection creation with provider pattern dependency injection."""
        # Create test provider with injected configuration using TEST_ prefixed variables
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            result = ConnectionFactory.get_source_connection(settings)
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://test_user:test_pass@test-host:3306/test_opendental' in call_args

    def test_get_replication_connection_with_provider_pattern(self):
        """Test replication connection creation with provider pattern dependency injection."""
        # Create test provider with injected configuration using TEST_ prefixed variables
        test_provider = DictConfigProvider(
            env={
                'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            result = ConnectionFactory.get_replication_connection(settings)
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://test_repl_user:test_repl_pass@test-repl-host:3305/test_opendental_replication' in call_args

    def test_get_analytics_connection_with_provider_pattern(self):
        """Test analytics connection creation with provider pattern dependency injection."""
        # Create test provider with injected configuration using TEST_ prefixed variables
        test_provider = DictConfigProvider(
            env={
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            result = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args
            
            # Check that schema is set in connect_args
            call_kwargs = mock_create_engine.call_args[1]
            assert call_kwargs['connect_args']['options'] == '-csearch_path=raw'

    def test_schema_specific_connections_with_provider_pattern(self):
        """Test schema-specific analytics connections with provider pattern."""
        # Create test provider with injected configuration using TEST_ prefixed variables
        test_provider = DictConfigProvider(
            env={
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        # Test all schema-specific methods
        schema_methods = [
            (ConnectionFactory.get_analytics_raw_connection, PostgresSchema.RAW),
            (ConnectionFactory.get_analytics_staging_connection, PostgresSchema.STAGING),
            (ConnectionFactory.get_analytics_intermediate_connection, PostgresSchema.INTERMEDIATE),
            (ConnectionFactory.get_analytics_marts_connection, PostgresSchema.MARTS)
        ]
        
        for method, expected_schema in schema_methods:
            with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
                mock_engine = MagicMock()
                mock_create_engine.return_value = mock_engine
                
                result = method(settings)
                
                assert result == mock_engine
                mock_create_engine.assert_called_once()
                
                # Check that correct schema is set in connect_args
                call_kwargs = mock_create_engine.call_args[1]
                assert call_kwargs['connect_args']['options'] == f'-csearch_path={expected_schema.value}'

    def test_environment_separation_with_provider_pattern(self):
        """Test that production and test environments use different configurations."""
        # Production environment provider (no TEST_ prefix)
        prod_provider = DictConfigProvider(
            env={
                'ETL_ENVIRONMENT': 'production',
                'OPENDENTAL_SOURCE_HOST': 'prod-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'opendental',
                'OPENDENTAL_SOURCE_USER': 'prod_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'prod_pass'
            }
        )
        
        # Test environment provider (TEST_ prefix)
        test_provider = DictConfigProvider(
            env={
                'ETL_ENVIRONMENT': 'test',
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }
        )
        
        # Create settings for both environments with explicit environment setting
        from etl_pipeline.config.settings import Settings
        prod_settings = Settings(environment='production', provider=prod_provider)
        test_settings = Settings(environment='test', provider=test_provider)
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Test production connection
            ConnectionFactory.get_source_connection(prod_settings)
            prod_call_args = mock_create_engine.call_args[0][0]
            assert 'prod-host' in prod_call_args
            assert 'opendental' in prod_call_args
            assert 'prod_user' in prod_call_args
            
            # Reset mock
            mock_create_engine.reset_mock()
            
            # Test test connection
            ConnectionFactory.get_source_connection(test_settings)
            test_call_args = mock_create_engine.call_args[0][0]
            assert 'test-host' in test_call_args
            assert 'test_opendental' in test_call_args
            assert 'test_user' in test_call_args

    def test_connection_pool_settings_with_provider_pattern(self):
        """Test connection pool settings with provider pattern."""
        # Create test provider with pipeline configuration
        test_provider = DictConfigProvider(
            pipeline={
                'connections': {
                    'source': {
                        'pool_size': 10,
                        'max_overflow': 20,
                        'pool_timeout': 60,
                        'pool_recycle': 3600
                    }
                }
            },
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }
        )
        
        settings = create_test_settings(
            pipeline_config=test_provider.get_config('pipeline'),
            env_vars=test_provider.get_config('env')
        )
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            ConnectionFactory.get_source_connection(settings)
            
            # Check that pool settings are applied
            call_kwargs = mock_create_engine.call_args[1]
            assert call_kwargs['pool_size'] == 10
            assert call_kwargs['max_overflow'] == 20
            assert call_kwargs['pool_timeout'] == 60
            assert call_kwargs['pool_recycle'] == 3600

    def test_connection_validation_with_provider_pattern(self):
        """Test connection parameter validation with provider pattern."""
        # Test validation with missing environment variables
        test_provider = DictConfigProvider(env={})
        with pytest.raises(ConfigurationError) as exc_info:
            from etl_pipeline.config.settings import Settings
            settings = Settings(provider=test_provider)
            ConnectionFactory.get_source_connection(settings)
        assert "Missing or invalid required environment variables" in str(exc_info.value)

    def test_database_type_enums_with_provider_pattern(self):
        """Test database type enums with provider pattern."""
        # Create test provider with TEST_ prefixed variables (complete set)
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Test all database types using enums
            ConnectionFactory.get_source_connection(settings)  # Uses DatabaseType.SOURCE internally
            ConnectionFactory.get_replication_connection(settings)  # Uses DatabaseType.REPLICATION internally
            ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)  # Uses DatabaseType.ANALYTICS + PostgresSchema.RAW
            
            # Should have been called 3 times
            assert mock_create_engine.call_count == 3

    def test_postgres_schema_enums_with_provider_pattern(self):
        """Test PostgreSQL schema enums with provider pattern."""
        # Create test provider with TEST_ prefixed variables (complete set)
        test_provider = DictConfigProvider(
            env={
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        # Test all schema enums
        schemas_to_test = [
            PostgresSchema.RAW,
            PostgresSchema.STAGING,
            PostgresSchema.INTERMEDIATE,
            PostgresSchema.MARTS
        ]
        
        for schema in schemas_to_test:
            with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
                mock_engine = MagicMock()
                mock_create_engine.return_value = mock_engine
                
                ConnectionFactory.get_analytics_connection(settings, schema)
                
                # Check that correct schema is set
                call_kwargs = mock_create_engine.call_args[1]
                assert call_kwargs['connect_args']['options'] == f'-csearch_path={schema.value}'

    def test_connection_manager_with_provider_pattern(self):
        """Test ConnectionManager with provider pattern."""
        # Create test provider with TEST_ prefixed variables (complete set)
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = [1]
            mock_connection.execute.return_value = mock_result
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_engine.connect.return_value.__exit__.return_value = None
            mock_create_engine.return_value = mock_engine
            
            # Get connection using modern architecture
            source_engine = ConnectionFactory.get_source_connection(settings)
            
            # Test ConnectionManager
            manager = create_connection_manager(source_engine, max_retries=3, retry_delay=1.0)
            
            assert isinstance(manager, ConnectionManager)
            assert manager.engine == source_engine
            assert manager.max_retries == 3
            assert manager.retry_delay == 1.0

    def test_connection_manager_context_manager_with_provider_pattern(self):
        """Test ConnectionManager as context manager with provider pattern."""
        # Create test provider with TEST_ prefixed variables (complete set)
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = [1]
            mock_connection.execute.return_value = mock_result
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_engine.connect.return_value.__exit__.return_value = None
            mock_create_engine.return_value = mock_engine
            
            # Get connection using modern architecture
            source_engine = ConnectionFactory.get_source_connection(settings)
            
            # Test ConnectionManager as context manager
            with create_connection_manager(source_engine) as manager:
                assert isinstance(manager, ConnectionManager)
                assert manager.engine == source_engine
                
                # Test connection execution - just verify the method was called
                result = manager.execute_with_retry("SELECT 1")
                assert result is not None  # Just verify we got a result
                # SQLAlchemy text() creates a TextClause object, not a string
                mock_connection.execute.assert_called_once()
                # Verify the call was made (the exact object comparison is handled by SQLAlchemy)

    def test_connection_factory_error_handling_with_provider_pattern(self):
        """Test ConnectionFactory error handling with provider pattern."""
        # Create test provider with TEST_ prefixed variables (complete set)
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_create_engine.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception) as exc_info:
                ConnectionFactory.get_source_connection(settings)
            
            # Update to expect the actual error message
            assert "Connection failed" in str(exc_info.value)

    @patch.dict(os.environ, {}, clear=True)  # Clear all environment variables
    def test_connection_factory_validation_with_provider_pattern(self):
        """Test connection factory validation with provider pattern."""
        # Test validation with missing environment variables
        test_provider = DictConfigProvider(env={})
        with pytest.raises(EnvironmentError) as exc_info:
            from etl_pipeline.config.settings import Settings
            settings = Settings(provider=test_provider)
            ConnectionFactory.get_source_connection(settings)
        assert "ETL_ENVIRONMENT environment variable is not set" in str(exc_info.value)

    def test_connection_factory_mysql_engine_creation_with_provider_pattern(self):
        """Test MySQL engine creation with provider pattern."""
        # Create test provider with TEST_ prefixed variables
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_create_engine.return_value = mock_engine
            
            # Test MySQL engine creation
            result = ConnectionFactory.create_mysql_engine(
                host='test-host',
                port=3306,
                database='test_opendental',
                user='test_user',
                password='test_pass'
            )
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://test_user:test_pass@test-host:3306/test_opendental' in call_args

    def test_connection_factory_postgres_engine_creation_with_provider_pattern(self):
        """Test PostgreSQL engine creation with provider pattern."""
        # Create test provider with TEST_ prefixed variables
        test_provider = DictConfigProvider(
            env={
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Test PostgreSQL engine creation
            result = ConnectionFactory.create_postgres_engine(
                host='test-pg-host',
                port=5432,
                database='test_opendental_analytics',
                schema='raw',
                user='test_analytics_user',
                password='test_analytics_pass'
            )
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args
            
            # Check that schema is set in connect_args
            call_kwargs = mock_create_engine.call_args[1]
            assert call_kwargs['connect_args']['options'] == '-csearch_path=raw'

    def test_connection_factory_connection_string_building_with_provider_pattern(self):
        """Test connection string building with provider pattern."""
        # Test MySQL connection string building
        mysql_config = {
            'host': 'test-host',
            'port': 3306,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'connect_timeout': 15,
            'read_timeout': 45,
            'write_timeout': 30,
            'charset': 'utf8mb4'
        }
        
        mysql_conn_str = ConnectionFactory._build_mysql_connection_string(mysql_config)
        expected_mysql = (
            "mysql+pymysql://test_user:test_pass@test-host:3306/test_db"
            "?connect_timeout=15&read_timeout=45&write_timeout=30&charset=utf8mb4"
        )
        assert mysql_conn_str == expected_mysql
        
        # Test PostgreSQL connection string building
        postgres_config = {
            'host': 'test-pg-host',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'schema': 'raw',
            'connect_timeout': 15,
            'application_name': 'etl_pipeline'
        }
        
        postgres_conn_str = ConnectionFactory._build_postgres_connection_string(postgres_config)
        expected_postgres = (
            "postgresql+psycopg2://test_user:test_pass@test-pg-host:5432/test_db"
            "?connect_timeout=15&application_name=etl_pipeline"
            "&options=-csearch_path%3Draw"
        )
        assert postgres_conn_str == expected_postgres

    def test_connection_factory_parameter_validation_with_provider_pattern(self):
        """Test parameter validation with provider pattern."""
        # Test successful validation
        valid_params = {
            'host': 'test-host',
            'port': '3306',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        # Should not raise any exception
        ConnectionFactory.validate_connection_params(valid_params, 'MySQL')
        
        # Test validation with missing parameters
        invalid_params = {
            'host': 'test-host',
            'port': '',  # Empty
            'database': None,  # None
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionFactory.validate_connection_params(invalid_params, 'MySQL')
        
        assert "Missing required MySQL connection parameters" in str(exc_info.value)
        assert "port" in str(exc_info.value)
        assert "database" in str(exc_info.value)

    def test_connection_factory_unified_interface_with_provider_pattern(self):
        """Test unified interface methods with provider pattern."""
        # Create test provider with TEST_ prefixed variables (complete set)
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Test all unified interface methods
            source_engine = ConnectionFactory.get_source_connection(settings)
            replication_engine = ConnectionFactory.get_replication_connection(settings)
            analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            
            # Test convenience methods
            raw_engine = ConnectionFactory.get_analytics_raw_connection(settings)
            staging_engine = ConnectionFactory.get_analytics_staging_connection(settings)
            intermediate_engine = ConnectionFactory.get_analytics_intermediate_connection(settings)
            marts_engine = ConnectionFactory.get_analytics_marts_connection(settings)
            
            # All should return the same mock engine
            assert source_engine == mock_engine
            assert replication_engine == mock_engine
            assert analytics_engine == mock_engine
            assert raw_engine == mock_engine
            assert staging_engine == mock_engine
            assert intermediate_engine == mock_engine
            assert marts_engine == mock_engine
            
            # Should have been called 7 times (one for each method)
            assert mock_create_engine.call_count == 7

    def test_fail_fast_on_missing_etl_environment(self):
        """Test FAIL FAST behavior when ETL_ENVIRONMENT not set."""
        with patch.dict(os.environ, {}, clear=True):
            from etl_pipeline.config.settings import Settings
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                Settings()

    def test_environment_detection_with_provider_pattern(self):
        """Test proper environment detection (production vs test) with provider pattern."""
        # Test production environment detection
        prod_provider = DictConfigProvider(
            env={
                'ETL_ENVIRONMENT': 'production',
                'OPENDENTAL_SOURCE_HOST': 'prod-host',
                'OPENDENTAL_SOURCE_PORT': '3306',
                'OPENDENTAL_SOURCE_DB': 'opendental',
                'OPENDENTAL_SOURCE_USER': 'prod_user',
                'OPENDENTAL_SOURCE_PASSWORD': 'prod_pass'
            }
        )
        
        # Test test environment detection
        test_provider = DictConfigProvider(
            env={
                'ETL_ENVIRONMENT': 'test',
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
            }
        )
        
        # Create settings for both environments with explicit environment setting
        from etl_pipeline.config.settings import Settings
        prod_settings = Settings(environment='production', provider=prod_provider)
        test_settings = Settings(environment='test', provider=test_provider)
        
        # Verify environment detection
        assert prod_settings.environment == 'production'
        assert test_settings.environment == 'test'
        
        # Test that different configurations are used
        prod_config = prod_settings.get_database_config(DatabaseType.SOURCE)
        test_config = test_settings.get_database_config(DatabaseType.SOURCE)
        
        assert prod_config['host'] == 'prod-host'
        assert test_config['host'] == 'test-host'
        assert prod_config['database'] == 'opendental'
        assert test_config['database'] == 'test_opendental'

    def test_settings_injection_with_provider_pattern(self):
        """Test Settings injection with provider pattern for environment-agnostic connections."""
        # Create test provider with TEST_ prefixed variables (complete set)
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        # Test Settings injection with ConnectionFactory
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Test unified interface with Settings injection
            source_engine = ConnectionFactory.get_source_connection(settings)
            analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            
            # Verify Settings injection works for both database types
            assert source_engine == mock_engine
            assert analytics_engine == mock_engine
            
            # Verify correct number of calls
            assert mock_create_engine.call_count == 2

    def test_connection_factory_with_settings_injection(self):
        """Test ConnectionFactory with Settings injection for environment-agnostic operation."""
        # Create test provider with TEST_ prefixed variables (complete set)
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
                'TEST_MYSQL_REPLICATION_PORT': '3305',
                'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                'TEST_MYSQL_REPLICATION_USER': 'test_repl_user',
                'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Test all ConnectionFactory methods with Settings injection
            source_engine = ConnectionFactory.get_source_connection(settings)
            replication_engine = ConnectionFactory.get_replication_connection(settings)
            analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            
            # Test schema-specific methods with Settings injection
            raw_engine = ConnectionFactory.get_analytics_raw_connection(settings)
            staging_engine = ConnectionFactory.get_analytics_staging_connection(settings)
            intermediate_engine = ConnectionFactory.get_analytics_intermediate_connection(settings)
            marts_engine = ConnectionFactory.get_analytics_marts_connection(settings)
            
            # Verify all methods work with Settings injection
            assert source_engine == mock_engine
            assert replication_engine == mock_engine
            assert analytics_engine == mock_engine
            assert raw_engine == mock_engine
            assert staging_engine == mock_engine
            assert intermediate_engine == mock_engine
            assert marts_engine == mock_engine
            
            # Verify correct number of calls (7 total)
            assert mock_create_engine.call_count == 7 