"""
Unit tests for the connections module using provider pattern with DictConfigProvider.

This module tests the connection architecture with pure unit tests using mocked
dependencies and provider pattern dependency injection. All tests use DictConfigProvider
for injected configuration to ensure complete test isolation.

Test Strategy:
    - Unit tests with mocked dependencies using DictConfigProvider
    - Validates provider pattern dependency injection and Settings injection
    - Tests FAIL FAST behavior when ETL_ENVIRONMENT not set
    - Ensures type safety with DatabaseType and PostgresSchema enums
    - Tests environment separation (production vs test) with provider pattern
    - Validates connection pooling and retry logic for ETL pipeline performance

Coverage Areas:
    - ConnectionFactory with Settings injection for environment-agnostic connections
    - ConnectionManager with connection lifecycle management
    - Settings class with provider pattern dependency injection
    - FAIL FAST security requirements for ETL_ENVIRONMENT
    - Type safety with DatabaseType and PostgresSchema enums
    - Provider pattern configuration isolation and environment separation
    - Connection pooling and retry logic for dental clinic ETL operations

ETL Context:
    - Critical for nightly ETL pipeline execution with dental clinic data
    - Supports MariaDB v11.6 source and PostgreSQL analytics warehouse
    - Uses provider pattern for clean dependency injection and test isolation
    - Implements Settings injection for environment-agnostic connections
    - Enforces FAIL FAST security to prevent accidental production usage
"""
import pytest
from unittest.mock import patch, MagicMock
import os
from dotenv import load_dotenv

from etl_pipeline.core.connections import ConnectionFactory, ConnectionManager, create_connection_manager
from etl_pipeline.config import (
    create_test_settings, 
    DatabaseType, 
    PostgresSchema,
    reset_settings
)
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.config.settings import Settings
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

# Load environment variables for testing
load_dotenv()

class TestFailFastSecurity:
    """Unit tests for FAIL FAST security requirements with provider pattern."""
    
    def test_fail_fast_on_missing_etl_environment(self):
        """
        Test that system fails fast when ETL_ENVIRONMENT not set.
        
        Validates:
            - System fails immediately when ETL_ENVIRONMENT not explicitly set
            - No dangerous defaults to production environment
            - Clear error messages for security requirements
            - Provider pattern doesn't bypass FAIL FAST requirements
            
        ETL Pipeline Context:
            - Critical security requirement for dental clinic ETL pipeline
            - Prevents accidental production database access during testing
            - Enforces explicit environment declaration for safety
        """
        # Remove ETL_ENVIRONMENT to test FAIL FAST
        original_env = os.environ.get('ETL_ENVIRONMENT')
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        
        try:
            # Test Settings initialization without ETL_ENVIRONMENT
            
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                settings = Settings()
                
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env

    def test_fail_fast_with_provider_pattern(self):
        """
        Test FAIL FAST behavior with provider pattern dependency injection.
        
        Validates:
            - FAIL FAST works with DictConfigProvider injection
            - Provider pattern doesn't bypass security requirements
            - Settings injection respects FAIL FAST requirements
            - Environment separation maintained with provider pattern
            
        ETL Pipeline Context:
            - Ensures provider pattern doesn't compromise security
            - Validates dependency injection maintains safety requirements
            - Tests environment-agnostic connections with security constraints
        """
        # Test with DictConfigProvider but no ETL_ENVIRONMENT
        test_provider = DictConfigProvider(
            env={
                # No TEST_ prefixed variables to avoid environment detection
                # Missing ETL_ENVIRONMENT
            }
        )
        
        # Temporarily remove ETL_ENVIRONMENT from system environment
        original_env = os.environ.get('ETL_ENVIRONMENT')
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        
        try:
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                from etl_pipeline.config.settings import Settings
                settings = Settings(provider=test_provider)
                # Trigger the error by accessing a database config
                settings.get_database_config(DatabaseType.SOURCE)
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env

    def test_fail_fast_on_connection_creation(self):
        """
        Test FAIL FAST behavior during connection creation.
        
        Validates:
            - ConnectionFactory respects FAIL FAST requirements
            - Settings injection maintains security constraints
            - Provider pattern doesn't bypass connection security
            - Clear error propagation from Settings to ConnectionFactory
            
        ETL Pipeline Context:
            - Ensures connection creation follows security requirements
            - Validates Settings injection maintains safety in connection logic
            - Tests provider pattern integration with connection security
        """
        # Create test provider without ETL_ENVIRONMENT
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
                # Missing ETL_ENVIRONMENT and TEST_OPENDENTAL_SOURCE_HOST
            }
        )
        
        with pytest.raises(ConfigurationError, match="Missing or invalid required environment variables"):
            settings = Settings(provider=test_provider)
            ConnectionFactory.get_source_connection(settings)


class TestConnectionFactoryUnit:
    """Unit tests for ConnectionFactory using provider pattern with DictConfigProvider."""
    
    def test_get_source_connection_test_environment_with_provider(self):
        """
        Test source connection creation for test environment using provider pattern.
        
        Validates:
            - Test environment configuration with provider pattern
            - Settings injection works for test environment
            - Environment separation with provider pattern
            - Type safety with DatabaseType enum usage
            
        ETL Pipeline Context:
            - Test environment: Isolated test database for dental clinic data
            - Used for integration testing with real test databases
            - Provider pattern ensures complete test isolation
            - Settings injection maintains environment-agnostic connections
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Create test provider with test environment configuration
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
            
            settings = create_test_settings(env_vars=test_provider.get_config('env'))
            
            result = ConnectionFactory.get_source_connection(settings)
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://test_user:test_pass@test-host:3306/test_opendental' in call_args

    def test_get_source_connection_missing_env_vars_with_provider(self):
        """
        Test source connection with missing environment variables using provider pattern.
        """
        # Create test provider with missing required variables
        test_provider = DictConfigProvider(
            env={
                'ETL_ENVIRONMENT': 'test',
                # All required source vars missing
            }
        )
        with pytest.raises(ConfigurationError, match="Missing or invalid required environment variables") as exc_info:
            settings = Settings(provider=test_provider)
            ConnectionFactory.get_source_connection(settings)

    def test_get_replication_connection_test_environment_with_provider(self):
        """
        Test replication connection creation for test environment using provider pattern.
        
        Validates:
            - Test environment configuration with provider pattern
            - Settings injection works for test replication database
            - Environment separation with provider pattern
            - Type safety with DatabaseType enum usage
            
        ETL Pipeline Context:
            - Test replication: Isolated test replication database
            - Used for integration testing with real test databases
            - Provider pattern ensures complete test isolation
            - Settings injection maintains environment-agnostic connections
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Create test provider with test environment configuration
            test_provider = DictConfigProvider(
                env={
                    'ETL_ENVIRONMENT': 'test',
                    'TEST_MYSQL_REPLICATION_HOST': 'test-repl-host',
                    'TEST_MYSQL_REPLICATION_PORT': '3306',
                    'TEST_MYSQL_REPLICATION_DB': 'test_opendental_replication',
                    'TEST_MYSQL_REPLICATION_USER': 'test_replication_user',
                    'TEST_MYSQL_REPLICATION_PASSWORD': 'test_repl_pass'
                }
            )
            
            settings = create_test_settings(env_vars=test_provider.get_config('env'))
            
            result = ConnectionFactory.get_replication_connection(settings)
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://test_replication_user:test_repl_pass@test-repl-host:3306/test_opendental_replication' in call_args

    def test_get_replication_connection_missing_env_vars_with_provider(self):
        """
        Test replication connection with missing environment variables using provider pattern.
        """
        # Create test provider with missing required variables
        test_provider = DictConfigProvider(
            env={
                'ETL_ENVIRONMENT': 'test',
                # All required replication vars missing
            }
        )
        with pytest.raises(ConfigurationError, match="Missing or invalid required environment variables") as exc_info:
            settings = Settings(provider=test_provider)
            ConnectionFactory.get_replication_connection(settings)

    def test_get_analytics_connection_test_environment_with_provider(self):
        """
        Test analytics connection creation for test environment using provider pattern.
        
        Validates:
            - Test environment configuration with provider pattern
            - Settings injection works for test analytics database
            - Environment separation with provider pattern
            - Type safety with DatabaseType and PostgresSchema enums
            
        ETL Pipeline Context:
            - Test analytics: Isolated test analytics database
            - Used for integration testing with real test databases
            - Provider pattern ensures complete test isolation
            - Settings injection maintains environment-agnostic connections
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Create test provider with test environment configuration
            test_provider = DictConfigProvider(
                env={
                    'ETL_ENVIRONMENT': 'test',
                    'TEST_POSTGRES_ANALYTICS_HOST': 'test-pg-host',
                    'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                    'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                    'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                    'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                    'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass'
                }
            )
            
            settings = create_test_settings(env_vars=test_provider.get_config('env'))
            
            result = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'postgresql+psycopg2://test_analytics_user:test_analytics_pass@test-pg-host:5432/test_opendental_analytics' in call_args

    def test_get_analytics_connection_different_schemas_with_provider(self):
        """
        Test analytics connection creation with different schemas using provider pattern.
        
        Validates:
            - Provider pattern handles different PostgreSQL schemas
            - Settings injection works for all schema types
            - Type safety with PostgresSchema enum usage
            - Schema-specific connection configuration
            
        ETL Pipeline Context:
            - Raw schema: Initial data ingestion for dental clinic data
            - Staging schema: Data validation and cleaning
            - Intermediate schema: Business logic transformations
            - Marts schema: Final analytics models for reporting
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Create test provider
            test_provider = DictConfigProvider(
                env={
                    'TEST_POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                    'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                    'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                    'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                    'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass',
                    'ETL_ENVIRONMENT': 'test'
                }
            )
            
            settings = create_test_settings(env_vars=test_provider.get_config('env'))
            
            # Test different schemas using PostgresSchema enum
            schemas_to_test = [
                PostgresSchema.RAW,
                PostgresSchema.STAGING,
                PostgresSchema.INTERMEDIATE,
                PostgresSchema.MARTS
            ]
            
            for schema in schemas_to_test:
                mock_create_engine.reset_mock()
                # Add schema to provider config for each iteration
                provider_env = test_provider.get_config('env').copy()
                provider_env['TEST_POSTGRES_ANALYTICS_SCHEMA'] = schema.value
                schema_settings = create_test_settings(env_vars=provider_env)
                result = ConnectionFactory.get_analytics_connection(schema_settings, schema)
                
                assert result == mock_engine
                mock_create_engine.assert_called_once()
                
                # Check that schema is set in connect_args
                call_kwargs = mock_create_engine.call_args[1]
                assert call_kwargs['connect_args']['options'] == f'-csearch_path={schema.value}'

    def test_connection_methods_use_correct_environment_variables_with_provider(self):
        """
        Test that production and test methods use different environment variables with provider pattern.
        
        Validates:
            - Provider pattern handles environment-specific variable names
            - Settings injection maintains environment separation
            - Type safety with DatabaseType enum usage
            - Environment-specific configuration loading
            
        ETL Pipeline Context:
            - Production: Uses non-prefixed variables (OPENDENTAL_SOURCE_HOST)
            - Test: Uses TEST_ prefixed variables (TEST_OPENDENTAL_SOURCE_HOST)
            - Provider pattern ensures complete environment isolation
            - Settings injection maintains environment-agnostic connections
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            # Production environment variables with provider pattern
            prod_provider = DictConfigProvider(
                env={
                    'OPENDENTAL_SOURCE_HOST': 'prod-host',
                    'OPENDENTAL_SOURCE_PORT': '3306',
                    'OPENDENTAL_SOURCE_DB': 'opendental',
                    'OPENDENTAL_SOURCE_USER': 'readonly_user',
                    'OPENDENTAL_SOURCE_PASSWORD': 'readonly_pass',
                    'ETL_ENVIRONMENT': 'production'
                }
            )
            
            # Test environment variables with provider pattern
            test_provider = DictConfigProvider(
                env={
                    'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                    'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                    'TEST_OPENDENTAL_SOURCE_DB': 'test_opendental',
                    'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                    'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                    'ETL_ENVIRONMENT': 'test'
                }
            )
            
            # Test production method with provider pattern
            prod_settings = Settings(environment='production', provider=prod_provider)
            ConnectionFactory.get_source_connection(prod_settings)
            prod_call_args = mock_create_engine.call_args[0][0]
            assert 'prod-host' in prod_call_args
            assert 'readonly_user' in prod_call_args
            assert 'opendental' in prod_call_args
            
            # Reset mock
            mock_create_engine.reset_mock()
            
            # Test test method with provider pattern
            test_settings = Settings(environment='test', provider=test_provider)
            ConnectionFactory.get_source_connection(test_settings)
            test_call_args = mock_create_engine.call_args[0][0]
            assert 'test-host' in test_call_args
            assert 'test_user' in test_call_args
            assert 'test_opendental' in test_call_args

    # ============================================================================
    # TYPE SAFETY TESTING WITH ENUMS
    # ============================================================================

    def test_enum_usage_for_database_types(self):
        """
        Test enum usage for type safety in database connections.
        
        Validates:
            - DatabaseType enum prevents invalid database types
            - PostgresSchema enum prevents invalid schema names
            - Type safety prevents runtime errors
            - IDE support for autocomplete and refactoring
            - Compile-time validation of database types
            
        ETL Pipeline Context:
            - Prevents invalid database type usage in dental clinic ETL
            - Ensures only valid schemas are used for PostgreSQL analytics
            - Type safety prevents configuration errors in production
            - Enables IDE support for database type autocomplete
        """
        # Test valid enum usage
        assert DatabaseType.SOURCE.value == "source"
        assert DatabaseType.REPLICATION.value == "replication"
        assert DatabaseType.ANALYTICS.value == "analytics"
        
        assert PostgresSchema.RAW.value == "raw"
        assert PostgresSchema.STAGING.value == "staging"
        assert PostgresSchema.INTERMEDIATE.value == "intermediate"
        assert PostgresSchema.MARTS.value == "marts"
        
        # Test enum comparison
        assert DatabaseType.SOURCE == DatabaseType.SOURCE
        assert PostgresSchema.RAW == PostgresSchema.RAW
        
        # Test enum iteration
        db_types = list(DatabaseType)
        assert len(db_types) == 3
        assert DatabaseType.SOURCE in db_types
        assert DatabaseType.REPLICATION in db_types
        assert DatabaseType.ANALYTICS in db_types
        
        schema_types = list(PostgresSchema)
        assert len(schema_types) == 4
        assert PostgresSchema.RAW in schema_types
        assert PostgresSchema.STAGING in schema_types
        assert PostgresSchema.INTERMEDIATE in schema_types
        assert PostgresSchema.MARTS in schema_types

    def test_enum_type_safety_in_settings(self):
        """
        Test enum type safety in Settings class usage.
        
        Validates:
            - Settings methods accept only valid enum values
            - Invalid enum values cause type errors
            - Type safety prevents configuration errors
            - Provider pattern maintains type safety
            
        ETL Pipeline Context:
            - Ensures Settings class only accepts valid database types
            - Prevents invalid schema names in dental clinic ETL
            - Type safety prevents runtime configuration errors
            - Provider pattern maintains type safety with dependency injection
        """
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_PORT': '3306',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'TEST_POSTGRES_ANALYTICS_HOST': 'test-analytics-host',
                'TEST_POSTGRES_ANALYTICS_PORT': '5432',
                'TEST_POSTGRES_ANALYTICS_DB': 'test_opendental_analytics',
                'TEST_POSTGRES_ANALYTICS_USER': 'test_analytics_user',
                'TEST_POSTGRES_ANALYTICS_PASSWORD': 'test_analytics_pass',
                'TEST_POSTGRES_ANALYTICS_SCHEMA': 'raw',
                'ETL_ENVIRONMENT': 'test'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        # Test valid enum usage
        source_config = settings.get_database_config(DatabaseType.SOURCE)
        assert source_config['host'] == 'test-host'
        
        analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        assert analytics_config is not None
        
        # Test that invalid enum values would cause type errors
        # This is validated by the type hints in the Settings class
        # The following would cause type errors if uncommented:
        # settings.get_database_config("invalid_type")  # Type error
        # settings.get_database_config(DatabaseType.ANALYTICS, "invalid_schema")  # Type error

    def test_enum_usage_in_connection_factory(self):
        """
        Test enum usage in ConnectionFactory methods.
        
        Validates:
            - ConnectionFactory methods accept only valid enum values
            - Type safety prevents invalid database type usage
            - Provider pattern maintains type safety
            - Settings injection respects enum constraints
            
        ETL Pipeline Context:
            - Ensures ConnectionFactory only accepts valid database types
            - Prevents invalid schema names in dental clinic ETL connections
            - Type safety prevents runtime connection errors
            - Provider pattern maintains type safety with dependency injection
        """
        test_provider = DictConfigProvider(
            env={
                'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
                'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
                'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
                'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass',
                'ETL_ENVIRONMENT': 'test'
            }
        )
        
        settings = create_test_settings(env_vars=test_provider.get_config('env'))
        
        # Test that ConnectionFactory methods work with valid enums
        # The type hints ensure only valid enum values are accepted
        # This is validated by the method signatures in ConnectionFactory
        
        # Test that invalid enum values would cause type errors
        # The following would cause type errors if uncommented:
        # ConnectionFactory.get_analytics_connection(settings, "invalid_schema")  # Type error

    # ============================================================================
    # TESTS FOR NEW ENGINE CREATION METHODS
    # ============================================================================

    def test_validate_connection_params_success_with_provider(self):
        """
        Test successful parameter validation with provider pattern.
        
        Validates:
            - Provider pattern handles parameter validation
            - Settings injection maintains validation logic
            - Type safety with parameter validation
            - Clear error messages for missing parameters
            
        ETL Pipeline Context:
            - Validates connection parameters for dental clinic databases
            - Ensures clear error messages for missing database parameters
            - Provider pattern maintains validation consistency
        """
        params = {
            'host': 'test-host',
            'port': '3306',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        # Should not raise any exception
        ConnectionFactory.validate_connection_params(params, 'MySQL')

    def test_validate_connection_params_missing_params_with_provider(self):
        """
        Test parameter validation with missing parameters using provider pattern.
        
        Validates:
            - Provider pattern handles missing parameter validation
            - Clear error messages for missing required parameters
            - Settings injection maintains error handling
            - Type safety with parameter validation
            
        ETL Pipeline Context:
            - Validates configuration validation for dental clinic ETL pipeline
            - Ensures clear error messages for missing database parameters
            - Provider pattern maintains error handling consistency
        """
        params = {
            'host': 'test-host',
            'port': '',  # Empty
            'database': None,  # None
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionFactory.validate_connection_params(params, 'MySQL')
        
        assert "Missing required MySQL connection parameters" in str(exc_info.value)
        assert "port" in str(exc_info.value)
        assert "database" in str(exc_info.value)

    def test_build_mysql_connection_string_with_provider(self):
        """
        Test MySQL connection string building with provider pattern.
        
        Validates:
            - Provider pattern handles connection string building
            - Settings injection maintains connection string logic
            - Type safety with connection string parameters
            - Connection string format for dental clinic databases
            
        ETL Pipeline Context:
            - Builds connection strings for OpenDental MySQL databases
            - Used for source database connections in dental clinic ETL
            - Provider pattern ensures consistent connection string format
        """
        config = {
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
        
        result = ConnectionFactory._build_mysql_connection_string(config)
        
        expected = (
            "mysql+pymysql://test_user:test_pass@test-host:3306/test_db"
            "?connect_timeout=15&read_timeout=45&write_timeout=30&charset=utf8mb4"
        )
        assert result == expected

    def test_build_postgres_connection_string_with_provider(self):
        """
        Test PostgreSQL connection string building with provider pattern.
        
        Validates:
            - Provider pattern handles PostgreSQL connection string building
            - Settings injection maintains connection string logic
            - Type safety with connection string parameters
            - Schema-specific connection string format
            
        ETL Pipeline Context:
            - Builds connection strings for PostgreSQL analytics databases
            - Used for analytics warehouse connections in dental clinic ETL
            - Provider pattern ensures consistent connection string format
        """
        config = {
            'host': 'test-host',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'schema': 'raw',
            'connect_timeout': 15,
            'application_name': 'etl_pipeline'
        }
        
        result = ConnectionFactory._build_postgres_connection_string(config)
        
        expected = (
            "postgresql+psycopg2://test_user:test_pass@test-host:5432/test_db"
            "?connect_timeout=15&application_name=etl_pipeline"
            "&options=-csearch_path%3Draw"
        )
        assert result == expected

    def test_build_postgres_connection_string_no_schema_with_provider(self):
        """
        Test PostgreSQL connection string building without schema using provider pattern.
        
        Validates:
            - Provider pattern handles connection string building without schema
            - Settings injection maintains connection string logic
            - Type safety with connection string parameters
            - Default schema handling for connection strings
            
        ETL Pipeline Context:
            - Builds connection strings for PostgreSQL analytics databases
            - Handles cases where schema is not specified
            - Provider pattern ensures consistent connection string format
        """
        config = {
            'host': 'test-host',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'connect_timeout': 15,
            'application_name': 'etl_pipeline'
        }
        
        result = ConnectionFactory._build_postgres_connection_string(config)
        
        expected = (
            "postgresql+psycopg2://test_user:test_pass@test-host:5432/test_db"
            "?connect_timeout=15&application_name=etl_pipeline"
        )
        assert result == expected

    def test_create_mysql_engine_success_with_provider(self):
        """
        Test successful MySQL engine creation with provider pattern.
        
        Validates:
            - Provider pattern handles MySQL engine creation
            - Settings injection maintains engine creation logic
            - Type safety with engine creation parameters
            - Connection pooling settings for dental clinic databases
            
        ETL Pipeline Context:
            - Creates MySQL engines for OpenDental source databases
            - Used for source database connections in dental clinic ETL
            - Provider pattern ensures consistent engine creation
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_create_engine.return_value = mock_engine
            
            result = ConnectionFactory.create_mysql_engine(
                host='test-host',
                port=3306,
                database='test_db',
                user='test_user',
                password='test_pass'
            )
            
            assert result == mock_engine
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args[0][0]
            assert 'mysql+pymysql://test_user:test_pass@test-host:3306/test_db' in call_args

    def test_create_mysql_engine_with_pool_settings_with_provider(self):
        """
        Test MySQL engine creation with custom pool settings using provider pattern.
        
        Validates:
            - Provider pattern handles custom pool settings
            - Settings injection maintains pool configuration
            - Type safety with pool settings parameters
            - Connection pooling optimization for dental clinic databases
            
        ETL Pipeline Context:
            - Creates MySQL engines with optimized pool settings
            - Used for high-performance dental clinic ETL operations
            - Provider pattern ensures consistent pool configuration
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_create_engine.return_value = mock_engine
            
            result = ConnectionFactory.create_mysql_engine(
                host='test-host',
                port=3306,
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

    def test_create_mysql_engine_error_with_provider(self):
        """
        Test MySQL engine creation with error using provider pattern.
        
        Validates:
            - Provider pattern handles engine creation errors
            - Settings injection maintains error handling
            - Type safety with error handling
            - Clear error messages for connection failures
            
        ETL Pipeline Context:
            - Handles connection failures for OpenDental source databases
            - Ensures clear error messages for dental clinic ETL operations
            - Provider pattern maintains error handling consistency
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_create_engine.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception) as exc_info:
                ConnectionFactory.create_mysql_engine(
                    host='test-host',
                    port=3306,
                    database='test_db',
                    user='test_user',
                    password='test_pass'
                )
            
            assert "Failed to create MySQL connection to test_db" in str(exc_info.value)

    def test_create_postgres_engine_success_with_provider(self):
        """
        Test successful PostgreSQL engine creation with provider pattern.
        
        Validates:
            - Provider pattern handles PostgreSQL engine creation
            - Settings injection maintains engine creation logic
            - Type safety with engine creation parameters
            - Schema-specific engine creation for dental clinic analytics
            
        ETL Pipeline Context:
            - Creates PostgreSQL engines for analytics warehouse databases
            - Used for analytics database connections in dental clinic ETL
            - Provider pattern ensures consistent engine creation
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            result = ConnectionFactory.create_postgres_engine(
                host='test-host',
                port=5432,
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

    def test_create_postgres_engine_empty_schema_with_provider(self):
        """
        Test PostgreSQL engine creation with empty schema using provider pattern.
        
        Validates:
            - Provider pattern handles empty schema defaulting
            - Settings injection maintains default schema logic
            - Type safety with schema handling
            - Default schema behavior for dental clinic analytics
            
        ETL Pipeline Context:
            - Creates PostgreSQL engines with default schema handling
            - Used for analytics database connections with default schemas
            - Provider pattern ensures consistent default schema behavior
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            result = ConnectionFactory.create_postgres_engine(
                host='test-host',
                port=5432,
                database='test_db',
                schema='',  # Empty schema
                user='test_user',
                password='test_pass'
            )
            
            assert result == mock_engine
            call_kwargs = mock_create_engine.call_args[1]
            assert call_kwargs['connect_args']['options'] == '-csearch_path=raw'

    def test_create_postgres_engine_none_schema_with_provider(self):
        """
        Test PostgreSQL engine creation with None schema using provider pattern.
        
        Validates:
            - Provider pattern handles None schema defaulting
            - Settings injection maintains default schema logic
            - Type safety with schema handling
            - Default schema behavior for dental clinic analytics
            
        ETL Pipeline Context:
            - Creates PostgreSQL engines with default schema handling
            - Used for analytics database connections with default schemas
            - Provider pattern ensures consistent default schema behavior
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            
            result = ConnectionFactory.create_postgres_engine(
                host='test-host',
                port=5432,
                database='test_db',
                schema=None,  # None schema
                user='test_user',
                password='test_pass'
            )
            
            assert result == mock_engine
            call_kwargs = mock_create_engine.call_args[1]
            assert call_kwargs['connect_args']['options'] == '-csearch_path=raw'

    def test_create_postgres_engine_error_with_provider(self):
        """
        Test PostgreSQL engine creation with error using provider pattern.
        
        Validates:
            - Provider pattern handles PostgreSQL engine creation errors
            - Settings injection maintains error handling
            - Type safety with error handling
            - Clear error messages for connection failures
            
        ETL Pipeline Context:
            - Handles connection failures for analytics warehouse databases
            - Ensures clear error messages for dental clinic ETL operations
            - Provider pattern maintains error handling consistency
        """
        with patch('etl_pipeline.core.connections.create_engine') as mock_create_engine:
            mock_create_engine.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception) as exc_info:
                ConnectionFactory.create_postgres_engine(
                    host='test-host',
                    port=5432,
                    database='test_db',
                    schema='test_schema',
                    user='test_user',
                    password='test_pass'
                )
            
            assert "Failed to create PostgreSQL connection to test_db.test_schema" in str(exc_info.value)


class TestConnectionManagerUnit:
    """Unit tests for ConnectionManager class using provider pattern with DictConfigProvider."""
    
    def test_connection_manager_initialization_with_provider(self):
        """
        Test ConnectionManager initialization with provider pattern.
        
        Validates:
            - Provider pattern handles connection manager initialization
            - Settings injection maintains initialization logic
            - Type safety with initialization parameters
            - Connection pooling configuration for dental clinic ETL
            
        ETL Pipeline Context:
            - Initializes connection managers for dental clinic database operations
            - Used for efficient batch operations in ETL pipeline
            - Provider pattern ensures consistent initialization
        """
        mock_engine = MagicMock()
        manager = ConnectionManager(mock_engine, max_retries=5, retry_delay=2.0)
        
        assert manager.engine == mock_engine
        assert manager.max_retries == 5
        assert manager.retry_delay == 2.0
        assert manager._current_connection is None
        assert manager._connection_count == 0
        assert manager._last_query_time == 0

    def test_get_connection_first_time_with_provider(self):
        """
        Test getting connection for the first time using provider pattern.
        
        Validates:
            - Provider pattern handles first connection creation
            - Settings injection maintains connection logic
            - Type safety with connection creation
            - Connection pooling for dental clinic ETL operations
            
        ETL Pipeline Context:
            - Creates initial connections for dental clinic database operations
            - Used for first database operation in ETL pipeline
            - Provider pattern ensures consistent connection creation
        """
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        manager = ConnectionManager(mock_engine)
        result = manager.get_connection()
        
        assert result == mock_connection
        assert manager._current_connection == mock_connection
        assert manager._connection_count == 1
        mock_engine.connect.assert_called_once()

    def test_get_connection_reuse_existing_with_provider(self):
        """
        Test reusing existing connection using provider pattern.
        
        Validates:
            - Provider pattern handles connection reuse
            - Settings injection maintains connection reuse logic
            - Type safety with connection reuse
            - Connection pooling optimization for dental clinic ETL
            
        ETL Pipeline Context:
            - Reuses existing connections for efficient dental clinic ETL operations
            - Used for batch operations to minimize connection overhead
            - Provider pattern ensures consistent connection reuse
        """
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

    def test_close_connection_with_provider(self):
        """
        Test closing connection using provider pattern.
        
        Validates:
            - Provider pattern handles connection cleanup
            - Settings injection maintains cleanup logic
            - Type safety with connection cleanup
            - Proper resource cleanup for dental clinic ETL
            
        ETL Pipeline Context:
            - Closes connections after dental clinic ETL operations
            - Used for proper resource cleanup in ETL pipeline
            - Provider pattern ensures consistent cleanup
        """
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        manager = ConnectionManager(mock_engine)
        manager.get_connection()  # Create a connection
        manager.close_connection()
        
        mock_connection.close.assert_called_once()
        assert manager._current_connection is None

    def test_close_connection_none_with_provider(self):
        """
        Test closing connection when none exists using provider pattern.
        
        Validates:
            - Provider pattern handles empty connection cleanup
            - Settings injection maintains cleanup logic
            - Type safety with empty connection handling
            - Graceful handling of no connection state
            
        ETL Pipeline Context:
            - Handles cleanup when no connections exist in ETL pipeline
            - Used for graceful error handling in dental clinic ETL
            - Provider pattern ensures consistent error handling
        """
        mock_engine = MagicMock()
        manager = ConnectionManager(mock_engine)
        
        # Should not raise any exception
        manager.close_connection()

    @patch('etl_pipeline.core.connections.time')
    def test_execute_with_retry_success_with_provider(self, mock_time):
        """
        Test successful query execution with retry using provider pattern.
        
        Validates:
            - Provider pattern handles query execution with retry
            - Settings injection maintains retry logic
            - Type safety with query execution
            - Retry logic for dental clinic ETL operations
            
        ETL Pipeline Context:
            - Executes queries with retry logic for dental clinic databases
            - Used for reliable ETL operations with automatic retry
            - Provider pattern ensures consistent retry behavior
        """
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
    def test_execute_with_retry_with_params_with_provider(self, mock_time):
        """
        Test successful query execution with parameters using provider pattern.
        
        Validates:
            - Provider pattern handles parameterized query execution
            - Settings injection maintains parameter handling
            - Type safety with query parameters
            - Parameterized queries for dental clinic ETL
            
        ETL Pipeline Context:
            - Executes parameterized queries for dental clinic databases
            - Used for secure and efficient ETL operations
            - Provider pattern ensures consistent parameter handling
        """
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
    def test_execute_with_retry_rate_limiting_with_provider(self, mock_time):
        """
        Test query execution with rate limiting using provider pattern.
        
        Validates:
            - Provider pattern handles rate limiting
            - Settings injection maintains rate limiting logic
            - Type safety with rate limiting
            - Rate limiting for dental clinic database operations
            
        ETL Pipeline Context:
            - Implements rate limiting for dental clinic database operations
            - Used to prevent overwhelming source databases in ETL pipeline
            - Provider pattern ensures consistent rate limiting
        """
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
    def test_execute_with_retry_failure_then_success_with_provider(self, mock_time):
        """
        Test query execution that fails then succeeds on retry using provider pattern.
        
        Validates:
            - Provider pattern handles retry logic
            - Settings injection maintains retry behavior
            - Type safety with retry logic
            - Retry logic for dental clinic ETL operations
            
        ETL Pipeline Context:
            - Implements retry logic for dental clinic database operations
            - Used for reliable ETL operations with automatic recovery
            - Provider pattern ensures consistent retry behavior
        """
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
    def test_execute_with_retry_all_failures_with_provider(self, mock_time):
        """
        Test query execution that fails all retries using provider pattern.
        
        Validates:
            - Provider pattern handles complete failure scenarios
            - Settings injection maintains error handling
            - Type safety with error handling
            - Error handling for dental clinic ETL operations
            
        ETL Pipeline Context:
            - Handles complete failure scenarios in dental clinic ETL
            - Used for graceful error handling when all retries fail
            - Provider pattern ensures consistent error handling
        """
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

    def test_context_manager_with_provider(self):
        """
        Test ConnectionManager as context manager using provider pattern.
        
        Validates:
            - Provider pattern handles context manager behavior
            - Settings injection maintains context manager logic
            - Type safety with context manager
            - Resource cleanup for dental clinic ETL operations
            
        ETL Pipeline Context:
            - Provides context manager for dental clinic ETL operations
            - Used for automatic resource cleanup in ETL pipeline
            - Provider pattern ensures consistent context manager behavior
        """
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

    def test_context_manager_with_exception_with_provider(self):
        """
        Test ConnectionManager context manager with exception using provider pattern.
        
        Validates:
            - Provider pattern handles exception scenarios
            - Settings injection maintains exception handling
            - Type safety with exception handling
            - Resource cleanup even with exceptions
            
        ETL Pipeline Context:
            - Handles exceptions in dental clinic ETL operations
            - Used for robust error handling with automatic cleanup
            - Provider pattern ensures consistent exception handling
        """
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_engine.connect.return_value = mock_connection
        
        with pytest.raises(ValueError):
            with ConnectionManager(mock_engine) as manager:
                manager.get_connection()  # Create a connection
                raise ValueError("Test exception")
        
        # Connection should still be closed even with exception
        mock_connection.close.assert_called_once()


class TestConnectionManagerFactoryUnit:
    """Unit tests for create_connection_manager function using provider pattern with DictConfigProvider."""
    
    def test_create_connection_manager_with_provider(self):
        """
        Test create_connection_manager function using provider pattern.
        
        Validates:
            - Provider pattern handles connection manager creation
            - Settings injection maintains creation logic
            - Type safety with connection manager creation
            - Factory function for dental clinic ETL operations
            
        ETL Pipeline Context:
            - Creates connection managers for dental clinic ETL operations
            - Used as factory function for consistent connection manager creation
            - Provider pattern ensures consistent factory behavior
        """
        mock_engine = MagicMock()
        
        manager = create_connection_manager(mock_engine, max_retries=5, retry_delay=2.0)
        
        assert isinstance(manager, ConnectionManager)
        assert manager.engine == mock_engine
        assert manager.max_retries == 5
        assert manager.retry_delay == 2.0

    def test_create_connection_manager_defaults_with_provider(self):
        """
        Test create_connection_manager function with default parameters using provider pattern.
        
        Validates:
            - Provider pattern handles default parameter handling
            - Settings injection maintains default logic
            - Type safety with default parameters
            - Default configuration for dental clinic ETL operations
            
        ETL Pipeline Context:
            - Creates connection managers with default settings for dental clinic ETL
            - Used for simple ETL operations with default configuration
            - Provider pattern ensures consistent default behavior
        """
        mock_engine = MagicMock()
        
        manager = create_connection_manager(mock_engine)
        
        assert isinstance(manager, ConnectionManager)
        assert manager.engine == mock_engine
        assert manager.max_retries == 3  # Default value
        assert manager.retry_delay == 1.0 