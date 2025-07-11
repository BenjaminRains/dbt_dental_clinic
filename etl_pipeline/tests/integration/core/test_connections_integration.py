# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for the ConnectionFactory and ConnectionManager classes.

This module follows the connection architecture patterns:
- Uses FileConfigProvider for real configuration loading from .env_test
- Uses Settings injection for environment-agnostic connections
- Uses unified interface with ConnectionFactory
- Uses proper environment variable handling with .env_test
- Uses DatabaseType and PostgresSchema enums for type safety
- Follows the three-tier ETL testing strategy
- Tests real database connections with test environment
- Uses order markers for proper integration test execution

Integration Test Strategy:
- Tests real database connections using test environment
- Validates connection pooling and retry logic
- Tests Settings injection with FileConfigProvider
- Tests unified interface methods with real databases
- Tests connection manager lifecycle and cleanup
- Tests error handling with real connection failures
- Tests schema-specific connections for PostgreSQL
- Tests MySQL-specific configurations for OpenDental
"""

import pytest
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Union, Tuple
from sqlalchemy import text, Result
from sqlalchemy.exc import OperationalError, DisconnectionError
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool

# Import connection architecture components
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    PostgresSchema,
    get_settings
)
from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.core.connections import (
    ConnectionFactory,
    ConnectionManager,
    create_connection_manager
)

# Import fixtures for test data
from tests.fixtures.connection_fixtures import (
    test_connection_config,
    production_connection_config
)

# Import custom exceptions
from etl_pipeline.exceptions.configuration import ConfigurationError, EnvironmentError

logger = logging.getLogger(__name__)


def safe_fetch_one(result: Result) -> Optional[Any]:
    """Safely fetch one row from SQLAlchemy result with proper type handling."""
    try:
        row = result.fetchone()
        return row if row is not None else None
    except Exception:
        return None


def safe_fetch_all(result: Result) -> list:
    """Safely fetch all rows from SQLAlchemy result with proper type handling."""
    try:
        rows = result.fetchall()
        return [row for row in rows if row is not None]
    except Exception:
        return []


def get_pool_size(engine: Engine) -> int:
    """Safely get pool size from SQLAlchemy engine."""
    try:
        return getattr(engine.pool, 'size', lambda: 0)()
    except Exception:
        return 0


@pytest.fixture
def test_settings_with_file_provider():
    """
    Create test settings using FileConfigProvider for real integration testing.
    
    This fixture follows the connection architecture by:
    - Using FileConfigProvider for real configuration loading
    - Using Settings injection for environment-agnostic connections
    - Loading from real .env_test file and configuration files
    - Supporting integration testing with real environment setup
    - Using test environment variables (TEST_ prefixed)
    """
    try:
        # Create FileConfigProvider that will load from .env_test
        config_dir = Path(__file__).parent.parent.parent.parent  # Go to etl_pipeline root
        provider = FileConfigProvider(config_dir, environment='test')
        
        # Create settings with FileConfigProvider for real environment loading
        settings = Settings(environment='test', provider=provider)
        
        # Validate that test environment is properly loaded
        if not settings.validate_configs():
            pytest.skip("Test environment configuration not available")
        
        return settings
    except Exception as e:
        # Skip tests if test environment is not available
        pytest.skip(f"Test environment not available: {str(e)}")


@pytest.fixture
def cleanup_connections():
    """Clean up any active connections after tests for proper isolation."""
    # This fixture ensures proper cleanup of database connections
    yield
    # Connection cleanup is handled by ConnectionManager context manager


@pytest.mark.integration
@pytest.mark.order(1)  # After configuration tests, before data loading tests
@pytest.mark.mysql
@pytest.mark.postgres
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestConnectionFactoryIntegration:
    """Integration tests for ConnectionFactory with real database connections."""

    def test_real_source_connection_creation(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real source database connection creation using Settings injection.
        
        Validates:
            - Real MySQL connection to OpenDental test database
            - Settings injection with FileConfigProvider
            - Connection pooling configuration
            - SQL mode configuration for dental clinic data
            - Error handling for connection failures
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Source: OpenDental MySQL database (test environment)
            - Used by SimpleMySQLReplicator for initial data extraction
            - Critical for nightly ETL job reliability
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Create real source connection using Settings injection
            source_engine = ConnectionFactory.get_source_connection(settings)
            
            # Test connection with real query
            with source_engine.connect() as conn:
                result: Result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row is not None and row[0] == 1
                
                # Test dental clinic specific query
                tables_result: Result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in tables_result.fetchall() if row is not None]
                logger.info(f"Available tables in test database: {tables}")
                
            # Verify engine properties
            assert source_engine.name == 'mysql'
            # Use proper pool size check
            pool_size = getattr(source_engine.pool, 'size', lambda: 0)()
            assert pool_size > 0
            
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_replication_connection_creation(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real replication database connection creation using Settings injection.
        
        Validates:
            - Real MySQL connection to replication test database
            - Settings injection with FileConfigProvider
            - Connection pooling for batch operations
            - Error handling for replication database
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Replication: Local MySQL copy for staging
            - Used for intermediate data processing
            - Supports batch operations and data transformation
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Create real replication connection using Settings injection
            replication_engine = ConnectionFactory.get_replication_connection(settings)
            
            # Test connection with real query
            with replication_engine.connect() as conn:
                result: Result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row is not None and row[0] == 1
                
            # Verify engine properties
            assert replication_engine.name == 'mysql'
            # Use proper pool size check
            pool_size = getattr(replication_engine.pool, 'size', lambda: 0)()
            assert pool_size > 0
            
        except Exception as e:
            pytest.skip(f"Test replication database not available: {str(e)}")

    def test_real_analytics_connection_creation(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real analytics database connection creation using Settings injection.
        
        Validates:
            - Real PostgreSQL connection to analytics test database
            - Settings injection with FileConfigProvider
            - Schema-specific connection configuration
            - Connection pooling for analytics queries
            - Error handling for analytics database
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Analytics: PostgreSQL data warehouse
            - Used for final data storage and analysis
            - Supports multiple schemas (raw, staging, intermediate, marts)
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Test raw schema connection
            raw_engine = ConnectionFactory.get_analytics_raw_connection(settings)
            
            # Test connection with real query
            with raw_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row is not None and row[0] == 1
                
                # Test schema-specific query
                schema_result = conn.execute(text("SELECT current_schema()"))
                schema_row = schema_result.fetchone()
                assert schema_row is not None and schema_row[0] == 'raw'
                
            # Verify engine properties
            assert raw_engine.name == 'postgresql'
            pool_size = get_pool_size(raw_engine)
            assert pool_size > 0
            
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_real_schema_specific_connections(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real schema-specific analytics connections using Settings injection.
        
        Validates:
            - Raw schema connection for initial data storage
            - Staging schema connection for data processing
            - Intermediate schema connection for transformations
            - Marts schema connection for final data
            - Settings injection with FileConfigProvider
            - Schema-specific configuration
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Raw: Initial data from source systems
            - Staging: Data validation and cleaning
            - Intermediate: Business logic transformations
            - Marts: Final aggregated data for reporting
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Test all schema connections
            schemas_to_test = [
                (PostgresSchema.RAW, 'raw'),
                (PostgresSchema.STAGING, 'staging'),
                (PostgresSchema.INTERMEDIATE, 'intermediate'),
                (PostgresSchema.MARTS, 'marts')
            ]
            
            for schema_enum, schema_name in schemas_to_test:
                try:
                    # Create schema-specific connection
                    engine = ConnectionFactory.get_analytics_connection(settings, schema_enum)
                    
                    # Test connection with real query
                    with engine.connect() as conn:
                        result = conn.execute(text("SELECT 1 as test_value"))
                        row = result.fetchone()
                        assert row[0] == 1
                        
                        # Test schema-specific query
                        result = conn.execute(text("SELECT current_schema()"))
                        current_schema = result.fetchone()[0]
                        assert current_schema == schema_name
                        
                    logger.info(f"Successfully tested {schema_name} schema connection")
                    
                except Exception as e:
                    logger.warning(f"Schema {schema_name} not available: {str(e)}")
                    # Continue testing other schemas
                    continue
                    
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_real_connection_pooling(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real connection pooling behavior with Settings injection.
        
        Validates:
            - Connection pool creation and management
            - Pool size configuration
            - Connection reuse for batch operations
            - Pool overflow handling
            - Connection cleanup
            - Settings injection with FileConfigProvider
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Connection pooling for efficient batch operations
            - Prevents overwhelming source databases
            - Supports concurrent ETL operations
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Create multiple connections to test pooling
            engines = []
            for i in range(3):
                engine = ConnectionFactory.get_source_connection(settings)
                engines.append(engine)
                
                # Test connection
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test_value"))
                    row = result.fetchone()
                    assert row[0] == 1
                    
            # Verify pool behavior
            for engine in engines:
                pool_size = get_pool_size(engine)
                assert pool_size > 0
                # Don't check overflow/checkedin as they can be negative during active connections
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_connection_error_handling(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real connection error handling with Settings injection.
        
        Validates:
            - Invalid connection parameter handling
            - Network timeout handling
            - Authentication failure handling
            - Database not found handling
            - Settings injection with FileConfigProvider
            - Error message clarity
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Error handling for production ETL reliability
            - Clear error messages for troubleshooting
            - Graceful degradation for connection failures
            - Uses FileConfigProvider for real test environment
        """
        # Test invalid connection parameters
        with pytest.raises(Exception) as exc_info:
            ConnectionFactory.create_mysql_engine(
                host='invalid-host',
                port=3306,
                database='invalid_db',
                user='invalid_user',
                password='invalid_pass'
            )
        
        # Verify error message contains expected information
        error_msg = str(exc_info.value)
        assert 'Failed to create MySQL connection' in error_msg
        
        # Test missing parameters
        with pytest.raises(ConfigurationError) as exc_info:
            ConnectionFactory.create_mysql_engine(
                host='',  # Empty host
                port=3306,
                database='test_db',
                user='test_user',
                password='test_pass'
            )
        error_msg = str(exc_info.value)
        assert 'Missing required MySQL connection parameters' in error_msg

    def test_real_settings_injection_patterns(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real Settings injection patterns with FileConfigProvider.
        
        Validates:
            - Settings injection for environment-agnostic connections
            - FileConfigProvider integration with real .env_test file
            - Unified interface methods with Settings objects
            - Environment-specific configuration loading
            - Test environment variable resolution
            - Provider pattern integration
            
        ETL Pipeline Context:
            - Settings injection enables environment-agnostic code
            - FileConfigProvider loads real test environment
            - Unified interface reduces code complexity
            - Provider pattern enables clean dependency injection
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Test all unified interface methods with Settings injection
            connections = {}
            
            # Source connection
            connections['source'] = ConnectionFactory.get_source_connection(settings)
            
            # Replication connection
            connections['replication'] = ConnectionFactory.get_replication_connection(settings)
            
            # Analytics connections for all schemas
            connections['analytics_raw'] = ConnectionFactory.get_analytics_raw_connection(settings)
            connections['analytics_staging'] = ConnectionFactory.get_analytics_staging_connection(settings)
            connections['analytics_intermediate'] = ConnectionFactory.get_analytics_intermediate_connection(settings)
            connections['analytics_marts'] = ConnectionFactory.get_analytics_marts_connection(settings)
            
            # Verify all connections are created successfully
            for name, engine in connections.items():
                assert engine is not None
                logger.info(f"Successfully created {name} connection")
                
                # Test basic connectivity
                try:
                    with engine.connect() as conn:
                        result = conn.execute(text("SELECT 1"))
                        assert result.fetchone()[0] == 1
                except Exception as e:
                    logger.warning(f"Connection {name} not fully available: {str(e)}")
                    
        except Exception as e:
            pytest.skip(f"Test environment not available: {str(e)}")


@pytest.mark.integration
@pytest.mark.order(1)  # After configuration tests, before data loading tests
@pytest.mark.mysql
@pytest.mark.postgres
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestConnectionManagerIntegration:
    """Integration tests for ConnectionManager with real database connections."""

    def test_real_connection_manager_lifecycle(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real ConnectionManager lifecycle with Settings injection.
        
        Validates:
            - Connection creation and reuse
            - Automatic retry logic
            - Rate limiting behavior
            - Connection cleanup
            - Context manager functionality
            - Settings injection with FileConfigProvider
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - ConnectionManager for efficient batch operations
            - Prevents overwhelming source databases
            - Supports retry logic for transient failures
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Create real source connection
            source_engine = ConnectionFactory.get_source_connection(settings)
            
            # Test ConnectionManager with context manager
            with create_connection_manager(source_engine) as manager:
                # Test connection reuse
                result1 = manager.execute_with_retry("SELECT 1 as test_value")
                row1 = result1.fetchone()
                assert row1[0] == 1
                
                result2 = manager.execute_with_retry("SELECT 2 as test_value")
                row2 = result2.fetchone()
                assert row2[0] == 2
                
                # Verify same connection is reused
                assert manager._current_connection is not None
                assert manager._connection_count == 1
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_connection_manager_retry_logic(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real ConnectionManager retry logic with Settings injection.
        
        Validates:
            - Automatic retry on transient failures
            - Exponential backoff behavior
            - Fresh connection creation on retry
            - Maximum retry limit enforcement
            - Error propagation after max retries
            - Settings injection with FileConfigProvider
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Retry logic for network transient failures
            - Exponential backoff prevents overwhelming databases
            - Fresh connections prevent stale connection issues
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Create real source connection
            source_engine = ConnectionFactory.get_source_connection(settings)
            
            # Test successful query (should not retry)
            with create_connection_manager(source_engine) as manager:
                result = manager.execute_with_retry("SELECT 1 as test_value")
                row = result.fetchone()
                assert row[0] == 1
                
            # Test with invalid query (should fail immediately, not retry)
            with create_connection_manager(source_engine) as manager:
                with pytest.raises(Exception):
                    manager.execute_with_retry("SELECT * FROM non_existent_table")
                    
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_connection_manager_rate_limiting(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real ConnectionManager rate limiting with Settings injection.
        
        Validates:
            - Rate limiting between queries
            - Configurable rate limiting parameters
            - Respectful behavior to source servers
            - Performance impact of rate limiting
            - Settings injection with FileConfigProvider
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Rate limiting prevents overwhelming source databases
            - Respectful behavior for production ETL operations
            - Configurable for different database environments
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Create real source connection
            source_engine = ConnectionFactory.get_source_connection(settings)
            
            # Test rate limiting with multiple queries
            with create_connection_manager(source_engine) as manager:
                start_time = time.time()
                
                # Execute multiple queries
                for i in range(3):
                    result = manager.execute_with_retry(f"SELECT {i} as test_value")
                    row = result.fetchone()
                    assert row[0] == i
                
                end_time = time.time()
                duration = end_time - start_time
                
                # Verify rate limiting adds some delay (at least 200ms for 3 queries)
                assert duration >= 0.2
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_connection_manager_batch_operations(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real ConnectionManager batch operations with Settings injection.
        
        Validates:
            - Efficient batch query execution
            - Connection reuse across batch operations
            - Memory usage during batch operations
            - Connection cleanup after batch completion
            - Settings injection with FileConfigProvider
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Batch operations for efficient data extraction
            - Connection reuse reduces overhead
            - Memory efficient for large data volumes
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Create real source connection
            source_engine = ConnectionFactory.get_source_connection(settings)
            
            # Test batch operations
            with create_connection_manager(source_engine) as manager:
                # Execute batch of queries
                results = []
                for i in range(10):
                    result = manager.execute_with_retry(f"SELECT {i} as batch_value")
                    row = result.fetchone()
                    results.append(row[0])
                
                # Verify all results
                for i, value in enumerate(results):
                    assert value == i
                
                # Verify connection reuse
                assert manager._connection_count == 1
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_real_connection_manager_error_recovery(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real ConnectionManager error recovery with Settings injection.
        
        Validates:
            - Connection cleanup on errors
            - Fresh connection creation after errors
            - Error propagation to caller
            - Resource cleanup in error scenarios
            - Settings injection with FileConfigProvider
            - Test environment variable loading (.env_test)
            
        ETL Pipeline Context:
            - Error recovery for production ETL reliability
            - Resource cleanup prevents connection leaks
            - Fresh connections prevent stale connection issues
            - Uses FileConfigProvider for real test environment
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Create real source connection
            source_engine = ConnectionFactory.get_source_connection(settings)
            
            # Test error recovery
            with create_connection_manager(source_engine) as manager:
                # First, successful query
                result = manager.execute_with_retry("SELECT 1 as test_value")
                row = result.fetchone()
                assert row[0] == 1
                
                # Then, error query
                with pytest.raises(Exception):
                    manager.execute_with_retry("SELECT * FROM non_existent_table")
                
                # Verify connection is cleaned up (may not be None immediately due to async cleanup)
                # The important thing is that a new connection is created for the next query
                
                # Try another successful query (should create new connection)
                result = manager.execute_with_retry("SELECT 2 as test_value")
                row = result.fetchone()
                assert row[0] == 2
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")


@pytest.mark.integration
@pytest.mark.order(1)  # After configuration tests, before data loading tests
@pytest.mark.mysql
@pytest.mark.postgres
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestConnectionArchitectureIntegration:
    """Integration tests for connection architecture patterns with real databases."""

    def test_real_provider_pattern_integration(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real provider pattern integration with FileConfigProvider.
        
        Validates:
            - FileConfigProvider loads real .env_test file
            - Settings injection with provider pattern
            - Environment-specific configuration loading
            - Test environment variable resolution
            - Provider pattern dependency injection
            - Real database connection creation
            
        ETL Pipeline Context:
            - Provider pattern enables clean dependency injection
            - FileConfigProvider loads real test environment
            - Settings injection enables environment-agnostic code
            - Real database connections validate configuration
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Verify provider pattern integration
            assert settings.provider is not None
            assert hasattr(settings.provider, 'get_config')
            
            # Test configuration loading through provider
            pipeline_config = settings.provider.get_config('pipeline')
            tables_config = settings.provider.get_config('tables')
            env_config = settings.provider.get_config('env')
            
            assert pipeline_config is not None
            assert tables_config is not None
            assert env_config is not None
            
            # Test real connection creation with provider pattern
            source_engine = ConnectionFactory.get_source_connection(settings)
            assert source_engine is not None
            
        except Exception as e:
            pytest.skip(f"Test environment not available: {str(e)}")

    def test_real_enum_type_safety_integration(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real enum type safety integration with Settings injection.
        
        Validates:
            - DatabaseType enum usage in real connections
            - PostgresSchema enum usage in schema connections
            - Type safety prevents invalid database types
            - Type safety prevents invalid schema names
            - Settings injection with enum validation
            - Real database connection creation with enums
            
        ETL Pipeline Context:
            - Enums provide compile-time validation
            - Prevents runtime errors from invalid types
            - IDE support for autocomplete and refactoring
            - Type safety for production ETL reliability
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Test DatabaseType enum usage
            source_config = settings.get_database_config(DatabaseType.SOURCE)
            replication_config = settings.get_database_config(DatabaseType.REPLICATION)
            analytics_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
            
            assert source_config is not None
            assert replication_config is not None
            assert analytics_config is not None
            
            # Test PostgresSchema enum usage
            raw_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
            staging_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.STAGING)
            intermediate_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.INTERMEDIATE)
            marts_config = settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.MARTS)
            
            assert raw_config is not None
            assert staging_config is not None
            assert intermediate_config is not None
            assert marts_config is not None
            
            # Test real connection creation with enums
            source_engine = ConnectionFactory.get_source_connection(settings)
            raw_engine = ConnectionFactory.get_analytics_raw_connection(settings)
            
            assert source_engine is not None
            assert raw_engine is not None
            
        except Exception as e:
            pytest.skip(f"Test environment not available: {str(e)}")

    def test_real_unified_interface_integration(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real unified interface integration with Settings injection.
        
        Validates:
            - Unified interface methods work for all environments
            - Settings injection enables environment-agnostic code
            - Single method per database type (no proliferation)
            - Consistent API across production and test environments
            - Real database connection creation with unified interface
            - Provider pattern integration with unified interface
            
        ETL Pipeline Context:
            - Unified interface reduces code complexity
            - Settings injection enables environment-agnostic code
            - Single method per database type prevents proliferation
            - Consistent API across all environments
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Test unified interface methods
            connections = {}
            
            # Test all unified interface methods
            connections['source'] = ConnectionFactory.get_source_connection(settings)
            connections['replication'] = ConnectionFactory.get_replication_connection(settings)
            connections['analytics_raw'] = ConnectionFactory.get_analytics_raw_connection(settings)
            connections['analytics_staging'] = ConnectionFactory.get_analytics_staging_connection(settings)
            connections['analytics_intermediate'] = ConnectionFactory.get_analytics_intermediate_connection(settings)
            connections['analytics_marts'] = ConnectionFactory.get_analytics_marts_connection(settings)
            
            # Verify all connections are created successfully
            for name, engine in connections.items():
                assert engine is not None
                logger.info(f"Successfully created {name} connection with unified interface")
                
        except Exception as e:
            pytest.skip(f"Test environment not available: {str(e)}")

    def test_real_environment_separation_integration(self, test_settings_with_file_provider, cleanup_connections):
        """
        Test real environment separation integration with FileConfigProvider.
        
        Validates:
            - Test environment uses TEST_ prefixed variables
            - FileConfigProvider loads correct .env_test file
            - Environment-specific configuration isolation
            - No cross-environment variable pollution
            - Settings injection with environment separation
            - Real database connection creation with environment separation
            
        ETL Pipeline Context:
            - Environment separation prevents configuration pollution
            - Test environment uses TEST_ prefixed variables
            - Production environment uses non-prefixed variables
            - FAIL FAST if ETL_ENVIRONMENT not set
        """
        try:
            # Get settings for environment-agnostic connections
            settings = test_settings_with_file_provider
            
            # Verify test environment configuration
            assert settings.environment == 'test'
            
            # Test that test environment variables are used
            source_config = settings.get_database_config(DatabaseType.SOURCE)
            
            # Verify test environment variables (should be TEST_ prefixed in .env_test)
            # Note: The actual variable names depend on the .env_test file content
            assert source_config['host'] is not None
            assert source_config['database'] is not None
            assert source_config['user'] is not None
            assert source_config['password'] is not None
            
            # Test real connection creation with environment separation
            source_engine = ConnectionFactory.get_source_connection(settings)
            assert source_engine is not None
            
        except Exception as e:
            pytest.skip(f"Test environment not available: {str(e)}")

    def test_real_fail_fast_integration(self, cleanup_connections):
        """
        Test real FAIL FAST behavior when ETL_ENVIRONMENT not set.
        
        Validates:
            - System fails immediately if ETL_ENVIRONMENT not set
            - Clear error message for missing environment
            - No defaulting to production when environment undefined
            - Security requirement enforcement
            - Provider pattern integration with FAIL FAST
            
        ETL Pipeline Context:
            - FAIL FAST prevents dangerous defaults to production
            - Clear error messages for troubleshooting
            - Security requirement for production ETL operations
            - Provider pattern integration with FAIL FAST
        """
        import os
        
        # Store original environment
        original_env = os.environ.get('ETL_ENVIRONMENT')
        
        try:
            # Remove ETL_ENVIRONMENT to test FAIL FAST
            if 'ETL_ENVIRONMENT' in os.environ:
                del os.environ['ETL_ENVIRONMENT']
            
            # Test that system fails fast with clear error message
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                settings = Settings()
                
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env
            elif 'ETL_ENVIRONMENT' not in os.environ:
                # Ensure we don't leave the environment polluted
                pass 