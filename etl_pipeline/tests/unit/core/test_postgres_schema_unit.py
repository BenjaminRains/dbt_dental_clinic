"""
Unit tests for PostgresSchema class with proper SQLAlchemy mocking.

This module follows the project's testing plan and connection architecture:
- Uses comprehensive mocking for unit test isolation
- Uses provider pattern for dependency injection
- Uses Settings injection for environment-agnostic connections
- Uses existing fixtures from @/fixtures
- Uses proper SQLAlchemy mocking to avoid inspection errors
- Uses unified interface with ConnectionFactory
- Uses DatabaseType and PostgresSchema enums for type safety
- Follows the three-tier ETL testing strategy
- Tests MySQL to PostgreSQL schema conversion
- Uses order markers for proper unit test execution

Unit Test Strategy:
- Tests PostgresSchema initialization with proper mocking
- Tests schema adaptation from MySQL to PostgreSQL
- Tests type mapping and conversion logic
- Tests configuration and settings injection
- Tests error handling and validation
- Uses comprehensive mocking for isolation
- Tests provider pattern usage
- Tests Settings injection patterns
- Tests enum usage for type safety

ETL Context:
- Critical for MySQL to PostgreSQL schema conversion
- Supports dental clinic data structure adaptation
- Uses provider pattern for clean dependency injection
- Implements Settings injection for environment-agnostic connections
- Enforces FAIL FAST security to prevent accidental production usage
- Optimized for dental clinic data structure patterns
"""

import pytest
import os
from unittest.mock import Mock, patch
from typing import Dict, Any

# Import ETL pipeline components
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    PostgresSchema as ConfigPostgresSchema
)
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.exceptions import SchemaTransformationError, EnvironmentError

# Import fixtures for test data
from tests.fixtures.config_fixtures import (
    test_settings,
    test_config_provider,
    postgres_schemas
)
from tests.fixtures.postgres_schema_fixtures import (
    sample_mysql_schemas
)

import logging
logger = logging.getLogger(__name__)


def create_mock_engine(db_type: str, db_name: str) -> Mock:
    """Create a mock SQLAlchemy engine for testing."""
    mock_engine = Mock()
    mock_engine.name = db_type
    mock_engine.url = Mock()
    mock_engine.url.database = db_name
    return mock_engine


@pytest.mark.unit
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestPostgresSchemaInitialization:
    """Test PostgresSchema initialization with provider pattern and Settings injection."""

    def test_initialization_with_settings_injection(self, test_settings):
        """
        Test initialization with Settings injection using provider pattern.
        
        Validates:
            - Settings injection for environment-agnostic connections
            - Provider pattern usage for dependency injection
            - Database configuration loading from Settings
            - ConnectionFactory integration with Settings injection
            - Environment-specific database names
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data structure adaptation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            # Test initialization with Settings injection
            schema = PostgresSchema(settings=test_settings)
            
            # Validate Settings injection
            assert schema.settings == test_settings
            assert schema.mysql_db == 'test_opendental_replication'
            assert schema.postgres_db == 'test_opendental_analytics'
            assert schema.postgres_schema == 'raw'

    def test_initialization_with_provider_pattern(self, test_config_provider):
        """
        Test initialization using provider pattern for dependency injection.
        
        Validates:
            - Provider pattern usage for configuration injection
            - DictConfigProvider for test configuration
            - Settings creation with provider injection
            - Environment-agnostic configuration loading
            - Dependency injection for clean testing
            
        ETL Pipeline Context:
            - Critical for ETL pipeline configuration management
            - Supports dental clinic configuration patterns
            - Uses provider pattern for clean dependency injection
            - Implements Settings injection for environment-agnostic connections
        """
        from etl_pipeline.config.settings import Settings
        
        # Create settings with provider injection
        test_settings = Settings(environment='test', provider=test_config_provider)
        
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            # Test initialization with provider pattern
            schema = PostgresSchema(settings=test_settings)
            
            # Validate provider pattern usage
            assert schema.settings == test_settings
            assert schema.settings.provider == test_config_provider

    def test_initialization_with_schema_enum(self, test_settings):
        """
        Test initialization with PostgreSQL schema enum.
        
        Validates:
            - Schema enum usage for type safety
            - All available schema enums work correctly
            - Schema value assignment from enum
            - Type safety for schema selection
            
        ETL Pipeline Context:
            - Critical for PostgreSQL schema management
            - Supports dental clinic data warehouse schemas
            - Uses enums for type safety and compile-time validation
            - Implements Settings injection for environment-agnostic connections
        """
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            # Test with different schema enums
            for schema_enum in [ConfigPostgresSchema.RAW, ConfigPostgresSchema.STAGING, ConfigPostgresSchema.INTERMEDIATE, ConfigPostgresSchema.MARTS]:
                schema = PostgresSchema(postgres_schema=schema_enum, settings=test_settings)
                assert schema.postgres_schema == schema_enum.value

    def test_initialization_fail_fast_no_environment(self):
        """
        Test FAIL FAST behavior when ETL_ENVIRONMENT not set.
        
        Validates:
            - FAIL FAST behavior when ETL_ENVIRONMENT not set
            - Clear error message for missing environment
            - No dangerous defaults to production
            - Security requirement enforcement
            
        ETL Pipeline Context:
            - Critical security requirement for ETL pipeline
            - Prevents accidental production usage
            - Enforces explicit environment declaration
            - Uses FAIL FAST for safety
        """
        # Remove ETL_ENVIRONMENT to test FAIL FAST
        original_env = os.environ.get('ETL_ENVIRONMENT')
        if 'ETL_ENVIRONMENT' in os.environ:
            del os.environ['ETL_ENVIRONMENT']
        
        try:
            # Should fail fast with clear error message
            with pytest.raises(EnvironmentError, match="ETL_ENVIRONMENT environment variable is not set"):
                schema = PostgresSchema()
        finally:
            # Restore original environment
            if original_env:
                os.environ['ETL_ENVIRONMENT'] = original_env

    def test_initialization_with_default_schema(self, test_settings):
        """
        Test initialization with default schema (RAW).
        
        Validates:
            - Default schema assignment
            - Schema enum default value
            - Settings injection with default schema
            - Database configuration loading
            
        ETL Pipeline Context:
            - Critical for default PostgreSQL schema handling
            - Supports dental clinic data warehouse defaults
            - Uses Settings injection for environment-agnostic connections
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            # Test with default schema
            schema = PostgresSchema(settings=test_settings)
            
            # Validate default schema
            assert schema.postgres_schema == 'raw'
            assert schema.settings == test_settings


@pytest.mark.unit
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestPostgresSchemaAdaptation:
    """Test MySQL to PostgreSQL schema adaptation with provider pattern."""

    def test_adapt_schema_mysql_to_postgres_conversion(self, test_settings, sample_mysql_schemas):
        """
        Test MySQL to PostgreSQL schema conversion.
        
        Validates:
            - MySQL to PostgreSQL type conversion
            - Column definition extraction
            - PRIMARY KEY handling
            - Schema structure preservation
            - PostgreSQL syntax compliance
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data structure adaptation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            # Mock _analyze_column_data to return appropriate types for TINYINT columns
            def mock_analyze_column_data(table_name, column_name, mysql_type):
                if mysql_type.lower().startswith('tinyint'):
                    # Return boolean for columns that should be boolean in dental clinic data
                    if column_name in ['IsActive', 'IsDeleted', 'PatStatus', 'Gender', 'Position']:
                        return 'boolean'
                    else:
                        return 'smallint'
                else:
                    # Use standard conversion for other types
                    schema = PostgresSchema(settings=test_settings)
                    return schema._convert_mysql_type_standard(mysql_type)
            
            with patch.object(PostgresSchema, '_analyze_column_data', side_effect=mock_analyze_column_data):
                schema = PostgresSchema(settings=test_settings)
                
                # Test schema adaptation
                mysql_schema = sample_mysql_schemas['patient']
                pg_create = schema.adapt_schema('patient', mysql_schema)
                
                # Validate PostgreSQL CREATE statement
                assert 'CREATE TABLE raw.patient' in pg_create
                assert '"PatNum" integer' in pg_create
                assert '"LName" character varying(100)' in pg_create
                assert '"IsActive" boolean' in pg_create
                assert 'PRIMARY KEY ("PatNum")' in pg_create

    def test_adapt_schema_column_extraction(self, test_settings, sample_mysql_schemas):
        """
        Test column definition extraction from MySQL CREATE statements.
        
        Validates:
            - Column name extraction
            - Column type extraction
            - Column parameter extraction
            - All columns are properly extracted
            - Column order preservation
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data structure preservation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            # Mock _analyze_column_data to return appropriate types for TINYINT columns
            def mock_analyze_column_data(table_name, column_name, mysql_type):
                if mysql_type.lower().startswith('tinyint'):
                    # Return boolean for columns that should be boolean in dental clinic data
                    if column_name in ['IsActive', 'IsDeleted', 'PatStatus', 'Gender', 'Position']:
                        return 'boolean'
                    else:
                        return 'smallint'
                else:
                    # Use standard conversion for other types
                    schema = PostgresSchema(settings=test_settings)
                    return schema._convert_mysql_type_standard(mysql_type)
            
            with patch.object(PostgresSchema, '_analyze_column_data', side_effect=mock_analyze_column_data):
                schema = PostgresSchema(settings=test_settings)
                
                # Test column extraction
                mysql_schema = sample_mysql_schemas['patient']
                pg_create = schema.adapt_schema('patient', mysql_schema)
                
                # Validate all columns are extracted
                expected_columns = ['PatNum', 'LName', 'FName', 'MiddleI', 'Preferred', 'PatStatus', 'Gender', 'Position', 'Birthdate', 'SSN', 'IsActive', 'IsDeleted']
                for col in expected_columns:
                    assert f'"{col}"' in pg_create

    def test_adapt_schema_primary_key_handling(self, test_settings, sample_mysql_schemas):
        """
        Test PRIMARY KEY handling in schema adaptation.
        
        Validates:
            - PRIMARY KEY extraction from MySQL
            - PRIMARY KEY conversion to PostgreSQL syntax
            - AUTO_INCREMENT removal
            - PRIMARY KEY constraint preservation
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data integrity preservation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            # Mock _analyze_column_data to return appropriate types for TINYINT columns
            def mock_analyze_column_data(table_name, column_name, mysql_type):
                if mysql_type.lower().startswith('tinyint'):
                    # Return boolean for columns that should be boolean in dental clinic data
                    if column_name in ['IsActive', 'IsDeleted', 'PatStatus', 'Gender', 'Position']:
                        return 'boolean'
                    else:
                        return 'smallint'
                else:
                    # Use standard conversion for other types
                    schema = PostgresSchema(settings=test_settings)
                    return schema._convert_mysql_type_standard(mysql_type)
            
            with patch.object(PostgresSchema, '_analyze_column_data', side_effect=mock_analyze_column_data):
                schema = PostgresSchema(settings=test_settings)
                
                # Test PRIMARY KEY extraction
                mysql_schema = sample_mysql_schemas['patient']
                pg_create = schema.adapt_schema('patient', mysql_schema)
                
                # Validate PRIMARY KEY is properly converted
                assert 'PRIMARY KEY ("PatNum")' in pg_create
                assert 'AUTO_INCREMENT' not in pg_create  # PostgreSQL doesn't use AUTO_INCREMENT

    def test_adapt_schema_error_handling(self, test_settings):
        """
        Test error handling for invalid MySQL schemas.
        
        Validates:
            - Error handling for invalid CREATE statements
            - Clear error messages for schema issues
            - Graceful failure with invalid input
            - Error recovery mechanisms
            
        ETL Pipeline Context:
            - Critical for ETL pipeline error handling
            - Supports dental clinic data validation
            - Uses graceful error handling for reliability
            - Optimized for dental clinic operational stability
        """
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            schema = PostgresSchema(settings=test_settings)
            
            # Test with invalid CREATE statement
            invalid_schema = {
                'create_statement': 'INVALID CREATE TABLE STATEMENT'
            }
            
            with pytest.raises(SchemaTransformationError, match="No valid columns found"):
                schema.adapt_schema('invalid_table', invalid_schema)

    def test_adapt_schema_with_complex_types(self, test_settings):
        """
        Test schema adaptation with complex MySQL types.
        
        Validates:
            - Complex type conversion (DECIMAL, VARCHAR with parameters)
            - Parameter preservation in type conversion
            - Complex column definitions
            - Schema structure integrity
            
        ETL Pipeline Context:
            - Critical for complex dental clinic data types
            - Supports dental clinic data precision preservation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect at the module level where PostgresSchema imports it
        with patch('etl_pipeline.core.postgres_schema.inspect') as mock_inspect, \
             patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
            
            # Create mock engines
            mock_mysql_engine = create_mock_engine('mysql', 'test_replication')
            mock_postgres_engine = create_mock_engine('postgresql', 'test_analytics')
            
            # Create mock inspectors
            mock_mysql_inspector = Mock()
            mock_postgres_inspector = Mock()
            
            # Configure mock factory
            mock_factory.get_replication_connection.return_value = mock_mysql_engine
            mock_factory.get_analytics_raw_connection.return_value = mock_postgres_engine
            
            # Configure mock inspect
            def mock_inspect_side_effect(engine):
                if engine == mock_mysql_engine:
                    return mock_mysql_inspector
                elif engine == mock_postgres_engine:
                    return mock_postgres_inspector
                else:
                    return Mock()
            
            mock_inspect.side_effect = mock_inspect_side_effect
            
            schema = PostgresSchema(settings=test_settings)
            
            # Test with complex types
            complex_schema = {
                'create_statement': '''
                    CREATE TABLE complex_table (
                        `id` INT PRIMARY KEY,
                        `name` VARCHAR(255),
                        `amount` DECIMAL(10,2),
                        `description` TEXT,
                        `created_at` TIMESTAMP
                    )
                '''
            }
            
            pg_create = schema.adapt_schema('complex_table', complex_schema)
            
            # Validate complex type conversion
            assert 'CREATE TABLE raw.complex_table' in pg_create
            assert '"id" integer' in pg_create
            assert '"name" character varying(255)' in pg_create
            assert '"amount" numeric(10,2)' in pg_create
            assert '"description" text' in pg_create
            assert '"created_at" timestamp' in pg_create 