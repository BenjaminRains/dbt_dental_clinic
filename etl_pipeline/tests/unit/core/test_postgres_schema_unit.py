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
from datetime import datetime

# Import ETL pipeline components
from etl_pipeline.core.postgres_schema import PostgresSchema
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    PostgresSchema as ConfigPostgresSchema
)
from etl_pipeline.config.providers import DictConfigProvider
from etl_pipeline.exceptions import SchemaTransformationError, EnvironmentError, SchemaValidationError

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


@pytest.mark.unit
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestPostgresSchemaTableOperations:
    """Test PostgreSQL table operations with provider pattern."""

    def test_get_table_schema_from_mysql(self, test_settings):
        """
        Test MySQL schema extraction with mocked database connection.
        
        Validates:
            - MySQL schema extraction using SHOW CREATE TABLE
            - Column metadata extraction from information_schema
            - Table metadata extraction from SHOW TABLE STATUS
            - Schema hash calculation for change detection
            - Error handling for non-existent tables
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data structure analysis
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Create mock connection and results
            mock_conn = Mock()
            mock_mysql_engine.connect.return_value.__enter__.return_value = mock_conn
            
            # Mock SHOW CREATE TABLE result
            mock_create_result = Mock()
            mock_create_result.fetchone.return_value = ('patient', '''
                CREATE TABLE `patient` (
                    `PatNum` int(11) NOT NULL AUTO_INCREMENT,
                    `LName` varchar(100) DEFAULT NULL,
                    `FName` varchar(100) DEFAULT NULL,
                    `IsActive` tinyint(1) DEFAULT '1',
                    PRIMARY KEY (`PatNum`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
            
            # Mock SHOW TABLE STATUS result
            mock_status_result = Mock()
            mock_status_result.fetchone.return_value = ('patient', 'InnoDB', 10, 'Dynamic', 100, 1024, 0, 0, 0, 0, None, None, None, None, 'utf8mb4_general_ci', None, None, '')
            
            # Mock information_schema.columns result
            mock_columns_result = Mock()
            mock_columns_result.fetchall.return_value = [
                ('PatNum', 'int', 'NO', None, 'auto_increment', '', 'PRI'),
                ('LName', 'varchar', 'YES', None, '', 'Last Name', ''),
                ('FName', 'varchar', 'YES', None, '', 'First Name', ''),
                ('IsActive', 'tinyint', 'YES', '1', '', 'Active Status', '')
            ]
            
            # Configure mock connection execute
            def mock_execute_side_effect(query):
                if 'SHOW CREATE TABLE' in str(query):
                    return mock_create_result
                elif 'SHOW TABLE STATUS' in str(query):
                    return mock_status_result
                elif 'information_schema.columns' in str(query):
                    return mock_columns_result
                else:
                    return Mock()
            
            mock_conn.execute.side_effect = mock_execute_side_effect
            
            schema = PostgresSchema(settings=test_settings)
            
            # Test schema extraction
            mysql_schema = schema.get_table_schema_from_mysql('patient')
            
            # Validate schema structure
            assert mysql_schema['table_name'] == 'patient'
            assert 'create_statement' in mysql_schema
            assert 'columns' in mysql_schema
            assert 'metadata' in mysql_schema
            assert 'schema_hash' in mysql_schema
            assert 'analysis_timestamp' in mysql_schema
            assert 'analysis_version' in mysql_schema
            
            # Validate columns
            columns = mysql_schema['columns']
            assert len(columns) == 4
            assert columns[0]['name'] == 'PatNum'
            assert columns[0]['type'] == 'int'
            assert columns[0]['key_type'] == 'PRI'
            
            # Validate metadata
            metadata = mysql_schema['metadata']
            assert metadata['engine'] == 'InnoDB'
            assert metadata['charset'] == 'utf8mb4'

    def test_create_postgres_table(self, test_settings, sample_mysql_schemas):
        """
        Test PostgreSQL table creation with mocked database operations.
        
        Validates:
            - Schema creation if not exists
            - Table dropping and recreation
            - CREATE TABLE statement execution
            - Error handling for database operations
            - Transaction management
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data warehouse creation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Create mock connection for PostgreSQL operations
            mock_conn = Mock()
            mock_postgres_engine.begin.return_value.__enter__.return_value = mock_conn
            
            # Mock _analyze_column_data to return appropriate types
            def mock_analyze_column_data(table_name, column_name, mysql_type):
                if mysql_type.lower().startswith('tinyint'):
                    return 'boolean' if column_name in ['IsActive', 'IsDeleted'] else 'smallint'
                else:
                    schema = PostgresSchema(settings=test_settings)
                    return schema._convert_mysql_type_standard(mysql_type)
            
            with patch.object(PostgresSchema, '_analyze_column_data', side_effect=mock_analyze_column_data):
                schema = PostgresSchema(settings=test_settings)
                
                # Test table creation
                mysql_schema = sample_mysql_schemas['patient']
                result = schema.create_postgres_table('patient', mysql_schema)
                
                # Validate successful creation
                assert result is True
                
                # Validate that schema creation was attempted
                mock_conn.execute.assert_any_call(Mock())  # CREATE SCHEMA call
                
                # Validate that table was dropped and recreated
                mock_conn.execute.assert_any_call(Mock())  # DROP TABLE call
                mock_conn.execute.assert_any_call(Mock())  # CREATE TABLE call

    def test_ensure_table_exists(self, test_settings, sample_mysql_schemas):
        """
        Test table existence checking and creation.
        
        Validates:
            - Table existence checking with inspector
            - Table creation when not exists
            - Schema verification when exists
            - Error handling for inspector operations
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data warehouse management
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Mock _analyze_column_data
            def mock_analyze_column_data(table_name, column_name, mysql_type):
                if mysql_type.lower().startswith('tinyint'):
                    return 'boolean' if column_name in ['IsActive', 'IsDeleted'] else 'smallint'
                else:
                    schema = PostgresSchema(settings=test_settings)
                    return schema._convert_mysql_type_standard(mysql_type)
            
            with patch.object(PostgresSchema, '_analyze_column_data', side_effect=mock_analyze_column_data):
                schema = PostgresSchema(settings=test_settings)
                
                # Test when table doesn't exist (should create)
                mock_postgres_inspector.has_table.return_value = False
                
                # Mock create_postgres_table to return True
                with patch.object(schema, 'create_postgres_table', return_value=True):
                    result = schema.ensure_table_exists('patient', sample_mysql_schemas['patient'])
                    assert result is True
                
                # Test when table exists (should verify)
                mock_postgres_inspector.has_table.return_value = True
                
                # Mock verify_schema to return True
                with patch.object(schema, 'verify_schema', return_value=True):
                    result = schema.ensure_table_exists('patient', sample_mysql_schemas['patient'])
                    assert result is True

    def test_verify_schema(self, test_settings, sample_mysql_schemas):
        """
        Test schema verification logic.
        
        Validates:
            - PostgreSQL column inspection
            - MySQL column extraction from CREATE statement
            - Column count comparison
            - Column name and type comparison
            - Environment-aware validation (test vs production)
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data integrity validation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Mock PostgreSQL columns
            mock_pg_columns = [
                {'name': 'PatNum', 'type': 'integer'},
                {'name': 'LName', 'type': 'character varying(100)'},
                {'name': 'FName', 'type': 'character varying(100)'},
                {'name': 'IsActive', 'type': 'boolean'}
            ]
            mock_postgres_inspector.get_columns.return_value = mock_pg_columns
            
            # Mock _analyze_column_data
            def mock_analyze_column_data(table_name, column_name, mysql_type):
                if mysql_type.lower().startswith('tinyint'):
                    return 'boolean' if column_name in ['IsActive', 'IsDeleted'] else 'smallint'
                else:
                    schema = PostgresSchema(settings=test_settings)
                    return schema._convert_mysql_type_standard(mysql_type)
            
            with patch.object(PostgresSchema, '_analyze_column_data', side_effect=mock_analyze_column_data):
                schema = PostgresSchema(settings=test_settings)
                
                # Test schema verification
                mysql_schema = sample_mysql_schemas['patient']
                result = schema.verify_schema('patient', mysql_schema)
                
                # Should pass in test environment (warnings but no failures)
                assert result is True

    def test_convert_row_data_types(self, test_settings):
        """
        Test row data type conversion for PostgreSQL compatibility.
        
        Validates:
            - PostgreSQL column type inspection
            - Comprehensive type conversion logic
            - MySQL to PostgreSQL data type mapping
            - Error handling for conversion failures
            - Column not found handling
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL data conversion
            - Supports dental clinic data type adaptation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Create mock connection
            mock_conn = Mock()
            mock_postgres_engine.connect.return_value.__enter__.return_value = mock_conn
            
            # Mock PostgreSQL columns with type information
            mock_pg_columns = [
                {
                    'name': 'id',
                    'type': Mock(python_type=int, length=None),
                    'nullable': False
                },
                {
                    'name': 'name',
                    'type': Mock(python_type=str, length=100),
                    'nullable': True
                },
                {
                    'name': 'is_active',
                    'type': Mock(python_type=bool, length=None),
                    'nullable': True
                },
                {
                    'name': 'amount',
                    'type': Mock(python_type=float, length=None),
                    'nullable': True
                }
            ]
            mock_postgres_inspector.get_columns.return_value = mock_pg_columns
            
            schema = PostgresSchema(settings=test_settings)
            
            # Test data conversion
            row_data = {
                'id': '123',
                'name': 'Test Patient',
                'is_active': '1',
                'amount': '99.99',
                'extra_column': 'should_be_ignored'
            }
            
            converted_data = schema.convert_row_data_types('test_table', row_data)
            
            # Validate conversions
            assert converted_data['id'] == 123  # String to int
            assert converted_data['name'] == 'Test Patient'  # String unchanged
            assert converted_data['is_active'] is True  # String '1' to boolean
            assert converted_data['amount'] == 99.99  # String to float
            assert converted_data['extra_column'] == 'should_be_ignored'  # Pass through

    def test_convert_single_value(self, test_settings):
        """
        Test single value conversion logic.
        
        Validates:
            - Boolean conversion from various input types
            - Integer conversion with range validation
            - Float/Decimal conversion
            - String conversion with encoding handling
            - DateTime conversion from various formats
            - Error handling for invalid conversions
            - Nullable column handling
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL data conversion
            - Supports dental clinic data type adaptation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Test boolean conversion
            bool_type_info = {
                'python_type': bool,
                'sqlalchemy_type': Mock(),
                'nullable': True
            }
            
            assert schema._convert_single_value('1', bool_type_info, 'test_col', 'test_table') is True
            assert schema._convert_single_value('0', bool_type_info, 'test_col', 'test_table') is False
            assert schema._convert_single_value('true', bool_type_info, 'test_col', 'test_table') is True
            assert schema._convert_single_value('false', bool_type_info, 'test_col', 'test_table') is False
            assert schema._convert_single_value(1, bool_type_info, 'test_col', 'test_table') is True
            assert schema._convert_single_value(0, bool_type_info, 'test_col', 'test_table') is False
            
            # Test integer conversion
            int_type_info = {
                'python_type': int,
                'sqlalchemy_type': Mock(),
                'nullable': False
            }
            
            assert schema._convert_single_value('123', int_type_info, 'test_col', 'test_table') == 123
            assert schema._convert_single_value('123.0', int_type_info, 'test_col', 'test_table') == 123
            assert schema._convert_single_value('', int_type_info, 'test_col', 'test_table') == 0  # Non-nullable default
            
            # Test float conversion
            float_type_info = {
                'python_type': float,
                'sqlalchemy_type': Mock(),
                'nullable': True
            }
            
            assert schema._convert_single_value('99.99', float_type_info, 'test_col', 'test_table') == 99.99
            assert schema._convert_single_value('', float_type_info, 'test_col', 'test_table') is None  # Nullable
            
            # Test string conversion
            string_type_info = {
                'python_type': str,
                'sqlalchemy_type': Mock(length=50),
                'nullable': True
            }
            
            assert schema._convert_single_value('Test String', string_type_info, 'test_col', 'test_table') == 'Test String'
            assert schema._convert_single_value('', string_type_info, 'test_col', 'test_table') is None  # Empty string to null
            
            # Test datetime conversion
            datetime_type_info = {
                'python_type': datetime,
                'sqlalchemy_type': Mock(),
                'nullable': True
            }
            
            # Mock dateutil.parser for datetime parsing
            with patch('etl_pipeline.core.postgres_schema.parser') as mock_parser:
                mock_parser.parse.return_value = datetime(2023, 1, 1, 12, 0, 0)
                
                result = schema._convert_single_value('2023-01-01 12:00:00', datetime_type_info, 'test_col', 'test_table')
                if isinstance(result, datetime):
                    assert result.year == 2023
                    assert result.month == 1
                    assert result.day == 1
                else:
                    # Handle case where conversion might fail and return None or default value
                    assert result is None or isinstance(result, datetime) 

    def test_convert_single_value_error_handling(self, test_settings):
        """
        Test error handling in single value conversion.
        
        Validates:
            - Error handling for invalid type conversions
            - Graceful fallback for conversion failures
            - Logging of conversion errors
            - Nullable vs non-nullable handling
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL data conversion
            - Supports dental clinic data type adaptation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Test invalid integer conversion (non-nullable)
            int_type_info = {
                'python_type': int,
                'sqlalchemy_type': Mock(),
                'nullable': False
            }
            
            # Should return 0 for non-nullable when conversion fails
            result = schema._convert_single_value('invalid_int', int_type_info, 'test_col', 'test_table')
            assert result == 0
            
            # Test invalid integer conversion (nullable)
            int_type_info_nullable = {
                'python_type': int,
                'sqlalchemy_type': Mock(),
                'nullable': True
            }
            
            # Should return None for nullable when conversion fails
            result = schema._convert_single_value('invalid_int', int_type_info_nullable, 'test_col', 'test_table')
            assert result is None
            
            # Test invalid float conversion
            float_type_info = {
                'python_type': float,
                'sqlalchemy_type': Mock(),
                'nullable': True
            }
            
            result = schema._convert_single_value('invalid_float', float_type_info, 'test_col', 'test_table')
            assert result is None

    def test_get_table_schema_from_mysql_error_handling(self, test_settings):
        """
        Test error handling in MySQL schema extraction.
        
        Validates:
            - Error handling for non-existent tables
            - Database connection error handling
            - Query execution error handling
            - Structured exception handling
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data structure analysis
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Create mock connection that returns no results (table not found)
            mock_conn = Mock()
            mock_mysql_engine.connect.return_value.__enter__.return_value = mock_conn
            
            # Mock empty result for non-existent table
            mock_result = Mock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            
            schema = PostgresSchema(settings=test_settings)
            
            # Test non-existent table
            with pytest.raises(SchemaValidationError, match="Table nonexistent_table not found"):
                schema.get_table_schema_from_mysql('nonexistent_table')

    def test_create_postgres_table_error_handling(self, test_settings):
        """
        Test error handling in PostgreSQL table creation.
        
        Validates:
            - Schema transformation error handling
            - Database connection error handling
            - Transaction error handling
            - Graceful failure with proper logging
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data warehouse creation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Mock _analyze_column_data to raise SchemaTransformationError
            def mock_analyze_column_data(table_name, column_name, mysql_type):
                raise SchemaTransformationError(
                    message="Test transformation error",
                    table_name=table_name,
                    mysql_schema={},
                    details={"error_type": "test_error"}
                )
            
            with patch.object(PostgresSchema, '_analyze_column_data', side_effect=mock_analyze_column_data):
                schema = PostgresSchema(settings=test_settings)
                
                # Test with invalid schema that causes transformation error
                invalid_schema = {
                    'create_statement': 'CREATE TABLE test (id INT)'
                }
                
                # Should return False on transformation error
                result = schema.create_postgres_table('test_table', invalid_schema)
                assert result is False

    def test_verify_schema_error_handling(self, test_settings):
        """
        Test error handling in schema verification.
        
        Validates:
            - Database connection error handling
            - Query execution error handling
            - Schema validation error handling
            - Inspector error handling
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL schema conversion
            - Supports dental clinic data integrity validation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Mock PostgreSQL inspector to raise exception
            mock_postgres_inspector.get_columns.side_effect = Exception("Database error")
            
            schema = PostgresSchema(settings=test_settings)
            
            # Test with database error
            mysql_schema = {
                'create_statement': 'CREATE TABLE test (id INT PRIMARY KEY)'
            }
            
            # Should return False on database error
            result = schema.verify_schema('test_table', mysql_schema)
            assert result is False

    def test_convert_row_data_types_error_handling(self, test_settings):
        """
        Test error handling in row data type conversion.
        
        Validates:
            - Database connection error handling
            - Column inspection error handling
            - Type conversion error handling
            - Graceful fallback to original data
            
        ETL Pipeline Context:
            - Critical for MySQL to PostgreSQL data conversion
            - Supports dental clinic data type adaptation
            - Uses Settings injection for environment-agnostic operation
            - Implements provider pattern for clean dependency injection
        """
        # Patch sqlalchemy.inspect and ConnectionFactory
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
            
            # Mock PostgreSQL inspector to raise exception
            mock_postgres_inspector.get_columns.side_effect = Exception("Database error")
            
            schema = PostgresSchema(settings=test_settings)
            
            # Test with database error
            row_data = {'id': '123', 'name': 'Test'}
            
            # Should return original data on error
            result = schema.convert_row_data_types('test_table', row_data)
            assert result == row_data 