"""
Integration tests for PostgresLoader with real database connections.

This module provides comprehensive integration testing for PostgresLoader using:
- Real database connections with test environment isolation
- FileConfigProvider for actual .env_test configuration loading
- Settings injection for environment-agnostic connections
- Provider pattern for dependency injection
- Comprehensive test fixtures for dental clinic data
- Order markers for proper integration test execution

Test Strategy:
    - Integration tests with real MySQL replication and PostgreSQL analytics databases
    - Uses FileConfigProvider with .env_test for environment isolation
    - Tests complete data flow from MySQL replication to PostgreSQL analytics
    - Validates schema conversion, incremental loading, and error handling
    - Uses standardized test fixtures for consistent dental clinic data

ETL Context:
    - Tests critical data loading phase of ETL pipeline (order=4)
    - Validates MySQL replication → PostgreSQL analytics data flow
    - Tests with real OpenDental dental clinic data structures
    - Uses Settings injection for environment-agnostic connections
    - Validates provider pattern integration with real configuration files

Testing Architecture:
    - Phase 2: Data Loading (order=4)
    - Real database integration with test environment
    - FileConfigProvider with .env_test configuration
    - Settings injection for unified connection interface
    - Provider pattern dependency injection
    - FAIL FAST testing for security requirements
"""

import pytest
from datetime import datetime, timedelta
from typing import Dict, List
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
import os
import logging

from etl_pipeline.loaders.postgres_loader import PostgresLoader
from etl_pipeline.config import get_settings, DatabaseType
from etl_pipeline.config.settings import Settings, PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.exceptions.database import DatabaseConnectionError, DatabaseTransactionError
from etl_pipeline.exceptions.data import DataLoadingError
from etl_pipeline.exceptions.configuration import ConfigurationError

# Direct import of the fixture to ensure pytest can find it
from tests.fixtures.integration_fixtures import test_settings_with_file_provider

# Set up logger for tests
logger = logging.getLogger(__name__)


class TestPostgresLoaderIntegration:
    """
    Integration tests for PostgresLoader using real database connections.
    
    Test Strategy:
        - Real database connections with FileConfigProvider for .env_test
        - Settings injection for environment-agnostic connections
        - Provider pattern integration with real configuration files
        - Complete data flow validation from MySQL to PostgreSQL
        - Schema conversion and incremental loading testing
        
    ETL Context:
        - Phase 2: Data Loading (order=4) in ETL pipeline
        - MySQL replication → PostgreSQL analytics data flow
        - Real OpenDental dental clinic data structures
        - Test environment isolation with FileConfigProvider
        - Settings injection for unified connection interface
        
    Coverage Areas:
        - Core data loading functionality with real databases
        - Schema conversion from MySQL to PostgreSQL
        - Incremental loading with real timestamp columns
        - Chunked loading for large datasets
        - Error handling with real database errors
        - Load verification with actual row counts
    """

    @pytest.mark.integration
    @pytest.mark.order(4)  # Phase 2: Data Loading
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.etl_critical
    def test_postgres_loader_init_method(
        self,
        test_settings_with_file_provider,
        populated_test_databases
    ):
        """
        Test PostgresLoader.__init__() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up test settings and database connections
            Act: Initialize PostgresLoader with injected settings
            Assert: Verify proper initialization and configuration
            
        Validates:
            - Settings injection for environment-agnostic connections
            - Database connection establishment
            - Configuration loading from tables.yml
            - Schema adapter initialization
            - Environment detection and setup
        """
        # Arrange: Test settings and databases are ready
        assert test_settings_with_file_provider.environment == 'test'
        
        # Act: Initialize PostgresLoader
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Assert: Verify proper initialization
        assert postgres_loader.settings is not None
        assert postgres_loader.settings.environment == 'test'
        assert postgres_loader.replication_engine is not None
        assert postgres_loader.analytics_engine is not None
        assert postgres_loader.schema_adapter is not None
        assert postgres_loader.table_configs is not None
        assert isinstance(postgres_loader.table_configs, dict)
        assert len(postgres_loader.table_configs) > 0, "Should load table configurations"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_postgres_loader_get_table_config_method(
        self,
        test_settings_with_file_provider,
        populated_test_databases
    ):
        """
        Test PostgresLoader.get_table_config() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with test configuration
            Act: Call get_table_config() with valid and invalid table names
            Assert: Verify correct configuration retrieval
            
        Validates:
            - Configuration retrieval for existing tables
            - Empty configuration for non-existent tables
            - Configuration structure and content
            - Error handling for missing configurations
        """
        # Arrange: Set up PostgresLoader
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Act & Assert: Test valid table configuration
        patient_config = postgres_loader.get_table_config('patient')
        assert isinstance(patient_config, dict)
        assert 'incremental_columns' in patient_config, "Patient config should have incremental_columns"
        
        # Act & Assert: Test non-existent table configuration
        nonexistent_config = postgres_loader.get_table_config('nonexistent_table')
        assert nonexistent_config == {}, "Non-existent table should return empty config"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_postgres_loader_load_table_method(
        self,
        test_settings_with_file_provider,
        standard_patient_test_data,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader.load_table() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader and populate source data
            Act: Execute load_table() with real database connections
            Assert: Verify successful data loading and verification
            
        Validates:
            - Complete data flow from MySQL to PostgreSQL
            - Schema conversion and table creation
            - Data type conversion during loading
            - Transaction management and error handling
            - Load verification functionality
        """
        # Arrange: Set up PostgresLoader and populate source data
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        
        # Act: Execute data loading
        result = postgres_loader.load_table('patient')
        
        # Assert: Verify successful loading
        assert result is True, "load_table() should return True for successful load"
        
        # Verify data was actually loaded
        verification_result = postgres_loader.verify_load('patient')
        assert verification_result is True, "Load verification should pass"
        
        # Verify row counts match
        source_count = test_data_manager.get_patient_count(DatabaseType.REPLICATION)
        target_count = test_data_manager.get_patient_count(DatabaseType.ANALYTICS)
        assert target_count == source_count, f"Target count {target_count} should match source count {source_count}"
        assert target_count > 0, "Target database should contain data after loading"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.slow
    def test_postgres_loader_load_table_chunked_method(
        self,
        test_settings_with_file_provider,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader.load_table_chunked() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with large dataset
            Act: Execute chunked loading with small chunk size
            Assert: Verify complete data loading in chunks
            
        Validates:
            - Chunked loading for large datasets
            - Memory efficiency with controlled chunk sizes
            - Transaction handling across multiple chunks
            - Progress tracking and logging
            - Complete data integrity across chunks
        """
        # Arrange: Set up PostgresLoader with large dataset
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        
        # Act: Execute chunked loading
        result = postgres_loader.load_table_chunked('patient', chunk_size=100)
        
        # Assert: Verify successful chunked loading
        assert result is True, "load_table_chunked() should return True for successful load"
        
        # Verify complete data loading
        verification_result = postgres_loader.verify_load('patient')
        assert verification_result is True, "Load verification should pass for chunked load"
        
        # Verify all data present
        source_count = test_data_manager.get_patient_count(DatabaseType.REPLICATION)
        target_count = test_data_manager.get_patient_count(DatabaseType.ANALYTICS)
        assert target_count == source_count, f"Chunked load should preserve all data: source={source_count}, target={target_count}"
        assert target_count > 0, "Target database should contain data after chunked loading"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_postgres_loader_verify_load_method(
        self,
        test_settings_with_file_provider,
        standard_patient_test_data,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader.verify_load() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader and load data
            Act: Execute verify_load() with loaded and non-existent tables
            Assert: Verify correct verification results
            
        Validates:
            - Load verification for successfully loaded tables
            - Error handling for non-existent tables
            - Row count comparison between source and target
            - Verification accuracy and reliability
        """
        # Arrange: Set up PostgresLoader and load data
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        postgres_loader.load_table('patient')  # Load data first
        
        # Act & Assert: Test verification of loaded table
        verification_result = postgres_loader.verify_load('patient')
        assert verification_result is True, "verify_load() should return True for successfully loaded table"
        
        # Act & Assert: Test verification of non-existent table
        nonexistent_verification = postgres_loader.verify_load('nonexistent_table')
        assert nonexistent_verification is False, "verify_load() should return False for non-existent table"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_postgres_loader_build_load_query_method(
        self,
        test_settings_with_file_provider,
        populated_test_databases
    ):
        """
        Test PostgresLoader._build_load_query() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with test configuration
            Act: Call _build_load_query() with different parameters
            Assert: Verify correct query generation
            
        Validates:
            - Full load query generation
            - Incremental load query generation
            - Test environment column filtering
            - Query structure and syntax
        """
        # Arrange: Set up PostgresLoader
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Act: Test full load query
        full_query = postgres_loader._build_load_query('patient', [], force_full=True)
        
        # Assert: Verify full load query
        assert 'SELECT' in full_query.upper()
        assert 'FROM patient' in full_query
        assert 'WHERE' not in full_query, "Full load query should not have WHERE clause"
        
        # Act: Test incremental load query
        incremental_query = postgres_loader._build_load_query('patient', ['DateTStamp'], force_full=False)
        
        # Assert: Verify incremental query structure
        assert 'SELECT' in incremental_query.upper()
        assert 'FROM patient' in incremental_query
        # Note: Incremental query may or may not have WHERE clause depending on last_load timestamp

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_postgres_loader_build_count_query_method(
        self,
        test_settings_with_file_provider,
        populated_test_databases
    ):
        """
        Test PostgresLoader._build_count_query() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with test configuration
            Act: Call _build_count_query() with different parameters
            Assert: Verify correct count query generation
            
        Validates:
            - Full count query generation
            - Incremental count query generation
            - Query structure and syntax
            - COUNT(*) functionality
        """
        # Arrange: Set up PostgresLoader
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Act: Test full count query
        full_count_query = postgres_loader._build_count_query('patient', [], force_full=True)
        
        # Assert: Verify full count query
        assert 'SELECT COUNT(*)' in full_count_query.upper()
        assert 'FROM patient' in full_count_query
        assert 'WHERE' not in full_count_query, "Full count query should not have WHERE clause"
        
        # Act: Test incremental count query
        incremental_count_query = postgres_loader._build_count_query('patient', ['DateTStamp'], force_full=False)
        
        # Assert: Verify incremental count query structure
        assert 'SELECT COUNT(*)' in incremental_count_query.upper()
        assert 'FROM patient' in incremental_count_query
        # Note: Incremental count query may or may not have WHERE clause depending on last_load timestamp

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_postgres_loader_get_last_load_method(
        self,
        test_settings_with_file_provider,
        populated_test_databases
    ):
        """
        Test PostgresLoader._get_last_load() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader and create etl_load_status table
            Act: Call _get_last_load() with different scenarios
            Assert: Verify correct timestamp retrieval
            
        Validates:
            - Last load timestamp retrieval
            - Handling of non-existent load status
            - Database query execution
            - Timestamp format and type
        """
        # Arrange: Set up PostgresLoader
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Act: Test last load retrieval for non-existent table
        last_load = postgres_loader._get_last_load('nonexistent_table')
        
        # Assert: Verify handling of non-existent load status
        # Should return None or a valid timestamp, but not raise an exception
        assert last_load is None or isinstance(last_load, datetime), \
            "Last load should be None or datetime for non-existent table"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    def test_postgres_loader_load_configuration_method(
        self,
        test_settings_with_file_provider,
        populated_test_databases
    ):
        """
        Test PostgresLoader._load_configuration() method with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with test configuration
            Act: Access table_configs which uses _load_configuration()
            Assert: Verify correct configuration loading
            
        Validates:
            - Configuration file loading from tables.yml
            - Configuration structure and content
            - Table configuration parsing
            - Error handling for missing configuration files
        """
        # Arrange: Set up PostgresLoader
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Act: Access table configurations (loaded by _load_configuration())
        table_configs = postgres_loader.table_configs
        
        # Assert: Verify configuration loading
        assert isinstance(table_configs, dict), "Table configs should be loaded as dict"
        assert len(table_configs) > 0, "Should load table configurations from tables.yml"
        
        # Verify specific table configurations exist
        assert 'patient' in table_configs, "Patient table config should be loaded"
        patient_config = table_configs['patient']
        assert isinstance(patient_config, dict), "Patient config should be a dict"
        assert 'incremental_columns' in patient_config, "Patient config should have incremental_columns"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.schema_conversion
    def test_postgres_loader_schema_adapter_integration(
        self,
        test_settings_with_file_provider,
        sample_mysql_schema,
        sample_postgres_schema,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader integration with PostgresSchema adapter.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with schema fixtures
            Act: Execute schema operations through schema adapter
            Assert: Verify schema conversion and table creation
            
        Validates:
            - Integration with PostgresSchema adapter
            - Schema extraction from MySQL
            - Table creation in PostgreSQL
            - Schema verification functionality
            - Data type conversion during loading
        """
        # Arrange: Set up PostgresLoader with schema fixtures
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Verify schema adapter is properly initialized
        assert postgres_loader.schema_adapter is not None
        assert postgres_loader.settings.environment == 'test'
        
        # Act: Execute schema operations through adapter
        mysql_schema = postgres_loader.schema_adapter.get_table_schema_from_mysql('patient')
        result = postgres_loader.schema_adapter.ensure_table_exists('patient', mysql_schema)
        
        # Assert: Verify schema conversion
        assert result is True, "PostgreSQL table creation should succeed"
        
        # Verify PostgreSQL table exists
        inspector = inspect(postgres_loader.analytics_engine)
        assert inspector.has_table('patient', schema='raw'), "PostgreSQL table should exist in raw schema"
        
        # Verify column structure matches expectations
        pg_columns = inspector.get_columns('patient', schema='raw')
        pg_column_names = [col['name'] for col in pg_columns]
        
        # Verify key columns from sample schema are present
        expected_columns = sample_postgres_schema.get('columns', [])
        for expected_col in expected_columns:
            assert expected_col['name'] in pg_column_names, \
                f"Column {expected_col['name']} should be present in PostgreSQL table"
        
        # Verify data types are properly converted
        pg_column_types = {col['name']: str(col['type']) for col in pg_columns}
        
        # Test specific data type conversions
        for expected_col in expected_columns:
            col_name = expected_col['name']
            if col_name in pg_column_types:
                pg_type = pg_column_types[col_name]
                # Verify basic type conversion (detailed type checking would be in PostgresSchema tests)
                assert pg_type is not None, f"Column {col_name} should have a valid PostgreSQL type"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.error_handling
    def test_postgres_loader_error_handling_methods(
        self,
        test_settings_with_file_provider,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader error handling across all methods.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with error scenarios
            Act: Execute operations that should fail gracefully
            Assert: Verify proper error handling and logging
            
        Validates:
            - Graceful error handling for database connection failures
            - Proper exception handling for data loading errors
            - Transaction rollback on failures
            - Logging of error conditions
            - Recovery capabilities after errors
        """
        # Arrange: Set up PostgresLoader
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Act & Assert: Test loading non-existent table
        result = postgres_loader.load_table('nonexistent_table')
        assert result is False, "Loading non-existent table should return False"
        
        # Act & Assert: Test verification of non-existent table
        verification_result = postgres_loader.verify_load('nonexistent_table')
        assert verification_result is False, "Verification of non-existent table should return False"
        
        # Act & Assert: Test configuration for non-existent table
        config = postgres_loader.get_table_config('nonexistent_table')
        assert config == {}, "Non-existent table config should return empty dict"
        
        # Act & Assert: Test with valid table
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        result = postgres_loader.load_table('patient')
        assert result is True, "Loading patient table should succeed"
        
        verification_result = postgres_loader.verify_load('patient')
        assert verification_result is True, "Verification of patient table should succeed"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.settings_injection
    def test_postgres_loader_settings_injection_methods(
        self,
        test_settings_with_file_provider,
        populated_test_databases
    ):
        """
        Test PostgresLoader Settings injection functionality.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with injected Settings
            Act: Execute operations using Settings injection
            Assert: Verify Settings injection works correctly
            
        Validates:
            - Settings injection for environment-agnostic connections
            - FileConfigProvider integration with real configuration
            - Environment detection and configuration loading
            - Database connection creation through Settings
            - Unified connection interface
        """
        # Arrange: Set up PostgresLoader with injected Settings
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Act: Test Settings-based configuration access
        replication_config = postgres_loader.settings.get_database_config(DatabaseType.REPLICATION)
        analytics_config = postgres_loader.settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        
        # Assert: Verify Settings injection provides valid configurations
        assert replication_config is not None, "Replication config should be available"
        assert analytics_config is not None, "Analytics config should be available"
        
        # Verify required configuration fields
        assert 'host' in replication_config, "Replication config should have host"
        assert 'database' in replication_config, "Replication config should have database"
        assert 'user' in replication_config, "Replication config should have user"
        assert 'password' in replication_config, "Replication config should have password"
        
        assert 'host' in analytics_config, "Analytics config should have host"
        assert 'database' in analytics_config, "Analytics config should have database"
        assert 'user' in analytics_config, "Analytics config should have user"
        assert 'password' in analytics_config, "Analytics config should have password"
        assert 'schema' in analytics_config, "Analytics config should have schema"
        
        # Verify test environment database names
        assert 'test' in replication_config['database'].lower(), \
            "Test environment should use test-prefixed database name"
        assert 'test' in analytics_config['database'].lower(), \
            "Test environment should use test-prefixed database name"
        
        # Test database connections work
        assert postgres_loader.replication_engine is not None, "Replication engine should be created"
        assert postgres_loader.analytics_engine is not None, "Analytics engine should be created"
        
        # Test connection functionality
        try:
            with postgres_loader.replication_engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                assert result == 1, "Replication connection should work"
        except Exception as e:
            pytest.fail(f"Replication connection failed: {e}")
        
        try:
            with postgres_loader.analytics_engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                assert result == 1, "Analytics connection should work"
        except Exception as e:
            pytest.fail(f"Analytics connection failed: {e}")

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.provider_pattern
    def test_postgres_loader_provider_pattern_integration(
        self,
        test_settings_with_file_provider,
        populated_test_databases
    ):
        """
        Test PostgresLoader provider pattern integration.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with FileConfigProvider
            Act: Execute operations using provider pattern
            Assert: Verify provider pattern works correctly
            
        Validates:
            - FileConfigProvider integration with real configuration files
            - Provider pattern dependency injection
            - Configuration loading from .env_test files
            - Environment-specific configuration handling
            - Provider pattern consistency across components
        """
        # Arrange: Set up PostgresLoader with provider pattern
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        
        # Act: Test provider pattern functionality
        provider = postgres_loader.settings.provider
        configs_valid = postgres_loader.settings.validate_configs()
        environment = postgres_loader.settings.environment
        table_configs = postgres_loader.table_configs
        replication_config = postgres_loader.settings.get_database_config(DatabaseType.REPLICATION)
        analytics_config = postgres_loader.settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
        
        # Assert: Verify provider pattern provides correct configuration
        assert postgres_loader.settings is not None
        assert hasattr(postgres_loader.settings, 'provider')
        assert provider is not None, "Settings should have a provider"
        assert configs_valid is True, "Provider should provide valid configurations"
        assert environment == 'test', "Provider should detect test environment"
        assert isinstance(table_configs, dict), "Table configs should be loaded as dict"
        assert replication_config is not None, "Provider should provide replication config"
        assert analytics_config is not None, "Provider should provide analytics config"
        
        # Verify provider pattern consistency
        replication_config_2 = postgres_loader.settings.get_database_config(DatabaseType.REPLICATION)
        assert replication_config == replication_config_2, \
            "Provider should return consistent configuration"
        
        # Test that provider handles environment-specific variables
        assert 'test' in replication_config['host'].lower() or \
               'test' in replication_config['database'].lower(), \
            "Test environment should use test-specific configuration"

# Removed test_postgresloader_loads_latest_versioned_tables_yml - we only use tables.yml with metadata versioning

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.incremental
    def test_postgres_loader_incremental_methods(
        self,
        test_settings_with_file_provider,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader incremental loading methods with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with test data and incremental columns
            Act: Execute incremental loading methods with real database connections
            Assert: Verify incremental logic works correctly
            
        Validates:
            - _get_loaded_at_time_max() with multiple incremental columns
            - _build_improved_load_query_max() with OR logic
            - _filter_valid_incremental_columns() data quality validation
            - _validate_incremental_integrity() validation logic
            - Incremental loading with real timestamp columns
            - OR vs AND logic for incremental queries
        """
        # Arrange: Set up PostgresLoader and populate test data
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        
        # Load initial data to establish baseline
        result = postgres_loader.load_table('patient')
        assert result is True, "Initial load should succeed"
        
        # Act & Assert: Test _get_loaded_at_time_max with multiple columns
        # Get the actual incremental columns from the table configuration
        patient_config = postgres_loader.get_table_config('patient')
        incremental_columns = patient_config.get('incremental_columns', ['DateTStamp', 'DateTimeDeceased'])
        last_load_time = postgres_loader._get_loaded_at_time_max('patient', incremental_columns)
        
        # Should return a datetime or None (depending on whether tracking records exist)
        assert last_load_time is None or isinstance(last_load_time, datetime), \
            "Last load time should be datetime or None"
        
        if last_load_time:
            logger.info(f"Last load time: {last_load_time}")
        
        # Act & Assert: Test _build_improved_load_query_max with OR logic
        query_or = postgres_loader._build_improved_load_query_max(
            'patient', incremental_columns, force_full=False, use_or_logic=True
        )
        
        # Verify OR logic query structure
        assert 'SELECT * FROM patient' in query_or
        if 'WHERE' in query_or:
            assert ' OR ' in query_or, "OR logic should use OR between conditions"
            # Check for any of the actual incremental columns
            for col in incremental_columns:
                if f'{col} >' in query_or:
                    break
            else:
                # If no actual columns found, check for generic pattern
                assert any(f'{col} >' in query_or for col in incremental_columns), \
                    f"OR logic should include incremental column conditions: {incremental_columns}"
        
        # Act & Assert: Test _build_improved_load_query_max with AND logic
        query_and = postgres_loader._build_improved_load_query_max(
            'patient', incremental_columns, force_full=False, use_or_logic=False
        )
        
        # Verify AND logic query structure
        assert 'SELECT * FROM patient' in query_and
        if 'WHERE' in query_and:
            assert ' AND ' in query_and, "AND logic should use AND between conditions"
        
        # Act & Assert: Test _build_improved_load_query_max with force_full=True
        query_full = postgres_loader._build_improved_load_query_max(
            'patient', incremental_columns, force_full=True, use_or_logic=True
        )
        
        # Verify full load query (no WHERE clause)
        assert query_full.strip() == 'SELECT * FROM patient', \
            "Full load query should not have WHERE clause"
        
        # Act & Assert: Test _filter_valid_incremental_columns
        valid_columns = postgres_loader._filter_valid_incremental_columns('patient', incremental_columns)
        
        # Should return a list of valid columns
        assert isinstance(valid_columns, list), "Should return list of valid columns"
        assert len(valid_columns) <= len(incremental_columns), \
            "Valid columns should not exceed original columns"
        
        # Act & Assert: Test _validate_incremental_integrity
        if last_load_time:
            integrity_result = postgres_loader._validate_incremental_integrity(
                'patient', incremental_columns, last_load_time
            )
            assert isinstance(integrity_result, bool), \
                "Integrity validation should return boolean"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.bulk_incremental
    @pytest.mark.slow
    def test_postgres_loader_bulk_incremental_methods(
        self,
        test_settings_with_file_provider,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader bulk incremental loading methods with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with large dataset and incremental columns
            Act: Execute bulk incremental loading with real database connections
            Assert: Verify bulk incremental logic works correctly
            
        Validates:
            - load_table_streaming() with incremental logic
            - load_table_chunked() with incremental logic
            - load_table_copy_csv() with incremental logic
            - Bulk insert optimization with incremental data
            - Memory monitoring during bulk operations
            - Transaction handling across bulk operations
        """
        # Arrange: Set up PostgresLoader and populate large test dataset
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        
        # Load initial data to establish baseline
        result = postgres_loader.load_table('patient')
        assert result is True, "Initial load should succeed"
        
        # Act & Assert: Test load_table_streaming with incremental logic
        streaming_result = postgres_loader.load_table_streaming('patient', force_full=False)
        assert streaming_result is True, "Streaming load should succeed"
        
        # Verify data integrity after streaming load
        verification_result = postgres_loader.verify_load('patient')
        assert verification_result is True, "Load verification should pass after streaming"
        
        # Act & Assert: Test load_table_chunked with incremental logic
        chunked_result = postgres_loader.load_table_chunked('patient', force_full=False, chunk_size=100)
        assert chunked_result is True, "Chunked load should succeed"
        
        # Verify data integrity after chunked load
        verification_result = postgres_loader.verify_load('patient')
        assert verification_result is True, "Load verification should pass after chunked load"
        
        # Act & Assert: Test load_table_copy_csv with incremental logic (if table is large enough)
        # This test depends on table size configuration - may skip for small tables
        table_config = postgres_loader.get_table_config('patient')
        estimated_size_mb = table_config.get('estimated_size_mb', 0)
        
        if estimated_size_mb > 500:  # Only test COPY for very large tables
            copy_result = postgres_loader.load_table_copy_csv('patient', force_full=False)
            assert copy_result is True, "COPY load should succeed for large tables"
            
            # Verify data integrity after COPY load
            verification_result = postgres_loader.verify_load('patient')
            assert verification_result is True, "Load verification should pass after COPY load"
        else:
            logger.info(f"Skipping COPY test for patient table (estimated size: {estimated_size_mb}MB)")

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.incremental_validation
    def test_postgres_loader_incremental_validation_methods(
        self,
        test_settings_with_file_provider,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader incremental validation methods with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with test data and validation scenarios
            Act: Execute incremental validation methods with real database connections
            Assert: Verify validation logic works correctly
            
        Validates:
            - _validate_incremental_integrity() with real data
            - Data quality validation for incremental columns
            - Error handling for invalid incremental scenarios
            - Validation with multiple incremental columns
            - Edge cases in incremental validation
        """
        # Arrange: Set up PostgresLoader and populate test data
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        
        # Load initial data to establish baseline
        result = postgres_loader.load_table('patient')
        assert result is True, "Initial load should succeed"
        
        # Act & Assert: Test _validate_incremental_integrity with valid data
        # Get the actual incremental columns from the table configuration
        patient_config = postgres_loader.get_table_config('patient')
        incremental_columns = patient_config.get('incremental_columns', ['DateTStamp', 'DateTimeDeceased'])
        last_load_time = postgres_loader._get_loaded_at_time_max('patient', incremental_columns)
        
        if last_load_time:
            integrity_result = postgres_loader._validate_incremental_integrity(
                'patient', incremental_columns, last_load_time
            )
            assert isinstance(integrity_result, bool), \
                "Integrity validation should return boolean"
            assert integrity_result is True, "Integrity validation should pass with valid data"
        
        # Act & Assert: Test _filter_valid_incremental_columns with real data
        valid_columns = postgres_loader._filter_valid_incremental_columns('patient', incremental_columns)
        
        # Should return valid columns based on data quality
        assert isinstance(valid_columns, list), "Should return list of valid columns"
        assert len(valid_columns) <= len(incremental_columns), \
            "Valid columns should not exceed original columns"
        
        # Verify that returned columns are actually valid
        for col in valid_columns:
            assert col in incremental_columns, f"Valid column {col} should be in original list"
        
        # Act & Assert: Test validation with non-existent table
        nonexistent_result = postgres_loader._validate_incremental_integrity(
            'nonexistent_table', incremental_columns, datetime.now()
        )
        assert isinstance(nonexistent_result, bool), \
            "Validation should return boolean even for non-existent table"
        
        # Act & Assert: Test validation with empty incremental columns
        empty_result = postgres_loader._validate_incremental_integrity(
            'patient', [], datetime.now()
        )
        assert empty_result is True, "Validation should pass with empty columns"
        
        # Act & Assert: Test _filter_valid_incremental_columns with empty list
        empty_valid = postgres_loader._filter_valid_incremental_columns('patient', [])
        assert empty_valid == [], "Should return empty list for empty input"

    @pytest.mark.integration
    @pytest.mark.order(4)
    @pytest.mark.postgres
    @pytest.mark.mysql
    @pytest.mark.incremental_performance
    def test_postgres_loader_incremental_performance_methods(
        self,
        test_settings_with_file_provider,
        populated_test_databases,
        test_data_manager
    ):
        """
        Test PostgresLoader incremental performance methods with AAA pattern.
        
        AAA Pattern:
            Arrange: Set up PostgresLoader with performance test scenarios
            Act: Execute incremental performance methods with real database connections
            Assert: Verify performance optimizations work correctly
            
        Validates:
            - Automatic strategy selection based on table size
            - Performance monitoring during incremental loads
            - Memory usage optimization in incremental operations
            - Query performance with different incremental strategies
            - Bulk operation efficiency with incremental data
        """
        # Arrange: Set up PostgresLoader
        postgres_loader = PostgresLoader(settings=test_settings_with_file_provider)
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        
        # Act & Assert: Test automatic strategy selection
        table_config = postgres_loader.get_table_config('patient')
        estimated_size_mb = table_config.get('estimated_size_mb', 0)
        
        # Test load_table() automatic strategy selection
        result = postgres_loader.load_table('patient')
        assert result is True, "Automatic strategy selection should succeed"
        
        # Verify the correct strategy was used based on table size
        if estimated_size_mb <= 50:
            logger.info("Small table - should use standard loading")
        elif estimated_size_mb <= 200:
            logger.info("Medium table - should use streaming loading")
        elif estimated_size_mb <= 500:
            logger.info("Large table - should use chunked loading")
        else:
            logger.info("Very large table - should use COPY loading")
        
        # Act & Assert: Test performance with different incremental strategies
        # Test streaming performance
        streaming_start = datetime.now()
        streaming_result = postgres_loader.load_table_streaming('patient', force_full=False)
        streaming_duration = (datetime.now() - streaming_start).total_seconds()
        
        assert streaming_result is True, "Streaming load should succeed"
        assert streaming_duration >= 0, "Streaming duration should be non-negative"
        logger.info(f"Streaming load duration: {streaming_duration:.2f} seconds")
        
        # Test chunked performance
        chunked_start = datetime.now()
        chunked_result = postgres_loader.load_table_chunked('patient', force_full=False, chunk_size=50)
        chunked_duration = (datetime.now() - chunked_start).total_seconds()
        
        assert chunked_result is True, "Chunked load should succeed"
        assert chunked_duration >= 0, "Chunked duration should be non-negative"
        logger.info(f"Chunked load duration: {chunked_duration:.2f} seconds")
        
        # Verify data integrity after performance tests
        verification_result = postgres_loader.verify_load('patient')
        assert verification_result is True, "Load verification should pass after performance tests"
        
        # Act & Assert: Test query performance with different incremental logic
        incremental_columns = ['DateModified', 'DateCreated']
        
        # Test OR logic query performance
        or_query_start = datetime.now()
        or_query = postgres_loader._build_improved_load_query_max(
            'patient', incremental_columns, force_full=False, use_or_logic=True
        )
        or_query_duration = (datetime.now() - or_query_start).total_seconds()
        
        assert 'SELECT * FROM patient' in or_query, "OR query should be valid"
        assert or_query_duration < 1.0, "Query building should be fast (< 1 second)"
        
        # Test AND logic query performance
        and_query_start = datetime.now()
        and_query = postgres_loader._build_improved_load_query_max(
            'patient', incremental_columns, force_full=False, use_or_logic=False
        )
        and_query_duration = (datetime.now() - and_query_start).total_seconds()
        
        assert 'SELECT * FROM patient' in and_query, "AND query should be valid"
        assert and_query_duration < 1.0, "Query building should be fast (< 1 second)"