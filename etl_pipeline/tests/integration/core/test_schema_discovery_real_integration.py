"""
Real Integration Testing Approach - Using Real MySQL Database

This approach tests the actual integration flow by using the REAL MySQL OpenDental database
with clearly identifiable test data that won't interfere with production.

Refactored to use new architectural patterns:
- Dependency injection with Settings
- Type-safe DatabaseType and PostgresSchema enums
- Modular fixture organization
- Clean separation of concerns
- Standardized test data management using IntegrationTestDataManager
"""

import pytest
import logging
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Load environment variables from .env file first
from tests.fixtures.env_fixtures import load_test_environment
load_test_environment()

from etl_pipeline.core.schema_discovery import SchemaDiscovery, SchemaNotFoundError
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config import (
    create_test_settings, 
    DatabaseType, 
    PostgresSchema,
    reset_settings
)
from tests.fixtures.test_data_manager import IntegrationTestDataManager
from tests.fixtures.test_data_definitions import (
    get_test_patient_data,
    get_test_appointment_data,
    STANDARD_TEST_PATIENTS,
    STANDARD_TEST_APPOINTMENTS
)

logger = logging.getLogger(__name__)


class TestSchemaDiscoveryRealIntegration:
    """Real integration tests using actual MySQL database with standardized test data."""

    @pytest.fixture(autouse=True)
    def debug_environment(self, test_settings):
        """Log environment and connection info for debugging."""
        import os
        logger.info(f"ETL_ENVIRONMENT: {os.getenv('ETL_ENVIRONMENT')}")
        logger.info(f"TEST_MYSQL_REPLICATION_HOST: {os.getenv('TEST_MYSQL_REPLICATION_HOST')}")
        logger.info(f"TEST_MYSQL_REPLICATION_PORT: {os.getenv('TEST_MYSQL_REPLICATION_PORT')}")
        logger.info(f"TEST_MYSQL_REPLICATION_DB: {os.getenv('TEST_MYSQL_REPLICATION_DB')}")
        logger.info(f"TEST_MYSQL_REPLICATION_USER: {os.getenv('TEST_MYSQL_REPLICATION_USER')}")
        logger.info(f"TEST_MYSQL_REPLICATION_PASSWORD: {os.getenv('TEST_MYSQL_REPLICATION_PASSWORD')}")
        # Print the connection string as seen by the Settings object
        try:
            conn_str = test_settings.get_connection_string(DatabaseType.REPLICATION)
            logger.info(f"Test REPLICATION connection string: {conn_str}")
        except Exception as e:
            logger.error(f"Could not get test replication connection string: {e}")
        yield
    
    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_real_schema_discovery_initialization(self, populated_test_databases, schema_discovery_instance):
        """Test real SchemaDiscovery initialization with actual MySQL database."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        appointment_count = populated_test_databases.get_appointment_count(DatabaseType.REPLICATION)
        
        assert patient_count >= len(STANDARD_TEST_PATIENTS), f"Expected at least {len(STANDARD_TEST_PATIENTS)} patients"
        assert appointment_count >= len(STANDARD_TEST_APPOINTMENTS), f"Expected at least {len(STANDARD_TEST_APPOINTMENTS)} appointments"
        
        # Test REAL SchemaDiscovery with REAL MySQL database
        assert schema_discovery_instance.source_engine is not None
        assert schema_discovery_instance.source_db is not None
        
        # Test real connection
        with schema_discovery_instance.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Real database connection failed"

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_real_table_schema_discovery(self, populated_test_databases, schema_discovery_instance):
        """Test real table schema discovery with actual MySQL database."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Test data not found in database"
        
        # Test REAL schema discovery for patient table
        schema = schema_discovery_instance.get_table_schema('patient')
        assert schema is not None, "Real schema discovery failed"
        assert 'table_name' in schema
        assert 'columns' in schema
        assert 'schema_hash' in schema
        assert schema['table_name'] == 'patient'
        
        # Verify real column discovery using standardized test data fields
        columns = schema['columns']
        column_names = [col['name'] for col in columns]
        
        # Check for key columns from standardized test data
        expected_columns = ['PatNum', 'LName', 'FName', 'MiddleI', 'Preferred', 'PatStatus', 'Gender', 'Position', 'Birthdate', 'SSN']
        
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column {expected_col} not discovered"
        
        # Test schema discovery for appointment table
        appointment_schema = schema_discovery_instance.get_table_schema('appointment')
        assert appointment_schema is not None, "Appointment schema discovery failed"
        assert appointment_schema['table_name'] == 'appointment'

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_real_table_size_discovery(self, populated_test_databases, schema_discovery_instance):
        """Test real table size discovery with actual MySQL database."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        appointment_count = populated_test_databases.get_appointment_count(DatabaseType.REPLICATION)
        
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Test data not found in database"
        assert appointment_count >= len(STANDARD_TEST_APPOINTMENTS), "Test data not found in database"
        
        # Test REAL table size discovery
        patient_size_info = schema_discovery_instance.get_table_size_info('patient')
        assert patient_size_info is not None, "Real table size discovery failed"
        assert patient_size_info['row_count'] >= len(STANDARD_TEST_PATIENTS), f"Expected at least {len(STANDARD_TEST_PATIENTS)} patients"
        
        appointment_size_info = schema_discovery_instance.get_table_size_info('appointment')
        assert appointment_size_info is not None, "Appointment table size discovery failed"
        assert appointment_size_info['row_count'] >= len(STANDARD_TEST_APPOINTMENTS), f"Expected at least {len(STANDARD_TEST_APPOINTMENTS)} appointments"

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_real_database_schema_overview(self, populated_test_databases, schema_discovery_instance):
        """Test real database schema overview with actual MySQL database."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Test data not found in database"
        
        # Test REAL database schema overview using discover_all_tables
        tables = schema_discovery_instance.discover_all_tables()
        assert tables is not None, "Real database schema overview failed"
        assert len(tables) > 0, "Expected at least one table"
        
        # Verify our test tables are in the overview
        expected_tables = ['patient', 'appointment']
        
        for expected_table in expected_tables:
            assert expected_table in tables, f"Table {expected_table} not found in overview"

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_real_schema_hash_consistency(self, populated_test_databases, schema_discovery_instance):
        """Test real schema hash consistency with actual MySQL database."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Test data not found in database"
        
        # Test that schema hash is consistent for the same table
        schema1 = schema_discovery_instance.get_table_schema('patient')
        schema2 = schema_discovery_instance.get_table_schema('patient')
        
        assert schema1['schema_hash'] == schema2['schema_hash'], "Schema hash should be consistent"
        
        # Test that different tables have different hashes
        patient_schema = schema_discovery_instance.get_table_schema('patient')
        appointment_schema = schema_discovery_instance.get_table_schema('appointment')
        
        assert patient_schema['schema_hash'] != appointment_schema['schema_hash'], "Different tables should have different hashes"

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_real_error_handling(self, test_data_manager, schema_discovery_instance):
        """Test real error handling with actual component failures."""
        # Test with non-existent table - should raise SchemaNotFoundError
        with pytest.raises(SchemaNotFoundError):
            schema_discovery_instance.get_table_schema('nonexistent_table')
        
        # Test with non-existent table size - should return zero values
        size_info = schema_discovery_instance.get_table_size_info('nonexistent_table')
        assert size_info['row_count'] == 0, "Should return 0 for non-existent table"

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_real_column_details_discovery(self, populated_test_databases, schema_discovery_instance):
        """Test real column details discovery with actual MySQL database."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Test data not found in database"
        
        # Test REAL column details discovery
        schema = schema_discovery_instance.get_table_schema('patient')
        columns = schema['columns']
        
        # Find PatNum column
        patnum_col = next((col for col in columns if col['name'] == 'PatNum'), None)
        assert patnum_col is not None, "PatNum column not found"
        assert 'type' in patnum_col, "Column type not discovered"
        assert 'is_nullable' in patnum_col, "Column nullable status not discovered"
        
        # Find LName column
        lname_col = next((col for col in columns if col['name'] == 'LName'), None)
        assert lname_col is not None, "LName column not found"
        assert 'type' in lname_col, "Column type not discovered"

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_real_multiple_table_schema_discovery(self, populated_test_databases, schema_discovery_instance):
        """Test schema discovery for multiple tables with real data."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        appointment_count = populated_test_databases.get_appointment_count(DatabaseType.REPLICATION)
        
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Test data not found in database"
        assert appointment_count >= len(STANDARD_TEST_APPOINTMENTS), "Test data not found in database"
        
        # Test schema discovery for multiple tables
        test_tables = ['patient', 'appointment']
        schemas = {}
        
        for table in test_tables:
            schema = schema_discovery_instance.get_table_schema(table)
            assert schema is not None, f"Schema discovery failed for {table}"
            assert schema['table_name'] == table, f"Table name mismatch for {table}"
            schemas[table] = schema
        
        # Verify all schemas are different
        schema_hashes = [schemas[table]['schema_hash'] for table in test_tables]
        assert len(set(schema_hashes)) == len(schema_hashes), "All tables should have different schema hashes"

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_type_safe_database_enum_usage(self, test_data_manager, schema_discovery_instance, test_env_vars):
        """Test that type-safe database enums work correctly with test environment."""
        # Test DatabaseType enum usage - compare enum values, not enum objects
        assert DatabaseType.SOURCE.value == "source", "DatabaseType.SOURCE should be 'source'"
        assert DatabaseType.REPLICATION.value == "replication", "DatabaseType.REPLICATION should be 'replication'"
        assert DatabaseType.ANALYTICS.value == "analytics", "DatabaseType.ANALYTICS should be 'analytics'"
        
        # Test PostgresSchema enum usage - compare enum values, not enum objects
        assert PostgresSchema.RAW.value == "raw", "PostgresSchema.RAW should be 'raw'"
        assert PostgresSchema.STAGING.value == "staging", "PostgresSchema.STAGING should be 'staging'"
        assert PostgresSchema.INTERMEDIATE.value == "intermediate", "PostgresSchema.INTERMEDIATE should be 'intermediate'"
        assert PostgresSchema.MARTS.value == "marts", "PostgresSchema.MARTS should be 'marts'"
        
        # Test that enums work with ConnectionFactory using test environment
        settings = create_test_settings(env_vars=test_env_vars)
        
        # Test replication connection (which we're using for this test)
        replication_engine = ConnectionFactory.get_replication_connection(settings)
        assert replication_engine is not None, "Replication engine should be created with test connection method"
        
        # Test analytics connection with schema
        analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
        assert analytics_engine is not None, "Analytics engine should be created with test connection method"

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_schema_discovery_with_tables_yml_validation(self, populated_test_databases, schema_discovery_instance):
        """Test that schema discovery finds tables that match tables.yml configuration."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Test data not found in database"
        
        # Discover all tables
        discovered_tables = schema_discovery_instance.discover_all_tables()
        assert discovered_tables is not None, "Table discovery failed"
        
        # Check that key dental tables are present (these should exist in any OpenDental database)
        key_tables = ['patient', 'appointment']
        for table in key_tables:
            assert table in discovered_tables, f"Key table {table} not found in discovered tables"
        
        # Log discovered tables for debugging
        logger.info(f"Discovered {len(discovered_tables)} tables: {discovered_tables[:10]}...")  # Show first 10

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_schema_discovery_against_configured_tables(self, populated_test_databases, schema_discovery_instance):
        """Test that schema discovery can find and validate tables that are configured in tables.yml."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Test data not found in database"
        
        # Load tables.yml configuration
        import yaml
        from pathlib import Path
        
        # Find tables.yml file
        config_dir = Path(__file__).parent.parent.parent.parent / 'etl_pipeline' / 'config'
        tables_yml_path = config_dir / 'tables.yml'
        
        if not tables_yml_path.exists():
            pytest.skip(f"tables.yml not found at {tables_yml_path}")
        
        # Load configuration
        with open(tables_yml_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Extract all table names from configuration
        configured_tables = list(config.get('tables', {}).keys())
        assert len(configured_tables) > 0, "No tables found in tables.yml configuration"
        
        logger.info(f"Found {len(configured_tables)} tables in tables.yml configuration")
        
        # Discover all tables from the database
        discovered_tables = schema_discovery_instance.discover_all_tables()
        assert discovered_tables is not None, "Table discovery failed"
        
        logger.info(f"Discovered {len(discovered_tables)} tables in source database")
        
        # For integration tests, we only validate that:
        # 1. The schema discovery mechanism works correctly
        # 2. We can find at least some of the configured tables (the ones in our test database)
        # 3. The configuration loading and comparison logic works
        
        # Find which configured tables are actually in our test database
        available_configured_tables = [table for table in configured_tables if table in discovered_tables]
        
        logger.info(f"Found {len(available_configured_tables)} configured tables in test database: {available_configured_tables}")
        
        # Assert that we have at least some configured tables available for testing
        assert len(available_configured_tables) > 0, (
            f"No configured tables found in test database. "
            f"Test database has: {discovered_tables}, "
            f"but none match configured tables from tables.yml"
        )
        
        # Test that we can get schema for available configured tables
        for table_name in available_configured_tables:
            try:
                schema = schema_discovery_instance.get_table_schema(table_name)
                assert schema is not None, f"Failed to get schema for {table_name}"
                assert schema['table_name'] == table_name, f"Schema table name mismatch for {table_name}"
                logger.info(f"Successfully retrieved schema for {table_name}")
            except Exception as e:
                logger.error(f"Failed to get schema for {table_name}: {e}")
                raise
        
        # Test that the configuration validation logic works by checking a few missing tables
        missing_tables = [table for table in configured_tables if table not in discovered_tables]
        logger.info(f"Note: {len(missing_tables)} configured tables not in test database (expected for integration tests)")
        
        # Verify that our test database has the expected test tables
        expected_test_tables = ['patient', 'appointment']
        for table in expected_test_tables:
            assert table in discovered_tables, f"Expected test table {table} not found in database"
        
        logger.info(f"Schema discovery integration test passed - found {len(available_configured_tables)} configured tables")

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_standardized_test_data_integration(self, populated_test_databases, schema_discovery_instance):
        """Test that the standardized test data is properly integrated and discoverable."""
        # Verify test data exists using standardized data manager
        patient_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
        appointment_count = populated_test_databases.get_appointment_count(DatabaseType.REPLICATION)
        
        assert patient_count >= len(STANDARD_TEST_PATIENTS), "Standardized test patients not found"
        assert appointment_count >= len(STANDARD_TEST_APPOINTMENTS), "Standardized test appointments not found"
        
        # Test that we can discover the standardized test data structure
        patient_schema = schema_discovery_instance.get_table_schema('patient')
        appointment_schema = schema_discovery_instance.get_table_schema('appointment')
        
        # Verify that the schema matches the standardized test data structure
        patient_columns = [col['name'] for col in patient_schema['columns']]
        appointment_columns = [col['name'] for col in appointment_schema['columns']]
        
        # Check that key fields from standardized test data are present
        expected_patient_fields = ['PatNum', 'LName', 'FName', 'Birthdate', 'SSN']
        expected_appointment_fields = ['AptNum', 'PatNum', 'AptDateTime', 'AptStatus']
        
        for field in expected_patient_fields:
            assert field in patient_columns, f"Expected patient field {field} not found in schema"
        
        for field in expected_appointment_fields:
            assert field in appointment_columns, f"Expected appointment field {field} not found in schema"
        
        logger.info("Standardized test data integration test passed")

    @pytest.mark.integration
    @pytest.mark.order(1)
    def test_test_environment_connection_methods(self):
        """Test that test environment connection methods work correctly.
        
        This test specifically verifies that we're using the test connection methods
        as specified in the connection environment separation documentation.
        """
        # Test the explicit test connection methods that use TEST_* environment variables
        source_engine = ConnectionFactory.get_opendental_source_test_connection()
        replication_engine = ConnectionFactory.get_mysql_replication_test_connection()
        analytics_engine = ConnectionFactory.get_postgres_analytics_test_connection()
        
        # Verify engines are created
        assert source_engine is not None, "Test source engine should be created"
        assert replication_engine is not None, "Test replication engine should be created"
        assert analytics_engine is not None, "Test analytics engine should be created"
        
        # Test basic connectivity to test databases
        with source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Test source engine connectivity failed"
        
        with replication_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Test replication engine connectivity failed"
        
        with analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Test analytics engine connectivity failed"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 