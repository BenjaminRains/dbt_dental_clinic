"""
Real Integration Testing Approach - Using Real MySQL Database

This approach tests the actual integration flow by using the REAL MySQL OpenDental database
with clearly identifiable test data that won't interfere with production.

Refactored to use new architectural patterns:
- Dependency injection with Settings
- Type-safe DatabaseType and PostgresSchema enums
- Modular fixture organization
- Clean separation of concerns
"""

import pytest
import logging
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import List, Dict, Any

from etl_pipeline.core.schema_discovery import SchemaDiscovery, SchemaNotFoundError
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config import (
    create_test_settings, 
    DatabaseType, 
    PostgresSchema,
    reset_settings
)

logger = logging.getLogger(__name__)


class TestSchemaDiscoveryRealIntegration:
    """Real integration tests using actual MySQL database with test data."""
    
    @pytest.mark.integration
    def test_real_schema_discovery_initialization(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test real SchemaDiscovery initialization with actual MySQL database."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL SchemaDiscovery with REAL MySQL database
        assert schema_discovery_instance.source_engine is not None
        assert schema_discovery_instance.source_db is not None
        
        # Test real connection
        with schema_discovery_instance.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Real database connection failed"

    @pytest.mark.integration
    def test_real_table_schema_discovery(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test real table schema discovery with actual MySQL database."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL schema discovery for patient table
        schema = schema_discovery_instance.get_table_schema('patient')
        assert schema is not None, "Real schema discovery failed"
        assert 'table_name' in schema
        assert 'columns' in schema
        assert 'schema_hash' in schema
        assert schema['table_name'] == 'patient'
        
        # Verify real column discovery
        columns = schema['columns']
        column_names = [col['name'] for col in columns]
        expected_columns = ['PatNum', 'LName', 'FName', 'MiddleI', 'Preferred', 'PatStatus', 'Gender', 'Position', 'Birthdate', 'SSN']
        
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column {expected_col} not discovered"
        
        # Test schema discovery for appointment table
        appointment_schema = schema_discovery_instance.get_table_schema('appointment')
        assert appointment_schema is not None, "Appointment schema discovery failed"
        assert appointment_schema['table_name'] == 'appointment'

    @pytest.mark.integration
    def test_real_table_size_discovery(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test real table size discovery with actual MySQL database."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL table size discovery
        patient_size_info = schema_discovery_instance.get_table_size_info('patient')
        assert patient_size_info is not None, "Real table size discovery failed"
        assert patient_size_info['row_count'] >= len(schema_discovery_test_data_manager.test_patients), f"Expected at least {len(schema_discovery_test_data_manager.test_patients)} patients"
        
        appointment_size_info = schema_discovery_instance.get_table_size_info('appointment')
        assert appointment_size_info is not None, "Appointment table size discovery failed"
        assert appointment_size_info['row_count'] >= len(schema_discovery_test_data_manager.test_appointments), f"Expected at least {len(schema_discovery_test_data_manager.test_appointments)} appointments"
        
        procedure_size_info = schema_discovery_instance.get_table_size_info('procedure')
        assert procedure_size_info is not None, "Procedure table size discovery failed"
        assert procedure_size_info['row_count'] >= len(schema_discovery_test_data_manager.test_procedures), f"Expected at least {len(schema_discovery_test_data_manager.test_procedures)} procedures"

    @pytest.mark.integration
    def test_real_database_schema_overview(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test real database schema overview with actual MySQL database."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL database schema overview using discover_all_tables
        tables = schema_discovery_instance.discover_all_tables()
        assert tables is not None, "Real database schema overview failed"
        assert len(tables) > 0, "Expected at least one table"
        
        # Verify our test tables are in the overview
        expected_tables = ['patient', 'appointment', 'procedure']
        
        for expected_table in expected_tables:
            assert expected_table in tables, f"Table {expected_table} not found in overview"

    @pytest.mark.integration
    def test_real_schema_hash_consistency(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test real schema hash consistency with actual MySQL database."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test that schema hash is consistent for the same table
        schema1 = schema_discovery_instance.get_table_schema('patient')
        schema2 = schema_discovery_instance.get_table_schema('patient')
        
        assert schema1['schema_hash'] == schema2['schema_hash'], "Schema hash should be consistent"
        
        # Test that different tables have different hashes
        patient_schema = schema_discovery_instance.get_table_schema('patient')
        appointment_schema = schema_discovery_instance.get_table_schema('appointment')
        
        assert patient_schema['schema_hash'] != appointment_schema['schema_hash'], "Different tables should have different hashes"

    @pytest.mark.integration
    def test_real_error_handling(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test real error handling with actual component failures."""
        # Test with non-existent table - should raise SchemaNotFoundError
        with pytest.raises(SchemaNotFoundError):
            schema_discovery_instance.get_table_schema('nonexistent_table')
        
        # Test with non-existent table size - should return zero values
        size_info = schema_discovery_instance.get_table_size_info('nonexistent_table')
        assert size_info['row_count'] == 0, "Should return 0 for non-existent table"

    @pytest.mark.integration
    def test_real_column_details_discovery(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test real column details discovery with actual MySQL database."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
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
    def test_real_multiple_table_schema_discovery(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test schema discovery for multiple tables with real data."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test schema discovery for multiple tables
        test_tables = ['patient', 'appointment', 'procedure']
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
    def test_type_safe_database_enum_usage(self, schema_discovery_test_data_manager, schema_discovery_instance, test_env_vars):
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
    def test_schema_discovery_with_tables_yml_validation(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test that schema discovery finds tables that match tables.yml configuration."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Discover all tables
        discovered_tables = schema_discovery_instance.discover_all_tables()
        assert discovered_tables is not None, "Table discovery failed"
        
        # Check that key dental tables are present (these should exist in any OpenDental database)
        key_tables = ['patient', 'appointment', 'procedure']
        for table in key_tables:
            assert table in discovered_tables, f"Key table {table} not found in discovered tables"
        
        # Log discovered tables for debugging
        logger.info(f"Discovered {len(discovered_tables)} tables: {discovered_tables[:10]}...")  # Show first 10

    @pytest.mark.integration
    def test_schema_discovery_against_configured_tables(self, schema_discovery_test_data_manager, schema_discovery_instance):
        """Test that schema discovery can find and validate tables that are configured in tables.yml."""
        # Verify test data exists
        assert schema_discovery_test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
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
        expected_test_tables = ['patient', 'appointment', 'procedure']
        for table in expected_test_tables:
            assert table in discovered_tables, f"Expected test table {table} not found in database"
        
        logger.info(f"Schema discovery integration test passed - found {len(available_configured_tables)} configured tables")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 