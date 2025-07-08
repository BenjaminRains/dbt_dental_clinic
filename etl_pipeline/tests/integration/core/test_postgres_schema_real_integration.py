"""
Real Integration Testing for PostgresSchema - Using Real MySQL and PostgreSQL Databases

This approach tests the actual PostgreSQL schema conversion flow by using the REAL MySQL and PostgreSQL databases
at with standardized test data that won't interfere with production.

Refactored to use new architectural patterns:
- Dependency injection with Settings
- Type-safe DatabaseType and PostgresSchema enums
- Modular fixture organization
- Clean separation of concerns
- Standardized test data management
"""

import pytest
import logging
import os
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Load environment variables from .env file first
from tests.fixtures.env_fixtures import load_test_environment
load_test_environment()

# Ensure test environment is set
os.environ['ETL_ENVIRONMENT'] = 'test'

from etl_pipeline.core.postgres_schema import PostgresSchema as PostgresSchemaClass
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.config.config_reader import ConfigReader

# Import new configuration system
from etl_pipeline.config import (
    create_test_settings, 
    DatabaseType, 
    PostgresSchema,
    reset_settings
)

# Import standardized test fixtures
from tests.fixtures import (
    populated_test_databases,
    test_data_manager,
    test_settings,
    get_test_patient_data,
    get_test_appointment_data
)

logger = logging.getLogger(__name__)


class TestPostgresSchemaRealIntegration:
    """Real integration tests using actual MySQL and PostgreSQL databases with standardized test data."""

    @pytest.fixture(autouse=True)
    def debug_environment(self, test_settings):
        """Log environment and connection info for debugging."""
        logger.info(f"ETL_ENVIRONMENT: {os.getenv('ETL_ENVIRONMENT')}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_HOST: {os.getenv('TEST_OPENDENTAL_SOURCE_HOST')}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_PORT: {os.getenv('TEST_OPENDENTAL_SOURCE_PORT')}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_DB: {os.getenv('TEST_OPENDENTAL_SOURCE_DB')}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_USER: {os.getenv('TEST_OPENDENTAL_SOURCE_USER')}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_PASSWORD: {os.getenv('TEST_OPENDENTAL_SOURCE_PASSWORD')}")
        logger.info(f"TEST_MYSQL_REPLICATION_HOST: {os.getenv('TEST_MYSQL_REPLICATION_HOST')}")
        logger.info(f"TEST_MYSQL_REPLICATION_DB: {os.getenv('TEST_MYSQL_REPLICATION_DB')}")
        logger.info(f"TEST_POSTGRES_ANALYTICS_HOST: {os.getenv('TEST_POSTGRES_ANALYTICS_HOST')}")
        logger.info(f"TEST_POSTGRES_ANALYTICS_DB: {os.getenv('TEST_POSTGRES_ANALYTICS_DB')}")
        
        # Print the connection configuration as seen by the Settings object
        try:
            source_config = test_settings.get_database_config(DatabaseType.SOURCE)
            logger.info(f"Test SOURCE connection config: {source_config}")
        except Exception as e:
            logger.error(f"Could not get test source connection config: {e}")
        yield
    
    @pytest.fixture
    def postgres_schema_test_settings(self, test_settings):
        """Provide test settings for PostgresSchema tests."""
        return test_settings

    @pytest.fixture
    def config_reader(self):
        """Create a ConfigReader instance for schema configuration."""
        return ConfigReader()

    @pytest.fixture
    def real_postgres_schema_instance(self, postgres_schema_test_settings):
        """Create a real PostgresSchema instance using test environment."""
        return PostgresSchemaClass(
            postgres_schema=PostgresSchema.RAW.value,
            settings=postgres_schema_test_settings
        )



    @pytest.fixture
    def postgres_schema_test_tables(self):
        """Define test tables for schema conversion testing."""
        return ['patient', 'appointment', 'procedure']

    @pytest.fixture
    def postgres_schema_error_cases(self):
        """Define error cases for testing error handling."""
        return {
            'malformed_schema': {
                'create_statement': 'INVALID SQL STATEMENT',
                'columns': []
            }
        }

    @pytest.mark.integration
    @pytest.mark.order(3)
    def test_real_postgres_schema_initialization(self, populated_test_databases, real_postgres_schema_instance):
        """Test real PostgresSchema initialization with actual databases."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        appointment_count = manager.get_appointment_count(DatabaseType.REPLICATION)
        
        assert patient_count > 0, "Test patient data not found in replication database"
        assert appointment_count > 0, "Test appointment data not found in replication database"
        
        # Test REAL PostgresSchema with REAL databases
        assert real_postgres_schema_instance.mysql_engine is not None
        assert real_postgres_schema_instance.postgres_engine is not None
        assert real_postgres_schema_instance.mysql_db is not None
        assert real_postgres_schema_instance.postgres_db is not None
        assert real_postgres_schema_instance.postgres_schema == 'raw'
        
        # Test real connections
        with real_postgres_schema_instance.mysql_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "MySQL database connection failed"
        
        with real_postgres_schema_instance.postgres_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "PostgreSQL database connection failed"

    @pytest.mark.integration
    @pytest.mark.order(3)
    def test_real_schema_adaptation(self, populated_test_databases, real_postgres_schema_instance, config_reader):
        """Test real schema adaptation from MySQL to PostgreSQL."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count > 0, "Test patient data not found in replication database"
        
        # Get table configuration from ConfigReader
        patient_config = config_reader.get_table_config('patient')
        assert patient_config is not None, "Patient table configuration not found"
        
        # Get MySQL schema for patient table using the new method
        mysql_schema = real_postgres_schema_instance.get_table_schema_from_mysql('patient')
        assert mysql_schema is not None, "MySQL schema discovery failed"
        
        # Test REAL schema adaptation
        pg_create_statement = real_postgres_schema_instance.adapt_schema('patient', mysql_schema)
        assert pg_create_statement is not None, "Schema adaptation failed"
        assert 'CREATE TABLE raw.patient' in pg_create_statement, "PostgreSQL CREATE statement not found"
        assert 'PatNum' in pg_create_statement, "Patient column not found in adapted schema"
        assert 'LName' in pg_create_statement, "LName column not found in adapted schema"

    @pytest.mark.integration
    @pytest.mark.order(3)
    def test_real_postgres_table_creation(self, populated_test_databases, real_postgres_schema_instance, config_reader):
        """Test real PostgreSQL table creation from MySQL schema."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count > 0, "Test patient data not found in replication database"
        
        # Get table configuration from ConfigReader
        patient_config = config_reader.get_table_config('patient')
        assert patient_config is not None, "Patient table configuration not found"
        
        # Get MySQL schema for patient table using the new method
        mysql_schema = real_postgres_schema_instance.get_table_schema_from_mysql('patient')
        assert mysql_schema is not None, "MySQL schema discovery failed"
        
        # Test REAL PostgreSQL table creation
        result = real_postgres_schema_instance.create_postgres_table('patient', mysql_schema)
        assert result, "PostgreSQL table creation failed"
        
        # Verify the table was created in PostgreSQL
        with real_postgres_schema_instance.postgres_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'raw' AND table_name = 'patient'
            """))
            assert result.fetchone() is not None, "Patient table not created in PostgreSQL"

    @pytest.mark.integration
    def test_real_schema_verification(self, populated_test_databases, real_postgres_schema_instance, config_reader):
        """Test real schema verification between MySQL and PostgreSQL."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count > 0, "Test patient data not found in replication database"
        
        # Get table configuration from ConfigReader
        patient_config = config_reader.get_table_config('patient')
        assert patient_config is not None, "Patient table configuration not found"
        
        # Get MySQL schema for patient table using the new method
        mysql_schema = real_postgres_schema_instance.get_table_schema_from_mysql('patient')
        assert mysql_schema is not None, "MySQL schema discovery failed"
        
        # Create PostgreSQL table
        assert real_postgres_schema_instance.create_postgres_table('patient', mysql_schema), "Failed to create PostgreSQL table"
        
        # Test REAL schema verification
        result = real_postgres_schema_instance.verify_schema('patient', mysql_schema)
        assert result, "Schema verification failed"

    @pytest.mark.integration
    def test_real_boolean_type_detection(self, test_data_manager, real_postgres_schema_instance, config_reader):
        """Test real boolean type detection for TINYINT columns."""
        # Set up boolean test data using standardized methods
        test_data_manager.setup_patient_data(include_all_fields=True, database_types=[DatabaseType.REPLICATION])
        
        # Verify test data exists using standardized methods
        patient_count = test_data_manager.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count > 0, "Test patient data not found in replication database"
        
        # Get table configuration from ConfigReader
        patient_config = config_reader.get_table_config('patient')
        assert patient_config is not None, "Patient table configuration not found"
        
        # Get MySQL schema for patient table (contains boolean fields) using the new method
        mysql_schema = real_postgres_schema_instance.get_table_schema_from_mysql('patient')
        assert mysql_schema is not None, "MySQL schema discovery failed"
        
        # Test REAL boolean type detection through schema adaptation
        pg_create_statement = real_postgres_schema_instance.adapt_schema('patient', mysql_schema)
        assert pg_create_statement is not None, "Schema adaptation failed"
        
        # Check that boolean columns are detected correctly
        # PatStatus, Gender, Position should be boolean (only 0/1 values in test data)
        assert '"PatStatus" boolean' in pg_create_statement, "PatStatus should be detected as boolean"
        assert '"Gender" boolean' in pg_create_statement, "Gender should be detected as boolean"
        assert '"Position" boolean' in pg_create_statement, "Position should be detected as boolean"

    @pytest.mark.integration
    def test_real_multiple_table_schema_conversion(self, populated_test_databases, real_postgres_schema_instance, config_reader, postgres_schema_test_tables):
        """Test schema conversion for multiple tables with real data."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        appointment_count = manager.get_appointment_count(DatabaseType.REPLICATION)
        
        assert patient_count > 0, "Test patient data not found in replication database"
        assert appointment_count > 0, "Test appointment data not found in replication database"
        
        # Test schema conversion for multiple tables
        for table in postgres_schema_test_tables:
            logger.info(f"Testing schema conversion for table: {table}")
            
            # Get table configuration from ConfigReader
            table_config = config_reader.get_table_config(table)
            assert table_config is not None, f"Table configuration not found for {table}"
            
            # Get MySQL schema using the new method
            mysql_schema = real_postgres_schema_instance.get_table_schema_from_mysql(table)
            assert mysql_schema is not None, f"MySQL schema discovery failed for {table}"
            
            # Adapt schema
            pg_create_statement = real_postgres_schema_instance.adapt_schema(table, mysql_schema)
            assert pg_create_statement is not None, f"Schema adaptation failed for {table}"
            assert f'CREATE TABLE raw.{table}' in pg_create_statement, f"PostgreSQL CREATE statement not found for {table}"
            
            # Create PostgreSQL table
            result = real_postgres_schema_instance.create_postgres_table(table, mysql_schema)
            assert result, f"PostgreSQL table creation failed for {table}"
            
            # Verify schema
            result = real_postgres_schema_instance.verify_schema(table, mysql_schema)
            assert result, f"Schema verification failed for {table}"
            
            logger.info(f"Successfully converted schema for table: {table}")

    @pytest.mark.integration
    def test_real_type_conversion_accuracy(self, populated_test_databases, real_postgres_schema_instance, config_reader):
        """Test real type conversion accuracy for various MySQL types."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count > 0, "Test patient data not found in replication database"
        
        # Get table configuration from ConfigReader
        patient_config = config_reader.get_table_config('patient')
        assert patient_config is not None, "Patient table configuration not found"
        
        # Get MySQL schema for patient table (has various data types) using the new method
        mysql_schema = real_postgres_schema_instance.get_table_schema_from_mysql('patient')
        assert mysql_schema is not None, "MySQL schema discovery failed"
        
        # Test REAL type conversion
        pg_create_statement = real_postgres_schema_instance.adapt_schema('patient', mysql_schema)
        assert pg_create_statement is not None, "Schema adaptation failed"
        
        # Check specific type conversions
        assert '"PatNum" bigint' in pg_create_statement, "INT should convert to bigint"
        assert '"LName" character varying' in pg_create_statement, "VARCHAR should convert to character varying"
        assert '"Birthdate" date' in pg_create_statement, "DATE should convert to date"
        # All these TINYINT columns are 0/1 in test data, so should be boolean
        assert '"PatStatus" boolean' in pg_create_statement, "TINYINT with 0/1 should convert to boolean"
        assert '"Gender" boolean' in pg_create_statement, "TINYINT with 0/1 should convert to boolean"
        assert '"Position" boolean' in pg_create_statement, "TINYINT with 0/1 should convert to boolean"

    @pytest.mark.integration
    def test_real_error_handling(self, real_postgres_schema_instance, postgres_schema_error_cases):
        """Test real error handling with actual component failures."""
        # Test with malformed schema that will cause an exception
        malformed_schema = postgres_schema_error_cases['malformed_schema']
        
        # Test schema adaptation with malformed schema (should raise exception)
        with pytest.raises(Exception):
            real_postgres_schema_instance.adapt_schema('nonexistent', malformed_schema)
        
        # Test table creation with malformed schema (should return False)
        result = real_postgres_schema_instance.create_postgres_table('nonexistent', malformed_schema)
        assert not result, "Should fail for malformed schema"
        
        # Test schema verification with non-existent table (should return False)
        result = real_postgres_schema_instance.verify_schema('nonexistent', malformed_schema)
        assert not result, "Should fail for non-existent table"

    @pytest.mark.integration
    def test_real_config_reader_integration(self, populated_test_databases, real_postgres_schema_instance, config_reader):
        """Test real ConfigReader integration with PostgresSchema."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count > 0, "Test patient data not found in replication database"
        
        # Test that ConfigReader is properly integrated
        patient_config = config_reader.get_table_config('patient')
        assert patient_config is not None, "ConfigReader integration failed"
        assert 'primary_key' in patient_config, "Patient table configuration missing primary_key"
        
        # Test schema discovery using the new method
        mysql_schema = real_postgres_schema_instance.get_table_schema_from_mysql('patient')
        assert mysql_schema is not None, "Schema discovery integration failed"
        assert 'create_statement' in mysql_schema, "Schema discovery create_statement not found"
        
        # Test schema adaptation using schema discovery output
        pg_create_statement = real_postgres_schema_instance.adapt_schema('patient', mysql_schema)
        assert pg_create_statement is not None, "Schema adaptation with schema discovery failed"

    @pytest.mark.integration
    def test_real_data_type_analysis(self, populated_test_databases, real_postgres_schema_instance):
        """Test real data type analysis for intelligent type conversion."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count > 0, "Test patient data not found in replication database"
        
        # Test boolean detection for PatStatus column (should be boolean)
        pg_type = real_postgres_schema_instance._convert_mysql_type('TINYINT', 'patient', 'PatStatus')
        assert pg_type == 'boolean', f"PatStatus should be detected as boolean, got {pg_type}"
        
        # Test boolean detection for Gender column (should be boolean)
        pg_type = real_postgres_schema_instance._convert_mysql_type('TINYINT', 'patient', 'Gender')
        assert pg_type == 'boolean', f"Gender should be detected as boolean, got {pg_type}"
        
        # Test boolean detection for Position column (should be boolean)
        pg_type = real_postgres_schema_instance._convert_mysql_type('TINYINT', 'patient', 'Position')
        assert pg_type == 'boolean', f"Position should be detected as boolean, got {pg_type}"

    @pytest.mark.integration
    def test_real_schema_cache_functionality(self, populated_test_databases, real_postgres_schema_instance, config_reader):
        """Test real schema cache functionality."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.REPLICATION)
        assert patient_count > 0, "Test patient data not found in replication database"
        
        # Get table configuration from ConfigReader
        patient_config = config_reader.get_table_config('patient')
        assert patient_config is not None, "Patient table configuration not found"
        
        # Get MySQL schema for patient table using the new method
        mysql_schema = real_postgres_schema_instance.get_table_schema_from_mysql('patient')
        assert mysql_schema is not None, "MySQL schema discovery failed"
        
        # Test schema adaptation multiple times (should use cache)
        pg_create_1 = real_postgres_schema_instance.adapt_schema('patient', mysql_schema)
        pg_create_2 = real_postgres_schema_instance.adapt_schema('patient', mysql_schema)
        
        assert pg_create_1 == pg_create_2, "Schema adaptation should be consistent (cached)"
        assert pg_create_1 is not None, "Schema adaptation failed"

    @pytest.mark.integration
    def test_new_architecture_connection_methods(self, test_settings):
        """Test that new architecture connection methods work correctly with test environment."""
        # Test new ConnectionFactory methods with Settings dependency injection
        # These should automatically use test environment when ETL_ENVIRONMENT=test
        
        # Test the new connection methods with test settings
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        analytics_engine = ConnectionFactory.get_analytics_connection(test_settings, PostgresSchema.RAW)
        
        # Verify engines are created
        assert source_engine is not None, "Test source engine should be created"
        assert replication_engine is not None, "Test replication engine should be created"
        assert analytics_engine is not None, "Test analytics engine should be created"
        
        # Test basic connectivity
        with source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Test source engine connectivity failed"
        
        with replication_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Test replication engine connectivity failed"
        
        with analytics_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Test analytics engine connectivity failed"

    @pytest.mark.integration
    def test_type_safe_database_enum_usage(self, test_settings):
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
        analytics_engine = ConnectionFactory.get_analytics_connection(test_settings, PostgresSchema.RAW)
        assert analytics_engine is not None, "Analytics engine should be created with test connection method"

    @pytest.mark.integration
    def test_test_environment_connection_methods(self, test_settings):
        """Test that test environment connection methods work correctly.
        
        This test specifically verifies that we're using the test connection methods
        as specified in the connection environment separation documentation.
        """
        # Test the new connection methods with test settings
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        analytics_engine = ConnectionFactory.get_analytics_connection(test_settings, PostgresSchema.RAW)
        
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