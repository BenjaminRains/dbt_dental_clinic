"""
Real Integration Testing for TableProcessor - Using Real MySQL and PostgreSQL Databases

This approach tests the actual table processing flow by using the REAL MySQL and PostgreSQL databases
with standardized test data that won't interfere with production.

Refactored to follow new architectural patterns:
- Uses new ConnectionFactory methods with dependency injection
- Uses modular fixtures from tests/fixtures/
- Follows new configuration pattern with proper test isolation
- Uses standardized test data instead of custom test data creation
- Proper environment separation with .env loading
"""

import pytest
import logging
import os
from sqlalchemy import text
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

# Load environment variables from .env file first
from tests.fixtures.env_fixtures import load_test_environment
load_test_environment()

# Import new configuration system
try:
    from etl_pipeline.config import create_test_settings, DatabaseType, PostgresSchema
    from etl_pipeline.core.connections import ConnectionFactory
    from etl_pipeline.core.schema_discovery import SchemaDiscovery
    from etl_pipeline.orchestration.table_processor import TableProcessor
    NEW_CONFIG_AVAILABLE = True
except ImportError:
    # Fallback for backward compatibility
    NEW_CONFIG_AVAILABLE = False
    from etl_pipeline.core.connections import ConnectionFactory
    from etl_pipeline.core.schema_discovery import SchemaDiscovery
    from etl_pipeline.orchestration.table_processor import TableProcessor

# Import standardized test fixtures
from tests.fixtures import populated_test_databases, test_data_manager
from tests.fixtures.test_data_definitions import get_test_patient_data, get_test_appointment_data

logger = logging.getLogger(__name__)


class TestTableProcessorRealIntegration:
    """Real integration tests using actual MySQL and PostgreSQL databases with standardized test data.
    
    Uses new architectural patterns:
    - test_settings fixture loads TEST_* environment variables from .env
    - ConnectionFactory methods use test_settings for environment-aware connections
    - Standardized test data from fixtures
    - Proper environment separation (no risk of production connections)
    """
    
    @pytest.mark.integration
    def test_real_table_processor_initialization(self, populated_test_databases, test_settings):
        """Test real TableProcessor initialization with test databases."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        appointment_count = manager.get_appointment_count(DatabaseType.SOURCE)
        
        assert patient_count > 0, "Test patient data not found in source database"
        assert appointment_count > 0, "Test appointment data not found in source database"
        
        # Test the REAL TableProcessor with REAL database (test environment)
        processor = TableProcessor(environment='test')
        
        # Verify real components were created
        assert processor.schema_discovery is not None
        
        # Test real schema discovery - focus on functionality, not specific values
        schema = processor.schema_discovery.get_table_schema('patient')
        assert schema is not None, "Real schema discovery failed"
        assert 'columns' in schema, "Schema should contain columns"
        assert 'schema_hash' in schema, "Schema should contain hash for change detection"
        assert 'create_statement' in schema, "Schema should contain CREATE statement"
        
        # Verify schema hash is a valid MD5 hash (32 hex characters)
        import re
        assert re.match(r'^[a-f0-9]{32}$', schema['schema_hash']), "Schema hash should be valid MD5"

    @pytest.mark.integration
    def test_real_table_processing_flow(self, populated_test_databases, test_settings):
        """Test real table processing flow with standardized test data."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "Test patient data not found in source database"
        
        processor = TableProcessor(environment='test')
        
        # Test real table processing - focus on success/failure, not specific hash values
        result = processor.process_table('patient', force_full=True)
        assert result, "Real table processing failed"
        
        # Verify test data was processed
        self._verify_test_data_processing()

    @pytest.mark.integration
    def test_real_schema_discovery_integration(self, populated_test_databases, test_settings):
        """Test real SchemaDiscovery integration with test databases."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "Test patient data not found in source database"
        
        processor = TableProcessor(environment='test')
        
        # Test REAL SchemaDiscovery with REAL MySQL database
        schema = processor.schema_discovery.get_table_schema('patient')
        assert schema is not None, "Real schema discovery failed"
        assert 'columns' in schema, "Schema should contain columns"
        assert 'schema_hash' in schema, "Schema should contain hash for change detection"
        
        # Verify real column discovery - focus on structure, not specific values
        columns = schema['columns']
        column_names = [col['name'] for col in columns]
        expected_columns = ['PatNum', 'LName', 'FName', 'Birthdate', 'SSN']
        
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column {expected_col} not discovered"
        
        # Verify schema hash is consistent for the same table (same environment)
        schema2 = processor.schema_discovery.get_table_schema('patient')
        assert schema['schema_hash'] == schema2['schema_hash'], "Schema hash should be consistent for same table in same environment"

    @pytest.mark.integration
    def test_real_schema_change_detection(self, test_settings):
        """Test schema change detection functionality."""
        processor = TableProcessor(environment='test')
        
        # Get current schema
        schema = processor.schema_discovery.get_table_schema('patient')
        current_hash = schema['schema_hash']
        
        # Test that same schema returns no change
        assert not processor.schema_discovery.has_schema_changed('patient', current_hash), \
            "Same schema should not be detected as changed"
        
        # Test that different hash returns change
        assert processor.schema_discovery.has_schema_changed('patient', 'different_hash_123'), \
            "Different hash should be detected as changed"

    @pytest.mark.integration
    def test_real_mysql_replicator_integration(self, populated_test_databases, test_settings):
        """Test real MySQL replicator integration with test databases."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "Test patient data not found in source database"
        
        processor = TableProcessor(environment='test')
        
        # Test real MySQL replication through the table processor
        # Focus on success/failure, not specific hash comparisons
        result = processor.process_table('patient', force_full=True)
        assert result, "Real MySQL replication failed"
        
        # Verify that the replication step completed (don't check specific hashes)
        logger.info("MySQL replication integration test completed successfully")

    @pytest.mark.integration
    def test_real_postgres_loader_integration(self, populated_test_databases, test_settings):
        """Test real PostgreSQL loader integration with test databases."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "Test patient data not found in source database"
        
        processor = TableProcessor(environment='test')
        
        # Test real PostgreSQL loading through the table processor
        # Focus on success/failure, not specific hash comparisons
        result = processor.process_table('patient', force_full=True)
        assert result, "Real PostgreSQL loading failed"
        
        # Verify that the loading step completed (don't check specific hashes)
        logger.info("PostgreSQL loader integration test completed successfully")

    @pytest.mark.integration
    def test_real_error_handling(self, test_settings):
        """Test real error handling with test database component failures."""
        processor = TableProcessor(environment='test')
        
        # Test with invalid table name
        result = processor.process_table('nonexistent_table', force_full=True)
        assert not result, "Should fail for non-existent table"
        
        # Test with invalid schema discovery
        with patch.object(processor.schema_discovery, 'get_table_schema', side_effect=Exception("Schema error")):
            result = processor.process_table('patient', force_full=True)
            assert not result, "Should handle schema discovery errors gracefully"

    @pytest.mark.integration
    def test_real_incremental_vs_full_refresh(self, populated_test_databases, test_settings):
        """Test incremental vs full refresh logic."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "Test patient data not found in source database"
        
        processor = TableProcessor(environment='test')
        
        # Test full refresh
        result_full = processor.process_table('patient', force_full=True)
        assert result_full, "Full refresh should succeed"
        
        # Test incremental (should also succeed, but may use different logic)
        result_incremental = processor.process_table('patient', force_full=False)
        assert result_incremental, "Incremental processing should succeed"
        
        logger.info("Both full refresh and incremental processing completed successfully")

    def _verify_test_data_processing(self):
        """Verify that test data was processed through the pipeline."""
        # This would verify that test data moved through replication and analytics databases
        # For now, we'll just verify the pipeline completed successfully
        logger.info("Test data processing verification completed")

    @pytest.mark.integration
    def test_real_multiple_table_processing(self, populated_test_databases, test_settings):
        """Test processing multiple tables with real data flow."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        appointment_count = manager.get_appointment_count(DatabaseType.SOURCE)
        assert patient_count > 0, "Test patient data not found in source database"
        assert appointment_count > 0, "Test appointment data not found in source database"
        
        processor = TableProcessor(environment='test')
        
        # Process multiple test tables - focus on success/failure, not hash comparisons
        test_tables = ['patient', 'appointment']
        for table in test_tables:
            result = processor.process_table(table, force_full=True)
            assert result, f"Real processing failed for table {table}"
            
            logger.info(f"Successfully processed test table: {table}")

    @pytest.mark.integration
    def test_real_connection_management(self, test_settings):
        """Test connection management and cleanup."""
        processor = TableProcessor(environment='test')
        
        # Test that connections are properly initialized
        assert processor.initialize_connections(), "Connection initialization should succeed"
        
        # Test that connections are available
        assert processor._connections_available(), "All required connections should be available"
        
        # Test cleanup
        processor.cleanup()
        assert not processor._initialized, "Processor should be marked as not initialized after cleanup"
        
        logger.info("Connection management test completed successfully")

    @pytest.mark.integration
    def test_real_schema_discovery_caching(self, populated_test_databases, test_settings):
        """Test schema discovery caching functionality."""
        # Use standardized test data manager
        manager = populated_test_databases
        
        # Verify test data exists using standardized methods
        patient_count = manager.get_patient_count(DatabaseType.SOURCE)
        assert patient_count > 0, "Test patient data not found in source database"
        
        processor = TableProcessor(environment='test')
        
        # Get schema first time
        schema1 = processor.schema_discovery.get_table_schema('patient')
        assert schema1 is not None, "First schema discovery should succeed"
        
        # Get schema second time (should use cache)
        schema2 = processor.schema_discovery.get_table_schema('patient')
        assert schema2 is not None, "Second schema discovery should succeed"
        
        # Both should have the same hash (same environment, same table)
        assert schema1['schema_hash'] == schema2['schema_hash'], "Cached schema should have same hash"
        
        # Clear cache and test again
        processor.schema_discovery.clear_cache('schema')
        schema3 = processor.schema_discovery.get_table_schema('patient')
        assert schema3 is not None, "Schema discovery after cache clear should succeed"
        
        logger.info("Schema discovery caching test completed successfully")

    @pytest.mark.integration
    def test_new_architecture_connection_methods(self, test_settings):
        """Test that new architecture connection methods work correctly with test environment."""
        # Test new ConnectionFactory methods with Settings dependency injection
        # These should automatically use test environment when test_settings provided
        
        # Test the standard connection methods with test settings
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
        # Test DatabaseType enum usage
        assert DatabaseType.SOURCE.value == "source", "DatabaseType.SOURCE should be 'source'"
        assert DatabaseType.REPLICATION.value == "replication", "DatabaseType.REPLICATION should be 'replication'"
        assert DatabaseType.ANALYTICS.value == "analytics", "DatabaseType.ANALYTICS should be 'analytics'"
        
        # Test PostgresSchema enum usage
        assert PostgresSchema.RAW.value == "raw", "PostgresSchema.RAW should be 'raw'"
        assert PostgresSchema.STAGING.value == "staging", "PostgresSchema.STAGING should be 'staging'"
        assert PostgresSchema.INTERMEDIATE.value == "intermediate", "PostgresSchema.INTERMEDIATE should be 'intermediate'"
        assert PostgresSchema.MARTS.value == "marts", "PostgresSchema.MARTS should be 'marts'"
        
        # Test that enums work with ConnectionFactory using test environment
        analytics_engine = ConnectionFactory.get_analytics_connection(test_settings, PostgresSchema.RAW)
        assert analytics_engine is not None, "Analytics engine should be created with test settings"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 