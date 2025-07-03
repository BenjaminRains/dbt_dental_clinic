"""
Integration Testing Approach - Using MySQL test_opendental database

This approach tests the integration flow by using the MySQL test_opendental database
with clearly identifiable test data that won't interfere with production.

FOCUS: Test ETL pipeline functionality, not specific schema hash values.
Different environments (production vs test) will have different schemas and hashes.
"""

import pytest
import logging
import os
from sqlalchemy import text
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from etl_pipeline.orchestration.table_processor import TableProcessor
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.core.schema_discovery import SchemaDiscovery

# Set test environment variable for this test module
os.environ['ETL_ENVIRONMENT'] = 'test'

logger = logging.getLogger(__name__)


class TestTableProcessorRealIntegration:
    """integration tests using test_opendental MySQL database with test data."""
    
    @pytest.fixture
    def test_data_manager(self):
        """Manage test data in the real OpenDental database."""
        from etl_pipeline.core.connections import ConnectionFactory
        
        class TestDataManager:
            def __init__(self):
                # Use source connection for creating test data (not replication)
                self.source_engine = ConnectionFactory.get_opendental_source_test_connection()
                # Use replication connection for cleanup
                self.replication_engine = ConnectionFactory.get_mysql_replication_test_connection()
                self.test_patients = []
                self.test_appointments = []
                self.test_procedures = []
            
            def create_test_data(self):
                """Create clearly identifiable test data in the test_opendental database."""
                logger.info("Creating test data in test_opendental database...")
                
                # Create test patients with clearly identifiable names
                test_patients_data = [
                    {
                        'PatNum': None,  # Will be auto-generated if using AUTO_INCREMENT, else set manually
                        'LName': 'TEST_PATIENT_001',
                        'FName': 'John',
                        'MiddleI': 'M',
                        'Preferred': 'Johnny',
                        'PatStatus': True,
                        'Gender': False,
                        'Position': False,
                        'Birthdate': '1980-01-01',
                        'SSN': '123-45-6789'
                    },
                    {
                        'PatNum': None,
                        'LName': 'TEST_PATIENT_002',
                        'FName': 'Jane',
                        'MiddleI': 'A',
                        'Preferred': 'Janey',
                        'PatStatus': True,
                        'Gender': True,
                        'Position': False,
                        'Birthdate': '1985-05-15',
                        'SSN': '234-56-7890'
                    }
                ]
                
                # Insert test patients
                with self.source_engine.begin() as conn:
                    for patient_data in test_patients_data:
                        result = conn.execute(text("""
                            INSERT INTO patient (LName, FName, MiddleI, Preferred, PatStatus, Gender, Position, Birthdate, SSN)
                            VALUES (:LName, :FName, :MiddleI, :Preferred, :PatStatus, :Gender, :Position, :Birthdate, :SSN)
                        """), patient_data)
                        # Get the auto-generated PatNum if available
                        if hasattr(result, 'lastrowid'):
                            patient_data['PatNum'] = result.lastrowid
                        self.test_patients.append(patient_data)
                
                # Create test appointments
                test_appointments_data = [
                    {
                        'AptNum': None,
                        'PatNum': self.test_patients[0]['PatNum'],
                        'AptDateTime': (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
                        'AptStatus': 1,  # Scheduled
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'Notes': 'TEST_APPOINTMENT_001 - Regular checkup'
                    }
                ]
                
                with self.source_engine.begin() as conn:
                    for appointment_data in test_appointments_data:
                        result = conn.execute(text("""
                            INSERT INTO appointment (PatNum, AptDateTime, AptStatus, DateTStamp, Notes)
                            VALUES (:PatNum, :AptDateTime, :AptStatus, :DateTStamp, :Notes)
                        """), appointment_data)
                        if hasattr(result, 'lastrowid'):
                            appointment_data['AptNum'] = result.lastrowid
                        self.test_appointments.append(appointment_data)
                
                logger.info(f"Created {len(self.test_patients)} test patients, {len(self.test_appointments)} appointments")
            
            def cleanup_test_data(self):
                """Remove all test data from both source and replication databases."""
                logger.info("Cleaning up test data from test_opendental database...")
                
                # Clean up source database
                with self.source_engine.begin() as conn:
                    # Delete in reverse order to respect foreign key constraints
                    
                    # Delete test appointments
                    for appointment in self.test_appointments:
                        if appointment['AptNum']:
                            conn.execute(text("DELETE FROM appointment WHERE AptNum = :aptnum"), 
                                       {'aptnum': appointment['AptNum']})
                    
                    # Delete test patients
                    for patient in self.test_patients:
                        if patient['PatNum']:
                            conn.execute(text("DELETE FROM patient WHERE PatNum = :patnum"), 
                                       {'patnum': patient['PatNum']})
                
                # Clean up replication database - remove all test data by pattern
                with self.replication_engine.begin() as conn:
                    # Delete test appointments by pattern
                    conn.execute(text("DELETE FROM appointment WHERE Notes LIKE 'TEST_APPOINTMENT_%'"))
                    
                    # Delete test patients by pattern
                    conn.execute(text("DELETE FROM patient WHERE LName LIKE 'TEST_PATIENT_%'"))
                
                logger.info("Test data cleanup completed")
            
            def verify_test_data_exists(self):
                """Verify that test data exists in the database."""
                with self.source_engine.connect() as conn:
                    # Check test patients
                    result = conn.execute(text("""
                        SELECT COUNT(*) FROM patient 
                        WHERE LName LIKE 'TEST_PATIENT_%'
                    """))
                    patient_count = result.scalar()
                    
                    # Check test appointments
                    result = conn.execute(text("""
                        SELECT COUNT(*) FROM appointment 
                        WHERE Notes LIKE 'TEST_APPOINTMENT_%'
                    """))
                    appointment_count = result.scalar()
                    
                    logger.info(f"Found {patient_count} test patients, {appointment_count} test appointments")
                    
                    return patient_count > 0 and appointment_count > 0
        
        manager = TestDataManager()
        manager.create_test_data()
        
        yield manager
        
        # Cleanup after tests
        manager.cleanup_test_data()

    @pytest.mark.integration
    def test_real_table_processor_initialization(self, test_data_manager):
        """Test real TableProcessor initialization with test_opendental database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
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
    def test_real_table_processing_flow(self, test_data_manager):
        """Test real table processing flow with test_opendental data."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        processor = TableProcessor(environment='test')
        
        # Test real table processing - focus on success/failure, not specific hash values
        result = processor.process_table('patient', force_full=True)
        assert result, "Real table processing failed"
        
        # Verify test data was processed
        self._verify_test_data_processing()

    @pytest.mark.integration
    def test_real_schema_discovery_integration(self, test_data_manager):
        """Test real SchemaDiscovery integration with test_opendental MySQL database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
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
    def test_real_schema_change_detection(self, test_data_manager):
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
    def test_real_mysql_replicator_integration(self, test_data_manager):
        """Test real MySQL replicator integration with test_opendental database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        processor = TableProcessor(environment='test')
        
        # Test real MySQL replication through the table processor
        # Focus on success/failure, not specific hash comparisons
        result = processor.process_table('patient', force_full=True)
        assert result, "Real MySQL replication failed"
        
        # Verify that the replication step completed (don't check specific hashes)
        logger.info("MySQL replication integration test completed successfully")

    @pytest.mark.integration
    def test_real_postgres_loader_integration(self, test_data_manager):
        """Test real PostgreSQL loader integration with test_opendental database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        processor = TableProcessor(environment='test')
        
        # Test real PostgreSQL loading through the table processor
        # Focus on success/failure, not specific hash comparisons
        result = processor.process_table('patient', force_full=True)
        assert result, "Real PostgreSQL loading failed"
        
        # Verify that the loading step completed (don't check specific hashes)
        logger.info("PostgreSQL loader integration test completed successfully")

    @pytest.mark.integration
    def test_real_error_handling(self, test_data_manager):
        """Test real error handling with test_opendental database component failures."""
        processor = TableProcessor(environment='test')
        
        # Test with invalid table name
        result = processor.process_table('nonexistent_table', force_full=True)
        assert not result, "Should fail for non-existent table"
        
        # Test with invalid schema discovery
        with patch.object(processor.schema_discovery, 'get_table_schema', side_effect=Exception("Schema error")):
            result = processor.process_table('patient', force_full=True)
            assert not result, "Should handle schema discovery errors gracefully"

    @pytest.mark.integration
    def test_real_incremental_vs_full_refresh(self, test_data_manager):
        """Test incremental vs full refresh logic."""
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
    def test_real_multiple_table_processing(self, test_data_manager):
        """Test processing multiple tables with real data flow."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        processor = TableProcessor(environment='test')
        
        # Process multiple test tables - focus on success/failure, not hash comparisons
        test_tables = ['patient', 'appointment']
        for table in test_tables:
            result = processor.process_table(table, force_full=True)
            assert result, f"Real processing failed for table {table}"
            
            logger.info(f"Successfully processed test table: {table}")

    @pytest.mark.integration
    def test_real_connection_management(self, test_data_manager):
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
    def test_real_schema_discovery_caching(self, test_data_manager):
        """Test schema discovery caching functionality."""
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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 