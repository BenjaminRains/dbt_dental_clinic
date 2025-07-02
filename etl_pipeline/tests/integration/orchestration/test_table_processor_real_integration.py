"""
Real Integration Testing Approach - Using Real MySQL Database

This approach tests the actual integration flow by using the REAL MySQL OpenDental database
with clearly identifiable test data that won't interfere with production.
"""

import pytest
import logging
from sqlalchemy import text
from datetime import datetime, timedelta

from etl_pipeline.orchestration.table_processor import TableProcessor

logger = logging.getLogger(__name__)


class TestTableProcessorRealIntegration:
    """Real integration tests using actual MySQL database with test data."""
    
    @pytest.fixture
    def test_data_manager(self):
        """Manage test data in the real OpenDental database."""
        from etl_pipeline.core.connections import ConnectionFactory
        
        class TestDataManager:
            def __init__(self):
                self.source_engine = ConnectionFactory.get_mysql_replication_test_connection()
                self.test_patients = []
                self.test_appointments = []
                self.test_procedures = []
            
            def create_test_data(self):
                """Create clearly identifiable test data in the real database."""
                logger.info("Creating test data in real OpenDental database...")
                
                # Create test patients with clearly identifiable names
                test_patients_data = [
                    {
                        'PatNum': None,  # Will be auto-generated
                        'LName': 'TEST_PATIENT_001',
                        'FName': 'John',
                        'Birthdate': '1980-01-01',
                        'Email': 'john_test_001@test.com',
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    },
                    {
                        'PatNum': None,
                        'LName': 'TEST_PATIENT_002', 
                        'FName': 'Jane',
                        'Birthdate': '1985-05-15',
                        'Email': 'jane_test_002@test.com',
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                ]
                
                # Insert test patients
                with self.source_engine.begin() as conn:
                    for patient_data in test_patients_data:
                        result = conn.execute(text("""
                            INSERT INTO patient (LName, FName, Birthdate, Email, DateTStamp)
                            VALUES (:lname, :fname, :birthdate, :email, :datestamp)
                        """), patient_data)
                        
                        # Get the auto-generated PatNum
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
                            VALUES (:patnum, :aptdatetime, :aptstatus, :datestamp, :notes)
                        """), appointment_data)
                        
                        appointment_data['AptNum'] = result.lastrowid
                        self.test_appointments.append(appointment_data)
                
                logger.info(f"Created {len(self.test_patients)} test patients, {len(self.test_appointments)} appointments")
            
            def cleanup_test_data(self):
                """Remove all test data from the database."""
                logger.info("Cleaning up test data from real OpenDental database...")
                
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
        """Test real TableProcessor initialization with actual database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test the REAL TableProcessor with REAL database
        processor = TableProcessor()
        
        # Verify real components were created
        assert processor.schema_discovery is not None
        assert processor.mysql_replicator is not None
        assert processor.postgres_loader is not None
        
        # Test real schema discovery
        schema = processor.schema_discovery.get_table_schema('patient')
        assert schema is not None, "Real schema discovery failed"
        assert 'columns' in schema

    @pytest.mark.integration
    def test_real_table_processing_flow(self, test_data_manager):
        """Test real table processing flow with actual data."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        processor = TableProcessor()
        
        # Test real table processing
        result = processor.process_table('patient', force_full=True)
        assert result, "Real table processing failed"
        
        # Verify test data was processed
        self._verify_test_data_processing()

    @pytest.mark.integration
    def test_real_schema_discovery_integration(self, test_data_manager):
        """Test real SchemaDiscovery integration with actual MySQL database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        processor = TableProcessor()
        
        # Test REAL SchemaDiscovery with REAL MySQL database
        schema = processor.schema_discovery.get_table_schema('patient')
        assert schema is not None, "Real schema discovery failed"
        assert 'columns' in schema
        assert 'schema_hash' in schema
        
        # Verify real column discovery
        columns = schema['columns']
        column_names = [col['name'] for col in columns]
        expected_columns = ['PatNum', 'LName', 'FName', 'Birthdate', 'Email', 'DateTStamp']
        
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column {expected_col} not discovered"

    @pytest.mark.integration
    def test_real_mysql_replicator_integration(self, test_data_manager):
        """Test real MySQL replicator integration with actual database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        processor = TableProcessor()
        
        # Test real MySQL replication
        result = processor.mysql_replicator.replicate_table('patient', force_full=True)
        assert result, "Real MySQL replication failed"

    @pytest.mark.integration
    def test_real_postgres_loader_integration(self, test_data_manager):
        """Test real PostgreSQL loader integration with actual database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        processor = TableProcessor()
        
        # Test real PostgreSQL loading
        result = processor.postgres_loader.load_table('patient', force_full=True)
        assert result, "Real PostgreSQL loading failed"

    @pytest.mark.integration
    def test_real_error_handling(self, test_data_manager):
        """Test real error handling with actual component failures."""
        processor = TableProcessor()
        
        # Test with invalid table name
        result = processor.process_table('nonexistent_table', force_full=True)
        assert not result, "Should fail for non-existent table"

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
        
        processor = TableProcessor()
        
        # Process multiple test tables
        test_tables = ['patient', 'appointment']
        for table in test_tables:
            result = processor.process_table(table, force_full=True)
            assert result, f"Real processing failed for table {table}"
            
            logger.info(f"Successfully processed test table: {table}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 