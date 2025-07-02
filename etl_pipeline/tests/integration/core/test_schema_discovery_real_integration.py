"""
Real Integration Testing Approach - Using Real MySQL Database

This approach tests the actual integration flow by using the REAL MySQL OpenDental database
with clearly identifiable test data that won't interfere with production.
"""

import pytest
import logging
from sqlalchemy import text
from datetime import datetime, timedelta

from etl_pipeline.core.schema_discovery import SchemaDiscovery, SchemaNotFoundError
from etl_pipeline.core.connections import ConnectionFactory

logger = logging.getLogger(__name__)


class TestSchemaDiscoveryRealIntegration:
    """Real integration tests using actual MySQL database with test data."""
    
    @pytest.fixture
    def test_data_manager(self):
        """Manage test data in the real OpenDental database."""
        class TestDataManager:
            def __init__(self):
                # Use the test MySQL replication connection for integration tests
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
                        'MiddleI': 'M',
                        'Preferred': 'Johnny',
                        'PatStatus': 0,
                        'Gender': 0,
                        'Position': 0,
                        'Birthdate': '1980-01-01',
                        'SSN': '123-45-6789'
                    },
                    {
                        'PatNum': None,
                        'LName': 'TEST_PATIENT_002', 
                        'FName': 'Jane',
                        'MiddleI': 'A',
                        'Preferred': 'Janey',
                        'PatStatus': 0,
                        'Gender': 1,
                        'Position': 0,
                        'Birthdate': '1985-05-15',
                        'SSN': '234-56-7890'
                    },
                    {
                        'PatNum': None,
                        'LName': 'TEST_PATIENT_003',
                        'FName': 'Bob',
                        'MiddleI': 'R',
                        'Preferred': 'Bobby',
                        'PatStatus': 0,
                        'Gender': 0,
                        'Position': 0,
                        'Birthdate': '1975-12-10',
                        'SSN': '345-67-8901'
                    }
                ]
                
                # Insert test patients
                with self.source_engine.begin() as conn:
                    for patient_data in test_patients_data:
                        params = {
                            'lname': patient_data['LName'],
                            'fname': patient_data['FName'],
                            'middlei': patient_data['MiddleI'],
                            'preferred': patient_data['Preferred'],
                            'patstatus': patient_data['PatStatus'],
                            'gender': patient_data['Gender'],
                            'position': patient_data['Position'],
                            'birthdate': patient_data['Birthdate'],
                            'ssn': patient_data['SSN']
                        }
                        result = conn.execute(text("""
                            INSERT INTO patient (LName, FName, MiddleI, Preferred, PatStatus, Gender, Position, Birthdate, SSN)
                            VALUES (:lname, :fname, :middlei, :preferred, :patstatus, :gender, :position, :birthdate, :ssn)
                        """), params)
                        
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
                    },
                    {
                        'AptNum': None,
                        'PatNum': self.test_patients[1]['PatNum'],
                        'AptDateTime': (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S'),
                        'AptStatus': 1,
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'Notes': 'TEST_APPOINTMENT_002 - Cleaning'
                    }
                ]
                
                with self.source_engine.begin() as conn:
                    for appointment_data in test_appointments_data:
                        params = {
                            'patnum': appointment_data['PatNum'],
                            'aptdatetime': appointment_data['AptDateTime'],
                            'aptstatus': appointment_data['AptStatus'],
                            'datestamp': appointment_data['DateTStamp'],
                            'notes': appointment_data['Notes']
                        }
                        result = conn.execute(text("""
                            INSERT INTO appointment (PatNum, AptDateTime, AptStatus, DateTStamp, Notes)
                            VALUES (:patnum, :aptdatetime, :aptstatus, :datestamp, :notes)
                        """), params)
                        
                        appointment_data['AptNum'] = result.lastrowid
                        self.test_appointments.append(appointment_data)
                
                # Create test procedures
                test_procedures_data = [
                    {
                        'ProcNum': None,
                        'PatNum': self.test_patients[0]['PatNum'],
                        'ProcDate': datetime.now().strftime('%Y-%m-%d'),
                        'ProcCode': 'TEST_PROC_001',
                        'ProcFee': 150.00,
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    },
                    {
                        'ProcNum': None,
                        'PatNum': self.test_patients[1]['PatNum'],
                        'ProcDate': datetime.now().strftime('%Y-%m-%d'),
                        'ProcCode': 'TEST_PROC_002',
                        'ProcFee': 200.00,
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                ]
                
                with self.source_engine.begin() as conn:
                    for procedure_data in test_procedures_data:
                        params = {
                            'patnum': procedure_data['PatNum'],
                            'procdate': procedure_data['ProcDate'],
                            'proccode': procedure_data['ProcCode'],
                            'procfee': procedure_data['ProcFee'],
                            'datestamp': procedure_data['DateTStamp']
                        }
                        result = conn.execute(text("""
                            INSERT INTO `procedure` (PatNum, ProcDate, ProcCode, ProcFee, DateTStamp)
                            VALUES (:patnum, :procdate, :proccode, :procfee, :datestamp)
                        """), params)
                        
                        procedure_data['ProcNum'] = result.lastrowid
                        self.test_procedures.append(procedure_data)
                
                logger.info(f"Created {len(self.test_patients)} test patients, {len(self.test_appointments)} appointments, {len(self.test_procedures)} procedures")
            
            def cleanup_test_data(self):
                """Remove all test data from the database."""
                logger.info("Cleaning up test data from real OpenDental database...")
                
                # Clean up test data
                try:
                    with self.source_engine.connect() as conn:
                        # Delete test procedures first (due to foreign key constraints)
                        conn.execute(text("DELETE FROM `procedure` WHERE ProcNum IN (1, 2)"))
                        conn.execute(text("DELETE FROM appointment WHERE AptNum IN (1, 2)"))
                        conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
                        conn.commit()
                        logger.info("Cleaned up test data from MySQL test database")
                except Exception as e:
                    logger.warning(f"Failed to clean up MySQL test data: {e}")
            
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
                    
                    # Check test procedures
                    result = conn.execute(text("""
                        SELECT COUNT(*) FROM `procedure` 
                        WHERE ProcCode LIKE 'TEST_PROC_%'
                    """))
                    procedure_count = result.scalar()
                    
                    logger.info(f"Found {patient_count} test patients, {appointment_count} test appointments, {procedure_count} test procedures")
                    
                    return patient_count > 0 and appointment_count > 0 and procedure_count > 0
        
        manager = TestDataManager()
        manager.create_test_data()
        
        yield manager
        
        # Cleanup after tests
        manager.cleanup_test_data()

    @pytest.fixture
    def schema_discovery(self):
        """Create real SchemaDiscovery instance with real MySQL connection."""
        from etl_pipeline.core.connections import ConnectionFactory
        
        source_engine = ConnectionFactory.get_mysql_replication_test_connection()
        # Extract database name from engine URL
        source_db = source_engine.url.database
        return SchemaDiscovery(source_engine, source_db)

    @pytest.mark.integration
    def test_real_schema_discovery_initialization(self, test_data_manager, schema_discovery):
        """Test real SchemaDiscovery initialization with actual MySQL database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL SchemaDiscovery with REAL MySQL database
        assert schema_discovery.source_engine is not None
        assert schema_discovery.source_db is not None
        
        # Test real connection
        with schema_discovery.source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1, "Real database connection failed"

    @pytest.mark.integration
    def test_real_table_schema_discovery(self, test_data_manager, schema_discovery):
        """Test real table schema discovery with actual MySQL database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL schema discovery for patient table
        schema = schema_discovery.get_table_schema('patient')
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
        appointment_schema = schema_discovery.get_table_schema('appointment')
        assert appointment_schema is not None, "Appointment schema discovery failed"
        assert appointment_schema['table_name'] == 'appointment'

    @pytest.mark.integration
    def test_real_table_size_discovery(self, test_data_manager, schema_discovery):
        """Test real table size discovery with actual MySQL database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL table size discovery
        patient_size_info = schema_discovery.get_table_size_info('patient')
        assert patient_size_info is not None, "Real table size discovery failed"
        assert patient_size_info['row_count'] >= len(test_data_manager.test_patients), f"Expected at least {len(test_data_manager.test_patients)} patients"
        
        appointment_size_info = schema_discovery.get_table_size_info('appointment')
        assert appointment_size_info is not None, "Appointment table size discovery failed"
        assert appointment_size_info['row_count'] >= len(test_data_manager.test_appointments), f"Expected at least {len(test_data_manager.test_appointments)} appointments"
        
        procedure_size_info = schema_discovery.get_table_size_info('procedure')
        assert procedure_size_info is not None, "Procedure table size discovery failed"
        assert procedure_size_info['row_count'] >= len(test_data_manager.test_procedures), f"Expected at least {len(test_data_manager.test_procedures)} procedures"

    @pytest.mark.integration
    def test_real_database_schema_overview(self, test_data_manager, schema_discovery):
        """Test real database schema overview with actual MySQL database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL database schema overview using discover_all_tables
        tables = schema_discovery.discover_all_tables()
        assert tables is not None, "Real database schema overview failed"
        assert len(tables) > 0, "Expected at least one table"
        
        # Verify our test tables are in the overview
        expected_tables = ['patient', 'appointment', 'procedure']
        
        for expected_table in expected_tables:
            assert expected_table in tables, f"Table {expected_table} not found in overview"

    @pytest.mark.integration
    def test_real_schema_hash_consistency(self, test_data_manager, schema_discovery):
        """Test real schema hash consistency with actual MySQL database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test that schema hash is consistent for the same table
        schema1 = schema_discovery.get_table_schema('patient')
        schema2 = schema_discovery.get_table_schema('patient')
        
        assert schema1['schema_hash'] == schema2['schema_hash'], "Schema hash should be consistent"
        
        # Test that different tables have different hashes
        patient_schema = schema_discovery.get_table_schema('patient')
        appointment_schema = schema_discovery.get_table_schema('appointment')
        
        assert patient_schema['schema_hash'] != appointment_schema['schema_hash'], "Different tables should have different hashes"

    @pytest.mark.integration
    def test_real_error_handling(self, test_data_manager, schema_discovery):
        """Test real error handling with actual component failures."""
        # Test with non-existent table - should raise SchemaNotFoundError
        with pytest.raises(SchemaNotFoundError):
            schema_discovery.get_table_schema('nonexistent_table')
        
        # Test with non-existent table size - should return zero values
        size_info = schema_discovery.get_table_size_info('nonexistent_table')
        assert size_info['row_count'] == 0, "Should return 0 for non-existent table"

    @pytest.mark.integration
    def test_real_column_details_discovery(self, test_data_manager, schema_discovery):
        """Test real column details discovery with actual MySQL database."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test REAL column details discovery
        schema = schema_discovery.get_table_schema('patient')
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
    def test_real_multiple_table_schema_discovery(self, test_data_manager, schema_discovery):
        """Test schema discovery for multiple tables with real data."""
        # Verify test data exists
        assert test_data_manager.verify_test_data_exists(), "Test data not found in database"
        
        # Test schema discovery for multiple tables
        test_tables = ['patient', 'appointment', 'procedure']
        schemas = {}
        
        for table in test_tables:
            schema = schema_discovery.get_table_schema(table)
            assert schema is not None, f"Schema discovery failed for {table}"
            assert schema['table_name'] == table, f"Table name mismatch for {table}"
            schemas[table] = schema
        
        # Verify all schemas are different
        schema_hashes = [schemas[table]['schema_hash'] for table in test_tables]
        assert len(set(schema_hashes)) == len(schema_hashes), "All tables should have different schema hashes"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"]) 