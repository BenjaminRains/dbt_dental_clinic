"""
End-to-End (E2E) Testing Suite

This suite tests the complete data flow through the entire ETL pipeline:
Source MySQL → Replication MySQL → Analytics PostgreSQL

E2E tests verify:
- Complete data movement through all stages
- Data integrity and consistency
- Business logic validation
- Performance characteristics
- Real-world scenarios
"""

import pytest
import logging
import time
from datetime import datetime, timedelta
from sqlalchemy import text

from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)


class TestCompletePipelineE2E:
    """End-to-end tests for complete data flow through the ETL pipeline."""
    
    @pytest.fixture
    def e2e_test_data_manager(self):
        """Manage E2E test data across all databases in the pipeline."""
        from etl_pipeline.core.connections import ConnectionFactory
        
        class E2ETestDataManager:
            def __init__(self):
                # Real connections to all databases in the pipeline (TEST ENVIRONMENT)
                self.source_engine = ConnectionFactory.get_opendental_source_test_connection()
                self.replication_engine = ConnectionFactory.get_mysql_replication_test_connection()
                self.analytics_engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
                
                # Test data tracking
                self.test_patients = []
                self.test_appointments = []
                self.test_procedures = []
                self.test_insurances = []
                
                # Test identifiers for easy cleanup
                self.test_identifiers = {
                    'patient_lname_prefix': 'E2E_TEST_PATIENT_',
                    'appointment_notes_prefix': 'E2E_TEST_APPOINTMENT_',
                    'procedure_code_prefix': 'E2E_TEST_PROC_',
                    'insurance_carrier_prefix': 'E2E_TEST_INSURANCE_'
                }
            
            def create_comprehensive_test_data(self):
                """Create comprehensive test data for E2E testing."""
                logger.info("Creating comprehensive E2E test data...")
                
                # Create test patients with comprehensive data
                test_patients_data = [
                    {
                        'PatNum': None,
                        'LName': f'{self.test_identifiers["patient_lname_prefix"]}001',
                        'FName': 'John',
                        'Birthdate': '1980-01-01',
                        'Email': 'john_e2e_001@test.com',
                        'Phone': '555-0101',
                        'Address': '123 E2E Test St',
                        'City': 'Test City',
                        'State': 'TS',
                        'Zip': '12345',
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    },
                    {
                        'PatNum': None,
                        'LName': f'{self.test_identifiers["patient_lname_prefix"]}002',
                        'FName': 'Jane',
                        'Birthdate': '1985-05-15',
                        'Email': 'jane_e2e_002@test.com',
                        'Phone': '555-0102',
                        'Address': '456 E2E Test Ave',
                        'City': 'Test City',
                        'State': 'TS',
                        'Zip': '12345',
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    },
                    {
                        'PatNum': None,
                        'LName': f'{self.test_identifiers["patient_lname_prefix"]}003',
                        'FName': 'Bob',
                        'Birthdate': '1975-12-10',
                        'Email': 'bob_e2e_003@test.com',
                        'Phone': '555-0103',
                        'Address': '789 E2E Test Blvd',
                        'City': 'Test City',
                        'State': 'TS',
                        'Zip': '12345',
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                ]
                
                # Insert test patients
                with self.source_engine.begin() as conn:
                    for patient_data in test_patients_data:
                        result = conn.execute(text("""
                            INSERT INTO patient (LName, FName, Birthdate, Email, Phone, Address, City, State, Zip, DateTStamp)
                            VALUES (:lname, :fname, :birthdate, :email, :phone, :address, :city, :state, :zip, :datestamp)
                        """), patient_data)
                        
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
                        'Notes': f'{self.test_identifiers["appointment_notes_prefix"]}001 - Regular checkup'
                    },
                    {
                        'AptNum': None,
                        'PatNum': self.test_patients[1]['PatNum'],
                        'AptDateTime': (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S'),
                        'AptStatus': 1,
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'Notes': f'{self.test_identifiers["appointment_notes_prefix"]}002 - Cleaning'
                    },
                    {
                        'AptNum': None,
                        'PatNum': self.test_patients[2]['PatNum'],
                        'AptDateTime': (datetime.now() + timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S'),
                        'AptStatus': 1,
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'Notes': f'{self.test_identifiers["appointment_notes_prefix"]}003 - Consultation'
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
                
                # Create test procedures
                test_procedures_data = [
                    {
                        'ProcNum': None,
                        'PatNum': self.test_patients[0]['PatNum'],
                        'ProcDate': datetime.now().strftime('%Y-%m-%d'),
                        'ProcCode': f'{self.test_identifiers["procedure_code_prefix"]}001',
                        'ProcFee': 150.00,
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    },
                    {
                        'ProcNum': None,
                        'PatNum': self.test_patients[1]['PatNum'],
                        'ProcDate': datetime.now().strftime('%Y-%m-%d'),
                        'ProcCode': f'{self.test_identifiers["procedure_code_prefix"]}002',
                        'ProcFee': 200.00,
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    },
                    {
                        'ProcNum': None,
                        'PatNum': self.test_patients[2]['PatNum'],
                        'ProcDate': datetime.now().strftime('%Y-%m-%d'),
                        'ProcCode': f'{self.test_identifiers["procedure_code_prefix"]}003',
                        'ProcFee': 175.50,
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                ]
                
                with self.source_engine.begin() as conn:
                    for procedure_data in test_procedures_data:
                        result = conn.execute(text("""
                            INSERT INTO procedure (PatNum, ProcDate, ProcCode, ProcFee, DateTStamp)
                            VALUES (:patnum, :procdate, :proccode, :procfee, :datestamp)
                        """), procedure_data)
                        
                        procedure_data['ProcNum'] = result.lastrowid
                        self.test_procedures.append(procedure_data)
                
                logger.info(f"Created {len(self.test_patients)} test patients, {len(self.test_appointments)} appointments, {len(self.test_procedures)} procedures")
            
            def run_complete_pipeline(self, tables=None):
                """Run the complete ETL pipeline for specified tables."""
                if tables is None:
                    tables = ['patient', 'appointment', 'procedure']
                
                logger.info(f"Running complete E2E pipeline for tables: {tables}")
                
                orchestrator = PipelineOrchestrator()
                orchestrator.initialize_connections()
                
                results = {}
                for table in tables:
                    start_time = time.time()
                    result = orchestrator.run_pipeline_for_table(table, force_full=True)
                    end_time = time.time()
                    
                    results[table] = {
                        'success': result,
                        'duration': end_time - start_time
                    }
                    
                    logger.info(f"Pipeline for {table}: {'SUCCESS' if result else 'FAILED'} ({end_time - start_time:.2f}s)")
                
                return results
            
            def verify_data_in_replication(self):
                """Verify test data appears in replication database."""
                logger.info("Verifying data in replication database...")
                
                verification_results = {}
                
                # Verify patients in replication
                with self.replication_engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM patient 
                        WHERE LName LIKE '{self.test_identifiers["patient_lname_prefix"]}%'
                    """))
                    patient_count = result.scalar()
                    verification_results['replication_patients'] = patient_count
                    
                    # Verify appointments in replication
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM appointment 
                        WHERE Notes LIKE '{self.test_identifiers["appointment_notes_prefix"]}%'
                    """))
                    appointment_count = result.scalar()
                    verification_results['replication_appointments'] = appointment_count
                    
                    # Verify procedures in replication
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM procedure 
                        WHERE ProcCode LIKE '{self.test_identifiers["procedure_code_prefix"]}%'
                    """))
                    procedure_count = result.scalar()
                    verification_results['replication_procedures'] = procedure_count
                
                logger.info(f"Replication verification: {verification_results}")
                return verification_results
            
            def verify_data_in_analytics(self):
                """Verify test data appears in analytics database."""
                logger.info("Verifying data in analytics database...")
                
                verification_results = {}
                
                # Verify patients in analytics raw schema
                with self.analytics_engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM raw.patient 
                        WHERE "LName" LIKE '{self.test_identifiers["patient_lname_prefix"]}%'
                    """))
                    patient_count = result.scalar()
                    verification_results['analytics_patients'] = patient_count
                    
                    # Verify appointments in analytics raw schema
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM raw.appointment 
                        WHERE "Notes" LIKE '{self.test_identifiers["appointment_notes_prefix"]}%'
                    """))
                    appointment_count = result.scalar()
                    verification_results['analytics_appointments'] = appointment_count
                    
                    # Verify procedures in analytics raw schema
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM raw.procedure 
                        WHERE "ProcCode" LIKE '{self.test_identifiers["procedure_code_prefix"]}%'
                    """))
                    procedure_count = result.scalar()
                    verification_results['analytics_procedures'] = procedure_count
                
                logger.info(f"Analytics verification: {verification_results}")
                return verification_results
            
            def verify_data_integrity(self):
                """Verify data integrity across all databases."""
                logger.info("Verifying data integrity across all databases...")
                
                integrity_results = {}
                
                # Check that all test patients exist in all databases
                expected_patient_count = len(self.test_patients)
                
                # Source database
                with self.source_engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM patient 
                        WHERE LName LIKE '{self.test_identifiers["patient_lname_prefix"]}%'
                    """))
                    source_count = result.scalar()
                
                # Replication database
                with self.replication_engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM patient 
                        WHERE LName LIKE '{self.test_identifiers["patient_lname_prefix"]}%'
                    """))
                    replication_count = result.scalar()
                
                # Analytics database
                with self.analytics_engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM raw.patient 
                        WHERE "LName" LIKE '{self.test_identifiers["patient_lname_prefix"]}%'
                    """))
                    analytics_count = result.scalar()
                
                integrity_results['patient_integrity'] = {
                    'expected': expected_patient_count,
                    'source': source_count,
                    'replication': replication_count,
                    'analytics': analytics_count,
                    'consistent': (source_count == replication_count == analytics_count == expected_patient_count)
                }
                
                logger.info(f"Data integrity verification: {integrity_results}")
                return integrity_results
            
            def cleanup_all_databases(self):
                """Clean up test data from all databases in the pipeline."""
                logger.info("Cleaning up E2E test data from all databases...")
                
                # Clean up analytics database first (raw schema)
                with self.analytics_engine.begin() as conn:
                    # Delete test procedures
                    conn.execute(text(f"""
                        DELETE FROM raw.procedure 
                        WHERE "ProcCode" LIKE '{self.test_identifiers["procedure_code_prefix"]}%'
                    """))
                    
                    # Delete test appointments
                    conn.execute(text(f"""
                        DELETE FROM raw.appointment 
                        WHERE "Notes" LIKE '{self.test_identifiers["appointment_notes_prefix"]}%'
                    """))
                    
                    # Delete test patients
                    conn.execute(text(f"""
                        DELETE FROM raw.patient 
                        WHERE "LName" LIKE '{self.test_identifiers["patient_lname_prefix"]}%'
                    """))
                
                # Clean up replication database
                with self.replication_engine.begin() as conn:
                    # Delete test procedures
                    conn.execute(text(f"""
                        DELETE FROM procedure 
                        WHERE ProcCode LIKE '{self.test_identifiers["procedure_code_prefix"]}%'
                    """))
                    
                    # Delete test appointments
                    conn.execute(text(f"""
                        DELETE FROM appointment 
                        WHERE Notes LIKE '{self.test_identifiers["appointment_notes_prefix"]}%'
                    """))
                    
                    # Delete test patients
                    conn.execute(text(f"""
                        DELETE FROM patient 
                        WHERE LName LIKE '{self.test_identifiers["patient_lname_prefix"]}%'
                    """))
                
                # Clean up source database
                with self.source_engine.begin() as conn:
                    # Delete test procedures
                    for procedure in self.test_procedures:
                        if procedure['ProcNum']:
                            conn.execute(text("DELETE FROM procedure WHERE ProcNum = :procnum"), 
                                       {'procnum': procedure['ProcNum']})
                    
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
                
                logger.info("E2E test data cleanup completed from all databases")
        
        manager = E2ETestDataManager()
        manager.create_comprehensive_test_data()
        
        yield manager
        
        # Cleanup after tests
        manager.cleanup_all_databases()

    @pytest.mark.e2e
    def test_complete_patient_pipeline_e2e(self, e2e_test_data_manager):
        """Test complete E2E pipeline for patient data flow."""
        # Run complete pipeline for patient table
        pipeline_results = e2e_test_data_manager.run_complete_pipeline(['patient'])
        
        # Verify pipeline success
        assert pipeline_results['patient']['success'], "Patient pipeline failed"
        
        # Verify data appears in replication
        replication_results = e2e_test_data_manager.verify_data_in_replication()
        assert replication_results['replication_patients'] == len(e2e_test_data_manager.test_patients), \
            f"Expected {len(e2e_test_data_manager.test_patients)} patients in replication, got {replication_results['replication_patients']}"
        
        # Verify data appears in analytics
        analytics_results = e2e_test_data_manager.verify_data_in_analytics()
        assert analytics_results['analytics_patients'] == len(e2e_test_data_manager.test_patients), \
            f"Expected {len(e2e_test_data_manager.test_patients)} patients in analytics, got {analytics_results['analytics_patients']}"
        
        # Verify data integrity
        integrity_results = e2e_test_data_manager.verify_data_integrity()
        assert integrity_results['patient_integrity']['consistent'], "Patient data integrity check failed"
        
        logger.info(f"Patient E2E test completed successfully in {pipeline_results['patient']['duration']:.2f}s")

    @pytest.mark.e2e
    def test_complete_appointment_pipeline_e2e(self, e2e_test_data_manager):
        """Test complete E2E pipeline for appointment data flow."""
        # Run complete pipeline for appointment table
        pipeline_results = e2e_test_data_manager.run_complete_pipeline(['appointment'])
        
        # Verify pipeline success
        assert pipeline_results['appointment']['success'], "Appointment pipeline failed"
        
        # Verify data appears in replication
        replication_results = e2e_test_data_manager.verify_data_in_replication()
        assert replication_results['replication_appointments'] == len(e2e_test_data_manager.test_appointments), \
            f"Expected {len(e2e_test_data_manager.test_appointments)} appointments in replication, got {replication_results['replication_appointments']}"
        
        # Verify data appears in analytics
        analytics_results = e2e_test_data_manager.verify_data_in_analytics()
        assert analytics_results['analytics_appointments'] == len(e2e_test_data_manager.test_appointments), \
            f"Expected {len(e2e_test_data_manager.test_appointments)} appointments in analytics, got {analytics_results['analytics_appointments']}"
        
        logger.info(f"Appointment E2E test completed successfully in {pipeline_results['appointment']['duration']:.2f}s")

    @pytest.mark.e2e
    def test_complete_procedure_pipeline_e2e(self, e2e_test_data_manager):
        """Test complete E2E pipeline for procedure data flow."""
        # Run complete pipeline for procedure table
        pipeline_results = e2e_test_data_manager.run_complete_pipeline(['procedure'])
        
        # Verify pipeline success
        assert pipeline_results['procedure']['success'], "Procedure pipeline failed"
        
        # Verify data appears in replication
        replication_results = e2e_test_data_manager.verify_data_in_replication()
        assert replication_results['replication_procedures'] == len(e2e_test_data_manager.test_procedures), \
            f"Expected {len(e2e_test_data_manager.test_procedures)} procedures in replication, got {replication_results['replication_procedures']}"
        
        # Verify data appears in analytics
        analytics_results = e2e_test_data_manager.verify_data_in_analytics()
        assert analytics_results['analytics_procedures'] == len(e2e_test_data_manager.test_procedures), \
            f"Expected {len(e2e_test_data_manager.test_procedures)} procedures in analytics, got {analytics_results['analytics_procedures']}"
        
        logger.info(f"Procedure E2E test completed successfully in {pipeline_results['procedure']['duration']:.2f}s")

    @pytest.mark.e2e
    def test_complete_multiple_table_pipeline_e2e(self, e2e_test_data_manager):
        """Test complete E2E pipeline for multiple tables simultaneously."""
        # Run complete pipeline for all test tables
        pipeline_results = e2e_test_data_manager.run_complete_pipeline(['patient', 'appointment', 'procedure'])
        
        # Verify all pipelines succeeded
        for table, result in pipeline_results.items():
            assert result['success'], f"{table} pipeline failed"
        
        # Verify data appears in replication
        replication_results = e2e_test_data_manager.verify_data_in_replication()
        assert replication_results['replication_patients'] == len(e2e_test_data_manager.test_patients)
        assert replication_results['replication_appointments'] == len(e2e_test_data_manager.test_appointments)
        assert replication_results['replication_procedures'] == len(e2e_test_data_manager.test_procedures)
        
        # Verify data appears in analytics
        analytics_results = e2e_test_data_manager.verify_data_in_analytics()
        assert analytics_results['analytics_patients'] == len(e2e_test_data_manager.test_patients)
        assert analytics_results['analytics_appointments'] == len(e2e_test_data_manager.test_appointments)
        assert analytics_results['analytics_procedures'] == len(e2e_test_data_manager.test_procedures)
        
        # Verify complete data integrity
        integrity_results = e2e_test_data_manager.verify_data_integrity()
        assert integrity_results['patient_integrity']['consistent'], "Complete data integrity check failed"
        
        total_duration = sum(result['duration'] for result in pipeline_results.values())
        logger.info(f"Complete multi-table E2E test completed successfully in {total_duration:.2f}s")

    @pytest.mark.e2e
    def test_pipeline_performance_e2e(self, e2e_test_data_manager):
        """Test E2E pipeline performance characteristics."""
        # Run pipeline and measure performance
        start_time = time.time()
        pipeline_results = e2e_test_data_manager.run_complete_pipeline(['patient', 'appointment', 'procedure'])
        total_time = time.time() - start_time
        
        # Verify all pipelines succeeded
        for table, result in pipeline_results.items():
            assert result['success'], f"{table} pipeline failed"
        
        # Performance assertions (adjust thresholds as needed)
        assert total_time < 300, f"Pipeline took too long: {total_time:.2f}s"  # 5 minutes max
        
        # Individual table performance
        for table, result in pipeline_results.items():
            assert result['duration'] < 120, f"{table} pipeline took too long: {result['duration']:.2f}s"  # 2 minutes max per table
        
        logger.info(f"Performance test completed: Total time {total_time:.2f}s")

    @pytest.mark.e2e
    def test_data_consistency_e2e(self, e2e_test_data_manager):
        """Test data consistency across all pipeline stages."""
        # Run pipeline
        pipeline_results = e2e_test_data_manager.run_complete_pipeline(['patient', 'appointment', 'procedure'])
        
        # Verify all pipelines succeeded
        for table, result in pipeline_results.items():
            assert result['success'], f"{table} pipeline failed"
        
        # Test data consistency by comparing specific records
        with e2e_test_data_manager.source_engine.connect() as source_conn:
            # Get test patient from source
            result = source_conn.execute(text(f"""
                SELECT PatNum, LName, FName, Email 
                FROM patient 
                WHERE LName = '{e2e_test_data_manager.test_identifiers["patient_lname_prefix"]}001'
            """))
            source_patient = result.fetchone()
        
        with e2e_test_data_manager.replication_engine.connect() as repl_conn:
            # Get same patient from replication
            result = repl_conn.execute(text(f"""
                SELECT PatNum, LName, FName, Email 
                FROM patient 
                WHERE LName = '{e2e_test_data_manager.test_identifiers["patient_lname_prefix"]}001'
            """))
            replication_patient = result.fetchone()
        
        with e2e_test_data_manager.analytics_engine.connect() as analytics_conn:
            # Get same patient from analytics
            result = analytics_conn.execute(text(f"""
                SELECT "PatNum", "LName", "FName", "Email" 
                FROM raw.patient 
                WHERE "LName" = '{e2e_test_data_manager.test_identifiers["patient_lname_prefix"]}001'
            """))
            analytics_patient = result.fetchone()
        
        # Verify data consistency
        assert source_patient is not None, "Source patient not found"
        assert replication_patient is not None, "Replication patient not found"
        assert analytics_patient is not None, "Analytics patient not found"
        
        # Compare key fields
        assert source_patient[1] == replication_patient[1] == analytics_patient[1], "LName mismatch"
        assert source_patient[2] == replication_patient[2] == analytics_patient[2], "FName mismatch"
        assert source_patient[3] == replication_patient[3] == analytics_patient[3], "Email mismatch"
        
        logger.info("Data consistency test passed - all records match across databases")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "e2e"]) 