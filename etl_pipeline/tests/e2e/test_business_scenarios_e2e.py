"""
Business Scenarios E2E Testing Suite

This suite tests real-world business scenarios and use cases:
- Patient registration and appointment scheduling
- Procedure billing and insurance processing
- Data quality and business rule validation
- Error handling in business scenarios
"""

import pytest
import logging
from datetime import datetime, timedelta
from sqlalchemy import text

from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)


class TestBusinessScenariosE2E:
    """E2E tests for real-world business scenarios."""
    
    @pytest.fixture
    def business_scenario_manager(self):
        """Manage business scenario test data."""
        from etl_pipeline.core.connections import ConnectionFactory
        
        class BusinessScenarioManager:
            def __init__(self):
                # Real connections to all databases in the pipeline (TEST ENVIRONMENT)
                self.source_engine = ConnectionFactory.get_opendental_source_test_connection()
                self.replication_engine = ConnectionFactory.get_mysql_replication_test_connection()
                self.analytics_engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
                
                # Business scenario identifiers
                self.scenario_prefix = 'BUSINESS_SCENARIO_'
                
                # Test data for scenarios
                self.scenario_data = {}
            
            def create_patient_registration_scenario(self):
                """Create a complete patient registration scenario."""
                logger.info("Creating patient registration business scenario...")
                
                # Create new patient with complete information
                patient_data = {
                    'LName': f'{self.scenario_prefix}NEW_PATIENT',
                    'FName': 'Sarah',
                    'Birthdate': '1990-03-15',
                    'Email': 'sarah.newpatient@test.com',
                    'Phone': '555-0201',
                    'Address': '100 Business Test Dr',
                    'City': 'Test City',
                    'State': 'TS',
                    'Zip': '12345',
                    'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                with self.source_engine.begin() as conn:
                    result = conn.execute(text("""
                        INSERT INTO patient (LName, FName, Birthdate, Email, Phone, Address, City, State, Zip, DateTStamp)
                        VALUES (:lname, :fname, :birthdate, :email, :phone, :address, :city, :state, :zip, :datestamp)
                    """), patient_data)
                    
                    patient_id = result.lastrowid
                    self.scenario_data['new_patient_id'] = patient_id
                    
                    # Create initial appointment
                    appointment_data = {
                        'PatNum': patient_id,
                        'AptDateTime': (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S'),
                        'AptStatus': 1,  # Scheduled
                        'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'Notes': f'{self.scenario_prefix}Initial consultation for new patient'
                    }
                    
                    result = conn.execute(text("""
                        INSERT INTO appointment (PatNum, AptDateTime, AptStatus, DateTStamp, Notes)
                        VALUES (:patnum, :aptdatetime, :aptstatus, :datestamp, :notes)
                    """), appointment_data)
                    
                    self.scenario_data['initial_appointment_id'] = result.lastrowid
                
                logger.info(f"Created patient registration scenario: Patient ID {patient_id}")
            
            def create_procedure_billing_scenario(self):
                """Create a complete procedure billing scenario."""
                logger.info("Creating procedure billing business scenario...")
                
                # Create patient for billing scenario
                patient_data = {
                    'LName': f'{self.scenario_prefix}BILLING_PATIENT',
                    'FName': 'Mike',
                    'Birthdate': '1982-07-22',
                    'Email': 'mike.billing@test.com',
                    'Phone': '555-0202',
                    'Address': '200 Billing Test Ave',
                    'City': 'Test City',
                    'State': 'TS',
                    'Zip': '12345',
                    'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                with self.source_engine.begin() as conn:
                    result = conn.execute(text("""
                        INSERT INTO patient (LName, FName, Birthdate, Email, Phone, Address, City, State, Zip, DateTStamp)
                        VALUES (:lname, :fname, :birthdate, :email, :phone, :address, :city, :state, :zip, :datestamp)
                    """), patient_data)
                    
                    patient_id = result.lastrowid
                    self.scenario_data['billing_patient_id'] = patient_id
                    
                    # Create multiple procedures with different fees
                    procedures = [
                        {
                            'ProcCode': f'{self.scenario_prefix}PROC_001',
                            'ProcFee': 250.00,
                            'Notes': 'Comprehensive exam'
                        },
                        {
                            'ProcCode': f'{self.scenario_prefix}PROC_002',
                            'ProcFee': 150.00,
                            'Notes': 'X-rays'
                        },
                        {
                            'ProcCode': f'{self.scenario_prefix}PROC_003',
                            'ProcFee': 500.00,
                            'Notes': 'Root canal'
                        }
                    ]
                    
                    for i, proc in enumerate(procedures):
                        procedure_data = {
                            'PatNum': patient_id,
                            'ProcDate': datetime.now().strftime('%Y-%m-%d'),
                            'ProcCode': proc['ProcCode'],
                            'ProcFee': proc['ProcFee'],
                            'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        result = conn.execute(text("""
                            INSERT INTO procedure (PatNum, ProcDate, ProcCode, ProcFee, DateTStamp)
                            VALUES (:patnum, :procdate, :proccode, :procfee, :datestamp)
                        """), procedure_data)
                        
                        self.scenario_data[f'procedure_{i+1}_id'] = result.lastrowid
                
                logger.info(f"Created procedure billing scenario: Patient ID {patient_id} with {len(procedures)} procedures")
            
            def create_appointment_scheduling_scenario(self):
                """Create a complex appointment scheduling scenario."""
                logger.info("Creating appointment scheduling business scenario...")
                
                # Create patient with multiple appointments
                patient_data = {
                    'LName': f'{self.scenario_prefix}SCHEDULE_PATIENT',
                    'FName': 'Lisa',
                    'Birthdate': '1988-11-08',
                    'Email': 'lisa.schedule@test.com',
                    'Phone': '555-0203',
                    'Address': '300 Schedule Test Blvd',
                    'City': 'Test City',
                    'State': 'TS',
                    'Zip': '12345',
                    'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                with self.source_engine.begin() as conn:
                    result = conn.execute(text("""
                        INSERT INTO patient (LName, FName, Birthdate, Email, Phone, Address, City, State, Zip, DateTStamp)
                        VALUES (:lname, :fname, :birthdate, :email, :phone, :address, :city, :state, :zip, :datestamp)
                    """), patient_data)
                    
                    patient_id = result.lastrowid
                    self.scenario_data['schedule_patient_id'] = patient_id
                    
                    # Create series of appointments
                    appointments = [
                        {
                            'date': datetime.now() + timedelta(days=1),
                            'status': 1,  # Scheduled
                            'notes': f'{self.scenario_prefix}Initial consultation'
                        },
                        {
                            'date': datetime.now() + timedelta(days=8),
                            'status': 1,  # Scheduled
                            'notes': f'{self.scenario_prefix}Follow-up appointment'
                        },
                        {
                            'date': datetime.now() + timedelta(days=15),
                            'status': 2,  # Completed
                            'notes': f'{self.scenario_prefix}Treatment session'
                        },
                        {
                            'date': datetime.now() + timedelta(days=22),
                            'status': 1,  # Scheduled
                            'notes': f'{self.scenario_prefix}Final review'
                        }
                    ]
                    
                    for i, apt in enumerate(appointments):
                        appointment_data = {
                            'PatNum': patient_id,
                            'AptDateTime': apt['date'].strftime('%Y-%m-%d %H:%M:%S'),
                            'AptStatus': apt['status'],
                            'DateTStamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'Notes': apt['notes']
                        }
                        
                        result = conn.execute(text("""
                            INSERT INTO appointment (PatNum, AptDateTime, AptStatus, DateTStamp, Notes)
                            VALUES (:patnum, :aptdatetime, :aptstatus, :datestamp, :notes)
                        """), appointment_data)
                        
                        self.scenario_data[f'appointment_{i+1}_id'] = result.lastrowid
                
                logger.info(f"Created appointment scheduling scenario: Patient ID {patient_id} with {len(appointments)} appointments")
            
            def run_pipeline_for_scenario(self, tables=None):
                """Run pipeline for business scenario tables."""
                if tables is None:
                    tables = ['patient', 'appointment', 'procedure']
                
                logger.info(f"Running pipeline for business scenario tables: {tables}")
                
                orchestrator = PipelineOrchestrator()
                orchestrator.initialize_connections()
                
                results = {}
                for table in tables:
                    result = orchestrator.run_pipeline_for_table(table, force_full=True)
                    results[table] = result
                    
                    logger.info(f"Pipeline for {table}: {'SUCCESS' if result else 'FAILED'}")
                
                return results
            
            def verify_business_scenario_data(self):
                """Verify business scenario data across all databases."""
                logger.info("Verifying business scenario data...")
                
                verification_results = {}
                
                # Verify patient registration scenario
                with self.source_engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM patient 
                        WHERE LName LIKE '{self.scenario_prefix}%'
                    """))
                    source_count = result.scalar()
                
                with self.replication_engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM patient 
                        WHERE LName LIKE '{self.scenario_prefix}%'
                    """))
                    replication_count = result.scalar()
                
                with self.analytics_engine.connect() as conn:
                    result = conn.execute(text(f"""
                        SELECT COUNT(*) FROM raw.patient 
                        WHERE "LName" LIKE '{self.scenario_prefix}%'
                    """))
                    analytics_count = result.scalar()
                
                verification_results['business_scenario_patients'] = {
                    'source': source_count,
                    'replication': replication_count,
                    'analytics': analytics_count,
                    'consistent': (source_count == replication_count == analytics_count)
                }
                
                logger.info(f"Business scenario verification: {verification_results}")
                return verification_results
            
            def cleanup_business_scenarios(self):
                """Clean up all business scenario test data."""
                logger.info("Cleaning up business scenario test data...")
                
                # Clean up analytics database
                with self.analytics_engine.begin() as conn:
                    conn.execute(text(f"""
                        DELETE FROM raw.procedure 
                        WHERE "ProcCode" LIKE '{self.scenario_prefix}%'
                    """))
                    conn.execute(text(f"""
                        DELETE FROM raw.appointment 
                        WHERE "Notes" LIKE '{self.scenario_prefix}%'
                    """))
                    conn.execute(text(f"""
                        DELETE FROM raw.patient 
                        WHERE "LName" LIKE '{self.scenario_prefix}%'
                    """))
                
                # Clean up replication database
                with self.replication_engine.begin() as conn:
                    conn.execute(text(f"""
                        DELETE FROM procedure 
                        WHERE ProcCode LIKE '{self.scenario_prefix}%'
                    """))
                    conn.execute(text(f"""
                        DELETE FROM appointment 
                        WHERE Notes LIKE '{self.scenario_prefix}%'
                    """))
                    conn.execute(text(f"""
                        DELETE FROM patient 
                        WHERE LName LIKE '{self.scenario_prefix}%'
                    """))
                
                # Clean up source database
                with self.source_engine.begin() as conn:
                    conn.execute(text(f"""
                        DELETE FROM procedure 
                        WHERE ProcCode LIKE '{self.scenario_prefix}%'
                    """))
                    conn.execute(text(f"""
                        DELETE FROM appointment 
                        WHERE Notes LIKE '{self.scenario_prefix}%'
                    """))
                    conn.execute(text(f"""
                        DELETE FROM patient 
                        WHERE LName LIKE '{self.scenario_prefix}%'
                    """))
                
                logger.info("Business scenario cleanup completed")
        
        manager = BusinessScenarioManager()
        
        # Create all business scenarios
        manager.create_patient_registration_scenario()
        manager.create_procedure_billing_scenario()
        manager.create_appointment_scheduling_scenario()
        
        yield manager
        
        # Cleanup after tests
        manager.cleanup_business_scenarios()

    @pytest.mark.e2e
    def test_patient_registration_scenario_e2e(self, business_scenario_manager):
        """Test complete patient registration business scenario."""
        # Run pipeline for patient registration scenario
        pipeline_results = business_scenario_manager.run_pipeline_for_scenario(['patient', 'appointment'])
        
        # Verify pipeline success
        assert pipeline_results['patient'], "Patient pipeline failed"
        assert pipeline_results['appointment'], "Appointment pipeline failed"
        
        # Verify business scenario data
        verification_results = business_scenario_manager.verify_business_scenario_data()
        assert verification_results['business_scenario_patients']['consistent'], "Business scenario data inconsistent"
        
        # Verify new patient exists in all databases
        new_patient_count = verification_results['business_scenario_patients']['source']
        assert new_patient_count >= 3, f"Expected at least 3 business scenario patients, got {new_patient_count}"
        
        logger.info("Patient registration business scenario E2E test passed")

    @pytest.mark.e2e
    def test_procedure_billing_scenario_e2e(self, business_scenario_manager):
        """Test complete procedure billing business scenario."""
        # Run pipeline for procedure billing scenario
        pipeline_results = business_scenario_manager.run_pipeline_for_scenario(['patient', 'procedure'])
        
        # Verify pipeline success
        assert pipeline_results['patient'], "Patient pipeline failed"
        assert pipeline_results['procedure'], "Procedure pipeline failed"
        
        # Verify billing scenario data
        verification_results = business_scenario_manager.verify_business_scenario_data()
        assert verification_results['business_scenario_patients']['consistent'], "Billing scenario data inconsistent"
        
        # Verify procedures exist in all databases
        with business_scenario_manager.source_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM procedure 
                WHERE ProcCode LIKE '{business_scenario_manager.scenario_prefix}%'
            """))
            source_proc_count = result.scalar()
        
        with business_scenario_manager.replication_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM procedure 
                WHERE ProcCode LIKE '{business_scenario_manager.scenario_prefix}%'
            """))
            replication_proc_count = result.scalar()
        
        assert source_proc_count == replication_proc_count, "Procedure counts don't match between source and replication"
        assert source_proc_count >= 3, f"Expected at least 3 business scenario procedures, got {source_proc_count}"
        
        logger.info("Procedure billing business scenario E2E test passed")

    @pytest.mark.e2e
    def test_appointment_scheduling_scenario_e2e(self, business_scenario_manager):
        """Test complete appointment scheduling business scenario."""
        # Run pipeline for appointment scheduling scenario
        pipeline_results = business_scenario_manager.run_pipeline_for_scenario(['patient', 'appointment'])
        
        # Verify pipeline success
        assert pipeline_results['patient'], "Patient pipeline failed"
        assert pipeline_results['appointment'], "Appointment pipeline failed"
        
        # Verify scheduling scenario data
        verification_results = business_scenario_manager.verify_business_scenario_data()
        assert verification_results['business_scenario_patients']['consistent'], "Scheduling scenario data inconsistent"
        
        # Verify appointments exist in all databases
        with business_scenario_manager.source_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM appointment 
                WHERE Notes LIKE '{business_scenario_manager.scenario_prefix}%'
            """))
            source_apt_count = result.scalar()
        
        with business_scenario_manager.replication_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM appointment 
                WHERE Notes LIKE '{business_scenario_manager.scenario_prefix}%'
            """))
            replication_apt_count = result.scalar()
        
        assert source_apt_count == replication_apt_count, "Appointment counts don't match between source and replication"
        assert source_apt_count >= 4, f"Expected at least 4 business scenario appointments, got {source_apt_count}"
        
        logger.info("Appointment scheduling business scenario E2E test passed")

    @pytest.mark.e2e
    def test_complete_business_workflow_e2e(self, business_scenario_manager):
        """Test complete business workflow with all scenarios."""
        # Run pipeline for all tables
        pipeline_results = business_scenario_manager.run_pipeline_for_scenario(['patient', 'appointment', 'procedure'])
        
        # Verify all pipelines succeeded
        for table, result in pipeline_results.items():
            assert result, f"{table} pipeline failed"
        
        # Verify complete business scenario data
        verification_results = business_scenario_manager.verify_business_scenario_data()
        assert verification_results['business_scenario_patients']['consistent'], "Complete business workflow data inconsistent"
        
        # Verify data quality - check for specific business scenario records
        with business_scenario_manager.source_engine.connect() as conn:
            # Check for patient registration scenario
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM patient 
                WHERE LName = '{business_scenario_manager.scenario_prefix}NEW_PATIENT'
            """))
            new_patient_count = result.scalar()
            
            # Check for billing scenario
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM patient 
                WHERE LName = '{business_scenario_manager.scenario_prefix}BILLING_PATIENT'
            """))
            billing_patient_count = result.scalar()
            
            # Check for scheduling scenario
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM patient 
                WHERE LName = '{business_scenario_manager.scenario_prefix}SCHEDULE_PATIENT'
            """))
            schedule_patient_count = result.scalar()
        
        assert new_patient_count == 1, "New patient scenario not found"
        assert billing_patient_count == 1, "Billing patient scenario not found"
        assert schedule_patient_count == 1, "Schedule patient scenario not found"
        
        logger.info("Complete business workflow E2E test passed")

    @pytest.mark.e2e
    def test_data_quality_business_rules_e2e(self, business_scenario_manager):
        """Test data quality and business rules in business scenarios."""
        # Run pipeline
        pipeline_results = business_scenario_manager.run_pipeline_for_scenario(['patient', 'appointment', 'procedure'])
        
        # Verify all pipelines succeeded
        for table, result in pipeline_results.items():
            assert result, f"{table} pipeline failed"
        
        # Test business rules and data quality
        with business_scenario_manager.source_engine.connect() as conn:
            # Rule 1: All patients should have valid email addresses
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM patient 
                WHERE LName LIKE '{business_scenario_manager.scenario_prefix}%'
                AND Email NOT LIKE '%@%'
            """))
            invalid_email_count = result.scalar()
            assert invalid_email_count == 0, f"Found {invalid_email_count} patients with invalid email addresses"
            
            # Rule 2: All appointments should have valid patient references
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM appointment a
                LEFT JOIN patient p ON a.PatNum = p.PatNum
                WHERE a.Notes LIKE '{business_scenario_manager.scenario_prefix}%'
                AND p.PatNum IS NULL
            """))
            orphaned_appointment_count = result.scalar()
            assert orphaned_appointment_count == 0, f"Found {orphaned_appointment_count} orphaned appointments"
            
            # Rule 3: All procedures should have valid patient references
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM procedure proc
                LEFT JOIN patient p ON proc.PatNum = p.PatNum
                WHERE proc.ProcCode LIKE '{business_scenario_manager.scenario_prefix}%'
                AND p.PatNum IS NULL
            """))
            orphaned_procedure_count = result.scalar()
            assert orphaned_procedure_count == 0, f"Found {orphaned_procedure_count} orphaned procedures"
            
            # Rule 4: All procedures should have positive fees
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM procedure 
                WHERE ProcCode LIKE '{business_scenario_manager.scenario_prefix}%'
                AND ProcFee <= 0
            """))
            invalid_fee_count = result.scalar()
            assert invalid_fee_count == 0, f"Found {invalid_fee_count} procedures with invalid fees"
        
        logger.info("Data quality and business rules E2E test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "e2e"]) 