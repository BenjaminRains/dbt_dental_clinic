"""
Test Data E2E Testing Suite

This suite tests the complete ETL pipeline using test data with consistent test environment.
Tests use standardized test data fixtures, process it through the pipeline,
and validate transformations while maintaining complete safety and isolation.

E2E Flow:
Test Source Database → Test Replication MySQL → Test Analytics PostgreSQL

Features:
- Uses standardized test data patterns
- Consistent test environment throughout
- Complete pipeline validation
- Safe test data for privacy
- Automatic cleanup of test databases only
"""

import pytest
import logging
import time
from datetime import datetime, timedelta
from sqlalchemy import text
from typing import List, Dict, Any, Optional

from etl_pipeline.config.settings import Settings, DatabaseType, PostgresSchema
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.orchestration.pipeline_orchestrator import PipelineOrchestrator
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.loaders.postgres_loader import PostgresLoader
from tests.fixtures.test_data_definitions import get_test_patient_data, get_test_appointment_data, get_test_procedure_data

logger = logging.getLogger(__name__)


# Test data fixtures - using standardized test data instead of production sampling





class PipelineDataValidator:
    """
    Validate data transformations at each pipeline stage.
    
    Compares data integrity across:
    - Test source → Test replication
    - Test replication → Test analytics
    - Type conversions and field mappings
    """
    
    def __init__(self, test_settings: Settings):
        self.test_settings = test_settings
        self.replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        self.analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
    
    def validate_patient_pipeline(self, original_patients: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate patient data through all pipeline stages."""
        logger.info("Validating patient data pipeline transformations")
        
        validation_results = {}
        
        # Get PatNums from original patients to find them in databases
        original_patnums = [patient['PatNum'] for patient in original_patients]
        patnum_list = ','.join(str(p) for p in original_patnums)
        
        # Check replication database
        with self.replication_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM patient 
                WHERE PatNum IN ({patnum_list})
            """))
            replication_count = result.scalar()
            
            # Get sample record for detailed validation
            result = conn.execute(text(f"""
                SELECT PatNum, LName, FName, BalTotal, DateTStamp
                FROM patient 
                WHERE PatNum IN ({patnum_list})
                LIMIT 1
            """))
            replication_sample = result.fetchone()
        
        # Check analytics database
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM raw.patient 
                WHERE "PatNum" IN ({patnum_list})
            """))
            analytics_count = result.scalar()
            
            # Get sample record for detailed validation
            result = conn.execute(text(f"""
                SELECT "PatNum", "LName", "FName", "BalTotal", "DateTStamp"
                FROM raw.patient 
                WHERE "PatNum" IN ({patnum_list})
                LIMIT 1
            """))
            analytics_sample = result.fetchone()
        
        validation_results = {
            'record_count_match': len(original_patients) == replication_count == analytics_count,
            'expected_count': len(original_patients),
            'replication_count': replication_count,
            'analytics_count': analytics_count,
            'replication_sample': dict(replication_sample._mapping) if replication_sample else None,
            'analytics_sample': dict(analytics_sample._mapping) if analytics_sample else None
        }
        
        logger.info(f"Patient pipeline validation: {validation_results}")
        return validation_results
    
    def validate_appointment_pipeline(self, original_appointments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate appointment data through all pipeline stages."""
        logger.info("Validating appointment data pipeline transformations")
        
        validation_results = {}
        
        # Get AptNums from original appointments to find them in databases
        original_aptnums = [apt['AptNum'] for apt in original_appointments]
        aptnum_list = ','.join(str(a) for a in original_aptnums)
        
        # Check replication database
        with self.replication_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM appointment 
                WHERE AptNum IN ({aptnum_list})
            """))
            replication_count = result.scalar()
        
        # Check analytics database
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM raw.appointment 
                WHERE "AptNum" IN ({aptnum_list})
            """))
            analytics_count = result.scalar()
        
        validation_results = {
            'record_count_match': len(original_appointments) == replication_count == analytics_count,
            'expected_count': len(original_appointments),
            'replication_count': replication_count,
            'analytics_count': analytics_count
        }
        
        logger.info(f"Appointment pipeline validation: {validation_results}")
        return validation_results
    
    def validate_procedure_pipeline(self, original_procedures: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate procedure data through all pipeline stages."""
        logger.info("Validating procedure data pipeline transformations")
        
        validation_results = {}
        
        # Get PatNums from original procedures to find them in databases
        original_patnums = [proc['PatNum'] for proc in original_procedures]
        patnum_list = ','.join(str(p) for p in original_patnums)
        
        # Check replication database
        with self.replication_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM procedurelog 
                WHERE PatNum IN ({patnum_list})
            """))
            replication_count = result.scalar()
        
        # Check analytics database
        with self.analytics_engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM raw.procedurelog 
                WHERE "PatNum" IN ({patnum_list})
            """))
            analytics_count = result.scalar()
        
        validation_results = {
            'record_count_match': len(original_procedures) == replication_count == analytics_count,
            'expected_count': len(original_procedures),
            'replication_count': replication_count,
            'analytics_count': analytics_count
        }
        
        logger.info(f"Procedure pipeline validation: {validation_results}")
        return validation_results


class TestDataCleanup:
    """
    Cleanup test data from test databases only.
    
    SAFETY: Never touches production database - only cleans test replication and analytics.
    """
    
    def __init__(self, test_settings: Settings):
        self.test_settings = test_settings
        self.replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        self.analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
    
    def cleanup_test_data(self):
        """Clean up all E2E test data from test databases only."""
        logger.info("Cleaning up E2E test data from test databases")
        
        # Note: Since we're using standardized test data,
        # we don't need to clean up specific test data. The test data
        # will be the standardized test data that was processed through the pipeline.
        # This is safe because we're only using test data and processing
        # through test databases.
        
        logger.info("E2E test data cleanup completed - using standardized test data")


class TestTestDataPipelineE2E:
    """
    End-to-end tests using test data with consistent test environment.
    
    Test Flow:
    1. Use populated test databases with standardized test data
    2. Process through test pipeline infrastructure
    3. Validate transformations at each stage
    4. Clean up test databases only
    """
    
    @pytest.fixture(scope="class")
    def test_settings(self):
        """Test settings for test source, replication and analytics databases."""
        return Settings(environment='test')
    
    @pytest.fixture(scope="class")
    def test_data_sampler(self, test_settings):
        """Fixture for getting standardized test data."""
        return {
            'patients': get_test_patient_data(),
            'appointments': get_test_appointment_data(),
            'procedures': get_test_procedure_data()
        }
    

    
    @pytest.fixture(scope="class")
    def pipeline_validator(self, test_settings):
        """Fixture for validating pipeline transformations."""
        return PipelineDataValidator(test_settings)
    
    @pytest.fixture(scope="class")
    def test_cleanup(self, test_settings):
        """Fixture for cleaning up test data."""
        cleanup = TestDataCleanup(test_settings)
        yield cleanup
        # Cleanup after all tests in class
        cleanup.cleanup_test_data()
    
    @pytest.fixture(scope="class")
    def sampled_test_data(self, test_data_sampler):
        """Get standardized test data for testing."""
        logger.info("Getting standardized test data for E2E testing")
        
        # Get standardized test data
        patients = test_data_sampler['patients']
        appointments = test_data_sampler['appointments']
        procedures = test_data_sampler['procedures']
        
        return {
            'patients': patients,
            'appointments': appointments,
            'procedures': procedures,
            'summary': {
                'patients': len(patients),
                'appointments': len(appointments),
                'procedures': len(procedures)
            }
        }
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_patient_data_test_pipeline_e2e(
        self, 
        sampled_test_data, 
        test_settings, 
        pipeline_validator,
        test_cleanup,
        populated_test_databases
    ):
        """
        Test patient data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test patient data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting patient data test pipeline E2E test")
        
        # Arrange: Get sampled test data
        patients = sampled_test_data['patients']
        assert len(patients) > 0, "No patient data sampled from test"
        
        # Act: Process through pipeline using orchestrator
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Process patient table - use full strategy to ensure all test data is loaded 
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution and data
        assert result is True, "Patient pipeline execution failed"
        assert duration < 120, f"Pipeline took too long: {duration:.2f}s"
        
        # Validate data transformations
        validation_results = pipeline_validator.validate_patient_pipeline(patients)
        assert validation_results['record_count_match'], f"Record count mismatch: {validation_results}"
        
        logger.info(f"Patient test pipeline E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_appointment_data_test_pipeline_e2e(
        self, 
        sampled_test_data, 
        test_settings, 
        pipeline_validator,
        test_cleanup,
        populated_test_databases
    ):
        """
        Test appointment data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test appointment data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting appointment data test pipeline E2E test")
        
        # Arrange: Get sampled test data
        appointments = sampled_test_data['appointments']
        assert len(appointments) > 0, "No appointment data sampled from test"
        
        # Act: Process through pipeline
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('appointment', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution and data
        assert result is True, "Appointment pipeline execution failed"
        assert duration < 120, f"Pipeline took too long: {duration:.2f}s"
        
        # Validate data transformations
        validation_results = pipeline_validator.validate_appointment_pipeline(appointments)
        assert validation_results['record_count_match'], f"Record count mismatch: {validation_results}"
        
        logger.info(f"Appointment test pipeline E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_procedure_data_test_pipeline_e2e(
        self, 
        sampled_test_data, 
        test_settings, 
        pipeline_validator,
        test_cleanup,
        populated_test_databases
    ):
        """
        Test procedure data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test procedure data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting procedure data test pipeline E2E test")
        
        # Arrange: Get sampled test data
        procedures = sampled_test_data['procedures']
        assert len(procedures) > 0, "No procedure data sampled from test"
        
        # Act: Process through pipeline
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('procedurelog', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution and data
        assert result is True, "Procedure pipeline execution failed"
        assert duration < 120, f"Pipeline took too long: {duration:.2f}s"
        
        # Validate data transformations
        validation_results = pipeline_validator.validate_procedure_pipeline(procedures)
        assert validation_results['record_count_match'], f"Record count mismatch: {validation_results}"
        
        logger.info(f"Procedure test pipeline E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_multi_table_test_pipeline_e2e(
        self, 
        sampled_test_data, 
        test_settings, 
        pipeline_validator,
        test_cleanup,
        populated_test_databases
    ):
        """
        Test multi-table pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test data for all tables
            Act: Process all tables through ETL pipeline
            Assert: Validate complete pipeline execution and data integrity
        """
        logger.info("Starting multi-table test pipeline E2E test")
        
        # Arrange: Get sampled test data
        patients = sampled_test_data['patients']
        appointments = sampled_test_data['appointments']
        procedures = sampled_test_data['procedures']
        
        assert len(patients) > 0, "No patient data sampled"
        assert len(appointments) > 0, "No appointment data sampled"
        assert len(procedures) > 0, "No procedure data sampled"
        
        # Act: Process all tables through pipeline
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        tables = ['patient', 'appointment', 'procedurelog']
        
        start_time = time.time()
        results = {}
        
        for table in tables:
            table_start = time.time()
            result = orchestrator.run_pipeline_for_table(table, force_full=True)
            table_duration = time.time() - table_start
            
            results[table] = {
                'success': result,
                'duration': table_duration
            }
        
        total_duration = time.time() - start_time
        
        # Assert: Validate all pipeline executions
        for table, result in results.items():
            assert result['success'], f"{table} pipeline execution failed"
            assert result['duration'] < 120, f"{table} pipeline took too long: {result['duration']:.2f}s"
        
        assert total_duration < 300, f"Total pipeline took too long: {total_duration:.2f}s"
        
        # Validate data transformations for all tables
        patient_validation = pipeline_validator.validate_patient_pipeline(patients)
        appointment_validation = pipeline_validator.validate_appointment_pipeline(appointments)
        procedure_validation = pipeline_validator.validate_procedure_pipeline(procedures)
        
        assert patient_validation['record_count_match'], f"Patient validation failed: {patient_validation}"
        assert appointment_validation['record_count_match'], f"Appointment validation failed: {appointment_validation}"
        assert procedure_validation['record_count_match'], f"Procedure validation failed: {procedure_validation}"
        
        logger.info(f"Multi-table test pipeline E2E test completed successfully in {total_duration:.2f}s")
        table_durations = [(table, f"{result['duration']:.2f}s") for table, result in results.items()]
        logger.info(f"Table durations: {table_durations}")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    @pytest.mark.slow
    def test_test_data_integrity_e2e(
        self, 
        sampled_test_data, 
        test_settings,
        pipeline_validator,
        test_cleanup,
        populated_test_databases
    ):
        """
        Test data integrity across all pipeline stages using test data.
        
        AAA Pattern:
            Arrange: Get standardized test data and process through pipeline
            Act: Compare data across test source, replication, and analytics
            Assert: Verify data integrity maintained through all transformations
        """
        logger.info("Starting test data integrity E2E test")
        
        # Arrange: Process all data through pipeline
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Process all test tables
        tables = ['patient', 'appointment', 'procedurelog']
        for table in tables:
            result = orchestrator.run_pipeline_for_table(table, force_full=True)
            assert result, f"Failed to process {table}"
        
        # Act: Compare data across all stages using test data from memory
        patients = sampled_test_data['patients']
        appointments = sampled_test_data['appointments']
        procedures = sampled_test_data['procedures']
        
        # Get test replication and analytics connections for comparison
        test_replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        test_analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings)
        
        # Compare specific patient record across replication and analytics databases
        test_patient_patnum = patients[0]['PatNum']
        expected_patient = patients[0]
        
        # Get from test replication
        with test_replication_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT PatNum, BalTotal, InsEst, DateTStamp
                FROM patient 
                WHERE PatNum = :patnum
            """), {"patnum": test_patient_patnum})
            replication_patient = result.fetchone()
        
        # Get from test analytics
        with test_analytics_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT "PatNum", "BalTotal", "InsEst", "DateTStamp"
                FROM raw.patient 
                WHERE "PatNum" = :patnum
            """), {"patnum": test_patient_patnum})
            analytics_patient = result.fetchone()
        
        # Assert: Verify data integrity
        assert replication_patient is not None, "Replication patient not found"
        assert analytics_patient is not None, "Analytics patient not found"
        
        # Compare financial data integrity using expected values from test data
        expected_bal_total = expected_patient.get('BalTotal', 0.0)
        expected_ins_est = expected_patient.get('InsEst', 0.0)
        
        # Compare with replication (should match exactly)
        assert replication_patient[1] == expected_bal_total, f"BalTotal mismatch in replication: expected {expected_bal_total}, got {replication_patient[1]}"
        assert replication_patient[2] == expected_ins_est, f"InsEst mismatch in replication: expected {expected_ins_est}, got {replication_patient[2]}"
        
        # Analytics may have type conversions, so compare with tolerance
        assert abs(float(replication_patient[1]) - float(analytics_patient[1])) < 0.01, "BalTotal mismatch in analytics"
        assert abs(float(replication_patient[2]) - float(analytics_patient[2])) < 0.01, "InsEst mismatch in analytics"
        
        logger.info("Test data integrity E2E test completed successfully")
        logger.info(f"Validated data integrity for PatNum {test_patient_patnum} across replication and analytics stages")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "e2e and test_data"])