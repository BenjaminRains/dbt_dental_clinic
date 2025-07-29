"""
Basic Pipeline E2E Tests

This module contains end-to-end tests for basic pipeline functionality:
- Patient data pipeline
- Appointment data pipeline  
- Procedure data pipeline
- Multi-table pipeline
- Data integrity validation
"""

import time
import logging
from typing import Dict, List, Any
import pytest
from sqlalchemy import text

from etl_pipeline.config.settings import Settings
from etl_pipeline.core.connections import ConnectionFactory
from etl_pipeline.orchestration import PipelineOrchestrator
from .pipeline_data_validator import PipelineDataValidator

logger = logging.getLogger(__name__)


class BasePipelineE2ETest:
    """Base class with common test setup and utilities."""
    
    @pytest.fixture(scope="class")
    def test_settings(self):
        """Test settings for test source, replication and analytics databases."""
        return Settings(environment='test')
    
    @pytest.fixture(scope="class")
    def pipeline_validator(self, test_settings):
        """Fixture for validating pipeline transformations."""
        validator = PipelineDataValidator(test_settings)
        yield validator
        validator.dispose()  # Ensure connections are disposed
    
    
    def _get_test_data_from_source(self, test_settings):
        """Get test data directly from source database for validation."""
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        
        with source_engine.connect() as conn:
            # Get patient data
            patient_result = conn.execute(text("SELECT * FROM patient WHERE PatNum IN (1, 2, 3) ORDER BY PatNum"))
            patients = [dict(row._mapping) for row in patient_result.fetchall()]
            
            # Get appointment data
            appointment_result = conn.execute(text("SELECT * FROM appointment WHERE AptNum IN (1, 2, 3) ORDER BY AptNum"))
            appointments = [dict(row._mapping) for row in appointment_result.fetchall()]
            
            # Get procedure data
            procedure_result = conn.execute(text("SELECT * FROM procedurelog WHERE ProcNum IN (1, 2, 3) ORDER BY ProcNum"))
            procedures = [dict(row._mapping) for row in procedure_result.fetchall()]
        
        source_engine.dispose()
        
        logger.info(f"Retrieved test data from source database: {len(patients)} patients, {len(appointments)} appointments, {len(procedures)} procedures")
        
        return {
            'patient': patients,
            'appointment': appointments,
            'procedurelog': procedures
        }


class TestBasicPipelineE2E(BasePipelineE2ETest):
    """
    Basic pipeline E2E tests using test data with consistent test environment.
    
    Test Flow:
    1. Use populated test databases with standardized test data
    2. Process through test pipeline infrastructure
    3. Validate transformations at each stage
    4. Clean up test databases only
    """
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    def test_patient_data_test_pipeline_e2e(
        self, 
        test_settings, 
        pipeline_validator
    ):
        """
        Test patient data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test patient data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting patient data test pipeline E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
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
        test_settings, 
        pipeline_validator
    ):
        """
        Test appointment data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test appointment data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting appointment data test pipeline E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        appointments = test_data['appointment']
        assert len(appointments) > 0, "No appointment data found in source database"
        
        # Act: Process through pipeline using orchestrator
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Process appointment table - use full strategy to ensure all test data is loaded
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
        test_settings, 
        pipeline_validator
    ):
        """
        Test procedure data pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test procedure data
            Act: Process through complete ETL pipeline to test databases
            Assert: Validate data integrity and transformations
        """
        logger.info("Starting procedure data test pipeline E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        procedures = test_data['procedurelog']
        assert len(procedures) > 0, "No procedure data found in source database"
        
        # Act: Process through pipeline using orchestrator
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Process procedurelog table - use full strategy to ensure all test data is loaded
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
        test_settings, 
        pipeline_validator
    ):
        """
        Test multi-table pipeline processing using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test data for multiple tables
            Act: Process through complete ETL pipeline for multiple tables
            Assert: Validate data integrity and transformations across tables
        """
        logger.info("Starting multi-table test pipeline E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        appointments = test_data['appointment']
        procedures = test_data['procedurelog']
        
        assert len(patients) > 0, "No patient data found in source database"
        assert len(appointments) > 0, "No appointment data found in source database"
        assert len(procedures) > 0, "No procedure data found in source database"
        
        # Act: Process through pipeline using orchestrator for multiple tables
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Process multiple tables - use full strategy to ensure all test data is loaded
        start_time = time.time()
        
        # Process patient table
        patient_result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        assert patient_result is True, "Patient pipeline execution failed"
        
        # Process appointment table
        appointment_result = orchestrator.run_pipeline_for_table('appointment', force_full=True)
        assert appointment_result is True, "Appointment pipeline execution failed"
        
        # Process procedurelog table
        procedure_result = orchestrator.run_pipeline_for_table('procedurelog', force_full=True)
        assert procedure_result is True, "Procedure pipeline execution failed"
        
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution and data
        assert duration < 300, f"Multi-table pipeline took too long: {duration:.2f}s"
        
        # Validate data transformations for all tables
        patient_validation = pipeline_validator.validate_patient_pipeline(patients)
        appointment_validation = pipeline_validator.validate_appointment_pipeline(appointments)
        procedure_validation = pipeline_validator.validate_procedure_pipeline(procedures)
        
        assert patient_validation['record_count_match'], f"Patient record count mismatch: {patient_validation}"
        assert appointment_validation['record_count_match'], f"Appointment record count mismatch: {appointment_validation}"
        assert procedure_validation['record_count_match'], f"Procedure record count mismatch: {procedure_validation}"
        
        logger.info(f"Multi-table test pipeline E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.test_data
    @pytest.mark.slow
    def test_test_data_integrity_e2e(
        self, 
        test_settings,
        pipeline_validator
    ):
        """
        Test data integrity across the entire pipeline using standardized test data.
        
        AAA Pattern:
            Arrange: Use standardized test data and verify source integrity
            Act: Process through complete ETL pipeline for all test tables
            Assert: Validate comprehensive data integrity and transformations
        """
        logger.info("Starting test data integrity E2E test")
        
        # Arrange: Get test data from source database
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        appointments = test_data['appointment']
        procedures = test_data['procedurelog']
        
        assert len(patients) > 0, "No patient data found in source database"
        assert len(appointments) > 0, "No appointment data found in source database"
        assert len(procedures) > 0, "No procedure data found in source database"
        
        # Verify source data integrity
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            # Verify patient data integrity
            patient_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert patient_count == len(patients), f"Source patient count mismatch: expected {len(patients)}, got {patient_count}"
            
            # Verify appointment data integrity
            appointment_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            assert appointment_count == len(appointments), f"Source appointment count mismatch: expected {len(appointments)}, got {appointment_count}"
            
            # Verify procedure data integrity
            procedure_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            assert procedure_count == len(procedures), f"Source procedure count mismatch: expected {len(procedures)}, got {procedure_count}"
        
        source_engine.dispose()
        
        # Act: Process through pipeline using orchestrator for all tables
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        # Process all tables - use full strategy to ensure all test data is loaded
        start_time = time.time()
        
        # Process patient table
        patient_result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        assert patient_result is True, "Patient pipeline execution failed"
        
        # Process appointment table
        appointment_result = orchestrator.run_pipeline_for_table('appointment', force_full=True)
        assert appointment_result is True, "Appointment pipeline execution failed"
        
        # Process procedurelog table
        procedure_result = orchestrator.run_pipeline_for_table('procedurelog', force_full=True)
        assert procedure_result is True, "Procedure pipeline execution failed"
        
        duration = time.time() - start_time
        
        # Assert: Validate comprehensive data integrity
        assert duration < 300, f"Data integrity pipeline took too long: {duration:.2f}s"
        
        # Validate data transformations for all tables
        patient_validation = pipeline_validator.validate_patient_pipeline(patients)
        appointment_validation = pipeline_validator.validate_appointment_pipeline(appointments)
        procedure_validation = pipeline_validator.validate_procedure_pipeline(procedures)
        
        assert patient_validation['record_count_match'], f"Patient record count mismatch: {patient_validation}"
        assert appointment_validation['record_count_match'], f"Appointment record count mismatch: {appointment_validation}"
        assert procedure_validation['record_count_match'], f"Procedure record count mismatch: {procedure_validation}"
        
        # Verify replication database integrity
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            # Verify patient replication integrity
            replication_patient_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_patient_count == len(patients), f"Replication patient count mismatch: expected {len(patients)}, got {replication_patient_count}"
            
            # Verify appointment replication integrity
            replication_appointment_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            assert replication_appointment_count == len(appointments), f"Replication appointment count mismatch: expected {len(appointments)}, got {replication_appointment_count}"
            
            # Verify procedure replication integrity
            replication_procedure_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            assert replication_procedure_count == len(procedures), f"Replication procedure count mismatch: expected {len(procedures)}, got {replication_procedure_count}"
        
        replication_engine.dispose()
        
        logger.info(f"Test data integrity E2E test completed successfully in {duration:.2f}s") 