"""
Copy Strategies E2E Tests

This module contains end-to-end tests for different copy strategies:
- Full Copy strategy
- Incremental Copy strategy
- Bulk Copy strategy
- Upsert Copy strategy
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


class BaseCopyStrategyE2ETest:
    """Base class with common test setup and utilities for copy strategies."""
    
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
    
    @pytest.fixture(scope="function")
    def clean_replication_db(self, test_settings):
        """Clean replication database before each test."""
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        
        with replication_engine.connect() as conn:
            # Clean all test tables using only columns that exist in each table
            try:
                # Patient table only has PatNum
                conn.execute(text("DELETE FROM patient WHERE PatNum IN (1, 2, 3)"))
                conn.commit()
            except Exception as e:
                logger.warning(f"Could not clean table patient: {e}")
            
            try:
                # Appointment table has AptNum and PatNum
                conn.execute(text("DELETE FROM appointment WHERE AptNum IN (1, 2, 3) OR PatNum IN (1, 2, 3)"))
                conn.commit()
            except Exception as e:
                logger.warning(f"Could not clean table appointment: {e}")
            
            try:
                # Procedurelog table has ProcNum, PatNum, and AptNum
                conn.execute(text("DELETE FROM procedurelog WHERE ProcNum IN (1, 2, 3) OR PatNum IN (1, 2, 3) OR AptNum IN (1, 2, 3)"))
                conn.commit()
            except Exception as e:
                logger.warning(f"Could not clean table procedurelog: {e}")
        
        replication_engine.dispose()
    
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


class TestFullCopyStrategyE2E(BaseCopyStrategyE2ETest):
    """Tests for FULL COPY strategy across all tables."""
    
    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.full_copy
    def test_patient_full_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test FULL COPY strategy for patient data pipeline.
        
        AAA Pattern:
            Arrange: Verify source patient data exists and replication database is empty
            Act: Execute full copy pipeline strategy for patients
            Assert: Validate complete patient data transfer and integrity
        """
        logger.info("Starting patient FULL COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert source_count == len(patients), f"Source patient count mismatch: expected {len(patients)}, got {source_count}"
        
        source_engine.dispose()
        
        # Arrange: Verify replication database is empty
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty, got {replication_count} records"
        
        replication_engine.dispose()
        
        # Act: Execute full copy pipeline strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('patient', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution
        assert result is True, "Patient full copy pipeline execution failed"
        assert duration < 120, f"Full copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == len(patients), f"Full copy replication count mismatch: expected {len(patients)}, got {replication_count}"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT PatNum, LName, FName FROM patient WHERE PatNum = 1")).fetchone()
            assert sample_result is not None, "Full copy: No sample patient data found in replication"
            assert sample_result[0] == 1, f"Full copy: Unexpected PatNum: {sample_result[0]}"
        
        logger.info(f"Patient FULL COPY strategy E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.full_copy
    def test_appointment_full_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test FULL COPY strategy for appointment data pipeline.
        
        AAA Pattern:
            Arrange: Verify source appointment data exists and replication database is empty
            Act: Execute full copy pipeline strategy for appointments
            Assert: Validate complete appointment data transfer and integrity
        """
        logger.info("Starting appointment FULL COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        appointments = test_data['appointment']
        assert len(appointments) > 0, "No appointment data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            assert source_count == len(appointments), f"Source appointment count mismatch: expected {len(appointments)}, got {source_count}"
        
        source_engine.dispose()
        
        # Arrange: Verify replication database is empty
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty, got {replication_count} records"
        
        replication_engine.dispose()
        
        # Act: Execute full copy pipeline strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('appointment', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution
        assert result is True, "Appointment full copy pipeline execution failed"
        assert duration < 120, f"Full copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM appointment WHERE AptNum IN (1, 2, 3)")).scalar()
            assert replication_count == len(appointments), f"Full copy replication count mismatch: expected {len(appointments)}, got {replication_count}"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT AptNum, PatNum, AptDateTime FROM appointment WHERE AptNum = 1")).fetchone()
            assert sample_result is not None, "Full copy: No sample appointment data found in replication"
            assert sample_result[0] == 1, f"Full copy: Unexpected AptNum: {sample_result[0]}"
        
        logger.info(f"Appointment FULL COPY strategy E2E test completed successfully in {duration:.2f}s")
    
    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.full_copy
    def test_procedure_full_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test FULL COPY strategy for procedure data pipeline.
        
        AAA Pattern:
            Arrange: Verify source procedure data exists and replication database is empty
            Act: Execute full copy pipeline strategy for procedures
            Assert: Validate complete procedure data transfer and integrity
        """
        logger.info("Starting procedure FULL COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        procedures = test_data['procedurelog']
        assert len(procedures) > 0, "No procedure data found in source database"
        
        # Arrange: Verify source data exists
        source_engine = ConnectionFactory.get_source_connection(test_settings)
        with source_engine.connect() as conn:
            source_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            assert source_count == len(procedures), f"Source procedure count mismatch: expected {len(procedures)}, got {source_count}"
        
        source_engine.dispose()
        
        # Arrange: Verify replication database is empty
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty, got {replication_count} records"
        
        replication_engine.dispose()
        
        # Act: Execute full copy pipeline strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('procedurelog', force_full=True)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution
        assert result is True, "Procedure full copy pipeline execution failed"
        assert duration < 120, f"Full copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM procedurelog WHERE ProcNum IN (1, 2, 3)")).scalar()
            assert replication_count == len(procedures), f"Full copy replication count mismatch: expected {len(procedures)}, got {replication_count}"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT ProcNum, PatNum, ProcDate FROM procedurelog WHERE ProcNum = 1")).fetchone()
            assert sample_result is not None, "Full copy: No sample procedure data found in replication"
            assert sample_result[0] == 1, f"Full copy: Unexpected ProcNum: {sample_result[0]}"
        
        logger.info(f"Procedure FULL COPY strategy E2E test completed successfully in {duration:.2f}s")


class TestIncrementalCopyStrategyE2E(BaseCopyStrategyE2ETest):
    """Tests for INCREMENTAL COPY strategy across all tables."""
    
    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.incremental_copy
    def test_patient_incremental_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test INCREMENTAL COPY strategy for patient data pipeline.
        
        AAA Pattern:
            Arrange: Verify source patient data exists and replication database is empty
            Act: Execute incremental copy pipeline strategy for patients
            Assert: Validate incremental patient data transfer and integrity
        """
        logger.info("Starting patient INCREMENTAL COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Arrange: Verify replication database is empty
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty, got {replication_count} records"
        
        replication_engine.dispose()
        
        # Act: Execute incremental copy pipeline strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('patient', force_full=False)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution
        assert result is True, "Patient incremental copy pipeline execution failed"
        assert duration < 120, f"Incremental copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            expected_count = len(patients)  # Should match the number of patients from source
            assert replication_count == expected_count, f"Incremental copy: Expected {expected_count} patient records, got {replication_count} records"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT PatNum, LName, FName FROM patient WHERE PatNum = 1")).fetchone()
            assert sample_result is not None, "Incremental copy: No sample patient data found in replication"
            assert sample_result[0] == 1, f"Incremental copy: Unexpected PatNum: {sample_result[0]}"
        
        logger.info(f"Patient INCREMENTAL COPY strategy E2E test completed successfully in {duration:.2f}s")


class TestBulkCopyStrategyE2E(BaseCopyStrategyE2ETest):
    """Tests for BULK COPY strategy across all tables."""
    
    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.bulk_copy
    def test_patient_bulk_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test BULK COPY strategy for patient data pipeline.
        
        AAA Pattern:
            Arrange: Verify source patient data exists and replication database is empty
            Act: Execute bulk copy pipeline strategy for patients
            Assert: Validate bulk patient data transfer and integrity
        """
        logger.info("Starting patient BULK COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Arrange: Verify replication database is empty
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty, got {replication_count} records"
        
        replication_engine.dispose()
        
        # Act: Execute bulk copy pipeline strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('patient', force_full=False)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution
        assert result is True, "Patient bulk copy pipeline execution failed"
        assert duration < 120, f"Bulk copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            expected_count = len(patients)  # Should match the number of patients from source
            assert replication_count == expected_count, f"Bulk copy: Expected {expected_count} patient records, got {replication_count} records"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT PatNum, LName, FName FROM patient WHERE PatNum = 1")).fetchone()
            assert sample_result is not None, "Bulk copy: No sample patient data found in replication"
            assert sample_result[0] == 1, f"Bulk copy: Unexpected PatNum: {sample_result[0]}"
        
        logger.info(f"Patient BULK COPY strategy E2E test completed successfully in {duration:.2f}s")


class TestUpsertCopyStrategyE2E(BaseCopyStrategyE2ETest):
    """Tests for UPSERT COPY strategy across all tables."""
    
    @pytest.mark.e2e
    @pytest.mark.copy_strategy
    @pytest.mark.upsert_copy
    def test_patient_upsert_copy_strategy_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test UPSERT COPY strategy for patient data pipeline.
        
        AAA Pattern:
            Arrange: Verify source patient data exists and replication database is empty
            Act: Execute upsert copy pipeline strategy for patients
            Assert: Validate upsert patient data transfer and integrity
        """
        logger.info("Starting patient UPSERT COPY strategy E2E test")
        
        # Arrange: Get test data and verify initial state
        test_data = self._get_test_data_from_source(test_settings)
        patients = test_data['patient']
        assert len(patients) > 0, "No patient data found in source database"
        
        # Arrange: Verify replication database is empty
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            assert replication_count == 0, f"Replication database should be empty, got {replication_count} records"
        
        replication_engine.dispose()
        
        # Act: Execute upsert copy pipeline strategy
        orchestrator = PipelineOrchestrator(settings=test_settings)
        orchestrator.initialize_connections()
        
        start_time = time.time()
        result = orchestrator.run_pipeline_for_table('patient', force_full=False)
        duration = time.time() - start_time
        
        # Assert: Validate pipeline execution
        assert result is True, "Patient upsert copy pipeline execution failed"
        assert duration < 120, f"Upsert copy pipeline took too long: {duration:.2f}s"
        
        # Assert: Verify data integrity in replication database
        with replication_engine.connect() as conn:
            replication_count = conn.execute(text("SELECT COUNT(*) FROM patient WHERE PatNum IN (1, 2, 3)")).scalar()
            expected_count = len(patients)  # Should match the number of patients from source
            assert replication_count == expected_count, f"Upsert copy: Expected {expected_count} patient records, got {replication_count} records"
            
            # Verify sample data integrity
            sample_result = conn.execute(text("SELECT PatNum, LName, FName FROM patient WHERE PatNum = 1")).fetchone()
            assert sample_result is not None, "Upsert copy: No sample patient data found in replication"
            assert sample_result[0] == 1, f"Upsert copy: Unexpected PatNum: {sample_result[0]}"
        
        logger.info(f"Patient UPSERT COPY strategy E2E test completed successfully in {duration:.2f}s") 