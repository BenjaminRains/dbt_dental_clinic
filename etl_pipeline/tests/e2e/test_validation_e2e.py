"""
Validation E2E Tests

This module contains end-to-end tests for data validation and integrity:
- UPSERT functionality validation
- Integrity validation
- Data quality validation
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


class BaseValidationE2ETest:
    """Base class with common test setup and utilities for validation tests."""
    
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
        import time
        replication_engine = ConnectionFactory.get_replication_connection(test_settings)
        
        with replication_engine.connect() as conn:
            # Clean all test tables - use table-specific primary keys
            cleanup_queries = {
                'patient': "DELETE FROM patient WHERE PatNum IN (1, 2, 3)",
                'appointment': "DELETE FROM appointment WHERE AptNum IN (1, 2, 3)",
                'procedurelog': "DELETE FROM procedurelog WHERE ProcNum IN (1, 2, 3)"
            }
            for table, query in cleanup_queries.items():
                try:
                    # Check if table exists first
                    check_table = conn.execute(text(f"""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() AND table_name = '{table}'
                    """))
                    if check_table.scalar() > 0:
                        result = conn.execute(text(query))
                        deleted_count = result.rowcount
                        conn.commit()
                        if deleted_count > 0:
                            logger.debug(f"Cleaned {deleted_count} test records from {table}")
                except Exception as e:
                    logger.warning(f"Could not clean table {table}: {e}")
        
        replication_engine.dispose()
        
        # Small delay to ensure cleanup is committed and visible to subsequent connections
        time.sleep(0.15)
    
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


class TestUpsertValidationE2E(BaseValidationE2ETest):
    """Tests for UPSERT functionality validation."""
    
    @pytest.mark.e2e
    @pytest.mark.upsert
    def test_upsert_functionality_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test UPSERT functionality across different tables.
        
        AAA Pattern:
            Arrange: Set up test data and verify UPSERT setup
            Act: Test UPSERT functionality for different tables
            Assert: Validate UPSERT behavior and data integrity
        """
        logger.info("Starting UPSERT functionality E2E test")
        
        # Test UPSERT functionality for different tables
        tables_to_test = [
            ('patient', ['PatNum']),
            ('appointment', ['AptNum']),
            ('procedurelog', ['ProcNum'])
        ]
        
        for table_name, primary_keys in tables_to_test:
            logger.info(f"Testing UPSERT functionality for {table_name}")
            
            # Get test data for this table
            test_data = self._get_test_data_from_source(test_settings)
            test_records = test_data.get(table_name, [])
            assert len(test_records) > 0, f"No test data found for {table_name}"
            
            # Validate UPSERT functionality
            validation_results = pipeline_validator.validate_upsert_functionality(table_name, test_records)
            
            # Assert UPSERT functionality is working
            assert validation_results['upsert_functionality_valid'], f"UPSERT functionality validation failed for {table_name}: {validation_results}"
            
            # Verify UPSERT query building
            query_building = validation_results['upsert_query_building']
            assert query_building['method_working'], f"UPSERT query building failed for {table_name}"
            
            # Verify conflict resolution
            conflict_resolution = validation_results['conflict_resolution']
            assert conflict_resolution['method_working'], f"Conflict resolution failed for {table_name}"
            
            # Verify data integrity after UPSERT
            data_integrity = validation_results['data_integrity']
            assert data_integrity['method_working'], f"Data integrity validation failed for {table_name}"
            
            logger.info(f"UPSERT functionality validation for {table_name}: {validation_results}")
        
        logger.info("UPSERT functionality E2E test completed successfully")


class TestIntegrityValidationE2E(BaseValidationE2ETest):
    """Tests for data integrity validation."""
    
    @pytest.mark.e2e
    @pytest.mark.integrity
    def test_integrity_validation_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test data integrity validation across different tables.
        
        AAA Pattern:
            Arrange: Set up test data and verify integrity validation setup
            Act: Test integrity validation for different tables
            Assert: Validate integrity validation behavior
        """
        logger.info("Starting integrity validation E2E test")
        
        # Test integrity validation for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing integrity validation for {table_name}")
            
            # Validate integrity validation
            validation_results = pipeline_validator.validate_integrity_validation(table_name, incremental_columns)
            
            # Assert integrity validation is working
            assert validation_results['integrity_validation_valid'], f"Integrity validation failed for {table_name}: {validation_results}"
            
            # Verify data quality checks
            data_quality = validation_results['data_quality_checks']
            assert data_quality['method_working'], f"Data quality checks failed for {table_name}"
            
            # Verify referential integrity
            referential_integrity = validation_results['referential_integrity']
            assert referential_integrity['method_working'], f"Referential integrity validation failed for {table_name}"
            
            # Verify constraint validation
            constraint_validation = validation_results['constraint_validation']
            assert constraint_validation['method_working'], f"Constraint validation failed for {table_name}"
            
            logger.info(f"Integrity validation for {table_name}: {validation_results}")
        
        logger.info("Integrity validation E2E test completed successfully")


class TestDataQualityValidationE2E(BaseValidationE2ETest):
    """Tests for data quality validation."""
    
    @pytest.mark.e2e
    @pytest.mark.data_quality
    def test_data_quality_validation_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test data quality validation across different tables.
        
        AAA Pattern:
            Arrange: Set up test data and verify data quality setup
            Act: Test data quality validation for different tables
            Assert: Validate data quality validation behavior
        """
        logger.info("Starting data quality validation E2E test")
        
        # Test data quality validation for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing data quality validation for {table_name}")
            
            # Validate data quality
            validation_results = pipeline_validator.validate_incremental_logic(table_name, incremental_columns)
            
            # Assert data quality validation is working
            data_quality = validation_results.get('data_quality_validation', {})
            
            # Verify we have validation results
            assert validation_results.get('incremental_logic_valid', False), \
                f"Incremental logic validation failed for {table_name}: {validation_results}"
            
            # Verify column validation (method returns 'total_columns_checked' not 'total_columns')
            assert data_quality.get('valid_columns', 0) > 0, f"No valid columns found for {table_name}"
            assert data_quality.get('total_columns_checked', 0) > 0, f"No columns checked for {table_name}"
            
            # Verify valid columns list (method returns 'valid_column_names' not 'valid_columns_list')
            valid_columns_list = data_quality.get('valid_column_names', [])
            assert len(valid_columns_list) > 0, f"No valid columns in list for {table_name}"
            
            logger.info(f"Data quality validation for {table_name}: {validation_results}")
        
        logger.info("Data quality validation E2E test completed successfully")


class TestComprehensiveValidationE2E(BaseValidationE2ETest):
    """Tests for comprehensive validation scenarios."""
    
    @pytest.mark.e2e
    @pytest.mark.comprehensive_validation
    @pytest.mark.slow
    def test_comprehensive_validation_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test comprehensive validation across all validation types.
        
        AAA Pattern:
            Arrange: Set up test data and verify comprehensive validation setup
            Act: Test comprehensive validation for different tables
            Assert: Validate comprehensive validation behavior
        """
        logger.info("Starting comprehensive validation E2E test")
        
        # Test comprehensive validation for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing comprehensive validation for {table_name}")
            
            # Get test data for this table
            test_data = self._get_test_data_from_source(test_settings)
            test_records = test_data.get(table_name, [])
            assert len(test_records) > 0, f"No test data found for {table_name}"
            
            # Test incremental logic validation
            incremental_validation = pipeline_validator.validate_incremental_logic(table_name, incremental_columns)
            assert incremental_validation['incremental_logic_valid'], f"Incremental logic validation failed for {table_name}"
            
            # Test UPSERT functionality validation
            upsert_validation = pipeline_validator.validate_upsert_functionality(table_name, test_records)
            assert upsert_validation['upsert_functionality_valid'], f"UPSERT functionality validation failed for {table_name}"
            
            # Test integrity validation
            integrity_validation = pipeline_validator.validate_integrity_validation(table_name, incremental_columns)
            assert integrity_validation['integrity_validation_valid'], f"Integrity validation failed for {table_name}"
            
            # Test data quality validation
            data_quality = incremental_validation['data_quality_validation']
            assert data_quality['valid_columns'] > 0, f"No valid columns found for {table_name}"
            
            logger.info(f"Comprehensive validation for {table_name} completed successfully")
        
        logger.info("Comprehensive validation E2E test completed successfully") 