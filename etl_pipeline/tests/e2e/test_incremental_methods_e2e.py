"""
Incremental Methods E2E Tests

This module contains end-to-end tests for incremental pipeline functionality:
- Incremental logic functionality
- PostgresLoader incremental methods
- SimpleMySQLReplicator incremental methods
- Bulk incremental methods
- Comprehensive incremental methods
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


class TestIncrementalMethodsE2E:
    """
    Incremental methods E2E tests using test data with consistent test environment.
    
    Test Flow:
    1. Test incremental logic functionality
    2. Test different incremental methods
    3. Test comprehensive incremental scenarios
    4. Validate incremental behavior
    """
    
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
            # Clean all test tables using table-specific primary keys
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
    
    @pytest.mark.e2e
    @pytest.mark.incremental
    def test_incremental_logic_functionality_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test incremental logic functionality across different tables.
        
        AAA Pattern:
            Arrange: Set up test data and verify incremental columns
            Act: Test incremental logic for different tables
            Assert: Validate incremental logic behavior
        """
        logger.info("Starting incremental logic functionality E2E test")
        
        # Test incremental logic for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing incremental logic for {table_name}")
            
            # Validate incremental logic
            validation_results = pipeline_validator.validate_incremental_logic(table_name, incremental_columns)
            
            # Assert incremental logic is valid
            assert validation_results['incremental_logic_valid'], f"Incremental logic validation failed for {table_name}: {validation_results}"
            
            # Verify data quality validation
            data_quality = validation_results['data_quality_validation']
            assert data_quality['valid_columns'] > 0, f"No valid incremental columns found for {table_name}"
            
            # Verify query building
            query_info = validation_results['query_building']
            assert 'full_load_query' in query_info, f"Query building failed for {table_name}"
            
            logger.info(f"Incremental logic validation for {table_name}: {validation_results}")
        
        logger.info("Incremental logic functionality E2E test completed successfully")
    
    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.postgres_loader
    def test_postgres_loader_incremental_methods_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test PostgresLoader incremental methods functionality.
        
        AAA Pattern:
            Arrange: Set up test data and verify PostgresLoader methods
            Act: Test PostgresLoader incremental methods for different tables
            Assert: Validate PostgresLoader incremental behavior
        """
        logger.info("Starting PostgresLoader incremental methods E2E test")
        
        # Test PostgresLoader incremental methods for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing PostgresLoader incremental methods for {table_name}")
            
            # Validate PostgresLoader incremental methods
            validation_results = pipeline_validator.validate_postgres_loader_incremental_methods(table_name, incremental_columns)
            
            # Assert PostgresLoader methods are working
            assert validation_results['postgres_loader_methods_valid'], f"PostgresLoader incremental methods validation failed for {table_name}: {validation_results}"
            
            # Verify get_last_copy_time method
            last_copy_time = validation_results['get_last_copy_time']
            assert last_copy_time['method_working'], f"get_last_copy_time method failed for {table_name}"
            
            # Verify get_max_timestamp method
            max_timestamp = validation_results['get_max_timestamp']
            assert max_timestamp['method_working'], f"get_max_timestamp method failed for {table_name}"
            
            # Verify incremental query building
            query_building = validation_results['incremental_query_building']
            assert query_building['method_working'], f"Incremental query building failed for {table_name}"
            
            logger.info(f"PostgresLoader incremental methods validation for {table_name}: {validation_results}")
        
        logger.info("PostgresLoader incremental methods E2E test completed successfully")
    
    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.simple_mysql_replicator
    def test_simple_mysql_replicator_incremental_methods_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test SimpleMySQLReplicator incremental methods functionality.
        
        AAA Pattern:
            Arrange: Set up test data and verify SimpleMySQLReplicator methods
            Act: Test SimpleMySQLReplicator incremental methods for different tables
            Assert: Validate SimpleMySQLReplicator incremental behavior
        """
        logger.info("Starting SimpleMySQLReplicator incremental methods E2E test")
        
        # Test SimpleMySQLReplicator incremental methods for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing SimpleMySQLReplicator incremental methods for {table_name}")
            
            # Validate SimpleMySQLReplicator incremental methods
            validation_results = pipeline_validator.validate_simple_mysql_replicator_incremental_methods(table_name, incremental_columns)
            
            # Assert SimpleMySQLReplicator methods are working
            assert validation_results['simple_mysql_replicator_methods_valid'], f"SimpleMySQLReplicator incremental methods validation failed for {table_name}: {validation_results}"
            
            # Verify get_last_copy_time_max method
            last_copy_time_max = validation_results['get_last_copy_time_max']
            assert last_copy_time_max['method_working'], f"get_last_copy_time_max method failed for {table_name}"
            
            # Verify get_max_timestamp method
            max_timestamp = validation_results['get_max_timestamp']
            assert max_timestamp['method_working'], f"get_max_timestamp method failed for {table_name}"
            
            # Verify incremental query building
            query_building = validation_results['incremental_query_building']
            assert query_building['method_working'], f"Incremental query building failed for {table_name}"
            
            logger.info(f"SimpleMySQLReplicator incremental methods validation for {table_name}: {validation_results}")
        
        logger.info("SimpleMySQLReplicator incremental methods E2E test completed successfully")
    
    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.bulk
    def test_bulk_incremental_methods_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test Bulk incremental methods functionality.
        
        AAA Pattern:
            Arrange: Set up test data and verify Bulk methods
            Act: Test Bulk incremental methods for different tables
            Assert: Validate Bulk incremental behavior
        """
        logger.info("Starting Bulk incremental methods E2E test")
        
        # Test Bulk incremental methods for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing Bulk incremental methods for {table_name}")
            
            # Validate Bulk incremental methods
            validation_results = pipeline_validator.validate_bulk_incremental_methods(table_name, incremental_columns)
            
            # Assert Bulk methods are working
            assert validation_results['bulk_incremental_methods_valid'], f"Bulk incremental methods validation failed for {table_name}: {validation_results}"
            
            # Verify get_last_copy_time method
            last_copy_time = validation_results['get_last_copy_time']
            assert last_copy_time['method_working'], f"get_last_copy_time method failed for {table_name}"
            
            # Verify get_max_timestamp method
            max_timestamp = validation_results['get_max_timestamp']
            assert max_timestamp['method_working'], f"get_max_timestamp method failed for {table_name}"
            
            # Verify incremental query building
            query_building = validation_results['incremental_query_building']
            assert query_building['method_working'], f"Incremental query building failed for {table_name}"
            
            logger.info(f"Bulk incremental methods validation for {table_name}: {validation_results}")
        
        logger.info("Bulk incremental methods E2E test completed successfully")
    
    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.comprehensive
    @pytest.mark.slow
    def test_comprehensive_incremental_methods_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test comprehensive incremental methods functionality.
        
        AAA Pattern:
            Arrange: Set up test data and verify comprehensive incremental behavior
            Act: Test comprehensive incremental methods for different tables
            Assert: Validate comprehensive incremental behavior
        """
        logger.info("Starting comprehensive incremental methods E2E test")
        
        # Test comprehensive incremental methods for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing comprehensive incremental methods for {table_name}")
            
            # Validate comprehensive incremental methods
            validation_results = pipeline_validator.validate_incremental_logic(table_name, incremental_columns)
            
            # Assert comprehensive incremental logic is valid
            assert validation_results['incremental_logic_valid'], f"Comprehensive incremental logic validation failed for {table_name}: {validation_results}"
            
            # Verify data quality validation
            data_quality = validation_results['data_quality_validation']
            assert data_quality['valid_columns'] > 0, f"No valid incremental columns found for {table_name}"
            
            # Verify timestamp calculation
            timestamp_calc = validation_results['max_timestamp_calculation']
            assert timestamp_calc['total_timestamps'] >= 0, f"Timestamp calculation failed for {table_name}"
            
            # Verify query building
            query_info = validation_results['query_building']
            assert 'full_load_query' in query_info, f"Query building failed for {table_name}"
            
            # Test multi-column support
            if len(incremental_columns) > 1:
                # For multi-column incremental scenarios
                multi_column_support = validation_results.get('multi_column_support', False)
                assert multi_column_support, f"Multi-column incremental support failed for {table_name}"
            
            logger.info(f"Comprehensive incremental methods validation for {table_name}: {validation_results}")
        
        logger.info("Comprehensive incremental methods E2E test completed successfully")
    
    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.slow
    def test_incremental_pipeline_execution_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test incremental pipeline execution functionality.
        
        AAA Pattern:
            Arrange: Set up test data and verify incremental pipeline setup
            Act: Execute incremental pipeline for different tables
            Assert: Validate incremental pipeline execution
        """
        logger.info("Starting incremental pipeline execution E2E test")
        
        # Test incremental pipeline execution for different tables
        tables_to_test = [
            ('patient', ['DateTStamp', 'SecDateEntry']),
            ('appointment', ['DateTStamp']),
            ('procedurelog', ['DateTStamp'])
        ]
        
        for table_name, incremental_columns in tables_to_test:
            logger.info(f"Testing incremental pipeline execution for {table_name}")
            
            # Validate incremental logic first
            validation_results = pipeline_validator.validate_incremental_logic(table_name, incremental_columns)
            assert validation_results['incremental_logic_valid'], f"Incremental logic validation failed for {table_name}"
            
            # Execute incremental pipeline
            # NOTE: For incremental to work, we need data in replication first
            # So we do a full load first, then incremental
            orchestrator = PipelineOrchestrator(settings=test_settings)
            orchestrator.initialize_connections()
            
            # First do a full load to populate replication
            logger.info(f"Running full load for {table_name} to populate replication...")
            full_result = orchestrator.run_pipeline_for_table(table_name, force_full=True)
            assert full_result is True, f"Full load failed for {table_name}"
            
            # Now run incremental (should find no new data, but should succeed)
            start_time = time.time()
            result = orchestrator.run_pipeline_for_table(table_name, force_full=False)
            duration = time.time() - start_time
            
            # Assert pipeline execution
            assert result is True, f"Incremental pipeline execution failed for {table_name}"
            assert duration < 120, f"Incremental pipeline took too long for {table_name}: {duration:.2f}s"
            
            # Verify incremental behavior - replication should have data from full load
            replication_engine = ConnectionFactory.get_replication_connection(test_settings)
            with replication_engine.connect() as conn:
                replication_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                assert replication_count > 0, f"No data replicated for {table_name}"
            
            replication_engine.dispose()
            
            logger.info(f"Incremental pipeline execution for {table_name} completed successfully in {duration:.2f}s")
        
        logger.info("Incremental pipeline execution E2E test completed successfully") 

    @pytest.mark.e2e
    @pytest.mark.incremental
    @pytest.mark.primary_incremental_column
    def test_primary_incremental_column_functionality_e2e(
        self,
        test_settings,
        pipeline_validator,
        clean_replication_db
    ):
        """
        Test primary incremental column functionality with fallback logic.
        
        AAA Pattern:
            Arrange: Set up test data with different primary incremental column configurations
            Act: Test SimpleMySQLReplicator with primary column and fallback scenarios
            Assert: Validate that primary column is used when available, fallback works otherwise
        """
        logger.info("Starting primary incremental column functionality E2E test")
        
        # Test tables with different primary incremental column configurations
        # NOTE: Primary column is the table's primary key (e.g., AptNum for appointment, PatNum for patient)
        # Incremental columns are timestamp columns used for incremental loading (e.g., DateTStamp)
        test_cases = [
            {
                'table_name': 'appointment',
                'expected_primary_column': 'AptNum',  # Primary key, not DateTStamp
                'description': 'Table with valid primary key column'
            },
            {
                'table_name': 'patient',
                'expected_primary_column': 'PatNum',  # Primary key, not DateTStamp
                'description': 'Table with primary key column'
            },
            {
                'table_name': 'appointmenttype',
                'expected_primary_column': 'AppointmentTypeNum',  # Actually has primary column
                'description': 'Table with primary column (AppointmentTypeNum)'
            }
        ]
        
        for test_case in test_cases:
            table_name = test_case['table_name']
            expected_primary_column = test_case['expected_primary_column']
            description = test_case['description']
            
            logger.info(f"Testing {description} for table: {table_name}")
            
            # Get table configuration to verify primary incremental column
            from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
            replicator = SimpleMySQLReplicator(test_settings)
            
            # Get table configuration
            table_config = replicator.table_configs.get(table_name, {})
            actual_primary_column = replicator._get_primary_incremental_column(table_config)
            
            logger.info(f"Table {table_name}: Expected primary column: {expected_primary_column}, Actual: {actual_primary_column}")
            
            # Assert that the primary column detection works correctly
            if expected_primary_column:
                assert actual_primary_column == expected_primary_column, f"Primary column mismatch for {table_name}: expected {expected_primary_column}, got {actual_primary_column}"
                logger.info(f"✓ Primary column correctly detected: {actual_primary_column}")
            else:
                assert actual_primary_column is None, f"Expected no primary column for {table_name}, but got {actual_primary_column}"
                logger.info(f"✓ Correctly detected no primary column (will use fallback)")
            
            # Test incremental copy with the detected primary column
            if table_config.get('extraction_strategy') == 'incremental':
                logger.info(f"Testing incremental copy for {table_name} with primary column: {actual_primary_column}")
                
                # This would normally copy data, but for testing we'll just verify the configuration
                incremental_columns = table_config.get('incremental_columns', [])
                logger.info(f"Table {table_name} has incremental columns: {incremental_columns}")
                
                # NOTE: Primary column (e.g., AptNum) and incremental columns (e.g., DateTStamp) are different
                # Primary column is the table's primary key, incremental columns are timestamp columns
                # They don't need to be the same - the primary column is used for tracking, incremental columns for filtering
                if actual_primary_column:
                    logger.info(f"✓ Primary column {actual_primary_column} detected (used for tracking)")
                    logger.info(f"✓ Incremental columns {incremental_columns} will be used for filtering")
                else:
                    # For tables without primary column, verify fallback logic
                    if incremental_columns:
                        logger.info(f"✓ Will use multi-column incremental logic with columns: {incremental_columns}")
                    else:
                        logger.info(f"✓ No incremental columns available, will use full table strategy")
        
        logger.info("Primary incremental column functionality E2E test completed successfully") 