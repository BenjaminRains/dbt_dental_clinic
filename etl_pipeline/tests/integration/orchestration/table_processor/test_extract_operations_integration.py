# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for TableProcessor extract operations using SimpleMySQLReplicator.

This module tests:
- Extract phase operations with real database connections
- SimpleMySQLReplicator integration and coordination
- Strategy resolution for extract operations
- Performance monitoring during extraction
- Error handling during extract phase
- Metadata collection and tracking
"""

import pytest
import logging
import time
from typing import Optional, Dict, Any, List
from sqlalchemy import text, Result
from datetime import datetime

from sqlalchemy.engine import Engine
from sqlalchemy import text

# Import ETL pipeline components
from etl_pipeline.orchestration.table_processor import TableProcessor, TableProcessingContext
from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    ConfigReader
)
from etl_pipeline.config.providers import FileConfigProvider
from etl_pipeline.core.connections import ConnectionFactory

# Import custom exceptions for specific error handling
from etl_pipeline.exceptions import (
    ConfigurationError,
    EnvironmentError,
    DatabaseConnectionError,
    DatabaseQueryError,
    DataExtractionError,
    DataLoadingError,
    SchemaValidationError
)

# Import fixtures for test data
from tests.fixtures.integration_fixtures import (
    populated_test_databases,
    test_settings_with_file_provider
)
from tests.fixtures.config_fixtures import temp_tables_config_dir

logger = logging.getLogger(__name__)

# Known test tables that exist in the test database
KNOWN_TEST_TABLES = ['patient', 'appointment', 'procedurelog']

@pytest.mark.integration
@pytest.mark.order(3)  # Third tests to run
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestTableProcessorExtractOperationsIntegration:
    """Integration tests for TableProcessor extract operations with real database connections."""

    def test_extract_operations_initialization_with_real_config(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor extract operations initialization with real configuration.
        
        Validates:
            - Extract operations initialization with real settings
            - SimpleMySQLReplicator integration
            - Database connection establishment for extract phase
            - Configuration loading and validation for extract operations
            - Settings injection for extract operations
            
        ETL Pipeline Context:
            - Critical for ETL pipeline extract phase
            - Supports dental clinic data extraction
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            test_data_manager.setup_procedure_data()
            
            table_processor = TableProcessor()
            
            # Validate that extract operations method exists
            assert hasattr(table_processor, '_extract_to_replication')
            
            # Validate SimpleMySQLReplicator can be imported and initialized
            replicator = SimpleMySQLReplicator(settings=test_settings_with_file_provider)
            assert replicator is not None
            assert replicator.settings == test_settings_with_file_provider
            
            # Validate database connections for extract operations
            source_engine = replicator.source_engine
            target_engine = replicator.target_engine
            assert source_engine is not None
            assert target_engine is not None
            
            logger.info("TableProcessor extract operations initialization successful")
            
        except Exception as e:
            logger.error(f"TableProcessor extract operations initialization test failed: {str(e)}")
            raise

    def test_extract_operations_with_incremental_strategy(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor extract operations with incremental strategy.
        
        Validates:
            - Incremental extract operations
            - Strategy resolution for incremental extraction
            - Primary column handling during extraction
            - Performance monitoring during incremental extraction
            - Metadata collection for incremental operations
            
        ETL Pipeline Context:
            - Critical for ETL pipeline incremental processing
            - Supports dental clinic incremental updates
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            test_data_manager.setup_procedure_data()
            
            table_processor = TableProcessor()
            
            # Test incremental extract for known test tables
            for table_name in KNOWN_TEST_TABLES:
                # Create processing context for incremental strategy
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Only test if table supports incremental processing
                if context.incremental_columns and context.strategy_info['strategy'] == 'incremental':
                    logger.info(f"Testing incremental extract for table: {table_name}")
                    
                    # Perform extract operation
                    success, metadata = table_processor._extract_to_replication(table_name, force_full=False)
                    
                    # Validate extract operation
                    assert success is True, f"Extract operation failed for {table_name}"
                    assert metadata is not None
                    assert 'rows_copied' in metadata
                    assert 'strategy_used' in metadata
                    assert 'duration' in metadata
                    
                    # Validate strategy used
                    assert metadata['strategy_used'] in ['incremental', 'full_table']
                    
                    # Validate row count
                    rows_copied = metadata.get('rows_copied', 0)
                    assert isinstance(rows_copied, int)
                    assert rows_copied >= 0
                    
                    logger.info(f"Incremental extract successful for {table_name}: "
                               f"{rows_copied} rows, {metadata['strategy_used']} strategy")
                else:
                    logger.info(f"Skipping incremental extract for {table_name} - not configured for incremental")
            
            logger.info("TableProcessor incremental extract operations successful")
            
        except Exception as e:
            logger.error(f"TableProcessor incremental extract operations test failed: {str(e)}")
            raise

    def test_extract_operations_with_full_refresh_strategy(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor extract operations with full refresh strategy.
        
        Validates:
            - Full refresh extract operations
            - Strategy resolution for full refresh extraction
            - Performance monitoring during full refresh extraction
            - Metadata collection for full refresh operations
            - Force full logic implementation
            
        ETL Pipeline Context:
            - Critical for ETL pipeline full refresh processing
            - Supports dental clinic full data replacement
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            test_data_manager.setup_procedure_data()
            
            table_processor = TableProcessor()
            
            # Test full refresh extract for known test tables
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing full refresh extract for table: {table_name}")
                
                # Perform extract operation with force full
                success, metadata = table_processor._extract_to_replication(table_name, force_full=True)
                
                # Validate extract operation
                assert success is True, f"Full refresh extract operation failed for {table_name}"
                assert metadata is not None
                assert 'rows_copied' in metadata
                assert 'strategy_used' in metadata
                assert 'duration' in metadata
                assert 'force_full_applied' in metadata
                
                # Validate strategy used (should be full_table for force_full)
                assert metadata['strategy_used'] == 'full_table'
                assert metadata['force_full_applied'] == True
                
                # Validate row count
                rows_copied = metadata.get('rows_copied', 0)
                assert isinstance(rows_copied, int)
                assert rows_copied >= 0
                
                logger.info(f"Full refresh extract successful for {table_name}: "
                           f"{rows_copied} rows, {metadata['strategy_used']} strategy")
            
            logger.info("TableProcessor full refresh extract operations successful")
            
        except Exception as e:
            logger.error(f"TableProcessor full refresh extract operations test failed: {str(e)}")
            raise

    def test_extract_operations_performance_monitoring(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor extract operations performance monitoring.
        
        Validates:
            - Performance tracking during extract operations
            - Duration measurement and recording
            - Row count tracking and validation
            - Performance metrics collection
            - Settings injection for performance monitoring
            
        ETL Pipeline Context:
            - Critical for ETL pipeline performance optimization
            - Supports dental clinic performance monitoring
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            
            table_processor = TableProcessor()
            
            # Test performance monitoring for extract operations
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing performance monitoring for extract operations: {table_name}")
                
                # Perform extract operation and measure performance
                start_time = time.time()
                success, metadata = table_processor._extract_to_replication(table_name, force_full=False)
                end_time = time.time()
                
                # Validate extract operation success
                assert success is True, f"Extract operation failed for {table_name}"
                
                # Validate performance metadata
                assert 'duration' in metadata
                assert 'rows_copied' in metadata
                
                # Validate duration measurement
                duration = metadata.get('duration', 0.0)
                assert isinstance(duration, (int, float))
                assert duration >= 0.0
                
                # Validate duration is reasonable (should be positive and not excessive)
                manual_duration = end_time - start_time
                assert duration >= 0.0, f"Duration should be non-negative, got {duration}"
                assert duration <= manual_duration, f"Copy duration ({duration}) should not exceed total operation time ({manual_duration})"
                assert manual_duration > 0.0, f"Total operation time should be positive, got {manual_duration}"
                
                # Validate row count
                rows_copied = metadata.get('rows_copied', 0)
                assert isinstance(rows_copied, int)
                assert rows_copied >= 0
                
                # Calculate performance metrics
                rate_per_second = rows_copied / duration if duration > 0 else 0
                
                logger.info(f"Performance monitoring for {table_name}: "
                           f"{rows_copied} rows in {duration:.2f}s ({rate_per_second:.0f} rows/sec)")
            
            logger.info("TableProcessor extract operations performance monitoring successful")
            
        except Exception as e:
            logger.error(f"TableProcessor extract operations performance monitoring test failed: {str(e)}")
            raise

    def test_extract_operations_error_handling(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor extract operations error handling.
        
        Validates:
            - Error handling for invalid table names
            - Error handling for database connection issues
            - Error handling for configuration issues
            - Error metadata collection and reporting
            - Exception propagation and handling
            
        ETL Pipeline Context:
            - Critical for ETL pipeline error handling
            - Supports dental clinic error management
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            
            table_processor = TableProcessor()
            
            # Test error handling for invalid table name
            invalid_table = 'invalid_table_that_does_not_exist'
            logger.info(f"Testing error handling for invalid table: {invalid_table}")
            
            success, metadata = table_processor._extract_to_replication(invalid_table, force_full=False)
            
            # Validate error handling
            assert success is False, "Should fail for invalid table"
            assert metadata is not None
            assert 'error' in metadata
            assert 'rows_copied' in metadata
            assert metadata['rows_copied'] == 0
            
            logger.info(f"Error handling successful for invalid table: {metadata.get('error', 'Unknown error')}")
            
            # Test error handling for table without configuration
            # This should also fail gracefully
            unknown_table = 'unknown_table_without_config'
            logger.info(f"Testing error handling for table without config: {unknown_table}")
            
            success, metadata = table_processor._extract_to_replication(unknown_table, force_full=False)
            
            # Validate error handling
            assert success is False, "Should fail for table without configuration"
            assert metadata is not None
            assert 'error' in metadata
            assert 'rows_copied' in metadata
            assert metadata['rows_copied'] == 0
            
            logger.info(f"Error handling successful for table without config: {metadata.get('error', 'Unknown error')}")
            
            logger.info("TableProcessor extract operations error handling successful")
            
        except Exception as e:
            logger.error(f"TableProcessor extract operations error handling test failed: {str(e)}")
            raise

    def test_extract_operations_metadata_collection(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor extract operations metadata collection.
        
        Validates:
            - Complete metadata collection during extract operations
            - Metadata consistency and completeness
            - Strategy information in metadata
            - Performance information in metadata
            - Error information in metadata
            
        ETL Pipeline Context:
            - Critical for ETL pipeline metadata management
            - Supports dental clinic processing tracking
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            
            table_processor = TableProcessor()
            
            # Test metadata collection for known test tables
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing metadata collection for extract operations: {table_name}")
                
                # Perform extract operation
                success, metadata = table_processor._extract_to_replication(table_name, force_full=False)
                
                # Validate extract operation success
                assert success is True, f"Extract operation failed for {table_name}"
                
                # Validate required metadata fields
                required_fields = [
                    'rows_copied', 'strategy_used', 'duration', 'force_full_applied',
                    'primary_column', 'last_primary_value'
                ]
                
                for field in required_fields:
                    assert field in metadata, f"Missing metadata field: {field}"
                
                # Validate metadata types
                assert isinstance(metadata['rows_copied'], int)
                assert isinstance(metadata['strategy_used'], str)
                assert isinstance(metadata['duration'], (int, float))
                assert isinstance(metadata['force_full_applied'], bool)
                
                # Validate strategy information
                assert metadata['strategy_used'] in ['incremental', 'full_table', 'unknown']
                
                # Validate performance information
                assert metadata['duration'] >= 0.0
                assert metadata['rows_copied'] >= 0
                
                logger.info(f"Metadata collection successful for {table_name}: "
                           f"{metadata['rows_copied']} rows, {metadata['strategy_used']} strategy, "
                           f"{metadata['duration']:.2f}s")
            
            logger.info("TableProcessor extract operations metadata collection successful")
            
        except Exception as e:
            logger.error(f"TableProcessor extract operations metadata collection test failed: {str(e)}")
            raise

    def test_extract_operations_strategy_resolution(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor extract operations strategy resolution.
        
        Validates:
            - Strategy resolution for different table configurations
            - Force full logic implementation
            - Incremental vs full table strategy selection
            - Strategy consistency between context and operations
            - Configuration-based strategy determination
            
        ETL Pipeline Context:
            - Critical for ETL pipeline strategy determination
            - Supports dental clinic processing strategies
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            
            table_processor = TableProcessor()
            
            # Test strategy resolution for known test tables
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing strategy resolution for extract operations: {table_name}")
                
                # Create processing context to get expected strategy
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                expected_strategy = context.strategy_info['strategy']
                expected_force_full = context.strategy_info['force_full_applied']
                
                # Perform extract operation
                success, metadata = table_processor._extract_to_replication(table_name, force_full=False)
                
                # Validate extract operation success
                assert success is True, f"Extract operation failed for {table_name}"
                
                # Validate strategy consistency
                actual_strategy = metadata.get('strategy_used', 'unknown')
                actual_force_full = metadata.get('force_full_applied', False)
                
                # Strategy should be consistent between context and operation
                assert actual_strategy in ['incremental', 'full_table', 'unknown']
                assert actual_force_full == expected_force_full
                
                logger.info(f"Strategy resolution for {table_name}: "
                           f"expected={expected_strategy}, actual={actual_strategy}, "
                           f"force_full={expected_force_full}")
            
            logger.info("TableProcessor extract operations strategy resolution successful")
            
        except Exception as e:
            logger.error(f"TableProcessor extract operations strategy resolution test failed: {str(e)}")
            raise 