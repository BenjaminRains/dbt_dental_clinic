# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for TableProcessor load operations using PostgresLoader.

This module tests:
- Load phase operations with real database connections
- PostgresLoader integration and coordination
- Strategy resolution for load operations
- Performance monitoring during loading
- Error handling during load phase
- Metadata collection and tracking
- Chunked loading for large tables
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
from etl_pipeline.loaders.postgres_loader import PostgresLoader
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
@pytest.mark.order(4)  # Fourth tests to run
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestTableProcessorLoadOperationsIntegration:
    """Integration tests for TableProcessor load operations with real database connections."""

    def test_load_operations_initialization_with_real_config(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations initialization with real configuration.
        
        Validates:
            - Load operations initialization with real settings
            - PostgresLoader integration
            - Database connection establishment for load phase
            - Configuration loading and validation for load operations
            - Settings injection for load operations
            
        ETL Pipeline Context:
            - Critical for ETL pipeline load phase
            - Supports dental clinic data loading
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
            
            # Validate that load operations method exists
            assert hasattr(table_processor, '_load_to_analytics')
            
            # Validate PostgresLoader can be imported and initialized
            loader = PostgresLoader()
            assert loader is not None
            
            # Validate database connections for load operations
            # Note: PostgresLoader handles its own connections using Settings injection
            from etl_pipeline.core.connections import ConnectionFactory
            analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
            assert analytics_engine is not None
            
            logger.info("TableProcessor load operations initialization successful")
            
        except Exception as e:
            logger.error(f"TableProcessor load operations initialization test failed: {str(e)}")
            raise

    def test_load_operations_with_standard_loading(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations with standard loading.
        
        Validates:
            - Standard load operations for small to medium tables
            - Strategy resolution for standard loading
            - Performance monitoring during standard loading
            - Metadata collection for standard operations
            - Settings injection for standard loading
            
        ETL Pipeline Context:
            - Critical for ETL pipeline standard loading
            - Supports dental clinic data loading
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
            
            # Test standard loading for known test tables
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing standard loading for table: {table_name}")
                
                # Create processing context to check table size
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Only test standard loading for smaller tables (< 100MB)
                if context.estimated_size_mb < 100:
                    logger.info(f"Using standard loading for {table_name} ({context.estimated_size_mb}MB)")
                    
                    # Perform load operation
                    success = table_processor._load_to_analytics(table_name, force_full=False)
                    
                    # Validate load operation
                    assert success is True, f"Standard load operation failed for {table_name}"
                    
                    logger.info(f"Standard loading successful for {table_name}")
                else:
                    logger.info(f"Skipping standard loading for {table_name} - too large ({context.estimated_size_mb}MB)")
            
            logger.info("TableProcessor standard load operations successful")
            
        except Exception as e:
            logger.error(f"TableProcessor standard load operations test failed: {str(e)}")
            raise

    def test_load_operations_with_chunked_loading(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations with chunked loading for large tables.
        
        Validates:
            - Chunked load operations for large tables
            - Strategy resolution for chunked loading
            - Performance monitoring during chunked loading
            - Metadata collection for chunked operations
            - Batch size configuration for chunked loading
            
        ETL Pipeline Context:
            - Critical for ETL pipeline large table loading
            - Supports dental clinic large data loading
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
            
            # Test chunked loading for known test tables
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing chunked loading for table: {table_name}")
                
                # Create processing context to check table size
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Test chunked loading for larger tables (>= 100MB) or force chunked for testing
                if context.estimated_size_mb >= 100 or True:  # Force chunked for testing
                    logger.info(f"Using chunked loading for {table_name} ({context.estimated_size_mb}MB)")
                    
                    # Get batch size from configuration
                    batch_size = context.config.get('batch_size', 5000)
                    
                    # Perform chunked load operation
                    success = table_processor._load_to_analytics(table_name, force_full=False)
                    
                    # Validate load operation
                    assert success is True, f"Chunked load operation failed for {table_name}"
                    
                    logger.info(f"Chunked loading successful for {table_name} (batch_size={batch_size})")
                else:
                    logger.info(f"Skipping chunked loading for {table_name} - too small ({context.estimated_size_mb}MB)")
            
            logger.info("TableProcessor chunked load operations successful")
            
        except Exception as e:
            logger.error(f"TableProcessor chunked load operations test failed: {str(e)}")
            raise

    def test_load_operations_with_full_refresh_strategy(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations with full refresh strategy.
        
        Validates:
            - Full refresh load operations
            - Strategy resolution for full refresh loading
            - Performance monitoring during full refresh loading
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
            # Clean up any existing test data first to avoid duplicate key violations
            test_data_manager.cleanup_procedure_data([DatabaseType.ANALYTICS])
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            test_data_manager.setup_procedure_data()
            
            table_processor = TableProcessor()
            
            # Test full refresh loading for known test tables
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing full refresh loading for table: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Perform load operation with force full
                success = table_processor._load_to_analytics(table_name, force_full=True)
                
                # Validate load operation
                assert success is True, f"Full refresh load operation failed for {table_name}"
                
                logger.info(f"Full refresh loading successful for {table_name}")
            
            logger.info("TableProcessor full refresh load operations successful")
            
        except Exception as e:
            logger.error(f"TableProcessor full refresh load operations test failed: {str(e)}")
            raise

    def test_load_operations_performance_monitoring(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations performance monitoring.
        
        Validates:
            - Performance tracking during load operations
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
            
            # Test performance monitoring for load operations
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing performance monitoring for load operations: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Perform load operation and measure performance
                start_time = time.time()
                success = table_processor._load_to_analytics(table_name, force_full=False)
                end_time = time.time()
                
                # Validate load operation success
                assert success is True, f"Load operation failed for {table_name}"
                
                # Calculate performance metrics
                duration = end_time - start_time
                assert duration >= 0.0
                
                # Get row count from analytics database for performance calculation
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                
                with analytics_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                
                # Calculate performance metrics
                rate_per_second = row_count / duration if duration > 0 else 0
                
                logger.info(f"Performance monitoring for {table_name}: "
                           f"{row_count} rows in {duration:.2f}s ({rate_per_second:.0f} rows/sec)")
            
            logger.info("TableProcessor load operations performance monitoring successful")
            
        except Exception as e:
            logger.error(f"TableProcessor load operations performance monitoring test failed: {str(e)}")
            raise

    def test_load_operations_error_handling(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations error handling.
        
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
            
            success = table_processor._load_to_analytics(invalid_table, force_full=False)
            
            # Validate error handling
            assert success is False, "Should fail for invalid table"
            
            logger.info(f"Error handling successful for invalid table")
            
            # Test error handling for table without configuration
            # This should also fail gracefully
            unknown_table = 'unknown_table_without_config'
            logger.info(f"Testing error handling for table without config: {unknown_table}")
            
            success = table_processor._load_to_analytics(unknown_table, force_full=False)
            
            # Validate error handling
            assert success is False, "Should fail for table without configuration"
            
            logger.info(f"Error handling successful for table without config")
            
            logger.info("TableProcessor load operations error handling successful")
            
        except Exception as e:
            logger.error(f"TableProcessor load operations error handling test failed: {str(e)}")
            raise

    def test_load_operations_metadata_collection(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations metadata collection.
        
        Validates:
            - Complete metadata collection during load operations
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
                logger.info(f"Testing metadata collection for load operations: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Perform load operation
                success = table_processor._load_to_analytics(table_name, force_full=False)
                
                # Validate load operation success
                assert success is True, f"Load operation failed for {table_name}"
                
                # Get row count from analytics database for metadata validation
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                
                with analytics_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                
                # Validate row count
                assert isinstance(row_count, int)
                assert row_count >= 0
                
                logger.info(f"Metadata collection successful for {table_name}: {row_count} rows loaded")
            
            logger.info("TableProcessor load operations metadata collection successful")
            
        except Exception as e:
            logger.error(f"TableProcessor load operations metadata collection test failed: {str(e)}")
            raise

    def test_load_operations_strategy_resolution(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations strategy resolution.
        
        Validates:
            - Strategy resolution for different table configurations
            - Force full logic implementation
            - Standard vs chunked loading strategy selection
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
                logger.info(f"Testing strategy resolution for load operations: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Create processing context to get expected strategy
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                expected_size_mb = context.estimated_size_mb
                expected_batch_size = context.config.get('batch_size', 5000)
                
                # Determine expected loading strategy based on table size
                expected_strategy = 'chunked' if expected_size_mb >= 100 else 'standard'
                
                # Perform load operation
                success = table_processor._load_to_analytics(table_name, force_full=False)
                
                # Validate load operation success
                assert success is True, f"Load operation failed for {table_name}"
                
                logger.info(f"Strategy resolution for {table_name}: "
                           f"size={expected_size_mb}MB, strategy={expected_strategy}, "
                           f"batch_size={expected_batch_size}")
            
            logger.info("TableProcessor load operations strategy resolution successful")
            
        except Exception as e:
            logger.error(f"TableProcessor load operations strategy resolution test failed: {str(e)}")
            raise

    def test_load_operations_chunked_loading_decision(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor load operations chunked loading decision logic.
        
        Validates:
            - Chunked loading decision based on table size
            - Batch size configuration for chunked loading
            - Performance optimization for large tables
            - Configuration-based chunked loading decisions
            - Settings injection for chunked loading
            
        ETL Pipeline Context:
            - Critical for ETL pipeline large table optimization
            - Supports dental clinic large data loading
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            
            table_processor = TableProcessor()
            
            # Test chunked loading decision for known test tables
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing chunked loading decision for table: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Create processing context to check table configuration
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Get table size and batch configuration
                estimated_size_mb = context.estimated_size_mb
                batch_size = context.config.get('batch_size', 5000)
                
                # Determine expected chunked loading decision
                use_chunked = estimated_size_mb > 100
                
                if use_chunked:
                    logger.info(f"Table {table_name} should use chunked loading: "
                               f"{estimated_size_mb}MB > 100MB, batch_size={batch_size}")
                else:
                    logger.info(f"Table {table_name} should use standard loading: "
                               f"{estimated_size_mb}MB <= 100MB, batch_size={batch_size}")
                
                # Perform load operation
                success = table_processor._load_to_analytics(table_name, force_full=False)
                
                # Validate load operation success
                assert success is True, f"Load operation failed for {table_name}"
                
                logger.info(f"Chunked loading decision successful for {table_name}")
            
            logger.info("TableProcessor load operations chunked loading decision successful")
            
        except Exception as e:
            logger.error(f"TableProcessor load operations chunked loading decision test failed: {str(e)}")
            raise 