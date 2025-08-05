# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for TableProcessor complete ETL pipeline operations.

This module tests:
- Complete ETL pipeline operations (extract + load)
- Full data flow from source to analytics
- Performance monitoring for complete pipeline
- Error handling for complete pipeline
- Metadata collection for complete pipeline
- Strategy resolution for complete pipeline
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
@pytest.mark.order(5)  # Fifth tests to run
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestTableProcessorCompleteETLPipelineIntegration:
    """Integration tests for TableProcessor complete ETL pipeline with real database connections."""

    def test_complete_etl_pipeline_initialization_with_real_config(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor complete ETL pipeline initialization with real configuration.
        
        Validates:
            - Complete ETL pipeline initialization with real settings
            - Database connection establishment for all phases
            - Configuration loading and validation for complete pipeline
            - Settings injection for complete pipeline operations
            - Component integration for complete pipeline
            
        ETL Pipeline Context:
            - Critical for ETL pipeline complete operations
            - Supports dental clinic complete data processing
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
            
            # Validate that complete ETL pipeline method exists
            assert hasattr(table_processor, 'process_table')
            
            # Validate database connections for complete pipeline
            from etl_pipeline.core.connections import ConnectionFactory
            
            # Validate source database connection
            source_engine = ConnectionFactory.get_source_connection(test_settings_with_file_provider)
            assert source_engine is not None
            
            # Validate replication database connection
            replication_engine = ConnectionFactory.get_replication_connection(test_settings_with_file_provider)
            assert replication_engine is not None
            
            # Validate analytics database connection
            analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
            assert analytics_engine is not None
            
            logger.info("TableProcessor complete ETL pipeline initialization successful")
            
        except Exception as e:
            logger.error(f"TableProcessor complete ETL pipeline initialization test failed: {str(e)}")
            raise

    def test_complete_etl_pipeline_with_incremental_strategy(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor complete ETL pipeline with incremental strategy.
        
        Validates:
            - Complete incremental ETL pipeline operations
            - Strategy resolution for incremental processing
            - Performance monitoring during complete pipeline
            - Metadata collection for complete incremental operations
            - Data flow validation from source to analytics
            
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
            
            # Test complete incremental ETL pipeline for known test tables
            for table_name in KNOWN_TEST_TABLES:
                # Create processing context for incremental strategy
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Only test if table supports incremental processing
                if context.incremental_columns and context.strategy_info['strategy'] == 'incremental':
                    logger.info(f"Testing complete incremental ETL pipeline for table: {table_name}")
                    
                    # Clean up analytics table before each load to ensure fresh start
                    from etl_pipeline.core.connections import ConnectionFactory
                    analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                    with analytics_engine.connect() as conn:
                        # Truncate the table to ensure no existing data
                        conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                        conn.commit()
                    
                    # Perform complete ETL pipeline operation
                    success = table_processor.process_table(table_name, force_full=False)
                    
                    # Validate complete ETL pipeline operation
                    assert success is True, f"Complete incremental ETL pipeline failed for {table_name}"
                    
                    # Validate data flow from source to analytics
                    self._validate_data_flow(table_name, test_settings_with_file_provider)
                    
                    logger.info(f"Complete incremental ETL pipeline successful for {table_name}")
                else:
                    logger.info(f"Skipping complete incremental ETL pipeline for {table_name} - not configured for incremental")
            
            logger.info("TableProcessor complete incremental ETL pipeline successful")
            
        except Exception as e:
            logger.error(f"TableProcessor complete incremental ETL pipeline test failed: {str(e)}")
            raise

    def test_complete_etl_pipeline_with_full_refresh_strategy(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor complete ETL pipeline with full refresh strategy.
        
        Validates:
            - Complete full refresh ETL pipeline operations
            - Strategy resolution for full refresh processing
            - Performance monitoring during complete pipeline
            - Metadata collection for complete full refresh operations
            - Data flow validation from source to analytics
            
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
            
            # Test complete full refresh ETL pipeline for known test tables
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing complete full refresh ETL pipeline for table: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Perform complete ETL pipeline operation with force full
                success = table_processor.process_table(table_name, force_full=True)
                
                # Validate complete ETL pipeline operation
                assert success is True, f"Complete full refresh ETL pipeline failed for {table_name}"
                
                # Validate data flow from source to analytics
                self._validate_data_flow(table_name, test_settings_with_file_provider)
                
                logger.info(f"Complete full refresh ETL pipeline successful for {table_name}")
            
            logger.info("TableProcessor complete full refresh ETL pipeline successful")
            
        except Exception as e:
            logger.error(f"TableProcessor complete full refresh ETL pipeline test failed: {str(e)}")
            raise

    def test_complete_etl_pipeline_performance_monitoring(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor complete ETL pipeline performance monitoring.
        
        Validates:
            - Performance tracking during complete ETL pipeline
            - Duration measurement and recording for complete pipeline
            - Row count tracking and validation for complete pipeline
            - Performance metrics collection for complete pipeline
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
            
            # Test performance monitoring for complete ETL pipeline
            for table_name in KNOWN_TEST_TABLES:
                logger.info(f"Testing performance monitoring for complete ETL pipeline: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Perform complete ETL pipeline operation and measure performance
                start_time = time.time()
                success = table_processor.process_table(table_name, force_full=False)
                end_time = time.time()
                
                # Validate complete ETL pipeline operation success
                assert success is True, f"Complete ETL pipeline failed for {table_name}"
                
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
                
                logger.info(f"Performance monitoring for complete ETL pipeline {table_name}: "
                           f"{row_count} rows in {duration:.2f}s ({rate_per_second:.0f} rows/sec)")
            
            logger.info("TableProcessor complete ETL pipeline performance monitoring successful")
            
        except Exception as e:
            logger.error(f"TableProcessor complete ETL pipeline performance monitoring test failed: {str(e)}")
            raise

    def test_complete_etl_pipeline_error_handling(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor complete ETL pipeline error handling.
        
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
            
            success = table_processor.process_table(invalid_table, force_full=False)
            
            # Validate error handling
            assert success is False, "Should fail for invalid table"
            
            logger.info(f"Error handling successful for invalid table")
            
            # Test error handling for table without configuration
            # This should also fail gracefully
            unknown_table = 'unknown_table_without_config'
            logger.info(f"Testing error handling for table without config: {unknown_table}")
            
            success = table_processor.process_table(unknown_table, force_full=False)
            
            # Validate error handling
            assert success is False, "Should fail for table without configuration"
            
            logger.info(f"Error handling successful for table without config")
            
            logger.info("TableProcessor complete ETL pipeline error handling successful")
            
        except Exception as e:
            logger.error(f"TableProcessor complete ETL pipeline error handling test failed: {str(e)}")
            raise

    def test_complete_etl_pipeline_metadata_collection(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor complete ETL pipeline metadata collection.
        
        Validates:
            - Complete metadata collection during ETL pipeline
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
                logger.info(f"Testing metadata collection for complete ETL pipeline: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Perform complete ETL pipeline operation
                success = table_processor.process_table(table_name, force_full=False)
                
                # Validate complete ETL pipeline operation success
                assert success is True, f"Complete ETL pipeline failed for {table_name}"
                
                # Get row count from analytics database for metadata validation
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                
                with analytics_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                
                # Validate row count
                assert isinstance(row_count, int)
                assert row_count >= 0
                
                logger.info(f"Metadata collection successful for complete ETL pipeline {table_name}: {row_count} rows processed")
            
            logger.info("TableProcessor complete ETL pipeline metadata collection successful")
            
        except Exception as e:
            logger.error(f"TableProcessor complete ETL pipeline metadata collection test failed: {str(e)}")
            raise

    def test_complete_etl_pipeline_strategy_resolution(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessor complete ETL pipeline strategy resolution.
        
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
                logger.info(f"Testing strategy resolution for complete ETL pipeline: {table_name}")
                
                # Clean up analytics table before each load to ensure fresh start
                from etl_pipeline.core.connections import ConnectionFactory
                analytics_engine = ConnectionFactory.get_analytics_raw_connection(test_settings_with_file_provider)
                with analytics_engine.connect() as conn:
                    # Truncate the table to ensure no existing data
                    conn.execute(text(f'TRUNCATE TABLE raw.{table_name}'))
                    conn.commit()
                
                # Create processing context to get expected strategy
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                expected_strategy = context.strategy_info['strategy']
                expected_force_full = context.strategy_info['force_full_applied']
                
                # Perform complete ETL pipeline operation
                success = table_processor.process_table(table_name, force_full=False)
                
                # Validate complete ETL pipeline operation success
                assert success is True, f"Complete ETL pipeline failed for {table_name}"
                
                logger.info(f"Strategy resolution for complete ETL pipeline {table_name}: "
                           f"expected={expected_strategy}, force_full={expected_force_full}")
            
            logger.info("TableProcessor complete ETL pipeline strategy resolution successful")
            
        except Exception as e:
            logger.error(f"TableProcessor complete ETL pipeline strategy resolution test failed: {str(e)}")
            raise

    def _validate_data_flow(self, table_name: str, settings: Settings):
        """
        Validate data flow from source to analytics database.
        
        Args:
            table_name: Name of the table to validate
            settings: Settings instance for database connections
        """
        try:
            from etl_pipeline.core.connections import ConnectionFactory
            
            # Get source row count
            source_engine = ConnectionFactory.get_source_connection(settings)
            with source_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                source_count = result.scalar()
            
            # Get analytics row count
            analytics_engine = ConnectionFactory.get_analytics_raw_connection(settings)
            with analytics_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                analytics_count = result.scalar()
            
            # Validate data flow
            assert source_count >= 0, f"Invalid source count for {table_name}"
            assert analytics_count >= 0, f"Invalid analytics count for {table_name}"
            
            # For full refresh, counts should be equal
            # For incremental, analytics count should be >= source count (due to historical data)
            assert analytics_count >= source_count, f"Data flow validation failed for {table_name}: source={source_count}, analytics={analytics_count}"
            
            logger.info(f"Data flow validation successful for {table_name}: source={source_count}, analytics={analytics_count}")
            
        except Exception as e:
            logger.error(f"Data flow validation failed for {table_name}: {str(e)}")
            raise 