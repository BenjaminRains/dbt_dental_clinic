# type: ignore  # SQLAlchemy type handling in integration tests

"""
Integration tests for TableProcessingContext strategy resolution and configuration.

This module tests:
- TableProcessingContext initialization with real configuration
- Strategy resolution for incremental vs full refresh
- Configuration access and validation
- Primary column resolution
- Performance category and priority handling
- Force full logic and strategy determination
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
@pytest.mark.order(2)  # Second tests to run
@pytest.mark.orchestration
@pytest.mark.etl_critical
@pytest.mark.provider_pattern
@pytest.mark.settings_injection
class TestTableProcessingContextIntegration:
    """Integration tests for TableProcessingContext with real database connections."""

    def test_processing_context_initialization_with_real_config(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessingContext initialization with real configuration.
        
        Validates:
            - TableProcessingContext initialization with real settings
            - Configuration loading and validation
            - Strategy resolution and determination
            - Performance category and priority extraction
            - Primary column resolution
            
        ETL Pipeline Context:
            - Critical for ETL pipeline strategy determination
            - Supports dental clinic configuration management
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
            
            # Test context initialization for known test tables
            for table_name in KNOWN_TEST_TABLES:
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Validate basic context properties
                assert context.table_name == table_name
                assert context.force_full == False
                assert context.config_reader == table_processor.config_reader
                
                # Validate configuration loading
                assert context.config is not None
                
                # Validate performance category
                assert context.performance_category in ['tiny', 'small', 'medium', 'large']
                
                # Validate processing priority
                assert isinstance(context.processing_priority, int)
                assert 1 <= context.processing_priority <= 10
                
                # Validate estimated metrics
                assert isinstance(context.estimated_size_mb, (int, float))
                assert isinstance(context.estimated_rows, int)
                
                # Validate strategy resolution
                assert context.strategy_info is not None
                assert 'strategy' in context.strategy_info
                assert 'force_full_applied' in context.strategy_info
                assert 'reason' in context.strategy_info
                
                logger.info(f"Processing context initialized successfully for table: {table_name}")
            
            logger.info("TableProcessingContext initialization with real config successful")
            
        except Exception as e:
            logger.error(f"TableProcessingContext initialization test failed: {str(e)}")
            raise

    def test_processing_context_strategy_resolution_incremental(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessingContext strategy resolution for incremental processing.
        
        Validates:
            - Incremental strategy resolution
            - Primary column identification
            - Force full logic for incremental tables
            - Strategy reason determination
            
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
            
            table_processor = TableProcessor()
            
            # Test incremental strategy resolution
            context = TableProcessingContext('patient', force_full=False, config_reader=table_processor.config_reader)
            
            # Validate strategy information
            strategy_info = context.strategy_info
            assert strategy_info is not None
            
            # Check if table supports incremental processing
            if context.incremental_columns:
                # Should use incremental strategy
                assert strategy_info['strategy'] in ['incremental', 'full_table']
                assert strategy_info['force_full_applied'] == False
                assert 'incremental' in strategy_info['reason'].lower() or 'full_table' in strategy_info['reason'].lower()
                
                # Validate primary column resolution
                if context.primary_column:
                    assert context.primary_column in context.incremental_columns
                
                logger.info(f"Incremental strategy resolved: {strategy_info['strategy']} - {strategy_info['reason']}")
            else:
                # Should use full table strategy
                assert strategy_info['strategy'] == 'full_table'
                assert strategy_info['force_full_applied'] == True
                assert 'no incremental columns' in strategy_info['reason'].lower()
                
                logger.info(f"Full table strategy resolved: {strategy_info['strategy']} - {strategy_info['reason']}")
            
            logger.info("TableProcessingContext incremental strategy resolution successful")
            
        except Exception as e:
            logger.error(f"TableProcessingContext incremental strategy resolution test failed: {str(e)}")
            raise

    def test_processing_context_strategy_resolution_force_full(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessingContext strategy resolution for force full processing.
        
        Validates:
            - Force full strategy resolution
            - Override of incremental logic
            - Strategy reason determination
            - Force full applied logic
            
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
            
            table_processor = TableProcessor()
            
            # Test force full strategy resolution
            context = TableProcessingContext('patient', force_full=True, config_reader=table_processor.config_reader)
            
            # Validate strategy information
            strategy_info = context.strategy_info
            assert strategy_info is not None
            
            # Should always use full table strategy when force_full=True
            assert strategy_info['strategy'] == 'full_table'
            assert strategy_info['force_full_applied'] == True
            assert 'explicit force_full' in strategy_info['reason'].lower()
            
            logger.info(f"Force full strategy resolved: {strategy_info['strategy']} - {strategy_info['reason']}")
            
            logger.info("TableProcessingContext force full strategy resolution successful")
            
        except Exception as e:
            logger.error(f"TableProcessingContext force full strategy resolution test failed: {str(e)}")
            raise

    def test_processing_context_primary_column_resolution(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessingContext primary column resolution.
        
        Validates:
            - Primary column identification from configuration
            - Fallback to first incremental column
            - Primary column validation
            - Configuration access for primary columns
            
        ETL Pipeline Context:
            - Critical for ETL pipeline incremental processing
            - Supports dental clinic primary key management
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            test_data_manager.setup_appointment_data()
            
            table_processor = TableProcessor()
            
            # Test primary column resolution for known test tables
            for table_name in KNOWN_TEST_TABLES:
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Validate primary column resolution
                if context.incremental_columns:
                    # Should have a primary column if incremental columns exist
                    if context.primary_column:
                        # Primary column should be in incremental columns
                        assert context.primary_column in context.incremental_columns
                        logger.info(f"Primary column resolved for {table_name}: {context.primary_column}")
                    else:
                        # No primary column configured
                        logger.info(f"No primary column configured for {table_name}")
                else:
                    # No incremental columns, so no primary column
                    assert context.primary_column is None
                    logger.info(f"No incremental columns for {table_name}, no primary column")
            
            logger.info("TableProcessingContext primary column resolution successful")
            
        except Exception as e:
            logger.error(f"TableProcessingContext primary column resolution test failed: {str(e)}")
            raise

    def test_processing_context_performance_metadata(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessingContext performance metadata extraction.
        
        Validates:
            - Performance category extraction
            - Processing priority extraction
            - Estimated size and row count extraction
            - Metadata completeness
            
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
            
            # Test performance metadata extraction for known test tables
            for table_name in KNOWN_TEST_TABLES:
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Validate performance category
                assert context.performance_category in ['tiny', 'small', 'medium', 'large']
                
                # Validate processing priority
                assert isinstance(context.processing_priority, int)
                assert 1 <= context.processing_priority <= 10
                
                # Validate estimated metrics
                assert isinstance(context.estimated_size_mb, (int, float))
                assert context.estimated_size_mb >= 0
                assert isinstance(context.estimated_rows, int)
                assert context.estimated_rows >= 0
                
                logger.info(f"Performance metadata for {table_name}: "
                           f"category={context.performance_category}, "
                           f"priority={context.processing_priority}, "
                           f"size={context.estimated_size_mb}MB, "
                           f"rows={context.estimated_rows}")
            
            logger.info("TableProcessingContext performance metadata extraction successful")
            
        except Exception as e:
            logger.error(f"TableProcessingContext performance metadata test failed: {str(e)}")
            raise

    def test_processing_context_get_processing_metadata(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessingContext get_processing_metadata method.
        
        Validates:
            - Complete metadata dictionary generation
            - All required metadata fields
            - Metadata consistency
            - Configuration integration
            
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
            
            # Test metadata generation for known test tables
            for table_name in KNOWN_TEST_TABLES:
                context = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Get complete processing metadata
                metadata = context.get_processing_metadata()
                
                # Validate required metadata fields
                assert 'table_name' in metadata
                assert 'config' in metadata
                assert 'strategy_info' in metadata
                assert 'performance_category' in metadata
                assert 'processing_priority' in metadata
                assert 'estimated_size_mb' in metadata
                assert 'estimated_rows' in metadata
                assert 'incremental_columns' in metadata
                assert 'primary_column' in metadata
                assert 'force_full' in metadata
                assert 'actual_force_full' in metadata
                
                # Validate metadata values
                assert metadata['table_name'] == table_name
                assert metadata['config'] == context.config
                assert metadata['strategy_info'] == context.strategy_info
                assert metadata['performance_category'] == context.performance_category
                assert metadata['processing_priority'] == context.processing_priority
                assert metadata['estimated_size_mb'] == context.estimated_size_mb
                assert metadata['estimated_rows'] == context.estimated_rows
                assert metadata['incremental_columns'] == context.incremental_columns
                assert metadata['primary_column'] == context.primary_column
                assert metadata['force_full'] == context.force_full
                assert metadata['actual_force_full'] == context.strategy_info['force_full_applied']
                
                logger.info(f"Processing metadata generated successfully for {table_name}")
            
            logger.info("TableProcessingContext get_processing_metadata successful")
            
        except Exception as e:
            logger.error(f"TableProcessingContext get_processing_metadata test failed: {str(e)}")
            raise

    def test_processing_context_with_no_configuration(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessingContext behavior with no table configuration.
        
        Validates:
            - Default values when no configuration exists
            - Strategy resolution for unknown tables
            - Error handling for missing configuration
            - Fallback behavior
            
        ETL Pipeline Context:
            - Critical for ETL pipeline error handling
            - Supports dental clinic configuration management
            - Uses real configuration for reliability
            - Optimized for dental clinic operational needs
        """
        try:
            # Setup test data
            test_data_manager = populated_test_databases
            test_data_manager.setup_patient_data()
            
            table_processor = TableProcessor()
            
            # Test context initialization for unknown table
            unknown_table = 'unknown_table_that_does_not_exist'
            context = TableProcessingContext(unknown_table, force_full=False, config_reader=table_processor.config_reader)
            
            # Validate default behavior
            assert context.config is None or context.config == {}
            assert context.performance_category == 'medium'  # Default value
            assert context.processing_priority == 5  # Default value
            assert context.estimated_size_mb == 0  # Default value
            assert context.estimated_rows == 0  # Default value
            assert context.incremental_columns == []  # Default value
            assert context.primary_column is None  # Default value
            
            # Validate strategy resolution for unknown table
            strategy_info = context.strategy_info
            assert strategy_info['strategy'] == 'full_table'
            assert strategy_info['force_full_applied'] == True
            assert 'no configuration found' in strategy_info['reason'].lower()
            
            logger.info(f"Processing context for unknown table: {strategy_info['strategy']} - {strategy_info['reason']}")
            
            logger.info("TableProcessingContext no configuration handling successful")
            
        except Exception as e:
            logger.error(f"TableProcessingContext no configuration test failed: {str(e)}")
            raise

    def test_processing_context_force_full_override_logic(self, test_settings_with_file_provider, populated_test_databases):
        """
        Test TableProcessingContext force full override logic.
        
        Validates:
            - Force full override of incremental logic
            - Strategy determination with force full
            - Configuration access with force full
            - Primary column handling with force full
            
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
            
            table_processor = TableProcessor()
            
            # Test force full override for known test tables
            for table_name in KNOWN_TEST_TABLES:
                # Test without force full
                context_normal = TableProcessingContext(table_name, force_full=False, config_reader=table_processor.config_reader)
                
                # Test with force full
                context_force_full = TableProcessingContext(table_name, force_full=True, config_reader=table_processor.config_reader)
                
                # Validate force full override
                assert context_force_full.force_full == True
                assert context_force_full.strategy_info['force_full_applied'] == True
                assert context_force_full.strategy_info['strategy'] == 'full_table'
                assert 'explicit force_full' in context_force_full.strategy_info['reason'].lower()
                
                # Compare with normal context
                if context_normal.strategy_info['strategy'] == 'incremental':
                    # Force full should override incremental strategy
                    assert context_force_full.strategy_info['strategy'] == 'full_table'
                    assert context_force_full.strategy_info['force_full_applied'] == True
                    assert context_normal.strategy_info['force_full_applied'] == False
                
                logger.info(f"Force full override for {table_name}: "
                           f"normal={context_normal.strategy_info['strategy']}, "
                           f"force_full={context_force_full.strategy_info['strategy']}")
            
            logger.info("TableProcessingContext force full override logic successful")
            
        except Exception as e:
            logger.error(f"TableProcessingContext force full override logic test failed: {str(e)}")
            raise 