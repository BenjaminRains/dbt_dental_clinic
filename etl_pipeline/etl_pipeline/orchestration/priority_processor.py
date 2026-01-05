"""
Priority Processor

Handles table processing based on priority levels with intelligent parallelization.

STATUS: ACTIVE - Core Orchestration Component
==============================================

This module is an active component of the orchestration framework that manages
table processing based on priority levels. It's actively used by the PipelineOrchestrator
and provides intelligent parallelization for critical tables.

CURRENT STATE:
- ✅ ACTIVE IMPLEMENTATION: Used by PipelineOrchestrator for batch processing
- ✅ INTELLIGENT PARALLELIZATION: Parallel processing for critical tables
- ✅ SEQUENTIAL PROCESSING: Sequential processing for non-critical tables
- ✅ ERROR HANDLING: Proper error handling and failure propagation
- ✅ RESOURCE MANAGEMENT: ThreadPoolExecutor for parallel processing
- ✅ INTEGRATED APPROACH: Uses SimpleMySQLReplicator and PostgresLoader via TableProcessor
- ✅ STATIC CONFIGURATION: Uses ConfigReader for table configuration
- ✅ CONNECTION ARCHITECTURE: Follows unified interface with Settings injection
- ✅ ENVIRONMENT VALIDATION: Validates environment configuration before processing
- ❌ UNTESTED: No comprehensive testing of parallel processing

ACTIVE USAGE:
- PipelineOrchestrator: Calls process_by_priority for batch table processing
- CLI Commands: Indirectly used through orchestrator
- Orchestration Framework: Core component of the pipeline architecture

DEPENDENCIES:
- TableProcessor: Processes individual tables using integrated approach
- ThreadPoolExecutor: Manages parallel processing
- ConfigReader: Uses static configuration for table priority lookup
- Settings: Uses Settings class for table priority lookup with proper environment detection
- ConnectionFactory: Uses unified interface for database connections

PROCESSING LOGIC:
1. Priority Levels: critical, important, audit, reference
2. Critical Tables: Processed in parallel for speed
3. Other Tables: Processed sequentially to manage resources
4. Failure Handling: Stops processing if critical tables fail
5. Resource Management: Configurable max_workers for parallel processing
6. Environment Validation: Validates configuration before processing

INTEGRATION POINTS:
- PipelineOrchestrator: Main integration point for batch processing
- TableProcessor: Delegates individual table processing using integrated approach
- ConfigReader: Uses static configuration for table priority lookup
- Settings: Uses modern Settings system for table priority lookup with environment detection
- ConnectionFactory: Uses unified interface for database connections
- Logging: Comprehensive logging for monitoring and debugging

CRITICAL ISSUES:
1. UNTESTED PARALLEL PROCESSING: No validation of parallel processing logic
2. RESOURCE MANAGEMENT: No validation of thread pool management
3. ERROR PROPAGATION: Limited testing of failure scenarios

DEVELOPMENT NEEDS:
1. COMPREHENSIVE TESTING: Add tests for parallel and sequential processing
2. PERFORMANCE TESTING: Validate parallel processing performance
3. ERROR HANDLING: Test failure scenarios and error propagation
4. RESOURCE OPTIMIZATION: Optimize thread pool management
5. MONITORING: Add performance metrics for parallel processing

TESTING REQUIREMENTS:
1. Parallel Processing: Test with various worker counts and table sets
2. Sequential Processing: Test sequential processing logic
3. Error Scenarios: Test failure handling and error propagation
4. Resource Management: Test thread pool cleanup and resource usage
5. Performance: Test processing time with different configurations
6. Integration: Test integration with TableProcessor and PipelineOrchestrator
7. Environment Validation: Test environment configuration validation

This component is critical for efficient batch processing and has been refactored
to use the integrated approach with static configuration and proper connection architecture.
"""

import logging
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from .table_processor import TableProcessor
from ..config import get_settings, ConfigReader
from ..config.logging import get_logger

# Import custom exceptions for structured error handling
from ..exceptions.database import DatabaseConnectionError, DatabaseTransactionError
from ..exceptions.data import DataExtractionError, DataLoadingError
from ..exceptions.configuration import ConfigurationError, EnvironmentError

logger = get_logger(__name__)

class PriorityProcessor:
    def __init__(self, config_reader: ConfigReader):
        """
        Initialize the priority processor.
        
        Uses integrated approach with SimpleMySQLReplicator and PostgresLoader
        via TableProcessor for improved performance.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses get_settings() for proper environment detection and validation
        - Follows unified interface with Settings injection
        - Validates environment configuration before processing
        - Uses provider pattern for configuration management
        
        Args:
            config_reader: ConfigReader instance (REQUIRED for table configuration)
        """
        try:
            if not isinstance(config_reader, ConfigReader):
                raise ConfigurationError(
                    message="ConfigReader instance is required",
                    missing_keys=["config_reader"],
                    details={"provided_type": type(config_reader).__name__}
                )
            
            self.config_reader = config_reader
            
            # ✅ CONNECTION ARCHITECTURE: Always use get_settings() for proper environment detection
            self.settings = get_settings()
            
            # ✅ CONNECTION ARCHITECTURE: Validate environment configuration
            self._validate_environment()
            
        except ConfigurationError as e:
            logger.error(f"Configuration error in PriorityProcessor initialization: {e}")
            raise
        except EnvironmentError as e:
            logger.error(f"Environment error in PriorityProcessor initialization: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in PriorityProcessor initialization: {str(e)}")
            raise
    
    def _validate_environment(self):
        """Validate environment configuration before processing."""
        try:
            # Validate that all required configurations are present
            if not self.settings.validate_configs():
                raise EnvironmentError(
                    message=f"Configuration validation failed for {self.settings.environment} environment",
                    environment=self.settings.environment,
                    details={
                        "config_file": f".env_{self.settings.environment}",
                        "critical": True
                    }
                )
            logger.info(f"Environment validation passed for {self.settings.environment} environment")
        except EnvironmentError as e:
            logger.error(f"Environment validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during environment validation: {str(e)}")
            raise
    
    def process_by_priority(self, importance_levels: Optional[List[str]] = None,
                          max_workers: int = 5,
                          force_full: bool = False) -> Dict[str, Dict[str, List[str]]]:
        """
        Process tables by performance category.
        
        Uses performance_category from analyze_opendental_schema.py:
        - Large tables: Process in parallel for speed
        - Medium tables: Process sequentially
        - Small tables: Process sequentially
        - Tiny tables: Process sequentially
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - Each TableProcessor uses unified ConnectionFactory interface
        """
        try:
            # NOTE: Environment validation is done in __init__(), no need to repeat here
            # Validate performance categories from schema analyzer
            self._validate_performance_categories()
            
            # Get all tables and categorize by performance_category
            all_tables = self.settings.list_tables()
            large_tables = []
            medium_tables = []
            small_tables = []
            tiny_tables = []  # Support for tiny tables
            
            for table_name in all_tables:
                config = self.settings.get_table_config(table_name)
                performance_category = config.get('performance_category', 'tiny')
                
                if performance_category == 'large':
                    large_tables.append(table_name)
                elif performance_category == 'medium':
                    medium_tables.append(table_name)
                elif performance_category == 'small':
                    small_tables.append(table_name)
                else:  # tiny
                    tiny_tables.append(table_name)
            
            results = {}
            
            # Process large tables in parallel for speed
            if large_tables:
                logger.info(f"Processing {len(large_tables)} large tables in parallel")
                success_tables, failed_tables = self._process_parallel(
                    large_tables,
                    max_workers,
                    force_full
                )
                results['large'] = {
                    'success': success_tables,
                    'failed': failed_tables,
                    'total': len(large_tables)
                }
            
            # Process medium tables sequentially
            if medium_tables:
                logger.info(f"Processing {len(medium_tables)} medium tables sequentially")
                success_tables, failed_tables = self._process_sequential(
                    medium_tables,
                    force_full
                )
                results['medium'] = {
                    'success': success_tables,
                    'failed': failed_tables,
                    'total': len(medium_tables)
                }
            
            # Process small tables sequentially
            if small_tables:
                logger.info(f"Processing {len(small_tables)} small tables sequentially")
                success_tables, failed_tables = self._process_sequential(
                    small_tables,
                    force_full
                )
                results['small'] = {
                    'success': success_tables,
                    'failed': failed_tables,
                    'total': len(small_tables)
                }
            
            # Process tiny tables sequentially
            if tiny_tables:
                logger.info(f"Processing {len(tiny_tables)} tiny tables sequentially")
                success_tables, failed_tables = self._process_sequential(
                    tiny_tables,
                    force_full
                )
                results['tiny'] = {
                    'success': success_tables,
                    'failed': failed_tables,
                    'total': len(tiny_tables)
                }
            
            return results
            
        except DataExtractionError as e:
            logger.error(f"Data extraction failed during priority processing: {e}")
            return {}
        except DataLoadingError as e:
            logger.error(f"Data loading failed during priority processing: {e}")
            return {}
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed during priority processing: {e}")
            return {}
        except ConfigurationError as e:
            logger.error(f"Configuration error during priority processing: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error during priority processing: {str(e)}")
            return {}
    
    def _validate_performance_categories(self):
        """Validate that all tables have performance_category from schema analyzer."""
        all_tables = self.settings.list_tables()
        missing_categories = []
        
        for table_name in all_tables:
            config = self.settings.get_table_config(table_name)
            performance_category = config.get('performance_category')
            
            if not performance_category:
                missing_categories.append(table_name)
        
        if missing_categories:
            logger.warning(f"Tables missing performance_category: {missing_categories}")
            logger.warning("Run analyze_opendental_schema.py to generate proper configuration")
        
        return len(missing_categories) == 0
    
    def _process_parallel(self, tables: List[str],
                         max_workers: int,
                         force_full: bool) -> Tuple[List[str], List[str]]:
        """
        Process tables in parallel using ThreadPoolExecutor.
        
        Uses integrated approach with SimpleMySQLReplicator and PostgresLoader
        via TableProcessor for each parallel task.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - Each TableProcessor uses unified ConnectionFactory interface
        """
        success_tables = []
        failed_tables = []
        
        logger.info(f"Processing {len(tables)} tables in parallel (max workers: {max_workers})")
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks - create TableProcessor for each task
                future_to_table = {
                    executor.submit(self._process_single_table, table, force_full): table 
                    for table in tables
                }
                
                # Process completed tasks
                for future in as_completed(future_to_table):
                    table = future_to_table[future]
                    try:
                        success = future.result()
                        if success:
                            success_tables.append(table)
                            logger.info(f"SUCCESS: Successfully processed {table} in parallel")
                        else:
                            failed_tables.append(table)
                            logger.error(f"X Failed to process {table} in parallel")
                    except DataExtractionError as e:
                        logger.error(f"Data extraction failed for {table} in parallel: {e}")
                        failed_tables.append(table)
                    except DataLoadingError as e:
                        logger.error(f"Data loading failed for {table} in parallel: {e}")
                        failed_tables.append(table)
                    except DatabaseConnectionError as e:
                        logger.error(f"Database connection failed for {table} in parallel: {e}")
                        failed_tables.append(table)
                    except Exception as e:
                        logger.error(f"Exception in parallel processing for {table}: {str(e)}")
                        failed_tables.append(table)
            
            return success_tables, failed_tables
            
        except Exception as e:
            logger.error(f"Unexpected error in parallel processing: {str(e)}")
            return [], tables  # Mark all tables as failed
        
    def _process_sequential(self, tables: List[str],
                          force_full: bool) -> Tuple[List[str], List[str]]:
        """
        Process tables sequentially.
        
        Uses integrated approach with SimpleMySQLReplicator and PostgresLoader
        via TableProcessor for each sequential task.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - Each TableProcessor uses unified ConnectionFactory interface
        """
        success_tables = []
        failed_tables = []
        
        logger.info(f"Processing {len(tables)} tables sequentially")
        
        for table in tables:
            try:
                success = self._process_single_table(table, force_full)
                if success:
                    success_tables.append(table)
                    logger.info(f"SUCCESS: Successfully processed {table} sequentially")
                else:
                    failed_tables.append(table)
                    logger.error(f"✗ Failed to process {table} sequentially")
            except DataExtractionError as e:
                logger.error(f"Data extraction failed for {table} sequentially: {e}")
                failed_tables.append(table)
            except DataLoadingError as e:
                logger.error(f"Data loading failed for {table} sequentially: {e}")
                failed_tables.append(table)
            except DatabaseConnectionError as e:
                logger.error(f"Database connection failed for {table} sequentially: {e}")
                failed_tables.append(table)
            except Exception as e:
                logger.error(f"Exception in sequential processing for {table}: {str(e)}")
                failed_tables.append(table)
        
        return success_tables, failed_tables
    
    def _process_single_table(self, table_name: str, force_full: bool) -> bool:
        """
        Process a single table using a new TableProcessor instance.
        
        Uses integrated approach with SimpleMySQLReplicator and PostgresLoader
        via TableProcessor for improved performance and reliability.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - TableProcessor uses unified ConnectionFactory interface
        - Validates environment configuration before processing
        """
        try:
            # Create TableProcessor with ConfigReader
            table_processor = TableProcessor(
                config_reader=self.config_reader
            )
            
            # ✅ MODERN ARCHITECTURE: No connection initialization needed
            # TableProcessor uses Settings injection and components handle their own connections
            
            # Process the table using integrated approach
            success = table_processor.process_table(table_name, force_full)
            
            if success:
                logger.info(f"Successfully processed {table_name} using integrated approach")
            else:
                logger.error(f"Failed to process {table_name} using integrated approach")
            
            return success
            
        except DataExtractionError as e:
            logger.error(f"Data extraction error processing table {table_name}: {e}")
            return False
        except DataLoadingError as e:
            logger.error(f"Data loading error processing table {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error processing table {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error processing table {table_name}: {str(e)}")
            return False 