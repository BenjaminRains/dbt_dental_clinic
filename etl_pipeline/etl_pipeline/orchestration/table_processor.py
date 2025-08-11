"""
Table Processor - Core ETL Component
===================================

This module is the ACTIVE core implementation of individual table ETL processing,
serving as the workhorse of the pipeline. It's actively used by both PipelineOrchestrator
and PriorityProcessor, making it the central component for table-level operations.

STATUS: ACTIVE - Core ETL Implementation (REFACTORED)
====================================================

CURRENT STATE:
- ✅ ACTIVE IMPLEMENTATION: Used by orchestrator and priority processor
- ✅ SIMPLIFIED ETL PIPELINE: Consolidated extract, load, and transform phases
- ✅ INTELLIGENT PROCESSING: Supports incremental and full refresh modes
- ✅ EFFICIENT CONFIGURATION: Uses Settings class efficiently
- ✅ STRAIGHTFORWARD LOGIC: Removed unnecessary abstraction layers
- ✅ TESTABLE: Simplified for easier testing and maintenance
- ✅ INTEGRATED APPROACH: Uses SimpleMySQLReplicator and PostgresLoader with static config

ACTIVE USAGE:
- PipelineOrchestrator: Calls process_table for individual table processing
- PriorityProcessor: Uses for parallel and sequential table processing
- CLI Commands: Indirectly used through orchestrator
- Integration Tests: Referenced in test files

SIMPLIFIED ARCHITECTURE:
1. SINGLE ETL METHOD: process_table handles all phases in one place
2. DIRECT SETTINGS USAGE: Uses Settings class directly without multiple lookups
3. SIMPLIFIED LOADING: Uses standard loading for all tables (chunked only when needed)
4. REDUCED DEPENDENCIES: Fewer abstraction layers and components
5. INTEGRATED APPROACH: Uses SimpleMySQLReplicator and PostgresLoader with static config

DEPENDENCIES:
- SimpleMySQLReplicator: MySQL-to-MySQL replication with static config
- PostgresLoader: MySQL-to-PostgreSQL loading with static config
- Settings: Configuration management
- ConnectionFactory: Database connections
- UnifiedMetricsCollector: Basic metrics collection

INTEGRATION POINTS:
- PipelineOrchestrator: Main orchestration integration
- PriorityProcessor: Batch processing integration
- Database Connections: Manages multiple database connections
- Configuration: Uses Settings for table-specific configuration
- Metrics: Integrates with basic metrics collection

ETL PIPELINE FLOW:
1. EXTRACT: Copy data from source MySQL to replication MySQL database using SimpleMySQLReplicator
2. LOAD: Copy data from replication MySQL to PostgreSQL analytics using PostgresLoader

CONSTRUCTOR REQUIREMENTS:
- config_reader: ConfigReader instance (optional, will be created if not provided)
- config_path: Path to configuration file (used for ConfigReader)

This component is the core of the ETL pipeline and has been refactored
to use the integrated approach with static configuration.
"""

import logging
import os
from typing import Dict, Optional, List, Tuple
from datetime import datetime
import time
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..core.connections import ConnectionFactory
from ..monitoring.unified_metrics import UnifiedMetricsCollector
from ..config.logging import get_logger
from ..core.postgres_schema import PostgresSchema
from ..config import get_settings, DatabaseType, PostgresSchema as ConfigPostgresSchema, ConfigReader

# Import custom exceptions for structured error handling
from ..exceptions.database import DatabaseConnectionError, DatabaseTransactionError
from ..exceptions.data import DataExtractionError, DataLoadingError
from ..exceptions.configuration import ConfigurationError, EnvironmentError

logger = get_logger(__name__)

class TableProcessingContext:
    """
    Unified configuration context for table processing operations.
    
    This class provides consistent access to table configuration, strategy resolution,
    and processing metadata to avoid repeated configuration loading and ensure
    consistency between SimpleMySQLReplicator and PostgresLoader.
    
    IMPROVED: Centralizes configuration access and strategy resolution.
    """
    
    def __init__(self, table_name: str, force_full: bool, config_reader: ConfigReader):
        """
        Initialize table processing context.
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force full refresh
            config_reader: ConfigReader instance for table configuration
        """
        self.table_name = table_name
        self.force_full = force_full
        self.config_reader = config_reader
        
        # Load table configuration once
        self.config = self.config_reader.get_table_config(table_name)
        
        # Extract common configuration values FIRST
        self.performance_category = self.config.get('performance_category', 'medium') if self.config else 'medium'
        self.processing_priority = self._convert_processing_priority(self.config.get('processing_priority', 5) if self.config else 5)
        self.estimated_size_mb = self.config.get('estimated_size_mb', 0) if self.config else 0
        self.estimated_rows = self.config.get('estimated_rows', 0) if self.config else 0
        self.incremental_columns = self.config.get('incremental_columns', []) if self.config else []
        self.primary_column = self._get_primary_incremental_column()
        
        # Resolve strategy information AFTER setting incremental_columns
        self.strategy_info = self._resolve_strategy()
        
        logger.debug(f"Created processing context for {table_name}: "
                    f"strategy={self.strategy_info['strategy']}, "
                    f"force_full={self.strategy_info['force_full_applied']}, "
                    f"category={self.performance_category}")
    
    def _convert_processing_priority(self, priority) -> int:
        """
        Convert processing priority to integer value.
        
        Args:
            priority: Priority value (string or int)
            
        Returns:
            Integer priority value (1-10, where 1 is highest priority)
        """
        if isinstance(priority, int):
            return max(1, min(10, priority))
        
        if isinstance(priority, str):
            priority_mapping = {
                'high': 1,
                'medium': 5,
                'low': 10
            }
            return priority_mapping.get(priority.lower(), 5)
        
        return 5  # Default to medium priority
    
    def _resolve_strategy(self) -> Dict:
        """
        Resolve extraction strategy for the table.
        
        Returns:
            Dict containing strategy information
        """
        if not self.config:
            return {
                'strategy': 'full_table',
                'force_full_applied': True,
                'reason': 'No configuration found, defaulting to full refresh',
                'incremental_columns': [],
                'extraction_strategy': 'full_table'
            }
        
        extraction_strategy = self.config.get('extraction_strategy', 'incremental')
        
        # Determine actual strategy
        actual_force_full = self.force_full
        strategy = 'full_table'
        reason = 'Explicit force_full requested'
        
        if not self.force_full:
            # Check if table has incremental columns
            if not self.incremental_columns:
                strategy = 'full_table'
                actual_force_full = True
                reason = 'No incremental columns available'
            elif extraction_strategy in ['incremental', 'incremental_chunked']:
                strategy = 'incremental'
                actual_force_full = False
                reason = f'{extraction_strategy} strategy with available incremental columns'
            else:
                strategy = 'full_table'
                actual_force_full = True
                reason = f'Configuration specifies {extraction_strategy} strategy'
        
        return {
            'strategy': strategy,
            'force_full_applied': actual_force_full,
            'reason': reason,
            'incremental_columns': self.incremental_columns,
            'extraction_strategy': extraction_strategy
        }
    
    def _get_primary_incremental_column(self) -> Optional[str]:
        """Get primary incremental column from configuration."""
        if not self.config:
            return None
        
        # Look for primary column in configuration
        primary_column = self.config.get('primary_column')
        if primary_column and primary_column != 'none':
            return primary_column
        
        # Fallback: use first incremental column as primary
        incremental_columns = self.config.get('incremental_columns', [])
        if incremental_columns:
            return incremental_columns[0]
        
        return None
    
    def get_processing_metadata(self) -> Dict:
        """
        Get comprehensive processing metadata for the table.
        
        Returns:
            Dict containing all processing metadata
        """
        return {
            'table_name': self.table_name,
            'config': self.config,
            'strategy_info': self.strategy_info,
            'performance_category': self.performance_category,
            'processing_priority': self.processing_priority,
            'estimated_size_mb': self.estimated_size_mb,
            'estimated_rows': self.estimated_rows,
            'incremental_columns': self.incremental_columns,
            'primary_column': self.primary_column,
            'force_full': self.force_full,
            'actual_force_full': self.strategy_info['force_full_applied']
        }

class TableProcessor:
    def __init__(self, config_reader: Optional[ConfigReader] = None, config_path: Optional[str] = None):
        """
        Initialize the table processor.
        
        MODERN ARCHITECTURE: Uses Settings injection and unified interface.
        Components handle their own connections - no direct connection management.
        
        CONNECTION ARCHITECTURE COMPLIANCE:
        - Uses Settings injection for environment-agnostic operation
        - Follows unified interface with Settings injection
        - Validates environment configuration before processing
        - Uses provider pattern for configuration management
        - No direct connection management - components handle their own connections
        
        Args:
            config_reader: ConfigReader instance (optional, will be created if not provided)
            config_path: Path to the configuration file (used for ConfigReader)
        """
        # ✅ MODERN ARCHITECTURE: Use Settings injection for environment-agnostic operation
        self.settings = get_settings()
        self.config_path = config_path or "etl_pipeline/config/tables.yml"
        self.metrics = UnifiedMetricsCollector(settings=self.settings)
        
        # ✅ MODERN ARCHITECTURE: Validate environment configuration
        self._validate_environment()
        
        # Create ConfigReader immediately if not provided
        if config_reader is None:
            # Use auto-detected path or provided path
            if config_path:
                self.config_reader = ConfigReader(config_path)
            else:
                self.config_reader = ConfigReader()
        else:
            self.config_reader = config_reader
        
        # ✅ MODERN ARCHITECTURE: No direct connection management
        # Components (SimpleMySQLReplicator, PostgresLoader) handle their own connections
        # using Settings injection for environment-agnostic operation
    
    def _validate_environment(self):
        """Validate environment configuration before processing."""
        try:
            # Validate that all required configurations are present
            if not self.settings.validate_configs():
                raise EnvironmentError(
                    message=f"Configuration validation failed for {self.settings.environment} environment",
                    environment=self.settings.environment,
                    missing_variables=[],
                    details={"critical": True, "error_type": "validation_failed"}
                )
            logger.info(f"Environment validation passed for {self.settings.environment} environment")
        except EnvironmentError:
            # Re-raise environment errors as-is
            raise
        except Exception as e:
            logger.error(f"Environment validation failed: {str(e)}")
            raise EnvironmentError(
                message=f"Unexpected error during environment validation",
                environment=self.settings.environment,
                details={"error_type": "unexpected", "original_error": str(e)},
                original_exception=e
            )
    
    def process_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Process a single table through the ETL pipeline.
        
        MODERN ARCHITECTURE: Uses Settings injection and unified interface.
        Components handle their own connections using Settings injection.
        
        ENHANCED: Includes performance monitoring and tracking.
        
        1. EXTRACT: Copy data from source to replication database using SimpleMySQLReplicator
        2. LOAD: Copy data from replication to analytics database using PostgresLoader
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force a full refresh
            
        Returns:
            bool: True if processing was successful
        """
        # ✅ MODERN ARCHITECTURE: Validate environment before processing
        self._validate_environment()
        
        start_time = time.time()
        
        try:
            logger.info(f"Starting ETL pipeline for table: {table_name}")
            
            # Get table configuration once
            table_config = self.config_reader.get_table_config(table_name)
            if not table_config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            # Get performance category for monitoring
            performance_category = table_config.get('performance_category', 'tiny')
            logger.info(f"Processing {table_name} ({performance_category} category)")
            
            is_incremental = table_config.get('extraction_strategy') == 'incremental' and not force_full
            
            # 1. Extract to replication using SimpleMySQLReplicator
            logger.info(f"Extracting {table_name} to replication database")
            extraction_start = time.time()
            extraction_success, extraction_metadata = self._extract_to_replication(table_name, force_full)
            extraction_duration = time.time() - extraction_start
            
            if not extraction_success:
                logger.error(f"Extraction failed for table: {table_name}")
                return False
            
            # IMPROVED: Get row count directly from metadata instead of querying tracking table
            rows_extracted = extraction_metadata.get('rows_copied', 0)
            strategy_used = extraction_metadata.get('strategy_used', 'unknown')
            
            # Track extraction performance
            self._track_performance_metrics(table_name, performance_category, extraction_duration, rows_extracted)
            
            # Check if any rows were extracted
            if rows_extracted == 0 and not force_full:
                logger.info(f"No new data extracted for {table_name}, but checking if analytics needs updating from replication")
                # Continue to load phase to check if analytics needs syncing with replication
                
            # 2. Load to analytics using PostgresLoader
            # For force_full mode, always run load phase even if row count is 0 (due to potential counting bugs)
            if force_full and rows_extracted == 0:
                logger.info(f"Force-full mode: Loading {table_name} to analytics database (row count may be incorrect)")
            else:
                logger.info(f"Loading {table_name} to analytics database ({rows_extracted} rows)")
                
            load_start = time.time()
            if not self._load_to_analytics(table_name, force_full):
                logger.error(f"Load to analytics failed for table: {table_name}")
                return False
            load_duration = time.time() - load_start
            
            # Track load performance
            self._track_performance_metrics(f"{table_name}_load", performance_category, load_duration, rows_extracted)
                
            processing_time = (time.time() - start_time) / 60
            logger.info(f"Successfully completed ETL pipeline for table: {table_name} in {processing_time:.2f} minutes")
            return True
            
        except DataExtractionError as e:
            processing_time = (time.time() - start_time) / 60
            logger.error(f"Data extraction failed for {table_name} after {processing_time:.2f} minutes: {e}")
            return False
        except DataLoadingError as e:
            processing_time = (time.time() - start_time) / 60
            logger.error(f"Data loading failed for {table_name} after {processing_time:.2f} minutes: {e}")
            return False
        except DatabaseConnectionError as e:
            processing_time = (time.time() - start_time) / 60
            logger.error(f"Database connection failed for {table_name} after {processing_time:.2f} minutes: {e}")
            return False
        except EnvironmentError as e:
            processing_time = (time.time() - start_time) / 60
            logger.error(f"Environment configuration error for {table_name} after {processing_time:.2f} minutes: {e}")
            return False
        except Exception as e:
            processing_time = (time.time() - start_time) / 60
            logger.error(f"Unexpected error in ETL pipeline for table {table_name} after {processing_time:.2f} minutes: {str(e)}")
            return False
            
    def _resolve_extraction_strategy(self, table_name: str, force_full: bool) -> Dict:
        """
        Single place to resolve extraction strategy for both components.
        
        This method ensures consistent force_full logic between SimpleMySQLReplicator
        and PostgresLoader by centralizing the strategy decision logic.
        
        Args:
            table_name: Name of the table to process
            force_full: Whether to force full refresh
            
        Returns:
            Dict containing unified strategy information:
                - strategy: str ('incremental' or 'full_table')
                - force_full_applied: bool (actual force_full value used)
                - reason: str (explanation of strategy choice)
        """
        try:
            # Get table configuration
            config = self.config_reader.get_table_config(table_name)
            if not config:
                return {
                    'strategy': 'full_table',
                    'force_full_applied': True,
                    'reason': 'No configuration found, defaulting to full refresh'
                }
            
            # Get incremental columns
            incremental_columns = config.get('incremental_columns', [])
            extraction_strategy = config.get('extraction_strategy', 'incremental')
            
            # Determine actual strategy
            actual_force_full = force_full
            strategy = 'full_table'
            reason = 'Explicit force_full requested'
            
            if not force_full:
                # Check if table has incremental columns
                if not incremental_columns:
                    strategy = 'full_table'
                    actual_force_full = True
                    reason = 'No incremental columns available'
                elif extraction_strategy in ['incremental', 'incremental_chunked']:
                    strategy = 'incremental'
                    actual_force_full = False
                    reason = f'{extraction_strategy} strategy with available incremental columns'
                else:
                    strategy = 'full_table'
                    actual_force_full = True
                    reason = f'Configuration specifies {extraction_strategy} strategy'
            
            return {
                'strategy': strategy,
                'force_full_applied': actual_force_full,
                'reason': reason,
                'incremental_columns': incremental_columns,
                'extraction_strategy': extraction_strategy
            }
            
        except Exception as e:
            logger.error(f"Error resolving extraction strategy for {table_name}: {str(e)}")
            return {
                'strategy': 'full_table',
                'force_full_applied': True,
                'reason': f'Error resolving strategy: {str(e)}'
            }
    
    def _extract_to_replication(self, table_name: str, force_full: bool) -> tuple[bool, Dict]:
        """
        Extract data from source to replication database using SimpleMySQLReplicator.
        
        MODERN ARCHITECTURE: Uses Settings injection for environment-agnostic operation.
        SimpleMySQLReplicator handles its own connections using Settings injection.
        
        IMPROVED: Use metadata returned from copy_table to eliminate redundant row count query.
        IMPROVED: Use unified strategy resolution for consistent force_full handling.
        IMPROVED: Use TableProcessingContext for unified configuration access.
        
        Returns:
            tuple: (success: bool, metadata: Dict)
        """
        try:
            # Import SimpleMySQLReplicator from core module
            from ..core.simple_mysql_replicator import SimpleMySQLReplicator
            
            # IMPROVED: Use unified processing context for consistent configuration access
            context = TableProcessingContext(table_name, force_full, self.config_reader)
            performance_category = context.performance_category
            actual_force_full = context.strategy_info['force_full_applied']
            strategy_reason = context.strategy_info['reason']

            logger.info(f"Strategy resolution for {table_name}: {strategy_reason}")

            # ✅ MODERN ARCHITECTURE: Initialize with Settings injection
            # SimpleMySQLReplicator handles its own connections using Settings injection
            replicator = SimpleMySQLReplicator(settings=self.settings)
            
            # IMPROVED: Use standard copy_table method and get metadata directly
            # The SimpleMySQLReplicator.copy_table method now returns detailed metadata
            logger.info(f"Using standard copy_table method for {table_name} ({performance_category})")
            # FIXED: Pass the actual configuration dictionary, not the context object
            success, metadata = replicator.copy_table(table_name, force_full=actual_force_full)
            
            if not success:
                logger.error(f"Extraction failed for {table_name}")
                return False, {
                    'rows_copied': 0,
                    'strategy_used': 'error',
                    'duration': 0.0,
                    'force_full_applied': actual_force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'error': 'Extraction failed'
                }
            
            # IMPROVED: Get row count directly from metadata instead of querying tracking table
            rows_extracted = metadata.get('rows_copied', 0)
            strategy_used = metadata.get('strategy_used', 'unknown')
            duration = metadata.get('duration', 0.0)
            
            # IMPROVED: Use consolidated tracking for consistency
            tracking_metadata = {
                'rows_processed': rows_extracted,
                'last_primary_value': metadata.get('last_primary_value'),
                'primary_column': metadata.get('primary_column'),
                'duration': duration,
                'strategy_used': strategy_used
            }
            self._update_pipeline_status(table_name, 'extract', 'success', tracking_metadata)
            
            logger.info(f"Successfully extracted {table_name} ({performance_category}) to replication database "
                       f"({rows_extracted} rows, {strategy_used} strategy, {duration:.2f}s)")
            return True, metadata
            
        except DataExtractionError as e:
            logger.error(f"Data extraction failed for {table_name}: {e}")
            return False, {
                'rows_copied': 0,
                'strategy_used': 'error',
                'duration': 0.0,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'error': str(e)
            }
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed during extraction for {table_name}: {e}")
            return False, {
                'rows_copied': 0,
                'strategy_used': 'error',
                'duration': 0.0,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error during extraction for {table_name}: {str(e)}")
            return False, {
                'rows_copied': 0,
                'strategy_used': 'error',
                'duration': 0.0,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'error': str(e)
            }
    
    def _track_performance_metrics(self, table_name: str, performance_category: str, 
                                 duration: float, rows_processed: int):
        """Track performance metrics for optimization."""
        try:
            from ..monitoring.unified_metrics import UnifiedMetricsCollector
            
            # Use the existing metrics instance that was properly initialized with settings
            rate = rows_processed / duration if duration > 0 else 0
            
            self.metrics.record_performance_metric(
                table_name=table_name,
                category=performance_category,
                duration=duration,
                rows_processed=rows_processed,
                rate_per_second=rate
            )
            
            logger.info(f"Performance: {table_name} ({performance_category}) - "
                       f"{rows_processed:,} rows in {duration:.2f}s ({rate:.0f} rows/sec)")
            
        except Exception as e:
            logger.warning(f"Could not track performance metrics for {table_name}: {str(e)}")
    
    def _update_pipeline_status(self, table_name: str, phase: str, status: str, metadata: Dict):
        """
        Update both MySQL and PostgreSQL tracking tables consistently.
        
        IMPROVED: Consolidated tracking to ensure consistency between both tracking systems.
        
        Args:
            table_name: Name of the table
            phase: 'extract' or 'load'
            status: 'success' or 'failed'
            metadata: Dict containing tracking information
        """
        try:
            rows_processed = metadata.get('rows_processed', 0)
            last_primary_value = metadata.get('last_primary_value')
            primary_column = metadata.get('primary_column')
            duration = metadata.get('duration', 0.0)
            
            if phase == 'extract':
                # Update MySQL tracking table (etl_copy_status)
                from ..core.simple_mysql_replicator import SimpleMySQLReplicator
                replicator = SimpleMySQLReplicator(settings=self.settings)
                
                success = replicator._update_copy_status(
                    table_name=table_name,
                    rows_copied=rows_processed,
                    copy_status=status,
                    last_primary_value=last_primary_value,
                    primary_column_name=primary_column
                )
                
                if success:
                    logger.debug(f"Updated MySQL tracking for {table_name} ({phase}): {status}")
                else:
                    logger.warning(f"Failed to update MySQL tracking for {table_name}")
                    
            elif phase == 'load':
                # Update PostgreSQL tracking table (etl_load_status)
                from ..loaders.postgres_loader import PostgresLoader
                # Detect test environment and pass appropriate parameters
                use_test_environment = self.settings.environment == 'test'
                loader = PostgresLoader(use_test_environment=use_test_environment, settings=self.settings)
                
                success = loader._update_load_status(
                    table_name=table_name,
                    rows_loaded=rows_processed,
                    load_status=status,
                    last_primary_value=last_primary_value,
                    primary_column_name=primary_column
                )
                
                if success:
                    logger.debug(f"Updated PostgreSQL tracking for {table_name} ({phase}): {status}")
                else:
                    logger.warning(f"Failed to update PostgreSQL tracking for {table_name}")
            
            # Log consolidated tracking information
            logger.info(f"Pipeline tracking updated for {table_name} ({phase}): "
                       f"{status}, {rows_processed} rows, {duration:.2f}s")
            
        except Exception as e:
            logger.error(f"Error updating pipeline status for {table_name} ({phase}): {str(e)}")
    
    def _load_to_analytics(self, table_name: str, force_full: bool) -> bool:
        """
        Load data from replication to analytics database using PostgresLoader.
        
        MODERN ARCHITECTURE: Uses Settings injection for environment-agnostic operation.
        PostgresLoader handles its own connections using Settings injection.
        
        IMPROVED: Use TableProcessingContext for unified configuration access.
        IMPROVED: Use metadata returned from PostgresLoader for consistent tracking.
        ENHANCED: Check if analytics needs updating from replication independently.
        """
        try:
            from ..loaders.postgres_loader import PostgresLoader
            
            # ✅ MODERN ARCHITECTURE: Initialize PostgresLoader
            # PostgresLoader handles its own connections using Settings injection
            # Detect test environment and pass appropriate parameters
            use_test_environment = self.settings.environment == 'test'
            loader = PostgresLoader(use_test_environment=use_test_environment, settings=self.settings)
            
            # ENHANCED: Check if analytics needs updating from replication
            if not force_full:
                needs_updating, replication_primary, analytics_primary, force_full_load_recommended = loader._check_analytics_needs_updating(table_name)
                if not needs_updating:
                    logger.info(f"Analytics database is up to date for {table_name}, skipping load phase")
                    # IMPORTANT: Update load status even when skipping load to reflect that table was processed
                    tracking_metadata = {
                        'rows_processed': 0,
                        'last_primary_value': None,
                        'primary_column': None,
                        'duration': 0.0,
                        'strategy_used': 'skipped_no_new_data'
                    }
                    self._update_pipeline_status(table_name, 'load', 'success', tracking_metadata)
                    return True
                elif force_full_load_recommended:
                    logger.info(f"Analytics table {table_name} is empty but has load timestamp, forcing full load")
                    force_full = True
            
            # IMPROVED: Use unified processing context for consistent configuration access
            context = TableProcessingContext(table_name, force_full, self.config_reader)
            estimated_size_mb = context.estimated_size_mb
            batch_size = context.config.get('batch_size', 5000)
            
            # Use chunked loading for large tables (> 100MB)
            use_chunked = estimated_size_mb > 100
            
            if use_chunked:
                logger.info(f"Using chunked loading for large table: {table_name} ({estimated_size_mb}MB)")
                success, metadata = loader.load_table_chunked(
                    table_name=table_name,
                    force_full=force_full,
                    chunk_size=batch_size
                )
            else:
                logger.info(f"Using standard loading for table: {table_name}")
                success, metadata = loader.load_table(
                    table_name=table_name,
                    force_full=force_full
                )
            
            if not success:
                logger.error(f"Failed to load table {table_name}")
                return False
            
            # IMPROVED: Use metadata from PostgresLoader for consistent tracking
            tracking_metadata = {
                'rows_processed': metadata.get('rows_loaded', 0),
                'last_primary_value': metadata.get('last_primary_value'),
                'primary_column': metadata.get('primary_column'),
                'duration': metadata.get('duration', 0.0),
                'strategy_used': metadata.get('strategy_used', 'unknown')
            }
            self._update_pipeline_status(table_name, 'load', 'success', tracking_metadata)
            
            logger.info(f"Successfully loaded {table_name} to analytics database "
                       f"({metadata.get('rows_loaded', 0)} rows, {metadata.get('strategy_used', 'unknown')} strategy)")
            return True
            
        except DataLoadingError as e:
            logger.error(f"Data loading failed for {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed during loading for {table_name}: {e}")
            return False
        except DatabaseTransactionError as e:
            logger.error(f"Database transaction failed during loading for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during loading for {table_name}: {str(e)}")
            return False 