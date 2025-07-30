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
from typing import Dict, Optional
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

logger = logging.getLogger(__name__)

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
            
            is_incremental = table_config.get('extraction_strategy') == 'incremental' and not force_full
            
            # 1. Extract to replication using SimpleMySQLReplicator
            logger.info(f"Extracting {table_name} to replication database")
            extraction_success, rows_extracted = self._extract_to_replication(table_name, force_full)
            
            if not extraction_success:
                logger.error(f"Extraction failed for table: {table_name}")
                return False
            
            # Check if any rows were extracted
            if rows_extracted == 0 and not force_full:
                logger.info(f"No new data extracted for {table_name}, skipping load phase")
                processing_time = (time.time() - start_time) / 60
                logger.info(f"Successfully completed ETL pipeline for table: {table_name} (no new data) in {processing_time:.2f} minutes")
                return True
                
            # 2. Load to analytics using PostgresLoader
            # For force_full mode, always run load phase even if row count is 0 (due to potential counting bugs)
            if force_full and rows_extracted == 0:
                logger.info(f"Force-full mode: Loading {table_name} to analytics database (row count may be incorrect)")
            else:
                logger.info(f"Loading {table_name} to analytics database ({rows_extracted} rows)")
                
            if not self._load_to_analytics(table_name, force_full):
                logger.error(f"Load to analytics failed for table: {table_name}")
                return False
                
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
            
    def _extract_to_replication(self, table_name: str, force_full: bool) -> tuple[bool, int]:
        """
        Extract data from source to replication database using SimpleMySQLReplicator.
        
        MODERN ARCHITECTURE: Uses Settings injection for environment-agnostic operation.
        SimpleMySQLReplicator handles its own connections using Settings injection.
        
        Returns:
            tuple: (success: bool, rows_extracted: int)
        """
        try:
            # Import SimpleMySQLReplicator from core module
            from ..core.simple_mysql_replicator import SimpleMySQLReplicator
            
            # ✅ MODERN ARCHITECTURE: Initialize with Settings injection
            # SimpleMySQLReplicator handles its own connections using Settings injection
            replicator = SimpleMySQLReplicator(settings=self.settings)
            
            # Copy table using SimpleMySQLReplicator
            success = replicator.copy_table(table_name, force_full=force_full)
            
            if not success:
                logger.error(f"Extraction failed for {table_name}")
                return False, 0
            
            # Get the number of rows extracted from the replication database
            rows_extracted = self._get_extracted_row_count(table_name)
            
            logger.info(f"Successfully extracted {table_name} to replication database ({rows_extracted} rows)")
            return True, rows_extracted
            
        except DataExtractionError as e:
            logger.error(f"Data extraction failed for {table_name}: {e}")
            return False, 0
        except DatabaseConnectionError as e:
            logger.error(f"Database connection failed during extraction for {table_name}: {e}")
            return False, 0
        except Exception as e:
            logger.error(f"Unexpected error during extraction for {table_name}: {str(e)}")
            return False, 0
    
    def _get_extracted_row_count(self, table_name: str) -> int:
        """
        Get the number of rows that were extracted to the replication database.
        
        This method queries the etl_copy_status table to get the rows_copied count
        from the most recent successful copy operation.
        """
        try:
            from ..core.connections import ConnectionFactory
            
            # Get replication connection using the correct method
            replication_engine = ConnectionFactory.get_replication_connection(self.settings)
            
            with replication_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT rows_copied 
                    FROM etl_copy_status 
                    WHERE table_name = :table_name 
                    AND copy_status = 'success'
                    ORDER BY last_copied DESC 
                    LIMIT 1
                """), {"table_name": table_name})
                
                row = result.fetchone()
                if row:
                    return row[0] or 0
                else:
                    return 0
                    
        except Exception as e:
            logger.warning(f"Could not get extracted row count for {table_name}: {str(e)}")
            return 0
            
    def _load_to_analytics(self, table_name: str, force_full: bool) -> bool:
        """
        Load data from replication to analytics database using PostgresLoader.
        
        MODERN ARCHITECTURE: Uses Settings injection for environment-agnostic operation.
        PostgresLoader handles its own connections using Settings injection.
        """
        try:
            from ..loaders.postgres_loader import PostgresLoader
            
            # ✅ MODERN ARCHITECTURE: Initialize PostgresLoader
            # PostgresLoader handles its own connections using Settings injection
            loader = PostgresLoader()
            
            # Get table configuration for chunked loading decision
            table_config = self.config_reader.get_table_config(table_name)
            estimated_size_mb = table_config.get('estimated_size_mb', 0)
            batch_size = table_config.get('batch_size', 5000)
            
            # Use chunked loading for large tables (> 100MB)
            use_chunked = estimated_size_mb > 100
            
            if use_chunked:
                logger.info(f"Using chunked loading for large table: {table_name} ({estimated_size_mb}MB)")
                success = loader.load_table_chunked(
                    table_name=table_name,
                    force_full=force_full,
                    chunk_size=batch_size
                )
            else:
                logger.info(f"Using standard loading for table: {table_name}")
                success = loader.load_table(
                    table_name=table_name,
                    force_full=force_full
                )
            
            if not success:
                logger.error(f"Failed to load table {table_name}")
                return False
                
            logger.info(f"Successfully loaded {table_name} to analytics database")
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