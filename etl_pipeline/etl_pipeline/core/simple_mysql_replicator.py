"""
Simple MySQL Replicator with ConnectionManager Integration
========================================================

A robust ETL pipeline that copies data from OpenDental MySQL to local replication
database using ConnectionManager for enhanced reliability and performance.

ARCHITECTURE:
- Source: Always remote OpenDental server (client location)
- Target: Always localhost replication database  
- Strategy: Cross-server copy only (no same-server logic)

Key Features:
- ConnectionManager integration with automatic retry logic and exponential backoff
- Rate limiting to prevent overwhelming source database
- Optimized batch sizes based on table characteristics
- Settings-centric architecture for environment-agnostic operation
- Multiple copy methods: small (direct), medium (chunked), large (progress-tracked)
- True incremental updates with change data capture
- Last processed tracking for minimal downtime
- Connection health checks and fresh connections on retry

ConnectionManager Benefits:
- Automatic retry logic with exponential backoff
- Connection health checks and fresh connections on retry
- Rate limiting to prevent overwhelming source database
- Proper connection cleanup and resource management
- Robust error handling for transient network issues

Copy Methods (HOW to copy - performance-based):
- SMALL (< 1MB): Direct cross-server copy with retry logic
- MEDIUM (1-100MB): Chunked cross-server copy with rate limiting  
- LARGE (> 100MB): Progress-tracked cross-server copy with optimized batches

Extraction Strategies (WHAT to copy - business logic-based):
- full_table: Drop and recreate entire table with optimized batch sizes
- incremental: Only copy new/changed data using incremental column
- incremental_chunked: Smaller batches for very large tables
"""

import yaml
import logging
from typing import Dict, List, Optional, Any
from sqlalchemy import text
import time
import os
from datetime import datetime

# Import ETL pipeline configuration and connections
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from etl_pipeline.config import get_settings, Settings
from etl_pipeline.core.connections import ConnectionFactory, create_connection_manager
# Removed find_latest_tables_config import - we only use tables.yml with metadata versioning

# Import custom exceptions for structured error handling
from ..exceptions.database import DatabaseConnectionError, DatabaseQueryError
from ..exceptions.data import DataExtractionError
from ..exceptions.configuration import ConfigurationError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleMySQLReplicator:
    """
    Simple MySQL replicator optimized for cross-server replication.
    
    ARCHITECTURE:
    - Source: Always remote OpenDental server (client location)  
    - Target: Always localhost replication database
    - Strategy: Cross-server copy only (no same-server logic)
    
    Connection Architecture Compliance:
    - Uses Settings injection for environment-agnostic operation
    - Uses unified ConnectionFactory API with Settings injection
    - Uses Settings-based configuration methods
    - No direct environment variable access
    - Environment-agnostic (works for both production and test)
    
    Copy Methods (HOW to copy - performance-based):
    - SMALL (< 1MB): Direct cross-server copy with retry logic
    - MEDIUM (1-100MB): Chunked cross-server copy with rate limiting  
    - LARGE (> 100MB): Progress-tracked cross-server copy with optimized batches
    
    Extraction Strategies (WHAT to copy - business logic-based):
    - full_table: Drop and recreate entire table
    - incremental: Only copy new/changed data using incremental column
    - incremental_chunked: Smaller batches for very large tables
    """
    
    def __init__(self, settings: Optional[Settings] = None, tables_config_path: Optional[str] = None):
        """
        Initialize the simple replicator using the new Settings-centric architecture.
        
        Connection Architecture:
        - Uses Settings injection for environment-agnostic database connections
        - Uses unified ConnectionFactory API with Settings injection
        - Automatically uses correct environment (production/test) based on Settings
        - Uses Settings-based configuration methods for database info
        
        Args:
            settings: Settings instance (uses global if None, mainly for table config)
            tables_config_path: Path to tables.yml (uses default if None)
        """
        try:
            # Use provided settings or get global settings (for table configuration)
            self.settings = settings or get_settings()
            
            # Set tables config path
            if tables_config_path is None:
                config_dir = os.path.join(os.path.dirname(__file__), '..', 'config')
                tables_config_path = os.path.join(config_dir, 'tables.yml')
            self.tables_config_path = tables_config_path
            logger.info(f"SimpleMySQLReplicator using tables config: {self.tables_config_path}")
            
            # Get database connections using unified ConnectionFactory API with Settings injection
            self.source_engine = ConnectionFactory.get_source_connection(self.settings)
            self.target_engine = ConnectionFactory.get_replication_connection(self.settings)
            
            # Load configuration
            self.table_configs = self._load_configuration()
            
            # Validate tracking tables exist
            if not self._validate_tracking_tables_exist():
                logger.error("MySQL tracking tables not found. Run initialize_etl_tracking_tables.py to create them.")
                raise ConfigurationError(
                    message="MySQL tracking tables not found",
                    details={"error_type": "missing_tracking_tables", "solution": "Run initialize_etl_tracking_tables.py"}
                )
            
            # Debug logging only (not used in production)
            if logger.isEnabledFor(logging.DEBUG):
                # Get connection info from settings for debug logging
                source_config = self.settings.get_source_connection_config()
                target_config = self.settings.get_replication_connection_config()
                
                logger.debug(f"SimpleMySQLReplicator initialized")
                logger.debug(f"Source: {source_config.get('host')}:{source_config.get('port')}/{source_config.get('database')}")
                logger.debug(f"Target: {target_config.get('host')}:{target_config.get('port')}/{target_config.get('database')}")
                logger.debug(f"Loaded {len(self.table_configs)} table configurations")
            else:
                logger.info(f"SimpleMySQLReplicator initialized with {len(self.table_configs)} table configurations")
                
        except ConfigurationError as e:
            logger.error(f"Configuration error in SimpleMySQLReplicator initialization: {e}")
            raise
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error in SimpleMySQLReplicator initialization: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in SimpleMySQLReplicator initialization: {str(e)}")
            raise

    def _validate_tracking_tables_exist(self) -> bool:
        """Validate that MySQL tracking tables exist in replication database."""
        try:
            with self.target_engine.connect() as conn:
                # Check if etl_copy_status table exists
                result = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE() 
                    AND table_name = 'etl_copy_status'
                """)).scalar()
                
                if result == 0:
                    logger.error("MySQL tracking table 'etl_copy_status' not found in replication database")
                    return False
                
                # Check if table has the expected structure with primary column support
                result = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = DATABASE() 
                    AND table_name = 'etl_copy_status' 
                    AND column_name IN ('last_primary_value', 'primary_column_name')
                """)).scalar()
                
                if result < 2:
                    logger.error("MySQL tracking table 'etl_copy_status' missing primary column support columns")
                    return False
                
                logger.info("MySQL tracking tables validated successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error validating MySQL tracking tables: {str(e)}")
            return False
    
    def _load_configuration(self) -> Dict:
        """Load table configuration from tables.yml."""
        try:
            with open(self.tables_config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            tables = config.get('tables', {})
            logger.info(f"Loaded configuration for {len(tables)} tables from {self.tables_config_path}")
            return tables
            
        except FileNotFoundError:
            raise ConfigurationError(
                message=f"Configuration file not found: {self.tables_config_path}",
                config_file=self.tables_config_path,
                details={"error_type": "file_not_found"}
            )
        except Exception as e:
            raise ConfigurationError(
                message=f"Failed to load configuration from {self.tables_config_path}",
                config_file=self.tables_config_path,
                details={"error": str(e)}
            )
    
    def _update_copy_status(self, table_name: str, rows_copied: int, 
                           copy_status: str = 'success',
                           last_primary_value: Optional[str] = None,
                           primary_column_name: Optional[str] = None) -> bool:
        """Update copy tracking after successful MySQL replication with primary column support."""
        try:
            # Note: This updates the replication database's tracking table
            # The PostgresLoader will later update the analytics database's tracking table
            with self.target_engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO etl_copy_status (
                        table_name, last_copied, last_primary_value, primary_column_name,
                        rows_copied, copy_status, _created_at, _updated_at
                    ) VALUES (
                        :table_name, CURRENT_TIMESTAMP, :last_primary_value, :primary_column_name,
                        :rows_copied, :copy_status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON DUPLICATE KEY UPDATE
                        last_copied = CURRENT_TIMESTAMP,
                        last_primary_value = :last_primary_value,
                        primary_column_name = :primary_column_name,
                        rows_copied = :rows_copied,
                        copy_status = :copy_status,
                        _updated_at = CURRENT_TIMESTAMP
                """), {
                    "table_name": table_name,
                    "last_primary_value": last_primary_value,
                    "primary_column_name": primary_column_name,
                    "rows_copied": rows_copied,
                    "copy_status": copy_status
                })
                conn.commit()
                logger.info(f"📋 Tracking table updated: etl_copy_status for {table_name}")
                logger.info(f"Updated copy status for {table_name}: {rows_copied} rows, {copy_status}, primary_value={last_primary_value}")
                return True
                
        except Exception as e:
            logger.error(f"Error updating copy status for {table_name}: {str(e)}")
            return False

    def _get_last_copy_time(self, table_name: str) -> Optional[datetime]:
        """Get last copy time for incremental loading."""
        try:
            with self.target_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT last_copied
                    FROM etl_copy_status
                    WHERE table_name = :table_name
                    AND copy_status = 'success'
                    ORDER BY last_copied DESC
                    LIMIT 1
                """), {"table_name": table_name}).scalar()
                
                return result
        except Exception as e:
            logger.error(f"Error getting last copy time for {table_name}: {str(e)}")
            return None
    
    def _create_connection_managers(self, table_name: str = None, config: Dict = None):
        """
        Create ConnectionManager instances for source and target databases.
        
        Args:
            table_name: Name of the table (for logging)
            config: Table configuration (for optimization)
            
        Returns:
            Tuple of (source_manager, target_manager)
        """
        # Get optimized configuration based on table characteristics
        manager_config = self._get_connection_manager_config(table_name, config or {})
        
        source_manager = create_connection_manager(
            self.source_engine,
            max_retries=manager_config['max_retries'],
            retry_delay=manager_config['retry_delay']
        )
        target_manager = create_connection_manager(
            self.target_engine,
            max_retries=manager_config['max_retries'],
            retry_delay=manager_config['retry_delay']
        )
        
        if table_name:
            logger.debug(f"Created ConnectionManagers for {table_name} with config: {manager_config}")
        
        return source_manager, target_manager
    
    def _get_connection_manager_config(self, table_name: str, config: Dict) -> Dict:
        """
        Get optimized ConnectionManager configuration based on table characteristics.
        
        Args:
            table_name: Name of the table
            config: Table configuration
            
        Returns:
            ConnectionManager configuration dictionary
        """
        estimated_size_mb = config.get('estimated_size_mb', 0)
        
        if estimated_size_mb > 100:
            # Large tables: more retries, longer delays
            return {
                'max_retries': 5,
                'retry_delay': 2.0
            }
        elif estimated_size_mb > 50:
            # Medium tables: moderate retries
            return {
                'max_retries': 3,
                'retry_delay': 1.0
            }
        else:
            # Small tables: standard configuration
            return {
                'max_retries': 3,
                'retry_delay': 0.5
            }
    
    def _get_optimized_batch_size(self, table_name: str, config: Dict) -> int:
        """
        Get optimized batch size based on table size and connection manager.
        
        Args:
            table_name: Name of the table
            config: Table configuration
            
        Returns:
            Optimized batch size
        """
        base_batch_size = config.get('batch_size', 5000)
        estimated_size_mb = config.get('estimated_size_mb', 0)
        
        # INCREASED BATCH SIZES for better performance
        # Large tables: Increased from 10K to 50K max
        if estimated_size_mb > 100:
            return min(base_batch_size // 2, 50000)  # Increased from 10K to 50K for large tables
        elif estimated_size_mb > 50:
            return min(base_batch_size, 50000)  # Increased from 25K to 50K for medium tables
        else:
            return base_batch_size
    
    def get_copy_method(self, table_name: str) -> str:
        """
        Determine copy method based on table size (HOW to copy).
        
        Args:
            table_name: Name of the table
            
        Returns:
            Copy method: 'small', 'medium', or 'large'
        """
        config = self.table_configs.get(table_name, {})
        size_mb = config.get('estimated_size_mb', 0)
        
        if size_mb < 1:
            return 'small'
        elif size_mb < 100:
            return 'medium'
        else:
            return 'large'
    
    def get_extraction_strategy(self, table_name: str) -> str:
        """
        Get extraction strategy from table configuration (WHAT to copy).
        
        Args:
            table_name: Name of the table
            
        Returns:
            Extraction strategy: 'full_table', 'incremental', or 'incremental_chunked'
        """
        config = self.table_configs.get(table_name, {})
        strategy = config.get('extraction_strategy', 'full_table')
        
        # Validate the strategy
        if not self._validate_extraction_strategy(strategy):
            logger.warning(f"Invalid extraction strategy '{strategy}' for {table_name}, using 'full_table' as fallback")
            return 'full_table'
        
        return strategy
    
    def _validate_extraction_strategy(self, strategy: str) -> bool:
        """Validate that the extraction strategy is supported."""
        valid_strategies = ['full_table', 'incremental', 'incremental_chunked']
        return strategy in valid_strategies
    
    def copy_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Copy a single table from source to target.
        
        Args:
            table_name: Name of the table to copy
            force_full: Force full copy (ignore incremental)
            
        Returns:
            True if successful, False otherwise
        """
        import psutil
        
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            logger.info(f"Starting copy of table: {table_name}")
            
            # Get table configuration
            config = self.table_configs.get(table_name, {})
            if not config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(config)
            incremental_columns = config.get('incremental_columns', [])
            
            # Log the strategy being used
            self._log_incremental_strategy(table_name, primary_column, incremental_columns)
            
            # 1. Determine WHAT to copy (extraction strategy)
            extraction_strategy = self.get_extraction_strategy(table_name)
            if force_full:
                extraction_strategy = 'full_table'
                logger.info(f"Forcing full table copy for {table_name}")
            
            # 2. Determine HOW to copy (copy method based on size)
            copy_method = self.get_copy_method(table_name)
            
            logger.info(f"Using {extraction_strategy} extraction strategy and {copy_method} copy method for {table_name}")
            
            # 3. Execute the appropriate copy operation
            success, rows_copied = self._execute_copy_operation(table_name, extraction_strategy, copy_method, config)
            
            if success:
                # Get the maximum value of the primary column from copied data
                last_primary_value = None
                if primary_column and primary_column != 'none':
                    # This would need to be implemented based on how data is copied
                    # For now, we'll set it to None and update it in the tracking
                    pass
                
                # Update copy tracking with primary column value
                self._update_copy_status(table_name, rows_copied, 'success', last_primary_value, primary_column)
                
                # Track performance metrics
                end_time = time.time()
                final_memory = psutil.Process().memory_info().rss / 1024 / 1024
                duration = end_time - start_time
                memory_used = final_memory - initial_memory
                
                self.track_performance_metrics(table_name, extraction_strategy, duration, memory_used, rows_copied)
                
                logger.info(f"Successfully copied {table_name} ({rows_copied} rows) in {duration:.2f}s")
                return True
            else:
                # Update tracking with failure status
                self._update_copy_status(table_name, 0, 'failed', None, None)
                logger.error(f"Failed to copy {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error copying table {table_name}: {str(e)}")
            # Update tracking with failure status
            self._update_copy_status(table_name, 0, 'failed', None, None)
            return False
    
    def _execute_copy_operation(self, table_name: str, extraction_strategy: str, copy_method: str, config: Dict) -> tuple[bool, int]:
        """
        Execute copy operation with clear separation of concerns.
        
        Args:
            table_name: Name of the table to copy
            extraction_strategy: WHAT to copy ('full_table', 'incremental', 'incremental_chunked')
            copy_method: HOW to copy ('small', 'medium', 'large')
            config: Table configuration
            
        Returns:
            Tuple of (success, rows_copied)
        """
        try:
            logger.info(f"Executing {extraction_strategy} extraction with {copy_method} copy method for {table_name}")
            
            # Execute based on extraction strategy (WHAT to copy)
            if extraction_strategy == 'full_table':
                return self._copy_full_table(table_name, config)
            elif extraction_strategy == 'incremental':
                return self._copy_incremental_table(table_name, config)
            elif extraction_strategy == 'incremental_chunked':
                return self._copy_chunked_incremental_table(table_name, config)
            else:
                logger.error(f"Unknown extraction strategy: {extraction_strategy}")
                return False, 0
                
        except Exception as e:
            logger.error(f"Error in copy operation for {table_name}: {str(e)}")
            return False, 0
    

    
  
    def _copy_full_table(self, table_name: str, config: Dict) -> tuple[bool, int]:
        """Copy entire table using full table strategy with optimized batch sizes."""
        try:
            logger.info(f"Starting full table copy for {table_name}")
            
            # Get copy method based on table size (HOW to copy)
            copy_method = self.get_copy_method(table_name)
            
            # Use optimized batch size based on table characteristics
            optimized_batch_size = self._get_optimized_batch_size(table_name, config)
            
            logger.info(f"Using {copy_method} copy method for {table_name} with batch size: {optimized_batch_size}")
            
            # Drop and recreate table structure
            self._recreate_table_structure(table_name)
            
            # Copy all data based on copy method
            if copy_method == 'small':
                success, rows_copied = self._copy_small_table(table_name)
            elif copy_method == 'medium':
                success, rows_copied = self._copy_medium_table(table_name, optimized_batch_size)
            else:  # large
                success, rows_copied = self._copy_large_table(table_name, optimized_batch_size)
                
            return success, rows_copied
                
        except Exception as e:
            logger.error(f"Error in full table copy for {table_name}: {str(e)}")
            return False, 0
    
    def _get_primary_incremental_column(self, config: Dict) -> Optional[str]:
        """
        Get the primary incremental column from configuration with fallback logic.
        
        Args:
            config: Table configuration dictionary
            
        Returns:
            Primary incremental column name, or None if not available
        """
        primary_column = config.get('primary_incremental_column')
        
        # Check if primary column is valid (not null, 'none', or empty)
        if primary_column and primary_column != 'none' and primary_column.strip():
            return primary_column
        
        # Fallback: if no primary column specified, return None to use multi-column logic
        return None
    
    def _log_incremental_strategy(self, table_name: str, primary_column: Optional[str], incremental_columns: List[str]):
        """Log which incremental strategy is being used."""
        if primary_column and primary_column != 'none':
            logger.info(f"Table {table_name}: Using primary incremental column '{primary_column}' for optimized incremental loading")
        else:
            logger.info(f"Table {table_name}: Using multi-column incremental logic with columns: {incremental_columns}")
    
    def _copy_chunked_incremental_table(self, table_name: str, config: Dict) -> tuple[bool, int]:
        """Copy table using chunked incremental strategy for very large tables with optimized batch sizes."""
        try:
            # Get all incremental columns from the list
            incremental_columns = config.get('incremental_columns', [])
            if not incremental_columns:
                logger.error(f"No incremental columns configured for table: {table_name}")
                return False, 0
            
            # Get primary incremental column from configuration
            primary_incremental_column = self._get_primary_incremental_column(config)
            
            # Use optimized batch size based on table characteristics (smaller for chunked)
            base_batch_size = self._get_optimized_batch_size(table_name, config)
            chunked_batch_size = min(base_batch_size // 2, 5000)  # Increased from 1000 to 5000 for chunked strategy
            
            logger.info(f"Copying chunked incremental data for {table_name} using columns: {incremental_columns} with batch size: {chunked_batch_size}")
            
            # Log which strategy is being used
            self._log_incremental_strategy(table_name, primary_incremental_column, incremental_columns)
            
            # Use primary incremental column if available, otherwise fall back to multi-column logic
            if primary_incremental_column:
                # Get last processed value using primary column
                last_processed = self._get_last_processed_value(table_name, primary_incremental_column)
                
                # If table doesn't exist, create it first
                if last_processed is None:
                    logger.info(f"Target table {table_name} does not exist, creating table structure")
                    if not self._recreate_table_structure(table_name):
                        logger.error(f"Failed to create table structure for {table_name}")
                        return False, 0
                
                # Get new/changed records from source using primary column
                new_records_count = self._get_new_records_count(table_name, primary_incremental_column, last_processed)
                
                if new_records_count == 0:
                    logger.info(f"No new records to copy for {table_name}")
                    return True, 0
                
                logger.info(f"Found {new_records_count} new records to copy for {table_name}")
                
                # Copy new records with smaller batches for chunked strategy using primary column
                return self._copy_new_records(table_name, primary_incremental_column, last_processed, chunked_batch_size)
            else:
                # Get last processed value from target using maximum across all columns
                last_processed = self._get_last_processed_value_max(table_name, incremental_columns)
                
                # If table doesn't exist, create it first
                if last_processed is None:
                    logger.info(f"Target table {table_name} does not exist, creating table structure")
                    if not self._recreate_table_structure(table_name):
                        logger.error(f"Failed to create table structure for {table_name}")
                        return False, 0
                
                # Get new/changed records from source
                new_records_count = self._get_new_records_count_max(table_name, incremental_columns, last_processed)
                
                if new_records_count == 0:
                    logger.info(f"No new records to copy for {table_name}")
                    return True, 0
                
                logger.info(f"Found {new_records_count} new records to copy for {table_name}")
                
                # Copy new records with smaller batches for chunked strategy
                return self._copy_new_records_max(table_name, incremental_columns, last_processed, chunked_batch_size)
            
        except Exception as e:
            logger.error(f"Error in chunked incremental copy for {table_name}: {str(e)}")
            return False, 0
    
    def _copy_incremental_table(self, table_name: str, config: Dict) -> tuple[bool, int]:
        """Copy only new/changed data using incremental columns with optimized batch sizes."""
        try:
            # Get all incremental columns from the list
            incremental_columns = config.get('incremental_columns', [])
            if not incremental_columns:
                logger.error(f"No incremental columns configured for table: {table_name}")
                return False, 0
            
            # Get primary incremental column from configuration
            primary_incremental_column = self._get_primary_incremental_column(config)
            
            # Use optimized batch size based on table characteristics
            optimized_batch_size = self._get_optimized_batch_size(table_name, config)
            
            logger.info(f"Copying incremental data for {table_name} using columns: {incremental_columns} with batch size: {optimized_batch_size}")
            
            # Log which strategy is being used
            self._log_incremental_strategy(table_name, primary_incremental_column, incremental_columns)
            
            # Use primary incremental column if available, otherwise fall back to multi-column logic
            if primary_incremental_column:
                # Get last processed value using primary column
                last_processed = self._get_last_processed_value(table_name, primary_incremental_column)
                
                # If table doesn't exist, create it first
                if last_processed is None:
                    logger.info(f"Target table {table_name} does not exist, creating table structure")
                    if not self._recreate_table_structure(table_name):
                        logger.error(f"Failed to create table structure for {table_name}")
                        return False, 0
                
                # Get new/changed records from source using primary column
                new_records_count = self._get_new_records_count(table_name, primary_incremental_column, last_processed)
                
                if new_records_count == 0:
                    logger.info(f"No new records to copy for {table_name}")
                    return True, 0
                
                logger.info(f"Found {new_records_count} new records to copy for {table_name}")
                
                # Copy new records with optimized batch size using primary column
                return self._copy_new_records(table_name, primary_incremental_column, last_processed, optimized_batch_size)
            else:
                # Get last processed value from target using maximum across all columns
                last_processed = self._get_last_processed_value_max(table_name, incremental_columns)
                
                # If table doesn't exist, create it first
                if last_processed is None:
                    logger.info(f"Target table {table_name} does not exist, creating table structure")
                    if not self._recreate_table_structure(table_name):
                        logger.error(f"Failed to create table structure for {table_name}")
                        return False, 0
                
                # Get new/changed records from source
                new_records_count = self._get_new_records_count_max(table_name, incremental_columns, last_processed)
                
                if new_records_count == 0:
                    logger.info(f"No new records to copy for {table_name}")
                    return True, 0
                
                logger.info(f"Found {new_records_count} new records to copy for {table_name}")
                
                # Copy new records with optimized batch size
                return self._copy_new_records_max(table_name, incremental_columns, last_processed, optimized_batch_size)
            
        except Exception as e:
            logger.error(f"Error in incremental copy for {table_name}: {str(e)}")
            return False, 0
    
    def _recreate_table_structure(self, table_name: str) -> bool:
        """Drop and recreate table structure for cross-server replication."""
        try:
            # Get target database configuration
            target_config = self.settings.get_replication_connection_config()
            target_db = target_config.get('database', 'opendental_replication')
            
            with self.target_engine.connect() as conn:
                # Drop table if exists (in target database only)
                drop_sql = f"DROP TABLE IF EXISTS `{target_db}`.`{table_name}`"
                logger.debug(f"Executing drop SQL: {drop_sql}")
                conn.execute(text(drop_sql))
                
                # Always get CREATE TABLE statement from source (cross-server)
                with self.source_engine.connect() as source_conn:
                    result = source_conn.execute(text(f"SHOW CREATE TABLE `{table_name}`"))
                    create_table_row = result.fetchone()
                    if create_table_row:
                        create_table_sql = create_table_row[1]
                        # Modify for target database
                        create_table_sql = create_table_sql.replace(
                            f"CREATE TABLE `{table_name}`", 
                            f"CREATE TABLE `{target_db}`.`{table_name}`"
                        )
                        logger.debug(f"Executing create SQL: {create_table_sql}")
                        conn.execute(text(create_table_sql))
                    else:
                        raise Exception(f"Could not get CREATE TABLE statement for {table_name}")
                
                conn.commit()
                logger.info(f"Recreated table structure for {table_name} in target database {target_db}")
                return True
                
        except Exception as e:
            logger.error(f"Error recreating table structure for {table_name}: {str(e)}")
            return False
    
    def _copy_small_table(self, table_name: str) -> tuple[bool, int]:
        """Copy small table using direct INSERT ... SELECT with ConnectionManager."""
        try:
            # Get table configuration for optimized settings
            config = self.table_configs.get(table_name, {})
            
            # Create ConnectionManagers with optimized configuration
            source_manager, target_manager = self._create_connection_managers(table_name, config)
            
            with source_manager as source_mgr:
                with target_manager as target_mgr:
                    # Cross-server copy - read from source and insert into target with retry logic
                    # Get all data from source with retry logic
                    result = source_mgr.execute_with_retry(f"SELECT * FROM `{table_name}`", rate_limit=True)
                    rows = result.fetchall()
                    
                    if rows:
                        # Get column names
                        columns = result.keys()
                        # Escape column names with backticks to handle reserved keywords
                        escaped_columns = [f"`{col}`" for col in columns]
                        
                        # Use bulk insert for better performance
                        if len(rows) > 1:
                            # Bulk insert with executemany pattern
                            values_list = []
                            for row in rows:
                                # Convert Row object to tuple for bulk insert
                                values_list.append(tuple(row))
                            
                            # Create INSERT statement for executemany
                            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(escaped_columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
                            
                            # Execute bulk insert using executemany pattern
                            # Use direct connection for executemany since ConnectionManager doesn't support it
                            with target_mgr.engine.connect() as conn:
                                # Get the underlying DBAPI connection for executemany
                                dbapi_conn = conn.connection.connection  # This gets the actual PyMySQL connection
                                cursor = dbapi_conn.cursor()
                                cursor.executemany(insert_sql, values_list)
                                conn.commit()
                        else:
                            # Single row insert (fallback for small tables)
                            param_names = [f":{col}" for col in columns]
                            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(escaped_columns)}) VALUES ({', '.join(param_names)})"
                            
                            # Convert Row object to dictionary for named parameter binding
                            row_dict = dict(zip(columns, rows[0]))
                            target_mgr.execute_with_retry(insert_sql, row_dict, rate_limit=True)
                        
                        # Commit the transaction after all inserts (only for single row case)
                        if len(rows) <= 1:
                            target_mgr.commit()
                    
                    result.rowcount = len(rows)
                    logger.info(f"Successfully copied {result.rowcount} records for small table {table_name}")
                
                return True, result.rowcount
                    
        except Exception as e:
            logger.error(f"Error copying small table {table_name}: {str(e)}")
            # Log additional context for debugging
            try:
                source_config = self.settings.get_source_connection_config()
                target_config = self.settings.get_replication_connection_config()
                logger.error(f"Source config: {source_config}")
                logger.error(f"Target config: {target_config}")
            except Exception as config_error:
                logger.error(f"Error getting config for debugging: {str(config_error)}")
            return False, 0
    
    def _copy_medium_table(self, table_name: str, batch_size: int) -> tuple[bool, int]:
        """Copy medium table using chunked INSERT with LIMIT/OFFSET and ConnectionManager."""
        try:
            # Get table configuration for optimized settings
            config = self.table_configs.get(table_name, {})
            
            # Create ConnectionManagers with optimized configuration
            source_manager, target_manager = self._create_connection_managers(table_name, config)
            
            with source_manager as source_mgr:
                with target_manager as target_mgr:
                    # Cross-server copy - read from source and insert into target with retry logic
                    # Get total count for progress tracking with retry
                    count_result = source_mgr.execute_with_retry(f"SELECT COUNT(*) FROM `{table_name}`")
                    total_count = count_result.scalar()
                    if total_count is None:
                        total_count = 0
                    
                    offset = 0
                    total_copied = 0
                    batch_num = 0
                    
                    while True:
                        batch_num += 1
                        
                        try:
                            # Get batch from source with retry logic
                            result = source_mgr.execute_with_retry(
                                f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}",
                                rate_limit=True
                            )
                            rows = result.fetchall()
                            
                            if not rows:
                                logger.info(f"Batch {batch_num}: No more rows to copy, stopping")
                                break
                            
                            # Get column names
                            columns = result.keys()
                            # Escape column names with backticks to handle reserved keywords
                            escaped_columns = [f"`{col}`" for col in columns]
                            
                            # Use bulk insert for better performance
                            if len(rows) > 1:
                                # Bulk insert with executemany pattern - FIXED VERSION
                                values_list = []
                                for row in rows:
                                    # Validate and clean row data before insertion
                                    cleaned_row = self._clean_row_data(row, columns, table_name)
                                    values_list.append(cleaned_row)
                                
                                insert_sql = f"INSERT INTO `{table_name}` ({', '.join(escaped_columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
                                
                                # FIXED: Use the correct way to get underlying connection for executemany
                                with target_mgr.engine.connect() as conn:
                                    # Get the underlying DBAPI connection
                                    dbapi_conn = conn.connection.connection  # This gets the actual PyMySQL connection
                                    cursor = dbapi_conn.cursor()
                                    cursor.executemany(insert_sql, values_list)
                                    conn.commit()
                            else:
                                # Single row insert (fallback for small batches)
                                param_names = [f":{col}" for col in columns]
                                insert_sql = f"INSERT INTO `{table_name}` ({', '.join(escaped_columns)}) VALUES ({', '.join(param_names)})"
                                
                                # Clean the single row data
                                cleaned_row = self._clean_row_data(rows[0], columns, table_name)
                                row_dict = dict(zip(columns, cleaned_row))
                                target_mgr.execute_with_retry(insert_sql, row_dict, rate_limit=True)
                            
                            # Commit the transaction after each batch (only for single row case)
                            if len(rows) <= 1:
                                target_mgr.commit()
                            
                            total_copied += len(rows)
                            offset += batch_size
                            
                            logger.info(f"Batch {batch_num}: Copied {len(rows)} rows, total: {total_copied} for {table_name}")
                            
                            # Force log flush to ensure progress is visible
                            for handler in logger.handlers:
                                handler.flush()
                        
                        except Exception as batch_error:
                            logger.error(f"Error in batch {batch_num} for {table_name}: {str(batch_error)}")
                            raise
                
                logger.info(f"Successfully copied {total_copied} records for medium table {table_name}")
                return True, total_copied
                    
        except Exception as e:
            logger.error(f"Error copying medium table {table_name}: {str(e)}")
            # Log additional context for debugging
            try:
                source_config = self.settings.get_source_connection_config()
                target_config = self.settings.get_replication_connection_config()
                logger.error(f"Source config: {source_config}")
                logger.error(f"Target config: {target_config}")
            except Exception as config_error:
                logger.error(f"Error getting config for debugging: {str(config_error)}")
            return False, 0

    def _clean_row_data(self, row, columns, table_name: str):
        """
        Clean and validate row data before insertion to prevent type conversion errors.
        
        Args:
            row: The row data to clean
            columns: Column names
            table_name: Table name for logging
            
        Returns:
            Cleaned row data
        """
        try:
            cleaned_row = []
            for i, value in enumerate(row):
                try:
                    # Handle None values
                    if value is None:
                        cleaned_row.append(None)
                        continue
                    
                    # Handle string values that might contain problematic characters
                    if isinstance(value, str):
                        # Remove any control characters that might cause issues
                        cleaned_value = ''.join(c for c in value if ord(c) >= 32 or c in '\t\n\r')
                        cleaned_row.append(cleaned_value)
                    else:
                        # For non-string values, ensure they're in a format that can be safely converted
                        cleaned_row.append(value)
                        
                except Exception as e:
                    logger.warning(f"Error cleaning value for column {columns[i]} in {table_name}: {str(e)}")
                    # Use None as fallback for problematic values
                    cleaned_row.append(None)
            
            return cleaned_row
            
        except Exception as e:
            logger.error(f"Error cleaning row data for {table_name}: {str(e)}")
            # Return original row if cleaning fails
            return row
    
    def _copy_large_table(self, table_name: str, batch_size: int) -> tuple[bool, int]:
        """Copy large table using chunked INSERT with WHERE conditions and ConnectionManager."""
        try:
            # Get table configuration for optimized settings
            config = self.table_configs.get(table_name, {})
            
            # Create ConnectionManagers with optimized configuration
            source_manager, target_manager = self._create_connection_managers(table_name, config)
            
            with source_manager as source_mgr:
                with target_manager as target_mgr:
                    # Cross-server copy - read from source and insert into target with retry logic
                    # Get total count for progress tracking with retry
                    count_result = source_mgr.execute_with_retry(f"SELECT COUNT(*) FROM `{table_name}`")
                    total_count = count_result.scalar()
                    if total_count is None:
                        total_count = 0
                    
                    offset = 0
                    total_copied = 0
                    
                    while total_copied < total_count:
                        # Get batch from source with retry logic
                        result = source_mgr.execute_with_retry(
                            f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}",
                            rate_limit=True
                        )
                        rows = result.fetchall()
                        
                        if not rows:
                            break
                        
                        # Get column names
                        columns = result.keys()
                        # Escape column names with backticks to handle reserved keywords
                        escaped_columns = [f"`{col}`" for col in columns]
                        # Create INSERT statement with named parameters
                        param_names = [f":{col}" for col in columns]
                        insert_sql = f"INSERT INTO `{table_name}` ({', '.join(escaped_columns)}) VALUES ({', '.join(param_names)})"
                        
                        # Insert batch using individual execute calls with retry logic
                        for row in rows:
                            # Clean and validate row data before insertion
                            cleaned_row = self._clean_row_data(row, columns, table_name)
                            # Convert Row object to dictionary for named parameter binding
                            row_dict = dict(zip(columns, cleaned_row))
                            target_mgr.execute_with_retry(insert_sql, row_dict, rate_limit=True)
                        
                        # Commit the transaction after each batch
                        target_mgr.commit()
                        
                        total_copied += len(rows)
                        offset += batch_size
                        
                        if total_count > 0:
                            progress = (total_copied / total_count) * 100
                            logger.info(f"Copied batch: {total_copied}/{total_count} ({progress:.1f}%) for {table_name}")
                        else:
                            logger.info(f"Copied batch: {total_copied} records for {table_name}")
                    
                    logger.info(f"Successfully copied {total_copied} records for large table {table_name}")
                    return True, total_copied
                    
        except Exception as e:
            logger.error(f"Error copying large table {table_name}: {str(e)}")
            # Log additional context for debugging
            try:
                source_config = self.settings.get_source_connection_config()
                target_config = self.settings.get_replication_connection_config()
                logger.error(f"Source config: {source_config}")
                logger.error(f"Target config: {target_config}")
            except Exception as config_error:
                logger.error(f"Error getting config for debugging: {str(config_error)}")
            return False, 0
    
    def _get_last_processed_value(self, table_name: str, incremental_column: str) -> Any:
        """Get the last processed value from target table using ConnectionManager."""
        try:
            # Create ConnectionManager for target database
            target_manager = create_connection_manager(self.target_engine)
            
            with target_manager as target_mgr:
                # Check if table exists with retry
                result = target_mgr.execute_with_retry(f"SHOW TABLES LIKE '{table_name}'")
                if not result.fetchone():
                    logger.info(f"Target table {table_name} does not exist, starting from beginning")
                    return None
                
                # Get max value of incremental column with retry
                result = target_mgr.execute_with_retry(f"SELECT MAX({incremental_column}) FROM `{table_name}`")
                max_value = result.scalar()
                
                if max_value is None:
                    logger.info(f"No data in target table {table_name}, starting from beginning")
                    return None
                
                logger.info(f"Last processed value for {table_name}.{incremental_column}: {max_value}")
                return max_value
                
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error getting last processed value for {table_name}: {e}")
            return None
        except DatabaseQueryError as e:
            logger.error(f"Database query error getting last processed value for {table_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting last processed value for {table_name}: {str(e)}")
            return None
    
    def _get_new_records_count(self, table_name: str, incremental_column: str, last_processed: Any) -> int:
        """Get count of new records since last processed value using ConnectionManager."""
        try:
            # Create ConnectionManager for source database
            source_manager = create_connection_manager(self.source_engine)
            
            with source_manager as source_mgr:
                if last_processed is None:
                    # Get total count if no last processed value with retry
                    result = source_mgr.execute_with_retry(f"SELECT COUNT(*) FROM `{table_name}`")
                else:
                    # Get count of records newer than last processed with retry
                    result = source_mgr.execute_with_retry(
                        f"SELECT COUNT(*) FROM `{table_name}` WHERE {incremental_column} > :last_processed",
                        {"last_processed": last_processed}
                    )
                
                count = result.scalar()
                return count or 0
                
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error getting new records count for {table_name}: {e}")
            return 0
        except DatabaseQueryError as e:
            logger.error(f"Database query error getting new records count for {table_name}: {e}")
            return 0
        except Exception as e:
            logger.error(f"Unexpected error getting new records count for {table_name}: {str(e)}")
            return 0
    
    def _copy_new_records(self, table_name: str, incremental_column: str, last_processed: Any, batch_size: int) -> tuple[bool, int]:
        """
        Copy new records using cross-server strategy only.
        
        This method uses ConnectionManager to provide:
        - Automatic retry logic with exponential backoff
        - Connection health checks and fresh connections on retry
        - Rate limiting to prevent overwhelming source database
        - Proper connection cleanup
        """
        try:
            # Get table configuration for optimized settings
            config = self.table_configs.get(table_name, {})
            
            # Create ConnectionManagers with optimized configuration
            source_manager, target_manager = self._create_connection_managers(table_name, config)
            
            with source_manager as source_mgr:
                with target_manager as target_mgr:
                    # Always use cross-server copy strategy
                    logger.info(f"Using cross-server copy strategy for {table_name}")
                    
                    offset = 0
                    total_copied = 0
                    
                    while True:
                        # Get batch from remote source
                        if last_processed is None:
                            result = source_mgr.execute_with_retry(
                                f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}",
                                rate_limit=True
                            )
                        else:
                            result = source_mgr.execute_with_retry(
                                f"SELECT * FROM `{table_name}` WHERE {incremental_column} > :last_processed LIMIT {batch_size} OFFSET {offset}",
                                {"last_processed": last_processed},
                                rate_limit=True
                            )
                        
                        rows = result.fetchall()
                        
                        if not rows:
                            break
                        
                        # Get column names
                        columns = result.keys()
                        # Escape column names with backticks to handle reserved keywords
                        escaped_columns = [f"`{col}`" for col in columns]
                        # Create UPSERT statement with named parameters
                        param_names = [f":{col}" for col in columns]
                        
                        # Use UPSERT for incremental loads to handle duplicate keys
                        if last_processed is not None:
                            # For incremental loads, use UPSERT to handle duplicate keys
                            insert_sql = self._build_mysql_upsert_sql(table_name, list(columns))
                        else:
                            # For full loads, use simple INSERT
                            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(escaped_columns)}) VALUES ({', '.join(param_names)})"
                        
                        # Insert batch using individual execute calls with retry logic
                        for row in rows:
                            # Clean and validate row data before insertion
                            cleaned_row = self._clean_row_data(row, columns, table_name)
                            # Convert Row object to dictionary for named parameter binding
                            row_dict = dict(zip(columns, cleaned_row))
                            target_mgr.execute_with_retry(insert_sql, row_dict, rate_limit=True)
                        
                        total_copied += len(rows)
                        offset += batch_size
                        
                        logger.info(f"Copied batch: {total_copied} total records for {table_name}")
                    
                    logger.info(f"Completed incremental copy: {total_copied} records for {table_name}")
                    return True, total_copied
                    
        except DataExtractionError as e:
            logger.error(f"Data extraction error copying new records for {table_name}: {e}")
            return False, 0
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error copying new records for {table_name}: {e}")
            return False, 0
        except DatabaseQueryError as e:
            logger.error(f"Database query error copying new records for {table_name}: {e}")
            return False, 0
        except Exception as e:
            logger.error(f"Unexpected error copying new records for {table_name}: {str(e)}")
            return False, 0
    
    def _get_last_copy_time_max(self, table_name: str, incremental_columns: List[str]) -> Optional[datetime]:
        """Get the maximum last copy time across all incremental columns."""
        try:
            timestamps = []
            
            with self.target_engine.connect() as conn:
                for column in incremental_columns:
                    # Get last copy time for this specific column
                    result = conn.execute(text("""
                        SELECT last_copied
                        FROM etl_copy_status
                        WHERE table_name = :table_name
                        AND copy_status = 'success'
                        ORDER BY last_copied DESC
                        LIMIT 1
                    """), {"table_name": table_name}).scalar()
                    
                    if result:
                        timestamps.append(result)
            
            return max(timestamps) if timestamps else None
            
        except Exception as e:
            logger.error(f"Error getting max last copy time for {table_name}: {str(e)}")
            return None

    def _get_last_processed_value_max(self, table_name: str, incremental_columns: List[str]) -> Any:
        """Get the maximum last processed value across all incremental columns."""
        try:
            max_values = []
            
            with self.target_engine.connect() as conn:
                for column in incremental_columns:
                    # Get max value for this specific column
                    result = conn.execute(text(f"""
                        SELECT MAX({column})
                        FROM {table_name}
                        WHERE {column} IS NOT NULL
                    """)).scalar()
                    
                    if result:
                        max_values.append(result)
            
            # Return the maximum value across all columns
            return max(max_values) if max_values else None
            
        except Exception as e:
            logger.error(f"Error getting max last processed value for {table_name}: {str(e)}")
            return None

    def _get_new_records_count_max(self, table_name: str, incremental_columns: List[str], last_processed: Any) -> int:
        """Get count of new records using maximum timestamp across all incremental columns."""
        try:
            if not last_processed:
                # If no last processed value, count all records
                with self.source_engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                    return result or 0
            
            # Build conditions for all incremental columns
            conditions = []
            for column in incremental_columns:
                conditions.append(f"{column} > :last_processed")
            
            # Use OR logic - if any column is newer than last_processed, include the record
            where_clause = " OR ".join(conditions)
            
            with self.source_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE {where_clause}
                """), {"last_processed": last_processed}).scalar()
                
                return result or 0
                
        except Exception as e:
            logger.error(f"Error getting new records count for {table_name}: {str(e)}")
            return 0

    def _copy_new_records_max(self, table_name: str, incremental_columns: List[str], last_processed: Any, batch_size: int) -> tuple[bool, int]:
        """
        Copy new records using maximum timestamp logic across multiple incremental columns.
        
        This method uses the maximum timestamp across all incremental columns to determine
        which records to copy, providing more comprehensive incremental loading.
        """
        try:
            # Get table configuration for optimized settings
            config = self.table_configs.get(table_name, {})
            
            # Create ConnectionManagers with optimized configuration
            source_manager, target_manager = self._create_connection_managers(table_name, config)
            
            with source_manager as source_mgr:
                with target_manager as target_mgr:
                    # Cross-server copy - read from source and insert into target with retry logic
                    offset = 0
                    total_rows_copied = 0
                    
                    while True:
                        # Get batch from source with retry logic
                        if last_processed is None:
                            result = source_mgr.execute_with_retry(
                                f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}",
                                rate_limit=True
                            )
                        else:
                            # Build OR conditions for multiple incremental columns
                            conditions = []
                            for col in incremental_columns:
                                conditions.append(f"{col} > :last_processed")
                            where_clause = " OR ".join(conditions)
                            
                            result = source_mgr.execute_with_retry(
                                f"SELECT * FROM `{table_name}` WHERE {where_clause} LIMIT {batch_size} OFFSET {offset}",
                                {"last_processed": last_processed},
                                rate_limit=True
                            )
                        
                        rows = result.fetchall()
                        
                        if not rows:
                            break
                        
                        # Get column names
                        columns = result.keys()
                        # Escape column names with backticks to handle reserved keywords
                        escaped_columns = [f"`{col}`" for col in columns]
                        # Create UPSERT statement with named parameters
                        param_names = [f":{col}" for col in columns]
                        
                        # Use UPSERT for incremental loads to handle duplicate keys
                        insert_sql = self._build_mysql_upsert_sql(table_name, list(columns))
                        
                        # Insert batch using individual execute calls with retry logic
                        with self.target_engine.connect() as target_conn:
                            for row in rows:
                                # Clean and validate row data before insertion
                                cleaned_row = self._clean_row_data(row, columns, table_name)
                                # Convert Row object to dictionary for named parameter binding
                                row_dict = dict(zip(columns, cleaned_row))
                                target_conn.execute(text(insert_sql), row_dict)
                            
                            target_conn.commit()
                            
                            total_rows_copied += len(rows)
                            logger.info(f"Copied batch of {len(rows)} rows for {table_name} (total: {total_rows_copied})")
                        
                        offset += batch_size
                        
                        # Safety check to prevent infinite loops
                        if offset > 1000000:  # 1M rows limit
                            logger.warning(f"Reached safety limit for {table_name}, stopping copy")
                            break
                    
                    logger.info(f"Completed incremental copy for {table_name}: {total_rows_copied} rows")
                    return True, total_rows_copied
                    
        except Exception as e:
            logger.error(f"Error copying new records for {table_name}: {str(e)}")
            return False, 0
    
    def _get_last_copy_primary_value(self, table_name: str) -> Optional[str]:
        """Get last copy primary column value for incremental loading."""
        try:
            with self.target_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT last_primary_value, primary_column_name
                    FROM etl_copy_status
                    WHERE table_name = :table_name
                    AND copy_status = 'success'
                    ORDER BY last_copied DESC
                    LIMIT 1
                """), {"table_name": table_name}).fetchone()
                
                if result:
                    last_primary_value, primary_column_name = result
                    logger.debug(f"Retrieved last copy primary value for {table_name}: {last_primary_value} (column: {primary_column_name})")
                    return last_primary_value
                return None
                
        except Exception as e:
            logger.error(f"Error getting last copy primary value for {table_name}: {str(e)}")
            return None

    def copy_all_tables(self, table_filter: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        Copy all tables or filtered tables.
        
        Args:
            table_filter: Optional list of table names to copy
            
        Returns:
            Dictionary mapping table names to success status
        """
        results = {}
        
        if table_filter:
            tables_to_copy = [t for t in table_filter if t in self.table_configs]
        else:
            tables_to_copy = list(self.table_configs.keys())
        
        logger.info(f"Starting copy of {len(tables_to_copy)} tables")
        
        for table_name in tables_to_copy:
            try:
                success = self.copy_table(table_name)
                results[table_name] = success
            except DataExtractionError as e:
                logger.error(f"Data extraction error copying table {table_name}: {e}")
                results[table_name] = False
            except DatabaseConnectionError as e:
                logger.error(f"Database connection error copying table {table_name}: {e}")
                results[table_name] = False
            except DatabaseQueryError as e:
                logger.error(f"Database query error copying table {table_name}: {e}")
                results[table_name] = False
            except Exception as e:
                logger.error(f"Unexpected error copying table {table_name}: {str(e)}")
                results[table_name] = False
        
        # Log summary
        successful = sum(1 for success in results.values() if success)
        failed = len(results) - successful
        
        logger.info(f"Copy completed: {successful} successful, {failed} failed")
        
        return results
    
    def copy_tables_by_importance(self, importance_level: str) -> Dict[str, bool]:
        """
        Copy tables by importance level.
        
        Args:
            importance_level: 'critical', 'important', 'standard', 'audit', 'reference'
            
        Returns:
            Dictionary mapping table names to success status
        """
        tables_to_copy = []
        
        # Define all valid importance levels
        known_importance_levels = {'critical', 'important', 'standard', 'audit', 'reference'}
        
        for table_name, config in self.table_configs.items():
            if config.get('table_importance') == importance_level:
                tables_to_copy.append(table_name)
        
        logger.info(f"Found {len(tables_to_copy)} tables with importance: {importance_level}")
        
        # If no tables match the importance level
        if not tables_to_copy:
            # If it's a known importance level but no tables match, return empty dict
            if importance_level in known_importance_levels:
                logger.info(f"No tables found with importance: {importance_level}")
                return {}
            # If it's an unknown importance level, fall back to copying all tables
            else:
                logger.info(f"Unknown importance level: {importance_level}, falling back to copying all tables")
                return self.copy_all_tables()
        
        return self.copy_all_tables(tables_to_copy)

    def _build_mysql_upsert_sql(self, table_name: str, column_names: List[str]) -> str:
        """
        Build MySQL UPSERT SQL with dynamic primary key handling.
        
        MySQL uses: INSERT ... ON DUPLICATE KEY UPDATE
        with VALUES() function for update values.
        
        Args:
            table_name: Name of the table
            column_names: List of column names to insert
            
        Returns:
            str: MySQL UPSERT SQL statement
        """
        # Get primary key from table configuration
        config = self.table_configs.get(table_name, {})
        primary_key = config.get('primary_key', 'id')
        
        # Build column lists
        columns = ', '.join([f'`{col}`' for col in column_names])
        placeholders = ', '.join([f':{col}' for col in column_names])
        
        # Build UPDATE clause (exclude primary key from updates)
        update_columns = [f'`{col}` = VALUES(`{col}`)' 
                         for col in column_names if col != primary_key]
        update_clause = ', '.join(update_columns) if update_columns else 'updated_at = NOW()'
        
        return f"""
            INSERT INTO `{table_name}` ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
                {update_clause}
        """ 

    def track_performance_metrics(self, table_name: str, strategy: str, duration: float, memory_mb: float, rows_processed: int):
        """Track performance metrics for different copy strategies."""
        if not hasattr(self, 'performance_metrics'):
            self.performance_metrics = {}
        
        self.performance_metrics[table_name] = {
            'strategy': strategy,
            'duration': duration,
            'memory_mb': memory_mb,
            'rows_processed': rows_processed,
            'rows_per_second': rows_processed / duration if duration > 0 else 0,
            'timestamp': datetime.now()
        }
        
        logger.info(f"Copy performance metrics for {table_name}: {strategy} strategy, "
                   f"{rows_processed} rows in {duration:.2f}s ({rows_processed / duration:.0f} rows/sec), "
                   f"Memory: {memory_mb:.1f}MB")

    def get_performance_report(self) -> str:
        """Generate a comprehensive copy performance report."""
        if not hasattr(self, 'performance_metrics') or not self.performance_metrics:
            return "No copy performance metrics available."
        
        report = ["# MySQL Copy Performance Report", ""]
        
        for table_name, metrics in self.performance_metrics.items():
            report.append(f"## {table_name}")
            report.append(f"- Strategy: {metrics['strategy']}")
            report.append(f"- Duration: {metrics['duration']:.2f}s")
            report.append(f"- Rows Processed: {metrics['rows_processed']:,}")
            report.append(f"- Rows/Second: {metrics['rows_per_second']:.0f}")
            report.append(f"- Memory Usage: {metrics['memory_mb']:.1f}MB")
            report.append("")
        
        return "\n".join(report) 