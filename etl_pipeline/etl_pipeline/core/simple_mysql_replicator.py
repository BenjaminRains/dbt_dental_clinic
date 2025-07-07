"""
Simple MySQL Replicator
=======================

A simplified ETL pipeline that copies data from OpenDental MySQL to local replication
database using the new Settings-centric architecture.

Key Features:
- Uses new Settings class for environment detection and configuration
- Reads from static tables.yml (no dynamic discovery)
- Uses table size to determine copy strategy
- Simple copy methods for different table sizes
- True incremental updates with change data capture
- Last processed tracking for minimal downtime
"""

import yaml
import logging
from typing import Dict, List, Optional, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import time
import os
from pathlib import Path
from datetime import datetime

# Import ETL pipeline configuration and connections
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from etl_pipeline.config import get_settings, Settings, DatabaseType
from etl_pipeline.core.connections import ConnectionFactory

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleMySQLReplicator:
    """
    Simple MySQL replicator that copies data using the new Settings-centric architecture.
    
    Copy Strategies:
    - SMALL (< 1MB): Direct INSERT ... SELECT
    - MEDIUM (1-100MB): Chunked INSERT with LIMIT/OFFSET
    - LARGE (> 100MB): Chunked INSERT with WHERE conditions
    
    Extraction Strategies:
    - full_table: Drop and recreate entire table
    - incremental: Only copy new/changed data using incremental column
    """
    
    def __init__(self, settings: Optional[Settings] = None, tables_config_path: Optional[str] = None):
        """
        Initialize the simple replicator using the new Settings-centric architecture.
        
        Args:
            settings: Settings instance (uses global if None, mainly for table config)
            tables_config_path: Path to tables.yml (uses default if None)
        """
        # Use provided settings or get global settings (for table configuration)
        self.settings = settings or get_settings()
        
        # Set tables config path
        if tables_config_path is None:
            tables_config_path = os.path.join(
                os.path.dirname(__file__), 
                '..', 
                'etl_pipeline', 
                'config', 
                'tables.yml'
            )
        self.tables_config_path = tables_config_path
        
        # Get database connections using explicit production methods
        self.source_engine = ConnectionFactory.get_opendental_source_connection()
        self.target_engine = ConnectionFactory.get_mysql_replication_connection()
        
        # Load configuration
        self.table_configs = self._load_configuration()
        
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
    
    def _load_configuration(self) -> Dict:
        """Load table configuration from tables.yml."""
        try:
            with open(self.tables_config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            tables = config.get('tables', {})
            logger.info(f"Loaded configuration for {len(tables)} tables from {self.tables_config_path}")
            return tables
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.tables_config_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to load configuration from {self.tables_config_path}: {e}")
            raise
    
    def get_copy_strategy(self, table_name: str) -> str:
        """
        Determine copy strategy based on table size.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Copy strategy: 'small', 'medium', or 'large'
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
        Get extraction strategy from table configuration.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Extraction strategy: 'full_table' or 'incremental'
        """
        config = self.table_configs.get(table_name, {})
        return config.get('extraction_strategy', 'full_table')
    
    def copy_table(self, table_name: str, force_full: bool = False) -> bool:
        """
        Copy a single table from source to target.
        
        Args:
            table_name: Name of the table to copy
            force_full: Force full copy (ignore incremental)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Starting copy of table: {table_name}")
            start_time = time.time()
            
            # Get table configuration
            config = self.table_configs.get(table_name, {})
            if not config:
                logger.error(f"No configuration found for table: {table_name}")
                return False
            
            # Determine extraction strategy
            extraction_strategy = self.get_extraction_strategy(table_name)
            if force_full:
                extraction_strategy = 'full_table'
                logger.info(f"Forcing full table copy for {table_name}")
            
            logger.info(f"Using {extraction_strategy} extraction strategy for {table_name}")
            
            # For incremental tables, we don't need to create schema
            # The table should already exist from previous runs
            # For full_table strategy, we would need schema creation, but that's not implemented yet
            if extraction_strategy == 'full_table':
                logger.error(f"Full table strategy not implemented yet for {table_name}")
                return False
            
            # Copy data based on extraction strategy
            success = False
            if extraction_strategy == 'incremental':
                success = self._copy_incremental_table(table_name, config)
            else:
                logger.error(f"Unknown extraction strategy: {extraction_strategy}")
                return False
            
            if success:
                elapsed = time.time() - start_time
                logger.info(f"Successfully copied {table_name} in {elapsed:.2f}s")
                return True
            else:
                logger.error(f"Failed to copy table: {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error copying table {table_name}: {e}")
            return False
    

    
  
    def _copy_incremental_table(self, table_name: str, config: Dict) -> bool:
        """Copy only new/changed data using incremental column."""
        try:
            incremental_column = config.get('incremental_column')
            if not incremental_column:
                logger.error(f"No incremental column configured for table: {table_name}")
                return False
            
            batch_size = config.get('batch_size', 5000)
            
            logger.info(f"Copying incremental data for {table_name} using column: {incremental_column}")
            
            # Get last processed value from target
            last_processed = self._get_last_processed_value(table_name, incremental_column)
            
            # Get new/changed records from source
            new_records_count = self._get_new_records_count(table_name, incremental_column, last_processed)
            
            if new_records_count == 0:
                logger.info(f"No new records to copy for {table_name}")
                return True
            
            logger.info(f"Found {new_records_count} new records to copy for {table_name}")
            
            # Copy new records
            return self._copy_new_records(table_name, incremental_column, last_processed, batch_size)
            
        except Exception as e:
            logger.error(f"Error in incremental copy for {table_name}: {e}")
            return False
    
    def _get_last_processed_value(self, table_name: str, incremental_column: str) -> Any:
        """Get the last processed value from target table."""
        try:
            with self.target_engine.connect() as conn:
                # Check if table exists
                result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
                if not result.fetchone():
                    logger.info(f"Target table {table_name} does not exist, starting from beginning")
                    return None
                
                # Get max value of incremental column
                result = conn.execute(text(f"SELECT MAX({incremental_column}) FROM `{table_name}`"))
                max_value = result.scalar()
                
                if max_value is None:
                    logger.info(f"No data in target table {table_name}, starting from beginning")
                    return None
                
                logger.info(f"Last processed value for {table_name}.{incremental_column}: {max_value}")
                return max_value
                
        except Exception as e:
            logger.error(f"Error getting last processed value for {table_name}: {e}")
            return None
    
    def _get_new_records_count(self, table_name: str, incremental_column: str, last_processed: Any) -> int:
        """Get count of new records since last processed value."""
        try:
            with self.source_engine.connect() as conn:
                if last_processed is None:
                    # Get total count if no last processed value
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                else:
                    # Get count of records newer than last processed
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}` WHERE {incremental_column} > %s"), (last_processed,))
                
                count = result.scalar()
                return count or 0
                
        except Exception as e:
            logger.error(f"Error getting new records count for {table_name}: {e}")
            return 0
    
    def _copy_new_records(self, table_name: str, incremental_column: str, last_processed: Any, batch_size: int) -> bool:
        """Copy new records in batches."""
        try:
            with self.source_engine.connect() as source_conn:
                with self.target_engine.connect() as target_conn:
                    offset = 0
                    total_copied = 0
                    
                    while True:
                        # Get batch of new records
                        # Use settings to get source database name
                        source_config = self.settings.get_source_connection_config()
                        source_db = source_config.get('database', 'opendental')
                        
                        if last_processed is None:
                            # Copy all records if no last processed value
                            copy_sql = f"""
                                INSERT INTO `{table_name}` 
                                SELECT * FROM `{source_db}`.`{table_name}` 
                                LIMIT {batch_size} OFFSET {offset}
                            """
                        else:
                            # Copy only records newer than last processed
                            copy_sql = f"""
                                INSERT INTO `{table_name}` 
                                SELECT * FROM `{source_db}`.`{table_name}` 
                                WHERE {incremental_column} > %s
                                LIMIT {batch_size} OFFSET {offset}
                            """
                        
                        if last_processed is None:
                            result = target_conn.execute(text(copy_sql))
                        else:
                            result = target_conn.execute(text(copy_sql), (last_processed,))
                        
                        rows_copied = result.rowcount
                        if rows_copied == 0:
                            break
                        
                        target_conn.commit()
                        total_copied += rows_copied
                        offset += batch_size
                        
                        logger.info(f"Copied batch: {total_copied} total records for {table_name}")
                    
                    logger.info(f"Completed incremental copy: {total_copied} records for {table_name}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error copying new records for {table_name}: {e}")
            return False
    
 
    
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
            success = self.copy_table(table_name)
            results[table_name] = success
        
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
        
        for table_name, config in self.table_configs.items():
            if config.get('table_importance') == importance_level:
                tables_to_copy.append(table_name)
        
        logger.info(f"Found {len(tables_to_copy)} tables with importance: {importance_level}")
        
        return self.copy_all_tables(tables_to_copy) 