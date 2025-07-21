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
from sqlalchemy import text
import time
import os

# Import ETL pipeline configuration and connections
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from etl_pipeline.config import get_settings, Settings
from etl_pipeline.core.connections import ConnectionFactory
# Removed find_latest_tables_config import - we only use tables.yml with metadata versioning

# Import custom exceptions for structured error handling
from ..exceptions.database import DatabaseConnectionError, DatabaseTransactionError, DatabaseQueryError
from ..exceptions.data import DataExtractionError
from ..exceptions.configuration import ConfigurationError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleMySQLReplicator:
    """
    Simple MySQL replicator that copies data using the new Settings-centric architecture.
    
    Connection Architecture Compliance:
    - Uses Settings injection for environment-agnostic operation
    - Uses unified ConnectionFactory API with Settings injection
    - Uses Settings-based configuration methods
    - No direct environment variable access
    - Environment-agnostic (works for both production and test)
    
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
            Extraction strategy: 'full_table', 'incremental', or 'chunked_incremental'
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
            
            # Copy data based on extraction strategy
            success = False
            if extraction_strategy == 'full_table':
                success = self._copy_full_table(table_name, config)
            elif extraction_strategy == 'incremental':
                success = self._copy_incremental_table(table_name, config)
            elif extraction_strategy == 'chunked_incremental':
                success = self._copy_chunked_incremental_table(table_name, config)
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
                
        except DataExtractionError as e:
            logger.error(f"Data extraction error copying table {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error copying table {table_name}: {e}")
            return False
        except DatabaseQueryError as e:
            logger.error(f"Database query error copying table {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error copying table {table_name}: {str(e)}")
            return False
    

    
  
    def _copy_full_table(self, table_name: str, config: Dict) -> bool:
        """Copy entire table using full table strategy."""
        try:
            logger.info(f"Starting full table copy for {table_name}")
            
            # Get copy strategy based on table size
            copy_strategy = self.get_copy_strategy(table_name)
            batch_size = config.get('batch_size', 5000)
            
            logger.info(f"Using {copy_strategy} copy strategy for {table_name}")
            
            # Drop and recreate table structure
            self._recreate_table_structure(table_name)
            
            # Copy all data based on copy strategy
            if copy_strategy == 'small':
                return self._copy_small_table(table_name)
            elif copy_strategy == 'medium':
                return self._copy_medium_table(table_name, batch_size)
            else:  # large
                return self._copy_large_table(table_name, batch_size)
                
        except Exception as e:
            logger.error(f"Error in full table copy for {table_name}: {str(e)}")
            return False
    
    def _copy_chunked_incremental_table(self, table_name: str, config: Dict) -> bool:
        """Copy table using chunked incremental strategy for very large tables."""
        try:
            # Get first incremental column from the list
            incremental_columns = config.get('incremental_columns', [])
            if not incremental_columns:
                logger.error(f"No incremental columns configured for table: {table_name}")
                return False
            
            incremental_column = incremental_columns[0]  # Use first column
            batch_size = config.get('batch_size', 1000)  # Smaller batches for chunked
            
            logger.info(f"Copying chunked incremental data for {table_name} using column: {incremental_column}")
            
            # Get last processed value from target
            last_processed = self._get_last_processed_value(table_name, incremental_column)
            
            # If table doesn't exist, create it first
            if last_processed is None:
                logger.info(f"Target table {table_name} does not exist, creating table structure")
                if not self._recreate_table_structure(table_name):
                    logger.error(f"Failed to create table structure for {table_name}")
                    return False
            
            # Get new/changed records from source
            new_records_count = self._get_new_records_count(table_name, incremental_column, last_processed)
            
            if new_records_count == 0:
                logger.info(f"No new records to copy for {table_name}")
                return True
            
            logger.info(f"Found {new_records_count} new records to copy for {table_name}")
            
            # Copy new records with smaller batches for chunked strategy
            return self._copy_new_records(table_name, incremental_column, last_processed, batch_size)
            
        except Exception as e:
            logger.error(f"Error in chunked incremental copy for {table_name}: {str(e)}")
            return False
    
    def _copy_incremental_table(self, table_name: str, config: Dict) -> bool:
        """Copy only new/changed data using incremental column."""
        try:
            # Get first incremental column from the list
            incremental_columns = config.get('incremental_columns', [])
            if not incremental_columns:
                logger.error(f"No incremental columns configured for table: {table_name}")
                return False
            
            incremental_column = incremental_columns[0]  # Use first column
            batch_size = config.get('batch_size', 5000)
            
            logger.info(f"Copying incremental data for {table_name} using column: {incremental_column}")
            
            # Get last processed value from target
            last_processed = self._get_last_processed_value(table_name, incremental_column)
            
            # If table doesn't exist, create it first
            if last_processed is None:
                logger.info(f"Target table {table_name} does not exist, creating table structure")
                if not self._recreate_table_structure(table_name):
                    logger.error(f"Failed to create table structure for {table_name}")
                    return False
            
            # Get new/changed records from source
            new_records_count = self._get_new_records_count(table_name, incremental_column, last_processed)
            
            if new_records_count == 0:
                logger.info(f"No new records to copy for {table_name}")
                return True
            
            logger.info(f"Found {new_records_count} new records to copy for {table_name}")
            
            # Copy new records
            return self._copy_new_records(table_name, incremental_column, last_processed, batch_size)
            
        except DataExtractionError as e:
            logger.error(f"Data extraction error in incremental copy for {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error in incremental copy for {table_name}: {e}")
            return False
        except DatabaseQueryError as e:
            logger.error(f"Database query error in incremental copy for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in incremental copy for {table_name}: {str(e)}")
            return False
    
    def _recreate_table_structure(self, table_name: str) -> bool:
        """Drop and recreate table structure in target database only."""
        try:
            with self.target_engine.connect() as conn:
                # Drop table if exists (in target database only)
                conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
                
                # Get table structure from source database
                source_config = self.settings.get_source_connection_config()
                source_db = source_config.get('database', 'opendental')
                
                # Check if source and target are on the same server
                source_host = source_config.get('host', 'localhost')
                target_host = self.settings.get_replication_connection_config().get('host', 'localhost')
                
                if source_host == target_host:
                    # Same server - use CREATE TABLE LIKE
                    create_sql = f"CREATE TABLE `{table_name}` LIKE `{source_db}`.`{table_name}`"
                    conn.execute(text(create_sql))
                else:
                    # Different servers - get CREATE TABLE statement from source
                    with self.source_engine.connect() as source_conn:
                        # Get the CREATE TABLE statement from source
                        result = source_conn.execute(text(f"SHOW CREATE TABLE `{table_name}`"))
                        create_table_row = result.fetchone()
                        if create_table_row:
                            create_table_sql = create_table_row[1]
                            # Execute the CREATE TABLE statement in target
                            conn.execute(text(create_table_sql))
                        else:
                            raise Exception(f"Could not get CREATE TABLE statement for {table_name}")
                
                conn.commit()
                logger.info(f"Recreated table structure for {table_name} in target database")
                return True
                
        except Exception as e:
            logger.error(f"Error recreating table structure for {table_name} in target database: {str(e)}")
            return False
    
    def _copy_small_table(self, table_name: str) -> bool:
        """Copy small table using direct INSERT ... SELECT."""
        try:
            with self.source_engine.connect() as source_conn:
                with self.target_engine.connect() as target_conn:
                    # Check if source and target are on the same server
                    source_config = self.settings.get_source_connection_config()
                    target_config = self.settings.get_replication_connection_config()
                    source_host = source_config.get('host', 'localhost')
                    target_host = target_config.get('host', 'localhost')
                    
                    if source_host == target_host:
                        # Same server - use direct INSERT ... SELECT
                        source_db = source_config.get('database', 'opendental')
                        copy_sql = f"INSERT INTO `{table_name}` SELECT * FROM `{source_db}`.`{table_name}`"
                        result = target_conn.execute(text(copy_sql))
                    else:
                        # Different servers - read from source and insert into target
                        # Get all data from source
                        result = source_conn.execute(text(f"SELECT * FROM `{table_name}`"))
                        rows = result.fetchall()
                        
                        if rows:
                            # Get column names
                            columns = result.keys()
                            # Escape column names with backticks to handle reserved keywords
                            escaped_columns = [f"`{col}`" for col in columns]
                            # Create INSERT statement with named parameters
                            param_names = [f":{col}" for col in columns]
                            insert_sql = f"INSERT INTO `{table_name}` ({', '.join(escaped_columns)}) VALUES ({', '.join(param_names)})"
                            
                            # Insert all rows
                            for row in rows:
                                # Convert Row object to dictionary for named parameter binding
                                row_dict = dict(zip(columns, row))
                                target_conn.execute(text(insert_sql), row_dict)
                        
                        result.rowcount = len(rows)
                    
                    target_conn.commit()
                    logger.info(f"Copied {result.rowcount} records for small table {table_name}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error copying small table {table_name}: {str(e)}")
            return False
    
    def _copy_medium_table(self, table_name: str, batch_size: int) -> bool:
        """Copy medium table using chunked INSERT with LIMIT/OFFSET."""
        try:
            with self.source_engine.connect() as source_conn:
                with self.target_engine.connect() as target_conn:
                    # Check if source and target are on the same server
                    source_config = self.settings.get_source_connection_config()
                    target_config = self.settings.get_replication_connection_config()
                    source_host = source_config.get('host', 'localhost')
                    target_host = target_config.get('host', 'localhost')
                    
                    if source_host == target_host:
                        # Same server - use direct INSERT ... SELECT
                        source_db = source_config.get('database', 'opendental')
                        offset = 0
                        total_copied = 0
                        
                        while True:
                            copy_sql = f"""
                                INSERT INTO `{table_name}` 
                                SELECT * FROM `{source_db}`.`{table_name}` 
                                LIMIT {batch_size} OFFSET {offset}
                            """
                            result = target_conn.execute(text(copy_sql))
                            rows_copied = result.rowcount
                            
                            if rows_copied == 0:
                                break
                            
                            target_conn.commit()
                            total_copied += rows_copied
                            offset += batch_size
                            
                            logger.info(f"Copied batch: {total_copied} total records for {table_name}")
                    else:
                        # Different servers - read from source and insert into target
                        offset = 0
                        total_copied = 0
                        
                        while True:
                            # Get batch from source
                            result = source_conn.execute(text(f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"))
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
                            
                            # Insert batch using individual execute calls
                            for row in rows:
                                # Convert Row object to dictionary for named parameter binding
                                row_dict = dict(zip(columns, row))
                                target_conn.execute(text(insert_sql), row_dict)
                            
                            target_conn.commit()
                            total_copied += len(rows)
                            offset += batch_size
                            
                            logger.info(f"Copied batch: {total_copied} total records for {table_name}")
                    
                    logger.info(f"Completed medium table copy: {total_copied} records for {table_name}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error copying medium table {table_name}: {str(e)}")
            return False
    
    def _copy_large_table(self, table_name: str, batch_size: int) -> bool:
        """Copy large table using chunked INSERT with WHERE conditions."""
        try:
            with self.source_engine.connect() as source_conn:
                with self.target_engine.connect() as target_conn:
                    # Check if source and target are on the same server
                    source_config = self.settings.get_source_connection_config()
                    target_config = self.settings.get_replication_connection_config()
                    source_host = source_config.get('host', 'localhost')
                    target_host = target_config.get('host', 'localhost')
                    
                    if source_host == target_host:
                        # Same server - use direct INSERT ... SELECT
                        source_db = source_config.get('database', 'opendental')
                        
                        # Get total count for progress tracking
                        count_result = source_conn.execute(text(f"SELECT COUNT(*) FROM `{source_db}`.`{table_name}`"))
                        total_count = count_result.scalar()
                        if total_count is None:
                            total_count = 0
                        
                        offset = 0
                        total_copied = 0
                        
                        while total_copied < total_count:
                            copy_sql = f"""
                                INSERT INTO `{table_name}` 
                                SELECT * FROM `{source_db}`.`{table_name}` 
                                LIMIT {batch_size} OFFSET {offset}
                            """
                            result = target_conn.execute(text(copy_sql))
                            rows_copied = result.rowcount
                            
                            if rows_copied == 0:
                                break
                            
                            target_conn.commit()
                            total_copied += rows_copied
                            offset += batch_size
                            
                            if total_count > 0:
                                progress = (total_copied / total_count) * 100
                                logger.info(f"Copied batch: {total_copied}/{total_count} ({progress:.1f}%) for {table_name}")
                            else:
                                logger.info(f"Copied batch: {total_copied} records for {table_name}")
                    else:
                        # Different servers - read from source and insert into target
                        # Get total count for progress tracking
                        count_result = source_conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                        total_count = count_result.scalar()
                        if total_count is None:
                            total_count = 0
                        
                        offset = 0
                        total_copied = 0
                        
                        while total_copied < total_count:
                            # Get batch from source
                            result = source_conn.execute(text(f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"))
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
                            
                            # Insert batch using individual execute calls
                            for row in rows:
                                # Convert Row object to dictionary for named parameter binding
                                row_dict = dict(zip(columns, row))
                                target_conn.execute(text(insert_sql), row_dict)
                            
                            target_conn.commit()
                            total_copied += len(rows)
                            offset += batch_size
                            
                            if total_count > 0:
                                progress = (total_copied / total_count) * 100
                                logger.info(f"Copied batch: {total_copied}/{total_count} ({progress:.1f}%) for {table_name}")
                            else:
                                logger.info(f"Copied batch: {total_copied} records for {table_name}")
                    
                    logger.info(f"Completed large table copy: {total_copied} records for {table_name}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error copying large table {table_name}: {str(e)}")
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
        """Get count of new records since last processed value."""
        try:
            with self.source_engine.connect() as conn:
                if last_processed is None:
                    # Get total count if no last processed value
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                else:
                    # Get count of records newer than last processed
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}` WHERE {incremental_column} > :last_processed"), {"last_processed": last_processed})
                
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
    
    def _copy_new_records(self, table_name: str, incremental_column: str, last_processed: Any, batch_size: int) -> bool:
        """
        Copy new records in batches.
        
        Note: This method uses direct engine connections. For production use,
        consider using ConnectionManager for better connection pooling and retry logic.
        """
        try:
            with self.source_engine.connect() as source_conn:
                with self.target_engine.connect() as target_conn:
                    # Check if source and target are on the same server
                    source_config = self.settings.get_source_connection_config()
                    target_config = self.settings.get_replication_connection_config()
                    source_host = source_config.get('host', 'localhost')
                    target_host = target_config.get('host', 'localhost')
                    
                    if source_host == target_host:
                        # Same server - use direct INSERT ... SELECT
                        source_db = source_config.get('database', 'opendental')
                        offset = 0
                        total_copied = 0
                        
                        while True:
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
                                    WHERE {incremental_column} > :last_processed
                                    LIMIT {batch_size} OFFSET {offset}
                                """
                            
                            if last_processed is None:
                                result = target_conn.execute(text(copy_sql))
                            else:
                                result = target_conn.execute(text(copy_sql), {"last_processed": last_processed})
                            
                            rows_copied = result.rowcount
                            if rows_copied == 0:
                                break
                            
                            target_conn.commit()
                            total_copied += rows_copied
                            offset += batch_size
                            
                            logger.info(f"Copied batch: {total_copied} total records for {table_name}")
                    else:
                        # Different servers - read from source and insert into target
                        offset = 0
                        total_copied = 0
                        
                        while True:
                            # Get batch from source
                            if last_processed is None:
                                result = source_conn.execute(text(f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"))
                            else:
                                result = source_conn.execute(text(f"SELECT * FROM `{table_name}` WHERE {incremental_column} > :last_processed LIMIT {batch_size} OFFSET {offset}"), {"last_processed": last_processed})
                            
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
                            
                            # Insert batch using individual execute calls
                            for row in rows:
                                # Convert Row object to dictionary for named parameter binding
                                row_dict = dict(zip(columns, row))
                                target_conn.execute(text(insert_sql), row_dict)
                            
                            target_conn.commit()
                            total_copied += len(rows)
                            offset += batch_size
                            
                            logger.info(f"Copied batch: {total_copied} total records for {table_name}")
                    
                    logger.info(f"Completed incremental copy: {total_copied} records for {table_name}")
                    return True
                    
        except DataExtractionError as e:
            logger.error(f"Data extraction error copying new records for {table_name}: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error copying new records for {table_name}: {e}")
            return False
        except DatabaseQueryError as e:
            logger.error(f"Database query error copying new records for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error copying new records for {table_name}: {str(e)}")
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