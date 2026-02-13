"""
Simple MySQL Replicator with ConnectionManager Integration
========================================================

A robust ETL pipeline that copies data from OpenDental MySQL to local replication
database using ConnectionManager for enhanced reliability and performance.
ENHANCED to leverage new configuration values from analyze_opendental_schema.py.

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
- PERFORMANCE OPTIMIZATIONS: Dynamic batch sizing, intelligent strategy selection, bulk operations
- SCHEMA ANALYZER INTEGRATION: Leverages performance metadata from analyze_opendental_schema.py

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

NEW FEATURES (Schema Analyzer Integration):
- Performance category-based batch sizing (large, medium, small, tiny)
- Processing priority-based table selection (1-10 scale)
- Time gap threshold analysis for intelligent full vs. incremental decisions
- Ultra-fast bulk operations with MySQL session optimizations
- Enhanced performance tracking with detailed metrics
- Schema analyzer configuration summary and reporting
"""

import yaml
import logging
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy import text
import time
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql.cursors

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

# Import method tracking for usage analysis
scripts_path = os.path.join(os.path.dirname(__file__), '..', '..', 'scripts')
sys.path.insert(0, str(scripts_path))
from method_tracker import track_method, save_tracking_report, print_tracking_report  # type: ignore

# Get logger (uses proper configuration from logging module)
logger = logging.getLogger(__name__)

class PerformanceOptimizations:
    """
    Performance optimization methods to be integrated into SimpleMySQLReplicator.
    Provides dynamic batch sizing, intelligent strategy selection, and bulk operations.
    Enhanced to leverage new configuration values from analyze_opendental_schema.py.
    """
    
    def __init__(self, replicator):
        self.replicator = replicator
        self.performance_history = {}
        self.batch_performance_threshold = 100  # records/second minimum
        
        # Enhanced bulk operation settings
        self.bulk_insert_buffer_size = 268435456  # 256MB
        self.max_bulk_batch_size = 100000
        self.min_bulk_batch_size = 1000
        
        logger.info("PerformanceOptimizations initialized with enhanced bulk operation settings")
    
    def calculate_adaptive_batch_size(self, table_name: str, config: Dict) -> int:
        """
        Calculate optimal batch size based on table characteristics and performance history.
        Enhanced to leverage new configuration values from schema analyzer.
        """
        # Handle case where TableProcessingContext object is passed instead of config dict
        if hasattr(config, 'config'):
            # Extract config from TableProcessingContext object
            config = config.config
        elif hasattr(config, 'incremental_columns'):
            # This is already a config-like object, extract incremental_columns from attributes
            incremental_columns = config.incremental_columns
            # Create a config dict with the necessary attributes
            config_dict = {
                'incremental_columns': incremental_columns,
                'primary_incremental_column': getattr(config, 'primary_incremental_column', None),
                'batch_size': getattr(config, 'batch_size', None),
                'performance_category': getattr(config, 'performance_category', 'medium'),
                'estimated_size_mb': getattr(config, 'estimated_size_mb', 0),
                'estimated_rows': getattr(config, 'estimated_rows', 0)
            }
            config = config_dict
        else:
            # Ensure config is a dictionary
            if not isinstance(config, dict):
                logger.error(f"Invalid config type for {table_name}: {type(config)}")
                return self.min_bulk_batch_size  # Return minimum batch size on error
        
        # First, try to use the batch_size from schema analyzer configuration
        config_batch_size = config.get('batch_size')
        if config_batch_size:
            logger.info(f"Using schema analyzer batch size for {table_name}: {config_batch_size:,}")
            return config_batch_size
        
        # Get performance category from config (generated by enhanced schema analyzer)
        performance_category = config.get('performance_category', 'medium')
        estimated_size_mb = config.get('estimated_size_mb', 0)
        row_count = config.get('estimated_rows', 0)
        
        # Use performance category-based sizing
        if performance_category == 'large' or row_count >= 1000000:  # 1M+ records
            base_batch = 100000
        elif performance_category == 'medium' or row_count >= 100000:  # 100K+ records
            base_batch = 50000
        elif performance_category == 'small' or row_count >= 10000:   # 10K+ records
            base_batch = 25000
        else:  # tiny or < 10K records
            base_batch = 10000
        
        # Use historical performance if available for fine-tuning
        if table_name in self.performance_history:
            last_performance = self.performance_history[table_name]
            records_per_second = last_performance.get('records_per_second', 0)
            
            if records_per_second > 0:
                # Calculate batch size for ~30 second batches
                optimal_batch = int(records_per_second * 30)
                base_batch = max(self.min_bulk_batch_size, 
                                min(self.max_bulk_batch_size, optimal_batch))
        
        # Final bounds check
        final_batch = max(self.min_bulk_batch_size, 
                         min(self.max_bulk_batch_size, base_batch))
        
        logger.info(f"Calculated adaptive batch size for {table_name}: {final_batch:,} "
                   f"(category: {performance_category}, records: {row_count:,})")
        
        return final_batch
    
    def should_use_full_refresh(self, table_name: str, config: Dict) -> bool:
        """
        Determine if we should use full refresh instead of incremental based on time gap.
        Enhanced to use time_gap_threshold_days from schema analyzer configuration.
        """
        # Handle case where TableProcessingContext object is passed instead of config dict
        if hasattr(config, 'config'):
            # Extract config from TableProcessingContext object
            config = config.config
        elif hasattr(config, 'incremental_columns'):
            # This is already a config-like object, extract incremental_columns from attributes
            incremental_columns = config.incremental_columns
            # Create a config dict with the necessary attributes
            config_dict = {
                'incremental_columns': incremental_columns,
                'primary_incremental_column': getattr(config, 'primary_incremental_column', None),
                'time_gap_threshold_days': getattr(config, 'time_gap_threshold_days', 30),
                'performance_category': getattr(config, 'performance_category', 'medium'),
                'estimated_size_mb': getattr(config, 'estimated_size_mb', 0)
            }
            config = config_dict
        else:
            # Ensure config is a dictionary
            if not isinstance(config, dict):
                logger.error(f"Invalid config type for {table_name}: {type(config)}")
                return True  # Default to full refresh on error
        
        incremental_columns = config.get('incremental_columns', [])
        if not incremental_columns:
            return True  # No incremental columns, use full refresh
        
        # Check last processed time
        last_copy_time = self.replicator._get_last_copy_time(table_name)
        if not last_copy_time:
            return True  # First time, use full refresh
        
        # Check time gap
        time_gap = datetime.now() - last_copy_time
        gap_days = time_gap.total_seconds() / (24 * 3600)
        
        # Use time gap threshold from schema analyzer configuration
        time_gap_threshold = config.get('time_gap_threshold_days', 30)
        performance_category = config.get('performance_category', 'medium')
        
        if gap_days > time_gap_threshold:
            logger.info(f"Large time gap ({gap_days:.1f} days > {time_gap_threshold} threshold) "
                       f"for {table_name}, recommending full refresh")
            return True
        
        # Check historical performance - if incremental was slow, try full refresh
        if table_name in self.performance_history:
            last_perf = self.performance_history[table_name]
            if last_perf.get('strategy') == 'incremental' and last_perf.get('records_per_second', 0) < 100:
                logger.info(f"Previous incremental copy was slow for {table_name}, trying full refresh")
                return True
        
        # For small-medium tables, full refresh might be faster even with shorter gaps
        estimated_size_mb = config.get('estimated_size_mb', 0)
        if estimated_size_mb < 100 and gap_days > 7:
            logger.info(f"Small table {table_name} with {gap_days:.1f} day gap, recommending full refresh")
            return True
        
        logger.info(f"Using incremental strategy for {table_name} "
                   f"(gap: {gap_days:.1f} days, category: {performance_category})")
        return False
    
    def _get_expected_rate_for_category(self, performance_category: str) -> int:
        """
        Get expected processing rate for performance category.
        Aligned with schema analyzer performance thresholds.
        """
        expected_rates = {
            'large': 4000,    # 4K records/sec for large tables
            'medium': 2500,   # 2.5K records/sec for medium tables  
            'small': 1500,    # 1.5K records/sec for small tables
            'tiny': 750       # 750 records/sec for tiny tables
        }
        return expected_rates.get(performance_category, 2000)
    
    def _apply_bulk_optimizations(self):
        """
        Apply MySQL session optimizations for bulk operations.
        Enhanced with ultra-fast settings from proposed OptimizedSimpleMySQLReplicator.
        """
        try:
            with self.replicator.target_engine.connect() as conn:
                # Key bulk operation optimizations
                try:
                    conn.execute(text(f"SET SESSION bulk_insert_buffer_size = {self.bulk_insert_buffer_size}"))
                except Exception as e:
                    if "Access denied" in str(e) or "SUPER" in str(e) or "SYSTEM_VARIABLES_ADMIN" in str(e):
                        logger.warning(f"Failed to set bulk_insert_buffer_size due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set bulk_insert_buffer_size: {e}")
                
                # Try to set innodb_flush_log_at_trx_commit, but handle GLOBAL variable gracefully
                try:
                    conn.execute(text("SET SESSION innodb_flush_log_at_trx_commit = 2"))
                except Exception as e:
                    if "GLOBAL variable" in str(e) or "Access denied" in str(e):
                        logger.warning("innodb_flush_log_at_trx_commit requires GLOBAL privileges, skipping")
                    else:
                        logger.warning(f"Failed to set innodb_flush_log_at_trx_commit: {e}")
                
                try:
                    conn.execute(text("SET SESSION autocommit = 0"))
                except Exception as e:
                    if "Access denied" in str(e):
                        logger.warning(f"Failed to set autocommit due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set autocommit: {e}")
                
                try:
                    conn.execute(text("SET SESSION unique_checks = 0"))
                except Exception as e:
                    if "Access denied" in str(e):
                        logger.warning(f"Failed to set unique_checks due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set unique_checks: {e}")
                
                try:
                    conn.execute(text("SET SESSION foreign_key_checks = 0"))
                except Exception as e:
                    if "Access denied" in str(e):
                        logger.warning(f"Failed to set foreign_key_checks due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set foreign_key_checks: {e}")
                
                try:
                    conn.execute(text("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'"))
                except Exception as e:
                    if "Access denied" in str(e):
                        logger.warning(f"Failed to set sql_mode due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set sql_mode: {e}")
                
                conn.commit()
                
                logger.debug("Applied MySQL bulk operation optimizations")
                
        except Exception as e:
            logger.warning(f"Failed to apply bulk optimizations: {e}")
    

    
    def _track_performance_optimized(self, table_name: str, duration: float, memory_mb: float, rows_processed: int):
        """
        Enhanced performance tracking with detailed metrics.
        """
        records_per_second = rows_processed / duration if duration > 0 else 0
        
        self.performance_history[table_name] = {
            'records_per_second': records_per_second,
            'duration': duration,
            'memory_mb': memory_mb,
            'rows_processed': rows_processed,
            'timestamp': datetime.now(),
            'strategy': 'optimized'
        }
        
        # Performance analysis
        if records_per_second < self.batch_performance_threshold:
            logger.warning(f"Performance below threshold for {table_name}: {records_per_second:.0f} records/sec")
        else:
            logger.info(f"Good performance for {table_name}: {records_per_second:.0f} records/sec")
        
        logger.info(f"Performance metrics for {table_name}: {rows_processed:,} rows in {duration:.2f}s "
                   f"({records_per_second:.0f} rows/sec), Memory: {memory_mb:.1f}MB")
    
    # REMOVED: _copy_large_table_optimized() - Replaced by unified _execute_table_copy()
    # REMOVED: _copy_full_table_bulk() - Replaced by unified _execute_table_copy()
    
    # REMOVED: _copy_large_table_ultra_fast() - Replaced by unified _copy_full_table_unified() in SimpleMySQLReplicator
    # REMOVED: _copy_medium_small_table_optimized() - Replaced by unified _copy_full_table_unified() in SimpleMySQLReplicator
    
    @track_method
    def _copy_incremental_bulk(self, table_name: str, config: Dict, batch_size: int) -> Tuple[bool, int]:
        """
        Optimized incremental copy using bulk operations.
        """
        try:
            # Handle case where TableProcessingContext object is passed instead of config dict
            if hasattr(config, 'config'):
                # Extract config from TableProcessingContext object
                config = config.config
            elif hasattr(config, 'incremental_columns'):
                # This is already a config-like object, extract incremental_columns from attributes
                incremental_columns = config.incremental_columns
                # Create a config dict with the necessary attributes
                config_dict = {
                    'incremental_columns': incremental_columns,
                    'primary_incremental_column': getattr(config, 'primary_incremental_column', None)
                }
                config = config_dict
            else:
                # Ensure config is a dictionary
                if not isinstance(config, dict):
                    logger.error(f"Invalid config type for {table_name}: {type(config)}")
                    return False, 0
            
            incremental_columns = config.get('incremental_columns', [])
            primary_column = self.replicator._get_primary_incremental_column(config)
            
            if primary_column:
                # Use single column incremental logic
                last_processed = self.replicator._get_last_copy_primary_value(table_name)
                
                # Get count of new records
                with self.replicator.source_engine.connect() as source_conn:
                    if last_processed is None:
                        count_result = source_conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                    else:
                        count_result = source_conn.execute(text(
                            f"SELECT COUNT(*) FROM `{table_name}` WHERE {primary_column} > :last_processed"
                        ), {"last_processed": last_processed})
                    
                    new_records_count = count_result.scalar()
                
                if new_records_count == 0:
                    logger.info(f"No new records for {table_name}")
                    return True, 0
                
                logger.info(f"Found {new_records_count:,} new records for {table_name}")
                
                # Process in batches using bulk operations
                return self._process_incremental_batches_bulk(
                    table_name, primary_column, last_processed, batch_size, new_records_count
                )
            else:
                # Fall back to existing multi-column logic but with bulk operations
                return self.replicator._copy_incremental_unified(table_name, config, batch_size)
                
        except Exception as e:
            logger.error(f"Error in incremental bulk copy for {table_name}: {str(e)}")
            return False, 0
    
    # REMOVED: _copy_medium_table_optimized() - Replaced by unified _execute_table_copy()
    
    @track_method
    def _process_incremental_batches_bulk(self, table_name: str, primary_column: str, 
                                        last_processed: Any, batch_size: int, 
                                        total_records: int) -> Tuple[bool, int]:
        """
        Process incremental batches using bulk operations for maximum performance.
        Uses cursor-based pagination instead of OFFSET for proper incremental loading.
        """
        try:
            start_time = time.time()
            total_copied = 0
            batch_num = 0
            current_cursor = last_processed
            
            while True:
                batch_num += 1
                batch_start_time = time.time()
                
                # Fetch batch from source using cursor-based pagination
                with self.replicator.source_engine.connect() as source_conn:
                    if current_cursor is None:
                        # First batch - get initial records
                        result = source_conn.execute(text(
                            f"SELECT * FROM `{table_name}` ORDER BY {primary_column} LIMIT {batch_size}"
                        ))
                    else:
                        # Subsequent batches - get records after current cursor
                        result = source_conn.execute(text(
                            f"SELECT * FROM `{table_name}` WHERE {primary_column} > :current_cursor "
                            f"ORDER BY {primary_column} LIMIT {batch_size}"
                        ), {"current_cursor": current_cursor})
                    
                    rows = result.fetchall()
                    columns = result.keys()
                
                if not rows:
                    logger.info(f"No more records to process for {table_name} after batch {batch_num-1}")
                    break
                
                # Use unified bulk operation for incremental data
                rows_processed = self._execute_bulk_operation(table_name, columns, rows, 'upsert')
                
                if rows_processed == 0:
                    logger.warning(f"No rows were processed in batch {batch_num} for {table_name}")
                    break
                
                total_copied += rows_processed
                
                # Update cursor to the last processed value in this batch
                if rows:
                    # Get the last row's primary column value as the new cursor
                    last_row = rows[-1]
                    column_index = list(columns).index(primary_column)
                    current_cursor = last_row[column_index]
                
                batch_duration = time.time() - batch_start_time
                batch_rate = rows_processed / batch_duration if batch_duration > 0 else 0
                
                progress = (total_copied / total_records) * 100 if total_records > 0 else 0
                logger.info(f"Incremental batch {batch_num}: {rows_processed:,} rows in {batch_duration:.2f}s "
                          f"({batch_rate:.0f} rows/sec) - Progress: {progress:.1f}% - Cursor: {current_cursor}")
                
                # Safety check: if we processed fewer rows than batch_size, we're done
                if len(rows) < batch_size:
                    logger.info(f"Reached end of data for {table_name} (batch {batch_num} had {len(rows)} rows)")
                    break
            
            total_duration = time.time() - start_time
            avg_rate = total_copied / total_duration if total_duration > 0 else 0
            
            logger.info(f"Incremental bulk copy completed for {table_name}: {total_copied:,} rows in "
                       f"{total_duration:.2f}s ({avg_rate:.0f} rows/sec)")
            
            return True, total_copied
            
        except Exception as e:
            logger.error(f"Error processing incremental batches for {table_name}: {str(e)}")
            return False, 0
    


    def _execute_bulk_operation(self, table_name: str, columns: List[str], 
                               rows: List, operation_type: str = 'insert') -> int:
        """
        Unified bulk operation handler.
        
        Args:
            table_name: Name of the table
            columns: List of column names
            rows: List of row data
            operation_type: 'insert', 'upsert', or 'replace'
            
        Returns:
            Number of rows processed
        """
        try:
            # Prepare data for bulk operation
            values_list = []
            for row in rows:
                cleaned_row = self.replicator._clean_row_data(row, columns, table_name)
                values_list.append(tuple(cleaned_row))
            
            # Create SQL statement based on operation type
            escaped_columns = [f"`{col}`" for col in columns]
            placeholders = ', '.join(['%s'] * len(columns))
            
            target_db = self.replicator.target_engine.url.database
            if operation_type == 'insert':
                # Simple INSERT
                sql = f"""
                    INSERT INTO `{target_db}`.`{table_name}` ({', '.join(escaped_columns)})
                    VALUES ({placeholders})
                """
            elif operation_type in ['upsert', 'replace']:
                # Get table configuration for primary key
                config = self.replicator.table_configs.get(table_name, {})
                primary_key = config.get('primary_key', 'id')
                
                # Build UPDATE clause (exclude primary key from updates)
                update_columns = [f"`{col}` = VALUES(`{col}`)" 
                                 for col in columns if col != primary_key]
                update_clause = ', '.join(update_columns) if update_columns else "`updated_at` = NOW()"
                
                sql = f"""
                    INSERT INTO `{target_db}`.`{table_name}` ({', '.join(escaped_columns)})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                """
            else:
                raise ValueError(f"Unsupported operation_type: {operation_type}")
            
            # Execute bulk operation
            with self.replicator.target_engine.connect() as conn:
                raw_conn = conn.connection.connection
                cursor = raw_conn.cursor()
                
                # Try to set session variables for ultra-fast bulk operations, but handle privilege errors gracefully
                try:
                    cursor.execute("SET SESSION bulk_insert_buffer_size = %s", (self.bulk_insert_buffer_size,))
                except Exception as e:
                    if "Access denied" in str(e) or "SUPER" in str(e) or "SYSTEM_VARIABLES_ADMIN" in str(e):
                        logger.warning(f"Failed to set bulk_insert_buffer_size due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set bulk_insert_buffer_size: {e}")
                
                # Try to set performance optimizations, but handle GLOBAL variable gracefully
                try:
                    cursor.execute("SET SESSION innodb_flush_log_at_trx_commit = 0")
                except Exception as e:
                    if "GLOBAL variable" in str(e) or "Access denied" in str(e):
                        logger.warning("innodb_flush_log_at_trx_commit requires GLOBAL privileges, skipping")
                    else:
                        logger.warning(f"Failed to set innodb_flush_log_at_trx_commit: {e}")
                
                try:
                    cursor.execute("SET SESSION sync_binlog = 0")
                except Exception as e:
                    if "GLOBAL variable" in str(e) or "Access denied" in str(e):
                        logger.warning("sync_binlog requires GLOBAL privileges, skipping")
                    else:
                        logger.warning(f"Failed to set sync_binlog: {e}")
                
                # Log target connection info
                try:
                    logger.info(
                        f"Replication target: host={self.replicator.target_engine.url.host} "
                        f"port={self.replicator.target_engine.url.port} db={target_db}"
                    )
                except Exception:
                    pass

                # Execute bulk operation
                cursor.executemany(sql, values_list)
                try:
                    logger.info(f"Bulk executemany affected rowcount={cursor.rowcount} for {table_name}")
                except Exception:
                    pass

                # Pre-commit verification inside same transaction
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM `{target_db}`.`{table_name}`")
                    pre_commit_count = cursor.fetchone()[0]
                    logger.info(f"Pre-commit replication count for {table_name} = {pre_commit_count}")
                    if (cursor.rowcount == 0 or pre_commit_count == 0) and values_list:
                        sample = values_list[0]
                        logger.error(
                            f"No rows reported inserted for {table_name}. Logging SQL and first bind sample.\n"
                            f"SQL: {sql.strip()}\n"
                            f"First values: {sample}"
                        )
                except Exception as dbg_err:
                    logger.warning(f"Pre-commit verification failed for {table_name}: {dbg_err}")
                
                # Reset session variables (only if they were set successfully)
                try:
                    cursor.execute("SET SESSION innodb_flush_log_at_trx_commit = 1")
                except Exception:
                    pass  # Ignore reset errors
                
                try:
                    cursor.execute("SET SESSION sync_binlog = 1")
                except Exception:
                    pass  # Ignore reset errors
                
                # Commit using the raw DB-API connection to avoid any wrapper mismatch
                try:
                    # Extra diagnostics before commit
                    try:
                        cursor.execute("SELECT DATABASE(), @@hostname, @@port")
                        dbg_db, dbg_host, dbg_port = cursor.fetchone()
                        logger.info(f"Pre-commit (raw) context: db={dbg_db} host={dbg_host} port={dbg_port}")
                    except Exception:
                        pass

                    raw_conn.commit()
                    logger.info("Raw connection commit succeeded")
                except Exception as commit_err:
                    logger.warning(f"Raw connection commit failed, falling back to SQLAlchemy commit: {commit_err}")
                    conn.commit()
                
                # Post-commit verification on a NEW pooled connection
                try:
                    with self.replicator.target_engine.connect() as verify_conn:
                        v_raw = verify_conn.connection.connection
                        v_cur = v_raw.cursor()
                        try:
                            v_cur.execute("SELECT DATABASE(), @@hostname, @@port")
                            v_db, v_host, v_port = v_cur.fetchone()
                            logger.info(f"Post-commit (new conn) context: db={v_db} host={v_host} port={v_port}")
                        except Exception:
                            pass
                        v_cur.execute(f"SELECT COUNT(*) FROM `{target_db}`.`{table_name}`")
                        post_commit_count = v_cur.fetchone()[0]
                        logger.info(f"Post-commit replication count for {table_name} = {post_commit_count}")
                except Exception as post_err:
                    logger.warning(f"Post-commit verification on new connection failed for {table_name}: {post_err}")
                
                return len(values_list)
                
        except Exception as e:
            logger.error(f"Error in unified bulk operation for {table_name}: {str(e)}")
            raise

class SimpleMySQLReplicator:
    """
    Simple MySQL replicator optimized for cross-server replication.
    
    ARCHITECTURE:
    - Source: Always remote OpenDental server (client location)  
    - Target: Always localhost replication database
    - Strategy: Cross-server copy only (no same-server logic)
    
    Connection Architecture Compliance:
    - Uses Settings injection for environment-agnostic database connections
    - Uses unified ConnectionFactory API with Settings injection
    - Uses Settings-based configuration methods
    - No direct environment variable access
    - Environment-agnostic (works for both clinic and test)
    
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
        - Automatically uses correct environment (clinic/test) based on Settings
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
            
            # Initialize performance optimizations
            self.performance_optimizer = PerformanceOptimizations(self)
            
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
                        rows_copied, copy_status
                    ) VALUES (
                        :table_name, CURRENT_TIMESTAMP, :last_primary_value, :primary_column_name,
                        :rows_copied, :copy_status
                    )
                    ON DUPLICATE KEY UPDATE
                        last_copied = CURRENT_TIMESTAMP,
                        last_primary_value = :last_primary_value,
                        primary_column_name = :primary_column_name,
                        rows_copied = :rows_copied,
                        copy_status = :copy_status
                """), {
                    "table_name": table_name,
                    "last_primary_value": last_primary_value,
                    "primary_column_name": primary_column_name,
                    "rows_copied": rows_copied,
                    "copy_status": copy_status
                })
                conn.commit()
                logger.info(f"Tracking table updated: etl_copy_status for {table_name}")
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
    

    
    def get_copy_method(self, table_name: str) -> str:
        """
        Determine copy method based on performance category (HOW to copy).
        All tables should have performance_category from analyze_opendental_schema.py.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Copy method: 'tiny', 'small', 'medium', or 'large'
        """
        config = self.table_configs.get(table_name, {})
        
        # All tables should have performance_category from schema analysis
        performance_category = config.get('performance_category')
        if not performance_category:
            raise ValueError(f"Table {table_name} missing performance_category in configuration. Run analyze_opendental_schema.py to generate proper configuration.")
        
        # Map performance categories to copy methods
        if performance_category == 'tiny':
            return 'tiny'
        elif performance_category == 'small':
            return 'small'
        elif performance_category == 'medium':
            return 'medium'
        elif performance_category == 'large':
            return 'large'
        else:
            raise ValueError(f"Invalid performance_category '{performance_category}' for table {table_name}")
    
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
    
    @track_method
    def copy_table(self, table_name: str, force_full: bool = False) -> Tuple[bool, Dict]:
        """
        Copy a single table from source to target.
        Enhanced to leverage new configuration values from analyze_opendental_schema.py.
        
        Args:
            table_name: Name of the table to copy
            force_full: Force full copy (ignore incremental)
            
        Returns:
            Tuple[bool, Dict]: (success, metadata_dict)
            metadata_dict contains:
                - rows_copied: int
                - strategy_used: str ('incremental' or 'full_table')
                - duration: float (seconds)
                - force_full_applied: bool
                - primary_column: Optional[str]
                - last_primary_value: Optional[str]
        """
        import psutil
        
        # Track memory usage
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Initialize timing variables
        start_time = None
        duration = 0.0
        
        try:
            logger.info(f"Starting optimized copy of table: {table_name}")
            
            # Get table configuration
            config = self.table_configs.get(table_name, {})
            if not config:
                logger.error(f"No configuration found for table: {table_name}")
                return False, {
                    'rows_copied': 0,
                    'strategy_used': 'error',
                    'duration': 0.0,
                    'force_full_applied': force_full,
                    'primary_column': None,
                    'last_primary_value': None,
                    'error': 'No configuration found'
                }
            
            # Log performance category and processing priority from schema analyzer
            performance_category = config.get('performance_category', 'medium')
            processing_priority = config.get('processing_priority', 5)
            estimated_processing_time = config.get('estimated_processing_time_minutes', 0)
            memory_requirements = config.get('memory_requirements_mb', 0)
            
            logger.info(f"Table {table_name}: {performance_category} category, priority {processing_priority}, "
                       f"estimated {estimated_processing_time:.1f}min, {memory_requirements}MB memory")
            
            # Get primary incremental column
            primary_column = self._get_primary_incremental_column(config)
            incremental_columns = config.get('incremental_columns', [])
            
            # Log the strategy being used
            self._log_incremental_strategy(table_name, primary_column, incremental_columns)
            
            # 1. Determine WHAT to copy (extraction strategy) with intelligent selection
            extraction_strategy = self.get_extraction_strategy(table_name)
            actual_force_full = force_full
            if force_full:
                extraction_strategy = 'full_table'
                logger.info(f"Forcing full table copy for {table_name}")
            else:
                # Use intelligent strategy selection for large time gaps
                if self.performance_optimizer.should_use_full_refresh(table_name, config):
                    extraction_strategy = 'full_table'
                    actual_force_full = True
                    logger.info(f"Intelligent strategy selection: Using full refresh for {table_name} due to large time gap")
            
            # 2. Determine HOW to copy (copy method based on size)
            copy_method = self.get_copy_method(table_name)
            
            logger.info(f"Using {extraction_strategy} extraction strategy and {copy_method} copy method for {table_name}")
            
            # 3. Execute the appropriate copy operation
            start_time = time.time()  # Start timing the actual copy operation
            success, rows_copied = self._execute_table_copy(table_name, config, extraction_strategy)
            
            # Calculate duration and memory usage
            end_time = time.time()
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024
            duration = end_time - start_time
            memory_used = final_memory - initial_memory
            
            if success:
                # Get the maximum value of the primary column from copied data
                last_primary_value = None
                if primary_column and primary_column != 'none':
                    # Get the actual max primary value from the copied data
                    last_primary_value = self._get_max_primary_value_from_copied_data(table_name, primary_column)
                    if last_primary_value:
                        logger.debug(f"Retrieved max primary value for {table_name}: {last_primary_value}")
                    else:
                        logger.debug(f"No primary value found for {table_name}, using tracking table value")
                        last_primary_value = self._get_last_copy_primary_value(table_name)
                
                # Update copy tracking with primary column value
                self._update_copy_status(table_name, rows_copied, 'success', last_primary_value, primary_column)
                
                # Use enhanced performance tracking
                self.performance_optimizer._track_performance_optimized(table_name, duration, memory_used, rows_copied)

                # Post-copy validation (test safe): confirm rows exist in replication DB
                try:
                    with self.target_engine.connect() as target_conn:
                        replication_db = self.target_engine.url.database
                        count = target_conn.execute(text(f"SELECT COUNT(*) FROM `{replication_db}`.`{table_name}`")).scalar()
                        logger.info(f"Post-copy validation for {table_name}: replication count={count}")
                        if rows_copied > 0 and (count is None or int(count) == 0):
                            logger.error(
                                f"Post-copy validation failed for {table_name}: reported rows_copied={rows_copied} but replication has 0. "
                                f"Check replication connection and fully-qualified writes."
                            )
                except Exception as v_err:
                    logger.warning(f"Post-copy validation error for {table_name}: {v_err}")
                
                # Return detailed metadata
                metadata = {
                    'rows_copied': rows_copied,
                    'strategy_used': extraction_strategy,
                    'duration': duration,
                    'force_full_applied': actual_force_full,
                    'primary_column': primary_column,
                    'last_primary_value': last_primary_value,
                    'memory_used_mb': memory_used,
                    'copy_method': copy_method,
                    'performance_category': performance_category
                }
                
                logger.info(f"Successfully copied {table_name} ({rows_copied:,} rows) in {duration:.2f}s")
                return True, metadata
            else:
                # Update tracking with failure status
                self._update_copy_status(table_name, 0, 'failed', None, None)
                
                metadata = {
                    'rows_copied': 0,
                    'strategy_used': extraction_strategy,
                    'duration': duration,
                    'force_full_applied': actual_force_full,
                    'primary_column': primary_column,
                    'last_primary_value': None,
                    'memory_used_mb': memory_used,
                    'copy_method': copy_method,
                    'performance_category': performance_category,
                    'error': 'Copy operation failed'
                }
                
                logger.error(f"Failed to copy {table_name}")
                return False, metadata
                
        except Exception as e:
            end_time = time.time()
            if start_time is not None:
                duration = end_time - start_time
            
            logger.error(f"Error copying table {table_name}: {str(e)}")
            # Update tracking with failure status
            self._update_copy_status(table_name, 0, 'failed', None, None)
            
            metadata = {
                'rows_copied': 0,
                'strategy_used': 'error',
                'duration': duration,
                'force_full_applied': force_full,
                'primary_column': None,
                'last_primary_value': None,
                'memory_used_mb': 0,
                'copy_method': 'error',
                'performance_category': 'unknown',
                'error': str(e)
            }
            
            return False, metadata
    
    @track_method
    def _execute_table_copy(self, table_name: str, config: Dict, 
                           extraction_strategy: str) -> Tuple[bool, int]:
        """
        Unified table copy execution with automatic optimization selection.
        Replaces all performance-category-specific copy methods.
        
        Args:
            table_name: Name of the table to copy
            config: Table configuration dictionary
            extraction_strategy: 'full_table', 'incremental', or 'incremental_chunked'
            
        Returns:
            Tuple of (success, rows_copied)
        """
        try:
            # Get performance category and batch size
            performance_category = config.get('performance_category', 'medium')
            batch_size = self.performance_optimizer.calculate_adaptive_batch_size(table_name, config)
            
            # Determine if full refresh is needed
            force_full = self.performance_optimizer.should_use_full_refresh(table_name, config)
            
            if force_full or extraction_strategy == 'full_table':
                logger.info(f"Using full refresh for {table_name} ({performance_category})")
                return self._copy_full_table_unified(table_name, batch_size, config)
            elif extraction_strategy == 'incremental_chunked':
                logger.info(f"Using incremental chunked copy for {table_name} ({performance_category})")
                return self._copy_incremental_chunked(table_name, config, batch_size)
            else:
                logger.info(f"Using incremental copy for {table_name} ({performance_category})")
                return self._copy_incremental_unified(table_name, config, batch_size)
                
        except Exception as e:
            logger.error(f"Error in unified table copy for {table_name}: {str(e)}")
            return False, 0
    
    @track_method
    def _copy_full_table_unified(self, table_name: str, batch_size: int, config: Dict) -> Tuple[bool, int]:
        """
        Unified full table copy handling all performance categories.
        """
        try:
            performance_category = config.get('performance_category', 'medium')
            # Log replication target before copy for visibility
            try:
                tgt = self.target_engine.url
                logger.info(f"Replication target connection: host={tgt.host} port={tgt.port} db={tgt.database}")
            except Exception:
                pass
            total_count = self._get_table_total_count(table_name)
            
            logger.info(f"Starting unified full table copy for {table_name} ({performance_category}, {total_count:,} records)")
            
            # FIX: Recreate table structure for full table copy to prevent primary key conflicts
            logger.info(f"Recreating table structure for full table copy of {table_name}")
            if not self._recreate_table_structure(table_name):
                logger.error(f"Failed to recreate table structure for {table_name}")
                return False, 0
            
            # Unified implementation for all performance categories
            offset = 0
            total_copied = 0
            batch_num = 0
            
            # Performance optimizations for large tables
            if performance_category == 'large':
                # Disable foreign key checks and autocommit for performance
                with self.target_engine.connect() as target_conn:
                    target_conn.execute(text("SET foreign_key_checks = 0"))
                    target_conn.execute(text("SET autocommit = 0"))
                    target_conn.commit()
            
            while offset < total_count:
                batch_num += 1
                batch_start_time = time.time()
                
                # Fetch batch from source
                with self.source_engine.connect() as source_conn:
                    if performance_category == 'large':
                        # Use ORDER BY for consistent results in large tables
                        primary_key = config.get('primary_key', 'id')
                        try:
                            result = source_conn.execute(text(
                                f"SELECT * FROM `{table_name}` ORDER BY `{primary_key}` LIMIT {batch_size} OFFSET {offset}"
                            ))
                        except Exception:
                            # Fallback without ORDER BY if primary key doesn't exist
                            result = source_conn.execute(text(
                                f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"
                            ))
                    else:
                        # Simple fetch for medium/small tables
                        result = source_conn.execute(text(
                            f"SELECT * FROM `{table_name}` LIMIT {batch_size} OFFSET {offset}"
                        ))
                    
                    rows = result.fetchall()
                    columns = result.keys()
                
                if not rows:
                    break
                
                # Use unified bulk operation
                operation_type = 'insert' if performance_category == 'large' else 'upsert'
                rows_inserted = self.performance_optimizer._execute_bulk_operation(table_name, columns, rows, operation_type)
                
                total_copied += rows_inserted
                offset += batch_size
                
                # Performance tracking and adaptive adjustment
                batch_duration = time.time() - batch_start_time
                batch_rate = rows_inserted / batch_duration if batch_duration > 0 else 0
                progress = (total_copied / total_count) * 100
                
                logger.info(f"Batch {batch_num}: {rows_inserted:,} rows in {batch_duration:.2f}s "
                          f"({batch_rate:.0f} rows/sec) - Progress: {progress:.1f}%")
                
                # Adaptive batch size adjustment for large tables
                if performance_category == 'large':
                    expected_rate = self.performance_optimizer._get_expected_rate_for_category(performance_category)
                    if batch_rate < expected_rate * 0.5:  # Very slow
                        batch_size = max(self.performance_optimizer.min_bulk_batch_size, batch_size // 2)
                        logger.info(f"Reducing batch size to {batch_size:,} due to slow performance")
                    elif batch_rate > expected_rate * 2:  # Very fast
                        batch_size = min(self.performance_optimizer.max_bulk_batch_size, int(batch_size * 1.5))
                        logger.info(f"Increasing batch size to {batch_size:,} due to excellent performance")
            
            # Re-enable foreign key checks for large tables
            if performance_category == 'large':
                try:
                    with self.target_engine.connect() as target_conn:
                        target_conn.execute(text("SET foreign_key_checks = 1"))
                        target_conn.execute(text("SET autocommit = 1"))
                        target_conn.commit()
                except Exception as e:
                    logger.warning(f"Error re-enabling foreign key checks for {table_name}: {e}")
            
            return True, total_copied
                
        except Exception as e:
            logger.error(f"Error in unified full table copy for {table_name}: {str(e)}")
            # Re-enable foreign key checks on error for large tables
            if config.get('performance_category', 'medium') == 'large':
                try:
                    with self.target_engine.connect() as target_conn:
                        target_conn.execute(text("SET foreign_key_checks = 1"))
                        target_conn.execute(text("SET autocommit = 1"))
                        target_conn.commit()
                except:
                    pass
            return False, 0
    
    @track_method
    def _copy_incremental_unified(self, table_name: str, config: Dict, batch_size: int) -> Tuple[bool, int]:
        """
        Unified incremental copy handling all performance categories.
        """
        try:
            # Handle both config dict and TableProcessingContext objects
            if hasattr(config, 'config'):
                config = config.config
            elif hasattr(config, 'incremental_columns'):
                # This is already a config-like object, extract incremental_columns from attributes
                incremental_columns = config.incremental_columns
                # Create a config dict with the necessary attributes
                config_dict = {
                    'incremental_columns': incremental_columns,
                    'primary_incremental_column': getattr(config, 'primary_incremental_column', None)
                }
                config = config_dict
            else:
                # Ensure config is a dictionary
                pass
            
            # Get all incremental columns from the list
            incremental_columns = config.get('incremental_columns', [])
            if not incremental_columns:
                logger.error(f"No incremental columns configured for table: {table_name}")
                return False, 0
            
            # Get primary incremental column from configuration
            primary_incremental_column = self._get_primary_incremental_column(config)
            
            logger.info(f"Copying incremental data for {table_name} using columns: {incremental_columns} with batch size: {batch_size}")
            
            # Log which strategy is being used
            self._log_incremental_strategy(table_name, primary_incremental_column, incremental_columns)
            
            # Use unified incremental metadata method
            if primary_incremental_column:
                # Use single column logic
                metadata = self._get_incremental_metadata(table_name, [primary_incremental_column])
            else:
                # Use multi-column logic
                metadata = self._get_incremental_metadata(table_name, incremental_columns)
            
            # If table doesn't exist, create it first
            if metadata['last_processed_value'] is None:
                logger.info(f"Target table {table_name} does not exist, creating table structure")
                if not self._recreate_table_structure(table_name):
                    logger.error(f"Failed to create table structure for {table_name}")
                    return False, 0
            
            if metadata['new_records_count'] == 0:
                logger.info(f"No new records to copy for {table_name}")
                return True, 0
            
            logger.info(f"Found {metadata['new_records_count']} new records to copy for {table_name}")
            
            # Use unified incremental copy method
            return self._copy_incremental_records(table_name, incremental_columns, 
                                                metadata['last_processed_value'], batch_size)
            
        except Exception as e:
            logger.error(f"Error in unified incremental copy for {table_name}: {str(e)}")
            return False, 0


    

    
    def _get_primary_incremental_column(self, config: Dict) -> Optional[str]:
        """
        Get the primary incremental column from configuration with fallback logic.
        
        Args:
            config: Table configuration dictionary
            
        Returns:
            Primary incremental column name, or None if not available
        """
        # Handle both config dict and TableProcessingContext objects
        if hasattr(config, 'config'):
            config = config.config
        elif hasattr(config, 'incremental_columns'):
            # This is already a config-like object, extract primary_column from attributes
            primary_column = getattr(config, 'primary_incremental_column', None)
            # Create a config dict with the necessary attributes
            config_dict = {
                'primary_incremental_column': primary_column
            }
            config = config_dict
        else:
            # Ensure config is a dictionary
            pass
        
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
                        # For non-string values, check if they're problematic objects
                        # Only convert problematic objects, preserve normal data types
                        if isinstance(value, (int, float, bool)):
                            # Preserve normal data types
                            cleaned_row.append(value)
                        else:
                            # Handle problematic objects (like raw Python objects)
                            try:
                                # Try to convert to a basic type that can be safely stored
                                if hasattr(value, '__str__'):
                                    # For objects that can be stringified, try to convert to string
                                    str_value = str(value)
                                    # Only use stringified value if it's not the default object representation
                                    if not str_value.startswith('<') or not str_value.endswith('>'):
                                        cleaned_row.append(str_value)
                                    else:
                                        # Default object representation, use None
                                        logger.warning(f"Converting problematic object to None for column {columns[i]} in {table_name}: {str_value}")
                                        cleaned_row.append(None)
                                else:
                                    # Object without string representation, use None
                                    logger.warning(f"Converting object without string representation to None for column {columns[i]} in {table_name}")
                                    cleaned_row.append(None)
                            except Exception:
                                # If any conversion fails, use None
                                cleaned_row.append(None)
                        
                except Exception as e:
                    logger.warning(f"Error cleaning value for column {columns[i]} in {table_name}: {str(e)}")
                    # Use None as fallback for problematic values
                    cleaned_row.append(None)
            
            return cleaned_row
            
        except Exception as e:
            logger.error(f"Error cleaning row data for {table_name}: {str(e)}")
            # Return original row if cleaning fails
            return row
    

    

    



    
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

    # REMOVED: _get_last_processed_value() - Replaced by unified _get_incremental_metadata()
    # REMOVED: _get_last_processed_value_max() - Replaced by unified _get_incremental_metadata()

    def _get_max_primary_value_from_copied_data(self, table_name: str, primary_column: str) -> Optional[str]:
        """Get the maximum primary column value from the copied data in replication database."""
        try:
            with self.target_engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT MAX({primary_column}) as max_value
                    FROM {table_name}
                    WHERE {primary_column} IS NOT NULL
                """)).scalar()
                
                if result:
                    logger.debug(f"Retrieved max primary value for {table_name}: {result} (column: {primary_column})")
                    return str(result)
                return None
                
        except Exception as e:
            logger.error(f"Error getting max primary value from copied data for {table_name}: {str(e)}")
            return None

    @track_method
    def copy_all_tables(self, table_filter: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        Copy all tables or filtered subset.
        
        Args:
            table_filter: Optional list of table names to copy (if None, copy all)
            
        Returns:
            Dictionary mapping table names to success status
        """
        try:
            # Get list of tables to copy
            if table_filter is None:
                tables_to_copy = list(self.table_configs.keys())
                logger.info(f"Copying all {len(tables_to_copy)} tables")
            else:
                tables_to_copy = table_filter
                logger.info(f"Copying {len(tables_to_copy)} filtered tables")
            
            results = {}
            
            # Copy each table
            for table_name in tables_to_copy:
                if table_name not in self.table_configs:
                    logger.warning(f"Table {table_name} not found in configuration, skipping")
                    results[table_name] = False
                    continue
                
                try:
                    logger.info(f"Starting copy of table: {table_name}")
                    success = self.copy_table(table_name)
                    results[table_name] = success
                    
                    if success:
                        logger.info(f"Successfully copied table: {table_name}")
                    else:
                        logger.error(f"Failed to copy table: {table_name}")
                        
                except Exception as e:
                    logger.error(f"Error copying table {table_name}: {e}")
                    results[table_name] = False
            
            # Log summary
            successful = sum(1 for success in results.values() if success)
            total = len(results)
            logger.info(f"Copy operation completed: {successful}/{total} tables successful")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in copy_all_tables: {e}")
            raise
    
    @track_method
    def copy_tables_by_processing_priority(self, max_priority: int = 10) -> Dict[str, bool]:
        """
        Copy tables by processing priority (new feature leveraging schema analyzer metadata).
        
        Args:
            max_priority: Maximum processing priority to include (1-10, where 1 is highest priority)
            
        Returns:
            Dictionary mapping table names to success status
        """
        tables_to_copy = []
        
        # Priority mapping for string values to numeric values
        priority_mapping = {
            'high': 1,
            'medium': 5, 
            'low': 10
        }
        
        # Sort tables by processing priority (ascending - lower numbers are higher priority)
        sorted_tables = []
        for table_name, config in self.table_configs.items():
            priority_value = config.get('processing_priority', 'medium')  # Default to medium priority
            
            # Convert string priority to numeric if needed
            if isinstance(priority_value, str):
                priority = priority_mapping.get(priority_value.lower(), 5)  # Default to 5 for unknown strings
            else:
                priority = priority_value
            
            sorted_tables.append((table_name, priority))
        
        # Sort by priority (ascending) and filter by max_priority
        sorted_tables.sort(key=lambda x: x[1])
        tables_to_copy = [table_name for table_name, priority in sorted_tables if priority <= max_priority]
        
        logger.info(f"Found {len(tables_to_copy)} tables with processing priority <= {max_priority}")
        
        if not tables_to_copy:
            logger.info(f"No tables found with processing priority <= {max_priority}")
            return {}
        
        # Log priority distribution
        priority_counts = {}
        for table_name, priority in sorted_tables[:len(tables_to_copy)]:
            priority_counts[priority] = priority_counts.get(priority, 0) + 1
        
        logger.info(f"Priority distribution: {priority_counts}")
        
        return self.copy_all_tables(tables_to_copy)
    
    @track_method
    def copy_tables_by_performance_category(self, category: str) -> Dict[str, bool]:
        """
        Copy tables by performance category (new feature leveraging schema analyzer metadata).
        
        Args:
            category: 'large', 'medium', 'small', 'tiny'
            
        Returns:
            Dictionary mapping table names to success status
        """
        tables_to_copy = []
        
        # Define all valid performance categories
        valid_categories = {'large', 'medium', 'small', 'tiny'}
        
        if category not in valid_categories:
            logger.error(f"Invalid performance category: {category}. Valid categories: {valid_categories}")
            return {}
        
        for table_name, config in self.table_configs.items():
            if config.get('performance_category') == category:
                tables_to_copy.append(table_name)
        
        logger.info(f"Found {len(tables_to_copy)} tables with performance category: {category}")
        
        if not tables_to_copy:
            logger.info(f"No tables found with performance category: {category}")
            return {}
        
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
    
    def get_schema_analyzer_summary(self) -> str:
        """
        Generate a summary of schema analyzer configuration values.
        New method to show how the replicator leverages the enhanced configuration.
        """
        report = ["# Schema Analyzer Configuration Summary", ""]
        
        # Count tables by performance category
        category_counts = {}
        priority_counts = {}
        strategy_counts = {}
        total_estimated_rows = 0
        total_estimated_size_mb = 0
        
        for table_name, config in self.table_configs.items():
            category = config.get('performance_category', 'medium')
            priority = config.get('processing_priority', 5)
            strategy = config.get('extraction_strategy', 'full_table')
            estimated_rows = config.get('estimated_rows', 0)
            estimated_size_mb = config.get('estimated_size_mb', 0)
            
            category_counts[category] = category_counts.get(category, 0) + 1
            priority_counts[priority] = priority_counts.get(priority, 0) + 1
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            total_estimated_rows += estimated_rows
            total_estimated_size_mb += estimated_size_mb
        
        report.append("## Performance Categories")
        for category, count in sorted(category_counts.items()):
            report.append(f"- {category}: {count} tables")
        
        report.append("")
        report.append("## Processing Priorities")
        for priority in sorted(priority_counts.keys()):
            count = priority_counts[priority]
            report.append(f"- Priority {priority}: {count} tables")
        
        report.append("")
        report.append("## Extraction Strategies")
        for strategy, count in sorted(strategy_counts.items()):
            report.append(f"- {strategy}: {count} tables")
        
        report.append("")
        report.append("## Overall Statistics")
        report.append(f"- Total Tables: {len(self.table_configs)}")
        report.append(f"- Total Estimated Rows: {total_estimated_rows:,}")
        report.append(f"- Total Estimated Size: {total_estimated_size_mb:.1f}MB")
        
        # Show top tables by processing time
        report.append("")
        report.append("## Top Tables by Estimated Processing Time")
        tables_by_time = []
        for table_name, config in self.table_configs.items():
            processing_time = config.get('estimated_processing_time_minutes', 0)
            if processing_time > 0:
                tables_by_time.append((table_name, processing_time))
        
        tables_by_time.sort(key=lambda x: x[1], reverse=True)
        for table_name, processing_time in tables_by_time[:10]:  # Top 10
            report.append(f"- {table_name}: {processing_time:.1f} minutes")
        
        return "\n".join(report) 

    def _get_incremental_metadata(self, table_name: str, incremental_columns: List[str]) -> Dict:
        """
        Unified method to get incremental loading metadata.
        Handles both single column (list with 1 item) and multi-column cases.
        
        Args:
            table_name: Name of the table
            incremental_columns: List of incremental columns (can be single or multiple)
            
        Returns:
            {
                'last_processed_value': value,
                'new_records_count': count,
                'column_strategy': 'single' | 'multi'
            }
        """
        try:
            # Determine strategy based on number of columns
            column_strategy = 'single' if len(incremental_columns) == 1 else 'multi'
            
            # Get last processed value from tracking table
            last_processed = self._get_last_copy_primary_value(table_name)
            
            # Debug logging
            logger.debug(f"DEBUG: Last processed value for {table_name}: {last_processed}")
            logger.debug(f"DEBUG: Using columns: {incremental_columns}")
            logger.debug(f"DEBUG: Column strategy: {column_strategy}")
            
            # Get new records count from source
            if column_strategy == 'single':
                # Single column case
                primary_column = incremental_columns[0]
                if last_processed is None:
                    # No previous copy, count all records
                    with self.source_engine.connect() as conn:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                        new_records_count = result.fetchone()[0]
                        logger.debug(f"DEBUG: No previous copy found, counting all records: {new_records_count}")
                else:
                    # Count records after last processed value
                    with self.source_engine.connect() as conn:
                        result = conn.execute(text(
                            f"SELECT COUNT(*) FROM `{table_name}` WHERE `{primary_column}` > :last_processed"
                        ), {"last_processed": last_processed})
                        new_records_count = result.fetchone()[0]
                        logger.debug(f"DEBUG: Found {new_records_count} records after {last_processed}")
            else:
                # Multi-column case - use MAX() for each column
                if last_processed is None:
                    # No previous copy, count all records
                    with self.source_engine.connect() as conn:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                        new_records_count = result.fetchone()[0]
                        logger.debug(f"DEBUG: No previous copy found (multi-column), counting all records: {new_records_count}")
                else:
                    # For multi-column, we need to parse the last_processed value
                    # This assumes last_processed is a JSON string or comma-separated values
                    try:
                        if isinstance(last_processed, str) and ',' in last_processed:
                            last_values = last_processed.split(',')
                        else:
                            last_values = [last_processed] * len(incremental_columns)
                        
                        # Build WHERE clause for multi-column comparison
                        where_conditions = []
                        for i, column in enumerate(incremental_columns):
                            if i < len(last_values):
                                where_conditions.append(f"`{column}` > :val_{i}")
                        
                        where_clause = " AND ".join(where_conditions)
                        params = {f"val_{i}": last_values[i] for i in range(len(last_values))}
                        
                        with self.source_engine.connect() as conn:
                            result = conn.execute(text(
                                f"SELECT COUNT(*) FROM `{table_name}` WHERE {where_clause}"
                            ), params)
                            new_records_count = result.fetchone()[0]
                            logger.debug(f"DEBUG: Found {new_records_count} records (multi-column) after {last_processed}")
                    except Exception as e:
                        logger.warning(f"Error parsing multi-column last_processed value for {table_name}: {e}")
                        # Fallback to counting all records
                        with self.source_engine.connect() as conn:
                            result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                            new_records_count = result.fetchone()[0]
                            logger.debug(f"DEBUG: Fallback count for {table_name}: {new_records_count}")
            
            return {
                'last_processed_value': last_processed,
                'new_records_count': new_records_count,
                'column_strategy': column_strategy
            }
            
        except Exception as e:
            logger.error(f"Error getting incremental metadata for {table_name}: {str(e)}")
            return {
                'last_processed_value': None,
                'new_records_count': 0,
                'column_strategy': 'single'
            }

    @track_method
    def _copy_incremental_records(self, table_name: str, incremental_columns: List[str], 
                                 last_processed: Any, batch_size: int) -> Tuple[bool, int]:
        """
        Unified incremental copy method handling both single and multi-column cases.
        Automatically detects strategy based on incremental_columns length.
        
        Args:
            table_name: Name of the table
            incremental_columns: List of incremental columns (can be single or multiple)
            last_processed: Last processed value
            batch_size: Batch size for processing
            
        Returns:
            Tuple of (success, rows_copied)
        """
        try:
            # Get table configuration to determine primary column
            config = self.table_configs.get(table_name, {})
            primary_incremental_column = self._get_primary_incremental_column(config)
            
            # Determine strategy based on primary column availability
            if primary_incremental_column and primary_incremental_column in incremental_columns:
                # Use primary column for optimized incremental loading
                logger.info(f"Using primary incremental column '{primary_incremental_column}' for {table_name}")
                return self.performance_optimizer._process_incremental_batches_bulk(
                    table_name, primary_incremental_column, last_processed, batch_size, 0
                )
            elif len(incremental_columns) == 1:
                # Single column case (no primary column specified)
                primary_column = incremental_columns[0]
                logger.info(f"Using single incremental column '{primary_column}' for {table_name}")
                return self.performance_optimizer._process_incremental_batches_bulk(
                    table_name, primary_column, last_processed, batch_size, 0
                )
            else:
                # Multi-column case - use the first column as primary for now
                # This is a simplified approach; in a full implementation, we'd handle multi-column properly
                primary_column = incremental_columns[0]
                logger.warning(f"Multi-column incremental copy for {table_name} using primary column {primary_column}")
                return self.performance_optimizer._process_incremental_batches_bulk(
                    table_name, primary_column, last_processed, batch_size, 0
                )
                
        except Exception as e:
            logger.error(f"Error in unified incremental copy for {table_name}: {str(e)}")
            return False, 0

    @track_method
    def _copy_incremental_chunked(self, table_name: str, config: Dict, batch_size: int) -> Tuple[bool, int]:
        """
        Copy incremental data using chunked processing for very large tables.
        This method ensures no records are missed by using smaller chunks and careful boundary handling.
        
        Args:
            table_name: Name of the table to copy
            config: Table configuration dictionary
            batch_size: Batch size for processing
            
        Returns:
            Tuple of (success, rows_copied)
        """
        try:
            # Handle both config dict and TableProcessingContext objects
            if hasattr(config, 'config'):
                config = config.config
            elif hasattr(config, 'incremental_columns'):
                # This is already a config-like object, extract incremental_columns from attributes
                incremental_columns = config.incremental_columns
                # Create a config dict with the necessary attributes
                config_dict = {
                    'incremental_columns': incremental_columns,
                    'primary_incremental_column': getattr(config, 'primary_incremental_column', None)
                }
                config = config_dict
            else:
                # Ensure config is a dictionary
                pass
            
            # Get all incremental columns from the list
            incremental_columns = config.get('incremental_columns', [])
            if not incremental_columns:
                logger.error(f"No incremental columns configured for table: {table_name}")
                return False, 0
            
            # Get primary incremental column from configuration
            primary_incremental_column = self._get_primary_incremental_column(config)
            
            logger.info(f"Copying incremental chunked data for {table_name} using columns: {incremental_columns} with batch size: {batch_size}")
            
            # Log which strategy is being used
            self._log_incremental_strategy(table_name, primary_incremental_column, incremental_columns)
            
            # Use unified incremental metadata method
            if primary_incremental_column:
                # Use single column logic
                metadata = self._get_incremental_metadata(table_name, [primary_incremental_column])
            else:
                # Use multi-column logic
                metadata = self._get_incremental_metadata(table_name, incremental_columns)
            
            last_processed = metadata.get('last_processed_value')
            new_records_count = metadata.get('new_records_count', 0)
            column_strategy = metadata.get('column_strategy', 'single')
            
            logger.info(f"Incremental chunked metadata for {table_name}: last_processed={last_processed}, new_records={new_records_count}, strategy={column_strategy}")
            
            if new_records_count == 0:
                logger.info(f"No new records to copy for {table_name}")
                return True, 0
            
            # Use smaller chunk size for chunked processing to ensure no records are missed
            chunk_size = min(batch_size // 2, 5000)  # Use smaller chunks for better precision
            logger.info(f"Using chunk size {chunk_size} for incremental chunked processing of {table_name}")
            
            # Use the chunked incremental copy method
            success, rows_copied = self._copy_incremental_records_chunked(
                table_name, incremental_columns, last_processed, chunk_size, primary_incremental_column
            )
            
            if success:
                # Update copy status with the last processed value
                if primary_incremental_column:
                    self._update_copy_status(table_name, rows_copied, 'success', 
                                           str(last_processed), primary_incremental_column)
                else:
                    self._update_copy_status(table_name, rows_copied, 'success')
                
                logger.info(f"Successfully copied {rows_copied:,} records using incremental chunked strategy for {table_name}")
            else:
                logger.error(f"Failed to copy incremental chunked data for {table_name}")
            
            return success, rows_copied
            
        except Exception as e:
            logger.error(f"Error in incremental chunked copy for {table_name}: {str(e)}")
            return False, 0

    @track_method
    def _copy_incremental_records_chunked(self, table_name: str, incremental_columns: List[str], 
                                         last_processed: Any, chunk_size: int, 
                                         primary_column: Optional[str] = None) -> Tuple[bool, int]:
        """
        Copy incremental records using chunked processing with careful boundary handling.
        
        Args:
            table_name: Name of the table
            incremental_columns: List of incremental columns
            last_processed: Last processed value
            chunk_size: Size of each chunk
            primary_column: Primary incremental column for ordering
            
        Returns:
            Tuple of (success, rows_copied)
        """
        try:
            start_time = time.time()
            total_copied = 0
            chunk_num = 0
            current_cursor = last_processed
            
            # Use primary column for ordering if available, otherwise use first incremental column
            order_column = primary_column if primary_column else incremental_columns[0]
            
            logger.info(f"Starting chunked incremental copy for {table_name} using order column: {order_column}")
            
            while True:
                chunk_num += 1
                chunk_start_time = time.time()
                
                # Fetch chunk from source using cursor-based pagination
                with self.source_engine.connect() as source_conn:
                    if current_cursor is None:
                        # First chunk - get initial records
                        result = source_conn.execute(text(
                            f"SELECT * FROM `{table_name}` ORDER BY `{order_column}` LIMIT {chunk_size}"
                        ))
                    else:
                        # Subsequent chunks - get records after current cursor
                        result = source_conn.execute(text(
                            f"SELECT * FROM `{table_name}` WHERE `{order_column}` > :current_cursor "
                            f"ORDER BY `{order_column}` LIMIT {chunk_size}"
                        ), {"current_cursor": current_cursor})
                    
                    rows = result.fetchall()
                    columns = result.keys()
                
                if not rows:
                    logger.info(f"No more records to process for {table_name} after chunk {chunk_num-1}")
                    break
                
                # Use bulk operation for chunk
                rows_inserted = self.performance_optimizer._execute_bulk_operation(table_name, columns, rows, 'upsert')
                
                if rows_inserted == 0:
                    logger.warning(f"No rows were processed in chunk {chunk_num} for {table_name}")
                    break
                
                total_copied += rows_inserted
                
                # Update cursor to the last processed value in this chunk
                if rows:
                    # Get the last row's order column value as the new cursor
                    last_row = rows[-1]
                    column_index = list(columns).index(order_column)
                    current_cursor = last_row[column_index]
                
                chunk_duration = time.time() - chunk_start_time
                chunk_rate = rows_inserted / chunk_duration if chunk_duration > 0 else 0
                
                logger.info(f"Incremental chunk {chunk_num}: {rows_inserted:,} rows in {chunk_duration:.2f}s "
                          f"({chunk_rate:.0f} rows/sec) - Cursor: {current_cursor}")
                
                # Safety check: if we processed fewer rows than chunk_size, we're done
                if len(rows) < chunk_size:
                    logger.info(f"Reached end of data for {table_name} (chunk {chunk_num} had {len(rows)} rows)")
                    break
            
            total_duration = time.time() - start_time
            avg_rate = total_copied / total_duration if total_duration > 0 else 0
            
            logger.info(f"Incremental chunked copy completed for {table_name}: {total_copied:,} rows in "
                       f"{total_duration:.2f}s ({avg_rate:.0f} rows/sec)")
            
            return True, total_copied
            
        except Exception as e:
            logger.error(f"Error in chunked incremental copy for {table_name}: {str(e)}")
            return False, 0

    def _get_table_total_count(self, table_name: str) -> int:
        """
        Get the total record count for a table from the source database.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Total number of records in the table
        """
        try:
            with self.source_engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                return result.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting total count for {table_name}: {str(e)}")
            return 0