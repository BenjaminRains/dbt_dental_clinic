# MySQL Replication Methods Documentation

This document provides a comprehensive overview of all methods available in the `simple_mysql_replicator.py` file, which handles MySQL to MySQL data replication in the ETL pipeline.

## PerformanceOptimizations Class Methods

### Core Performance Methods
*   `__init__(self, replicator)`: Initializes the PerformanceOptimizations class with a replicator instance and performance settings.
*   `calculate_adaptive_batch_size(self, table_name: str, config: Dict) -> int`: Calculates an optimal batch size based on table characteristics and historical performance.
*   `should_use_full_refresh(self, table_name: str, config: Dict) -> bool`: Determines if a full refresh is recommended over incremental copy based on time gaps and historical performance.
*   `_get_expected_rate_for_category(self, performance_category: str) -> int`: Returns the expected processing rate (records/second) for a given performance category.

### Optimization Methods
*   `_apply_bulk_optimizations(self)`: Applies MySQL session-level optimizations for bulk operations to improve performance.
*   `_track_performance_optimized(self, table_name: str, duration: float, memory_mb: float, rows_processed: int)`: Tracks and logs detailed performance metrics for copy operations.

### Bulk Operation Methods
*   `_copy_incremental_bulk(self, table_name: str, config: Dict, batch_size: int) -> Tuple[bool, int]`: Performs an optimized incremental copy using bulk operations.
*   `_process_incremental_batches_bulk(self, table_name: str, primary_column: str, last_processed: Any, batch_size: int, total_records: int) -> Tuple[bool, int]`: Processes incremental data in batches using bulk operations for performance.
*   `_execute_bulk_operation(self, table_name: str, columns: List[str], rows: List, operation_type: str = 'insert') -> int`: Handles unified bulk insert, upsert, or replace operations to the target database.

## SimpleMySQLReplicator Class Methods

### Initialization and Configuration
*   `__init__(self, settings: Optional[Settings] = None, tables_config_path: Optional[str] = None)`: Initializes the replicator with settings, loads table configurations, and sets up database connections.
*   `_validate_tracking_tables_exist(self) -> bool`: Validates the existence and structure of MySQL tracking tables in the replication database.
*   `_load_configuration(self) -> Dict`: Loads table configurations from the `tables.yml` file.

### Tracking and Status Methods
*   `_update_copy_status(self, table_name: str, rows_copied: int, copy_status: str = 'success', last_primary_value: Optional[str] = None, primary_column_name: Optional[str] = None) -> bool`: Updates the `etl_copy_status` tracking table in the replication database.
*   `_get_last_copy_time(self, table_name: str) -> Optional[datetime]`: Retrieves the timestamp of the last successful copy for a given table.
*   `_get_last_copy_primary_value(self, table_name: str) -> Optional[str]`: Retrieves the last processed primary column value from the tracking table for incremental loading.
*   `_get_max_primary_value_from_copied_data(self, table_name: str, primary_column: str) -> Optional[str]`: Gets the maximum primary column value from the data already copied to the replication database.

### Connection Management
*   `_create_connection_managers(self, table_name: str = None, config: Dict = None)`: Creates ConnectionManager instances for source and target databases with optimized configurations.
*   `_get_connection_manager_config(self, table_name: str, config: Dict) -> Dict`: Determines optimized ConnectionManager configuration based on table characteristics.

### Strategy and Method Selection
*   `get_copy_method(self, table_name: str) -> str`: Determines the appropriate copy method ('tiny', 'small', 'medium', 'large') based on the table's performance category.
*   `get_extraction_strategy(self, table_name: str) -> str`: Retrieves the data extraction strategy ('full_table', 'incremental', 'incremental_chunked') from table configuration.
*   `_validate_extraction_strategy(self, strategy: str) -> bool`: Validates if the provided extraction strategy is supported.

### Core Copy Operations
*   `copy_table(self, table_name: str, force_full: bool = False) -> Tuple[bool, Dict]`: Orchestrates the copying of a single table, including strategy selection, execution, and performance tracking.
*   `_execute_table_copy(self, table_name: str, config: Dict, extraction_strategy: str) -> Tuple[bool, int]`: Unified method to execute table copy, selecting between full or incremental based on strategy.
*   `_copy_full_table_unified(self, table_name: str, batch_size: int, config: Dict) -> Tuple[bool, int]`: Unified method for performing a full table copy, handling all performance categories directly.
*   `_copy_incremental_unified(self, table_name: str, config: Dict, batch_size: int) -> Tuple[bool, int]`: Unified method for performing an incremental table copy, handling all performance categories.

### Incremental Processing Methods
*   `_get_primary_incremental_column(self, config: Dict) -> Optional[str]`: Extracts the primary incremental column name from the table configuration.
*   `_log_incremental_strategy(self, table_name: str, primary_column: Optional[str], incremental_columns: List[str])`: Logs the specific incremental strategy being used for a table.
*   `_get_incremental_metadata(self, table_name: str, incremental_columns: List[str]) -> Dict`: Unified method to retrieve metadata required for incremental loading, including last processed value and new record count.
*   `_copy_incremental_records(self, table_name: str, incremental_columns: List[str], last_processed: Any, batch_size: int) -> Tuple[bool, int]`: Unified method to copy incremental records, handling single and multi-column strategies.

### Data Management Methods
*   `_recreate_table_structure(self, table_name: str) -> bool`: Drops and recreates the table structure in the target replication database based on the source schema.
*   `_clean_row_data(self, row, columns, table_name: str)`: Cleans and validates row data to prevent type conversion errors during insertion.
*   `_get_table_total_count(self, table_name: str) -> int`: Retrieves the total number of records in a table from the source database.

### Batch Copy Operations
*   `copy_all_tables(self, table_filter: Optional[List[str]] = None) -> Dict[str, bool]`: Copies all configured tables or a filtered subset.
*   `copy_tables_by_processing_priority(self, max_priority: int = 10) -> Dict[str, bool]`: Copies tables based on their defined processing priority.
*   `copy_tables_by_performance_category(self, category: str) -> Dict[str, bool]`: Copies tables belonging to a specific performance category.

### SQL Generation and Reporting
*   `_build_mysql_upsert_sql(self, table_name: str, column_names: List[str]) -> str`: Builds a MySQL UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) SQL statement.
*   `get_performance_report(self) -> str`: Generates a comprehensive report of copy performance metrics.
*   `get_schema_analyzer_summary(self) -> str`: Generates a summary of schema analyzer configuration values used by the replicator.

## Method Categories Summary

### PerformanceOptimizations (9 methods)
- **Core Performance**: 3 methods for batch sizing and refresh decisions
- **Optimization**: 2 methods for session optimizations and performance tracking  
- **Bulk Operations**: 4 methods for high-performance data copying

### SimpleMySQLReplicator (28 methods)
- **Initialization & Configuration**: 3 methods for setup and validation
- **Tracking & Status**: 4 methods for monitoring copy progress
- **Connection Management**: 2 methods for database connection handling
- **Strategy Selection**: 3 methods for determining copy approaches
- **Core Copy Operations**: 4 methods for main copy functionality
- **Incremental Processing**: 4 methods for incremental data handling
- **Data Management**: 3 methods for data cleaning and structure management
- **Batch Operations**: 3 methods for multi-table copying
- **SQL & Reporting**: 3 methods for SQL generation and reporting

**Total: 37 methods** across both classes, providing comprehensive MySQL replication functionality with performance optimization and unified method handling.
