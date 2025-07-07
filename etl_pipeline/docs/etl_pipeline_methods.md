# ETL Pipeline Methods Reference

This document provides a comprehensive list of all methods across the ETL pipeline components, organized by module and class. Each method includes a brief description of its purpose and functionality.

## Table of Contents

1. [Logging Module](#logging-module)
2. [Configuration Module](#configuration-module)
3. [Core Connections](#core-connections)
4. [Core Postgres Schema](#core-postgres-schema)
5. [Core Simple MySQL Replicator](#core-simple-mysql-replicator)
6. [Loaders - Postgres Loader](#loaders---postgres-loader)
7. [Monitoring - Unified Metrics](#monitoring---unified-metrics)
8. [Orchestration - Pipeline Orchestrator](#orchestration---pipeline-orchestrator)
9. [Orchestration - Priority Processor](#orchestration---priority-processor)
10. [Orchestration - Table Processor](#orchestration---table-processor)
11. [Scripts - Pipeline Management](#scripts---pipeline-management)

---

## Logging Module

### `etl_pipeline/config/logging.py`

#### Functions
- **`setup_logging(log_level, log_file, log_dir, format_type)`** - Set up logging configuration for the ETL pipeline with file/console handlers
- **`configure_sql_logging(enabled, level)`** - Configure SQL query logging for database operations
- **`get_logger(name)`** - Get a logger instance with the specified name
- **`init_default_logger()`** - Initialize default logger with basic configuration

#### Class: `ETLLogger`
- **`__init__(name, log_level)`** - Initialize ETL Logger with name and optional log level
- **`info(message, **kwargs)`** - Log info message
- **`debug(message, **kwargs)`** - Log debug message
- **`warning(message, **kwargs)`** - Log warning message
- **`error(message, **kwargs)`** - Log error message
- **`critical(message, **kwargs)`** - Log critical message
- **`log_sql_query(query, params)`** - Log SQL query for debugging purposes
- **`log_etl_start(table_name, operation)`** - Log the start of an ETL operation
- **`log_etl_complete(table_name, operation, records_count)`** - Log the successful completion of an ETL operation
- **`log_etl_error(table_name, operation, error)`** - Log ETL operation errors
- **`log_validation_result(table_name, passed, issues_count)`** - Log data validation results
- **`log_performance(operation, duration, records_count)`** - Log performance metrics for operations

---

## Configuration Module

### `etl_pipeline/config/config_reader.py`

#### Class: `ConfigReader`
- **`__init__(config_path)`** - Initialize config reader with path to tables.yml
- **`_load_configuration()`** - Load configuration from tables.yml file
- **`reload_configuration()`** - Reload configuration from file
- **`get_table_config(table_name)`** - Get configuration for a specific table

- **`get_tables_by_importance(importance_level)`** - Get tables by importance level
- **`get_tables_by_strategy(strategy)`** - Get tables by extraction strategy
- **`get_large_tables(size_threshold_mb)`** - Get tables larger than specified threshold
- **`get_monitored_tables()`** - Get list of tables configured for monitoring
- **`get_table_dependencies(table_name)`** - Get table dependencies from configuration
- **`get_configuration_summary()`** - Get summary of all table configurations
- **`validate_configuration()`** - Validate configuration file structure and content
- **`get_configuration_path()`** - Get the path to the configuration file
- **`get_last_loaded()`** - Get timestamp of last configuration load

### `etl_pipeline/config/providers.py`

#### Class: `ConfigProvider` (Abstract)
- **`get_config(config_type)`** - Abstract method to get configuration by type

#### Class: `FileConfigProvider`
- **`__init__(config_dir)`** - Initialize file-based configuration provider
- **`get_config(config_type)`** - Load configuration from files or environment
- **`_load_yaml_config(name)`** - Load YAML configuration file

#### Class: `DictConfigProvider`
- **`__init__(**configs)`** - Initialize with configuration dictionaries
- **`get_config(config_type)`** - Get configuration by type

### `etl_pipeline/config/settings.py`

#### Class: `Settings`
- **`__init__(environment, provider)`** - Initialize settings with environment and provider
- **`_detect_environment()`** - Detect environment from environment variables
- **`get_database_config(db_type, schema)`** - Get database configuration for specified type and schema
- **`get_source_connection_config()`** - Get OpenDental source connection configuration
- **`get_replication_connection_config()`** - Get MySQL replication connection configuration
- **`get_analytics_connection_config(schema)`** - Get PostgreSQL analytics connection configuration
- **`get_analytics_raw_connection_config()`** - Get PostgreSQL analytics raw schema connection
- **`get_analytics_staging_connection_config()`** - Get PostgreSQL analytics staging schema connection
- **`get_analytics_intermediate_connection_config()`** - Get PostgreSQL analytics intermediate schema connection
- **`get_analytics_marts_connection_config()`** - Get PostgreSQL analytics marts schema connection
- **`_get_base_config(db_type)`** - Get base configuration from environment variables
- **`_add_connection_defaults(config, db_type)`** - Add default connection parameters based on database type
- **`validate_configs()`** - Validate that all required configurations are present
- **`get_pipeline_setting(key, default)`** - Get pipeline setting with optional default
- **`get_table_config(table_name)`** - Get table configuration from tables config
- **`_get_default_table_config()`** - Get default table configuration template
- **`list_tables()`** - List all configured tables
- **`get_tables_by_importance(importance_level)`** - Get tables by importance level
- **`should_use_incremental(table_name)`** - Check if table should use incremental loading

#### Functions
- **`get_settings()`** - Get global settings instance
- **`reset_settings()`** - Reset global settings to None
- **`set_settings(settings)`** - Set global settings instance
- **`create_settings(environment, config_dir, **test_configs)`** - Create settings with optional test configs
- **`create_test_settings(pipeline_config, tables_config, env_vars)`** - Create test settings

---

## Core Connections

### `etl_pipeline/core/connections.py`

#### Class: `ConnectionFactory`
- **`validate_connection_params(params, connection_type)`** - Validate required connection parameters
- **`_build_mysql_connection_string(config)`** - Build MySQL connection string from configuration
- **`_build_postgres_connection_string(config)`** - Build PostgreSQL connection string from configuration
- **`create_mysql_engine(host, port, database, user, password, **kwargs)`** - Create MySQL engine with proper configuration
- **`create_postgres_engine(host, port, database, user, password, schema, **kwargs)`** - Create PostgreSQL engine with configuration
- **`get_opendental_source_connection()`** - Get OpenDental source database connection (production)
- **`get_mysql_replication_connection()`** - Get MySQL replication database connection (production)
- **`get_postgres_analytics_connection()`** - Get PostgreSQL analytics database connection (production)
- **`get_opendental_analytics_raw_connection()`** - Get PostgreSQL analytics raw schema connection (production)
- **`get_opendental_analytics_staging_connection()`** - Get PostgreSQL analytics staging schema connection (production)
- **`get_opendental_analytics_intermediate_connection()`** - Get PostgreSQL analytics intermediate schema connection (production)
- **`get_opendental_analytics_marts_connection()`** - Get PostgreSQL analytics marts schema connection (production)
- **`get_opendental_source_test_connection()`** - Get OpenDental source database connection (test)
- **`get_mysql_replication_test_connection()`** - Get MySQL replication database connection (test)
- **`get_postgres_analytics_test_connection()`** - Get PostgreSQL analytics database connection (test)
- **`get_opendental_analytics_raw_test_connection()`** - Get PostgreSQL analytics raw schema connection (test)
- **`get_opendental_analytics_staging_test_connection()`** - Get PostgreSQL analytics staging schema connection (test)
- **`get_opendental_analytics_intermediate_test_connection()`** - Get PostgreSQL analytics intermediate schema connection (test)
- **`get_opendental_analytics_marts_test_connection()`** - Get PostgreSQL analytics marts schema connection (test)

#### Class: `ConnectionManager`
- **`__init__(engine, max_retries, retry_delay)`** - Initialize connection manager with retry settings
- **`get_connection()`** - Get database connection from pool
- **`close_connection()`** - Close database connection
- **`execute_with_retry(query, params, rate_limit)`** - Execute query with retry logic and rate limiting
- **`__enter__()`** - Context manager entry
- **`__exit__(exc_type, exc_val, exc_tb)`** - Context manager exit

#### Functions
- **`create_connection_manager(engine, max_retries, retry_delay)`** - Create connection manager instance

---

## Core Postgres Schema

### `etl_pipeline/core/postgres_schema.py`

#### Class: `PostgresSchema`
- **`__init__(postgres_schema, settings)`** - Initialize PostgreSQL schema adaptation
- **`get_table_schema_from_mysql(table_name)`** - Get MySQL table schema directly from replication database
- **`_calculate_schema_hash(create_statement)`** - Calculate hash of create statement for change detection
- **`_analyze_column_data(table_name, column_name, mysql_type)`** - Analyze actual column data to determine best PostgreSQL type
- **`_convert_mysql_type_standard(mysql_type)`** - Convert MySQL type to PostgreSQL type using standard mapping
- **`_convert_mysql_type(mysql_type, table_name, column_name)`** - Convert MySQL type to PostgreSQL type with intelligent analysis
- **`adapt_schema(table_name, mysql_schema)`** - Adapt MySQL schema to PostgreSQL format
- **`_convert_mysql_to_postgres_intelligent(mysql_create, table_name)`** - Convert MySQL CREATE statement to PostgreSQL with intelligent type mapping
- **`create_postgres_table(table_name, mysql_schema)`** - Create PostgreSQL table from MySQL schema
- **`verify_schema(table_name, mysql_schema)`** - Verify PostgreSQL table schema matches MySQL schema

---

## Core Simple MySQL Replicator

### `etl_pipeline/core/simple_mysql_replicator.py`

#### Class: `SimpleMySQLReplicator`
- **`__init__(settings, tables_config_path)`** - Initialize simple replicator with settings and config path
- **`_load_configuration()`** - Load table configuration from tables.yml
- **`get_copy_strategy(table_name)`** - Determine copy strategy based on table size (small/medium/large)
- **`get_extraction_strategy(table_name)`** - Get extraction strategy from table configuration
- **`copy_table(table_name, force_full)`** - Copy a single table from source to target
- **`_copy_incremental_table(table_name, config)`** - Copy table using incremental strategy
- **`_get_last_processed_value(table_name, incremental_column)`** - Get last processed value for incremental loading
- **`_get_new_records_count(table_name, incremental_column, last_processed)`** - Get count of new records since last processed
- **`_copy_new_records(table_name, incremental_column, last_processed, batch_size)`** - Copy new records in batches
- **`copy_all_tables(table_filter)`** - Copy all tables with optional filter
- **`copy_tables_by_importance(importance_level)`** - Copy tables by importance level

---

## Loaders - Postgres Loader

### `etl_pipeline/loaders/postgres_loader.py`

#### Class: `PostgresLoader`
- **`__init__(tables_config_path, use_test_environment)`** - Initialize PostgreSQL loader with config path and environment
- **`_load_configuration()`** - Load table configuration from tables.yml
- **`get_table_config(table_name)`** - Get configuration for specific table
- **`load_table(table_name, force_full)`** - Load table from MySQL replication to PostgreSQL analytics
- **`load_table_chunked(table_name, force_full, chunk_size)`** - Load large table in chunks for memory efficiency
- **`verify_load(table_name)`** - Verify that table was loaded correctly by comparing row counts
- **`_ensure_postgres_table(table_name, mysql_schema)`** - Ensure PostgreSQL table exists with correct schema
- **`_build_load_query(table_name, incremental_columns, force_full)`** - Build SQL query for loading data
- **`_build_count_query(table_name, incremental_columns, force_full)`** - Build SQL query for counting records
- **`_get_last_load(table_name)`** - Get timestamp of last successful load for table
- **`_convert_row_data_types(table_name, row_data)`** - Convert row data types from MySQL to PostgreSQL

---

## Monitoring - Unified Metrics

### `etl_pipeline/monitoring/unified_metrics.py`

#### Class: `UnifiedMetricsCollector`
- **`__init__(analytics_engine, enable_persistence, use_test_connections)`** - Initialize unified metrics collector
- **`_get_analytics_connection()`** - Get appropriate analytics connection based on environment
- **`reset_metrics()`** - Reset metrics to initial state
- **`start_pipeline()`** - Start pipeline timing and metrics collection
- **`end_pipeline()`** - End pipeline timing and return final metrics
- **`record_table_processed(table_name, rows_processed, processing_time, success, error)`** - Record metrics for single table
- **`record_error(error, table_name)`** - Record error in metrics
- **`get_pipeline_status(table)`** - Get current pipeline status and metrics
- **`get_table_status(table_name)`** - Get status for specific table
- **`get_pipeline_stats()`** - Get comprehensive pipeline statistics
- **`save_metrics()`** - Save metrics to database for persistence
- **`cleanup_old_metrics(retention_days)`** - Clean up old metrics from database
- **`_initialize_metrics_table()`** - Initialize metrics table in database

#### Functions
- **`create_metrics_collector(use_test_connections, enable_persistence)`** - Create metrics collector with specified settings
- **`create_production_metrics_collector(enable_persistence)`** - Create production metrics collector
- **`create_test_metrics_collector(enable_persistence)`** - Create test metrics collector

---

## Orchestration - Pipeline Orchestrator

### `etl_pipeline/orchestration/pipeline_orchestrator.py`

#### Class: `PipelineOrchestrator`
- **`__init__(config_path, environment)`** - Initialize pipeline orchestrator with config and environment
- **`initialize_connections()`** - Initialize all database connections and components
- **`cleanup()`** - Clean up all resources and connections
- **`run_pipeline_for_table(table_name, force_full)`** - Run complete ETL pipeline for single table
- **`process_tables_by_priority(importance_levels, max_workers, force_full)`** - Process tables by priority with parallelization
- **`__enter__()`** - Context manager entry
- **`__exit__(exc_type, exc_val, exc_tb)`** - Context manager exit with cleanup

---

## Orchestration - Priority Processor

### `etl_pipeline/orchestration/priority_processor.py`

#### Class: `PriorityProcessor`
- **`__init__(config_reader, settings)`** - Initialize priority processor with config reader and settings
- **`process_by_priority(importance_levels, max_workers, force_full)`** - Process tables by priority with intelligent parallelization
- **`_process_parallel(tables, max_workers, force_full)`** - Process tables in parallel using ThreadPoolExecutor
- **`_process_sequential(tables, force_full)`** - Process tables sequentially
- **`_process_single_table(table_name, force_full)`** - Process single table using TableProcessor

---

## Orchestration - Table Processor

### `etl_pipeline/orchestration/table_processor.py`

#### Class: `TableProcessor`
- **`__init__(config_reader, config_path, environment)`** - Initialize table processor with config and environment
- **`initialize_connections(source_engine, replication_engine, analytics_engine)`** - Initialize database connections
- **`cleanup()`** - Clean up database connections
- **`__enter__()`** - Context manager entry
- **`__exit__(exc_type, exc_val, exc_tb)`** - Context manager exit with cleanup
- **`_connections_available()`** - Check if all required connections are available
- **`process_table(table_name, force_full)`** - Process single table through complete ETL pipeline
- **`_extract_to_replication(table_name, force_full)`** - Extract data from source to replication database
- **`_load_to_analytics(table_name, force_full)`** - Load data from replication to analytics database

---

## Scripts - Pipeline Management

### `etl_pipeline/scripts/analyze_opendental_schema.py`

**STATUS**: ✅ ACTIVE - Schema Analysis Tool (Outside Nightly ETL Scope)

This script performs comprehensive schema analysis of the OpenDental database and generates the `tables.yml` configuration file. It's used for initial setup and configuration updates, not part of the nightly ETL job. Uses modern connection handling and direct database analysis.

#### Class: `OpenDentalSchemaAnalyzer`
- **`__init__()`** - Initialize analyzer with source database connection using modern connection handling
- **`discover_all_tables()`** - Discover all tables in the source database using SQLAlchemy inspector
- **`get_table_schema(table_name)`** - Get detailed schema information for a table (columns, keys, indexes)
- **`get_table_size_info(table_name)`** - Get table size and row count information from database
- **`discover_dbt_models()`** - Discover dbt models in the project (staging, mart, intermediate)
- **`determine_table_importance(table_name, schema_info, size_info)`** - Determine table importance based on schema and size analysis
- **`determine_extraction_strategy(table_name, schema_info, size_info)`** - Determine optimal extraction strategy for a table
- **`find_incremental_columns(table_name, schema_info)`** - Find suitable incremental columns for a table
- **`generate_complete_configuration(output_dir)`** - Generate complete configuration for all tables
- **`analyze_complete_schema(output_dir)`** - Perform complete schema analysis and generate all outputs
- **`_generate_detailed_analysis_report(config)`** - Generate detailed analysis report with all metadata
- **`_generate_summary_report(config, output_path, analysis_path)`** - Generate and display summary report

#### Functions
- **`main()`** - Main function to generate complete schema analysis and configuration

**Key Features:**
- **Modern Connection Handling**: Uses `ConnectionFactory.get_opendental_source_connection()` for explicit production connections
- **Direct Database Analysis**: Uses SQLAlchemy inspector for real-time schema and size information
- **dbt Model Discovery**: Automatically discovers staging, mart, and intermediate models
- **Intelligent Configuration**: Determines table importance, extraction strategies, and monitoring settings
- **Comprehensive Reporting**: Generates detailed analysis with recommendations

**Outputs:**
- `etl_pipeline/config/tables.yml` (pipeline configuration)
- `etl_pipeline/logs/schema_analysis_YYYYMMDD_HHMMSS.json` (detailed analysis)
- Console summary report

### `etl_pipeline/scripts/setup_test_databases.py`

**STATUS**: ✅ ACTIVE - Test Environment Setup (Outside Nightly ETL Scope)

This script sets up test databases for integration testing. It creates test databases, tables, and loads sample data for the ETL pipeline testing environment.

#### Functions
- **`load_test_environment()`** - Load environment variables from .env file for testing
- **`validate_test_environment()`** - Validate that we're running in a test environment
- **`validate_database_names()`** - Validate that all database names contain 'test' to prevent production accidents
- **`confirm_database_creation()`** - Ask for explicit confirmation before creating/modifying databases
- **`ping_mysql_server(host, port)`** - Ping MySQL server to ensure it's running and accessible
- **`setup_postgresql_test_database()`** - Set up PostgreSQL test analytics database using ConnectionFactory
- **`setup_mysql_test_database(database_type)`** - Set up MySQL test database using ConnectionFactory
- **`main()`** - Main function to set up all test databases

**Safety Features:**
- Only runs in test environments (ETL_ENVIRONMENT=test)
- Requires explicit confirmation for database creation
- Validates all database names contain 'test' to prevent production accidents
- Uses test-specific environment variables (TEST_*)

### `etl_pipeline/scripts/update_pipeline_config.py`

**STATUS**: ✅ ACTIVE - Configuration Management Tool (Outside Nightly ETL Scope)

This script provides a programmatic way to add, modify, or validate pipeline configuration. It's used for configuration management and updates, not part of the nightly ETL job.

#### Class: `PipelineConfigManager`
- **`__init__(config_path)`** - Initialize with path to pipeline configuration file
- **`_load_config()`** - Load the current pipeline configuration
- **`_save_config()`** - Save the current configuration to file
- **`add_setting(section, key, value)`** - Add or update a setting in the configuration
- **`add_nested_setting(path, value)`** - Add or update a nested setting using dot notation
- **`add_connection_config(db_type, **kwargs)`** - Add or update database connection configuration
- **`validate_connection_config(db_type)`** - Validate connection configuration using modern ConnectionFactory
- **`validate_all_connections()`** - Validate all connection configurations
- **`add_stage_config(stage, **kwargs)`** - Add or update pipeline stage configuration
- **`add_alert_config(alert_type, **kwargs)`** - Add or update alerting configuration
- **`validate_config()`** - Validate the current configuration structure
- **`validate_test_environment()`** - Validate test environment configuration
- **`show_config(section)`** - Display the current configuration

#### Functions
- **`validate_configuration(config)`** - Validate the configuration structure
- **`save_configuration(config, output_path)`** - Save configuration to file
- **`main()`** - Main function to update pipeline configuration

**Usage:**
- Add/modify pipeline settings
- Update database connection configurations
- Validate configuration structure
- Manage pipeline stages and alerts

---

## Summary

This ETL pipeline consists of **10 major components** with **over 100 methods** total, plus **3 management scripts**:

- **Logging**: 15 methods for comprehensive logging and monitoring
- **Configuration**: 22 methods for settings and configuration management (legacy compatibility methods removed)
- **Core Connections**: 25 methods for database connection management
- **Core Postgres Schema**: 10 methods for schema conversion and adaptation
- **Core Simple MySQL Replicator**: 10 methods for MySQL-to-MySQL replication
- **Loaders**: 10 methods for PostgreSQL data loading
- **Monitoring**: 15 methods for metrics collection and persistence
- **Orchestration**: 15 methods for pipeline coordination and table processing
- **Scripts**: 3 management scripts for setup and configuration (outside nightly ETL scope)
  - **Schema Analysis**: 12 methods for comprehensive database analysis and configuration generation
  - **Test Setup**: 8 functions for test environment management
  - **Config Management**: 15 methods for configuration validation and updates

## Pipeline Scope

### ✅ **Nightly ETL Job Components** (Core Pipeline)
The following components are part of the nightly ETL job execution:

- **PipelineOrchestrator**: Main orchestration and coordination
- **TableProcessor**: Core ETL engine for individual table processing
- **PriorityProcessor**: Batch processing with priority-based execution
- **SimpleMySQLReplicator**: MySQL-to-MySQL data extraction
- **PostgresLoader**: MySQL-to-PostgreSQL data loading
- **Settings**: Configuration management
- **ConfigReader**: Static configuration access
- **ConnectionFactory**: Database connection management
- **PostgresSchema**: Schema conversion
- **UnifiedMetricsCollector**: Metrics collection and monitoring

### 🔧 **Management Scripts** (Outside Nightly ETL Scope)
The following scripts are used for setup, configuration, and maintenance but are **NOT part of the nightly ETL job**:

- **`analyze_opendental_schema.py`**: Schema analysis and `tables.yml` generation (12 methods)
  - Modern connection handling with explicit production connections
  - Direct database analysis using SQLAlchemy inspector
  - dbt model discovery and intelligent configuration generation
- **`setup_test_databases.py`**: Test environment setup and database creation (8 functions)
- **`update_pipeline_config.py`**: Configuration management and updates (15 methods)

## Architecture

The pipeline follows a **three-stage architecture**:
1. **Extract**: MySQL source → MySQL replication (SimpleMySQLReplicator)
2. **Load**: MySQL replication → PostgreSQL analytics (PostgresLoader)
3. **Transform**: PostgreSQL analytics → dbt models (external to this pipeline)

All components use **static configuration** from `tables.yml` and support both **production and test environments** with explicit connection separation.

**✅ LEGACY CODE REMOVED**: All compatibility methods for the old SchemaDiscovery interface have been removed. The pipeline now uses only modern, static configuration approaches. 