# ETL Pipeline Test Fixtures Reference

This document provides a comprehensive reference for all test fixtures available in the ETL pipeline test suite. These fixtures follow the new architectural patterns documented in `etl_pipeline/docs/connection_architecture.md`.

## Table of Contents

- [Configuration Fixtures](#configuration-fixtures)
- [Environment Fixtures](#environment-fixtures)
- [Connection Fixtures](#connection-fixtures)
- [Test Data Fixtures](#test-data-fixtures)
- [Integration Fixtures](#integration-fixtures)
- [Test Data Manager](#test-data-manager)
- [Test Data Definitions](#test-data-definitions)
- [Orchestrator Fixtures](#orchestrator-fixtures)
- [Transformer Fixtures](#transformer-fixtures)
- [Loader Fixtures](#loader-fixtures)
- [Additional Fixture Files](#additional-fixture-files)
- [Usage Examples](#usage-examples)

## Configuration Fixtures

**File:** `etl_pipeline/tests/fixtures/config_fixtures.py`

### Core Configuration Fixtures

- **`database_types()`** - Database type enums for testing
- **`postgres_schemas()`** - PostgreSQL schema enums for testing
- **`test_pipeline_config()`** - Standard test pipeline configuration
- **`test_tables_config()`** - Standard test tables configuration
- **`complete_config_environment()`** - Complete configuration environment with all environment variables
- **`valid_pipeline_config()`** - Valid production pipeline configuration
- **`minimal_pipeline_config()`** - Minimal pipeline configuration for basic testing
- **`invalid_pipeline_config()`** - Invalid pipeline configuration for error testing
- **`complete_tables_config()`** - Complete tables configuration with all table definitions
- **`mock_settings_environment()`** - Mock settings environment for testing
- **`test_settings_with_enums()`** - Test settings using enums for database types and schemas
- **`database_config_test_cases()`** - Test cases for database configuration validation
- **`schema_specific_configs()`** - Schema-specific configuration test cases
- **`connection_config_validation_cases()`** - Connection validation test cases

## Environment Fixtures

**File:** `etl_pipeline/tests/fixtures/env_fixtures.py`

### Environment Management Fixtures

- **`reset_global_settings()`** - Reset global settings before and after each test (autouse)
- **`test_env_vars()`** - Standard test environment variables for integration tests
- **`production_env_vars()`** - Production environment variables for integration tests
- **`test_settings()`** - Create isolated test settings using new configuration system
- **`mock_env_test_settings()`** - Create test settings with mocked environment variables
- **`production_settings()`** - Create production settings for integration tests
- **`setup_test_environment()`** - Set up test environment with proper isolation (autouse)
- **`test_environment()`** - Session-scoped test environment setup
- **`environment_detection_test_cases()`** - Test cases for environment detection logic
- **`database_environment_mappings()`** - Database environment variable mappings
- **`test_environment_prefixes()`** - Environment prefixes for different database types
- **`schema_environment_variables()`** - Schema-specific environment variables

## Connection Fixtures

**File:** `etl_pipeline/tests/fixtures/connection_fixtures.py`

### Database Connection Fixtures

- **`database_types()`** - Database type enums for testing
- **`postgres_schemas()`** - PostgreSQL schema enums for testing
- **`mock_database_engines()`** - Session-scoped mock database engines
- **`mock_source_engine()`** - Mock source database engine
- **`mock_replication_engine()`** - Mock replication database engine
- **`mock_analytics_engine()`** - Mock analytics database engine
- **`mock_connection_factory()`** - Mock connection factory for testing
- **`mock_connection_factory_methods()`** - Mock ConnectionFactory methods following new architecture
- **`mock_postgres_connection()`** - Mock PostgreSQL connection for testing
- **`mock_mysql_connection()`** - Mock MySQL connection for testing
- **`mock_engine_with_connection()`** - Mock engine that returns a mock connection
- **`mock_connection_pool()`** - Mock connection pool for testing
- **`mock_database_urls()`** - Mock database URLs for testing
- **`mock_connection_config()`** - Mock connection configuration for testing
- **`mock_connection_error()`** - Mock connection error for testing
- **`mock_connection_timeout()`** - Mock connection timeout for testing
- **`connection_factory_test_cases()`** - Test cases for ConnectionFactory methods
- **`mock_connection_manager()`** - Mock ConnectionManager for testing
- **`connection_string_test_cases()`** - Test cases for connection string building
- **`pool_config_test_cases()`** - Test cases for connection pool configuration

## Test Data Fixtures

**File:** `etl_pipeline/tests/fixtures/test_data_fixtures.py`

### Standardized Test Data Fixtures

- **`standard_patient_test_data()`** - Standardized patient test data for all integration tests
- **`incremental_patient_test_data()`** - Test data for incremental loading tests
- **`partial_patient_test_data()`** - Test data for partial loading tests (2 patients instead of 3)
- **`etl_tracking_test_data()`** - Standardized ETL tracking test data
- **`invalid_schema_test_data()`** - Test data for invalid schema tests
- **`composite_pk_test_data()`** - Test data for composite primary key tests
- **`large_table_test_data()`** - Test data for large table tests (no primary key)
- **`simple_test_table_data()`** - Simple test data for basic table operations
- **`patient_with_all_fields_test_data()`** - Patient test data with all fields populated for comprehensive testing

## Integration Fixtures

**File:** `etl_pipeline/tests/fixtures/integration_fixtures.py`

### Integration Test Fixtures

- **`test_data_manager()`** - Provides a test data manager for integration tests
- **`populated_test_databases()`** - Provides databases with comprehensive test data
- **`test_database_engines()`** - Provides test database engines using explicit test connection methods
- **`test_source_engine()`** - Provides test source database engine using explicit test connection method
- **`test_replication_engine()`** - Provides test replication database engine using explicit test connection method
- **`test_analytics_engine()`** - Provides test analytics database engine using explicit test connection method
- **`test_raw_engine()`** - Provides test raw schema engine using explicit test connection method
- **`test_staging_engine()`** - Provides test staging schema engine using explicit test connection method
- **`test_intermediate_engine()`** - Provides test intermediate schema engine using explicit test connection method
- **`test_marts_engine()`** - Provides test marts schema engine using explicit test connection method
- **`setup_patient_table()`** - Sets up patient table with test data
- **`setup_etl_tracking()`** - Sets up ETL tracking table

## Test Data Manager

**File:** `etl_pipeline/tests/fixtures/test_data_manager.py`

### Test Data Management Class

- **`IntegrationTestDataManager`** - Manages test data for integration tests using standardized data and new architecture

### Key Methods:

- `setup_patient_data()` - Set up standardized patient test data in specified databases
- `setup_appointment_data()` - Set up standardized appointment test data in specified databases
- `cleanup_patient_data()` - Clean up patient test data from specified databases
- `cleanup_appointment_data()` - Clean up appointment test data from specified databases
- `cleanup_all_test_data()` - Clean up all test data from all databases
- `get_patient_count()` - Get patient count from specified database
- `get_appointment_count()` - Get appointment count from specified database
- `dispose()` - Dispose of all database connections

## Test Data Definitions

**File:** `etl_pipeline/tests/fixtures/test_data_definitions.py`

### Standardized Test Data Functions

- **`get_test_patient_data()`** - Get standardized patient test data
- **`get_test_appointment_data()`** - Get standardized appointment test data
- **`get_test_data_for_table()`** - Get standardized test data for any table
- **`STANDARD_TEST_PATIENTS`** - Standard test patients that match the real OpenDental schema
- **`STANDARD_TEST_APPOINTMENTS`** - Standard test appointments that match the real OpenDental schema

## Orchestrator Fixtures

**File:** `etl_pipeline/tests/fixtures/orchestrator_fixtures.py`

### Pipeline Orchestration Fixtures

- **`mock_components()`** - Mock pipeline components for orchestrator testing
- **`orchestrator()`** - Pipeline orchestrator instance with mocked components
- **`mock_orchestrator_config()`** - Mock orchestrator configuration for testing
- **`mock_table_processing_result()`** - Mock table processing result for testing
- **`mock_priority_processing_result()`** - Mock priority processing result for testing
- **`mock_priority_processing_result_with_failures()`** - Mock priority processing result with failures for testing
- **`mock_schema_discovery_result()`** - Mock schema discovery result for testing
- **`mock_workflow_steps()`** - Mock workflow steps for orchestrator testing
- **`mock_workflow_execution()`** - Mock workflow execution for testing
- **`mock_workflow_execution_with_errors()`** - Mock workflow execution with errors for testing
- **`mock_orchestrator_stats()`** - Mock orchestrator statistics for testing
- **`mock_dependency_graph()`** - Mock dependency graph for testing
- **`mock_orchestrator_error()`** - Mock orchestrator error for testing
- **`mock_workflow_scheduler()`** - Mock workflow scheduler for testing
- **`mock_workflow_monitor()`** - Mock workflow monitor for testing

## Transformer Fixtures

**File:** `etl_pipeline/tests/fixtures/transformer_fixtures.py`

### Data Transformation Fixtures

- **`mock_table_processor_engines()`** - Mock engines for table processor testing
- **`table_processor_standard_config()`** - Standard configuration for table processor testing
- **`table_processor_large_config()`** - Large table configuration for table processor testing
- **`table_processor_medium_large_config()`** - Medium-large table configuration for table processor testing
- **`sample_transformation_data()`** - Sample data for transformation testing
- **`mock_transformation_rules()`** - Mock transformation rules for testing
- **`mock_transformation_stats()`** - Mock transformation statistics for testing
- **`mock_transformer_config()`** - Mock transformer configuration for testing
- **`sample_validation_rules()`** - Sample validation rules for testing
- **`mock_transformation_error()`** - Mock transformation error for testing
- **`mock_validation_result()`** - Mock validation result for testing
- **`mock_validation_result_with_errors()`** - Mock validation result with errors for testing

## Loader Fixtures

**File:** `etl_pipeline/tests/fixtures/loader_fixtures.py`

### Data Loading Fixtures

- **`postgres_loader()`** - PostgresLoader instance with mocked engines
- **`sample_table_data()`** - Sample table data for testing
- **`sample_mysql_schema()`** - Sample MySQL schema for testing
- **`sample_postgres_schema()`** - Sample PostgreSQL schema for testing
- **`mock_loader_config()`** - Mock loader configuration for testing
- **`mock_loader_stats()`** - Mock loader statistics for testing
- **`sample_create_statement()`** - Sample CREATE TABLE statement for testing
- **`sample_drop_statement()`** - Sample DROP TABLE statement for testing
- **`sample_insert_statement()`** - Sample INSERT statement for testing
- **`sample_select_statement()`** - Sample SELECT statement for testing
- **`mock_loader_error()`** - Mock loader error for testing
- **`mock_validation_error()`** - Mock validation error for testing

## Additional Fixture Files

### Schema Discovery Fixtures (`schema_discovery_fixtures.py`) - DEPRECATED
**Note:** All schema discovery fixtures have been deprecated and removed. The file is kept for debugging purposes only.

- **`schema_discovery_test_settings()`** - DEPRECATED: Create test settings specifically for schema discovery testing
- **`schema_discovery_test_data_manager()`** - DEPRECATED: Manage test data in the real OpenDental database for schema discovery testing
- **`schema_discovery_instance()`** - DEPRECATED: Create real SchemaDiscovery instance with real MySQL connection using new configuration system
- **`mock_schema_discovery()`** - DEPRECATED: Mock SchemaDiscovery instance for unit testing
- **`sample_table_schemas()`** - DEPRECATED: Sample table schemas for testing
- **`sample_table_size_info()`** - DEPRECATED: Sample table size information for testing

**Replacement Guidance:**
- Use `test_settings()` from `env_fixtures.py` instead of `schema_discovery_test_settings()`
- Use `test_data_manager()` from `integration_fixtures.py` instead of `schema_discovery_test_data_manager()`
- Use `test_replication_engine()` from `integration_fixtures.py` instead of `schema_discovery_instance()`
- Use `mock_components()` from `orchestrator_fixtures.py` instead of `mock_schema_discovery()`
- Use `sample_mysql_schema()` from `loader_fixtures.py` instead of `sample_table_schemas()`
- Use `mock_loader_stats()` from `loader_fixtures.py` instead of `sample_table_size_info()`

### Priority Processor Fixtures (`priority_processor_fixtures.py`)
- **`mock_priority_processor()`** - Mock priority processor for testing
- **`mock_priority_config()`** - Mock priority configuration for testing
- **`mock_table_priorities()`** - Mock table priorities for testing
- **`mock_priority_processing_result()`** - Mock priority processing result for testing
- **`mock_priority_processing_result_with_failures()`** - Mock priority processing result with failures for testing
- **`mock_priority_processing_stats()`** - Mock priority processing statistics for testing
- **`mock_priority_processing_error()`** - Mock priority processing error for testing
- **`mock_priority_processing_timeout()`** - Mock priority processing timeout for testing
- **`mock_priority_processing_cancellation()`** - Mock priority processing cancellation for testing
- **`mock_priority_processing_retry()`** - Mock priority processing retry for testing
- **`mock_priority_processing_rollback()`** - Mock priority processing rollback for testing
- **`mock_priority_processing_cleanup()`** - Mock priority processing cleanup for testing

### Replicator Fixtures (`replicator_fixtures.py`)
- **`mock_replicator()`** - Mock replicator for testing
- **`mock_replication_config()`** - Mock replication configuration for testing
- **`mock_replication_stats()`** - Mock replication statistics for testing
- **`mock_replication_error()`** - Mock replication error for testing
- **`mock_replication_timeout()`** - Mock replication timeout for testing
- **`mock_replication_cancellation()`** - Mock replication cancellation for testing
- **`mock_replication_retry()`** - Mock replication retry for testing
- **`mock_replication_rollback()`** - Mock replication rollback for testing
- **`mock_replication_cleanup()`** - Mock replication cleanup for testing
- **`mock_replication_validation()`** - Mock replication validation for testing
- **`mock_replication_monitoring()`** - Mock replication monitoring for testing

### PostgreSQL Schema Fixtures (`postgres_schema_fixtures.py`)
- **`mock_postgres_schema()`** - Mock PostgreSQL schema for testing
- **`mock_schema_adapter()`** - Mock schema adapter for testing
- **`mock_schema_validation()`** - Mock schema validation for testing
- **`mock_schema_migration()`** - Mock schema migration for testing
- **`mock_schema_comparison()`** - Mock schema comparison for testing
- **`mock_schema_creation()`** - Mock schema creation for testing
- **`mock_schema_destruction()`** - Mock schema destruction for testing
- **`mock_schema_backup()`** - Mock schema backup for testing
- **`mock_schema_restore()`** - Mock schema restore for testing
- **`mock_schema_cleanup()`** - Mock schema cleanup for testing
- **`mock_schema_monitoring()`** - Mock schema monitoring for testing

### Metrics Fixtures (`metrics_fixtures.py`)
- **`mock_metrics_collector()`** - Mock metrics collector for testing
- **`mock_metrics_config()`** - Mock metrics configuration for testing
- **`mock_metrics_data()`** - Mock metrics data for testing
- **`mock_metrics_aggregation()`** - Mock metrics aggregation for testing
- **`mock_metrics_reporting()`** - Mock metrics reporting for testing
- **`mock_metrics_storage()`** - Mock metrics storage for testing
- **`mock_metrics_validation()`** - Mock metrics validation for testing
- **`mock_metrics_export()`** - Mock metrics export for testing
- **`mock_metrics_import()`** - Mock metrics import for testing
- **`mock_metrics_cleanup()`** - Mock metrics cleanup for testing
- **`mock_metrics_monitoring()`** - Mock metrics monitoring for testing
- **`mock_metrics_alerting()`** - Mock metrics alerting for testing
- **`mock_metrics_dashboard()`** - Mock metrics dashboard for testing
- **`mock_metrics_api()`** - Mock metrics API for testing

### Logging Fixtures (`logging_fixtures.py`)
- **`mock_logger()`** - Mock logger for testing
- **`mock_log_handler()`** - Mock log handler for testing
- **`mock_log_formatter()`** - Mock log formatter for testing
- **`mock_log_config()`** - Mock log configuration for testing
- **`mock_log_rotation()`** - Mock log rotation for testing
- **`mock_log_compression()`** - Mock log compression for testing
- **`mock_log_cleanup()`** - Mock log cleanup for testing

### Legacy Fixtures (`legacy_fixtures.py`) - DEPRECATED
**Note:** All legacy fixtures have been deprecated and removed. The file is kept for debugging purposes only.

- **`legacy_settings()`** - DEPRECATED: Legacy settings for backward compatibility testing
- **`legacy_connection_factory()`** - DEPRECATED: Legacy connection factory for backward compatibility testing
- **`legacy_config()`** - DEPRECATED: Legacy configuration for backward compatibility testing
- **`legacy_table_config()`** - DEPRECATED: Legacy table configuration for backward compatibility testing
- **`legacy_loader()`** - DEPRECATED: Legacy loader for backward compatibility testing
- **`legacy_transformer()`** - DEPRECATED: Legacy transformer for backward compatibility testing
- **`legacy_replicator()`** - DEPRECATED: Legacy replicator for backward compatibility testing
- **`legacy_orchestrator()`** - DEPRECATED: Legacy orchestrator for backward compatibility testing
- **`legacy_metrics()`** - DEPRECATED: Legacy metrics for backward compatibility testing
- **`legacy_validation()`** - DEPRECATED: Legacy validation for backward compatibility testing
- **`legacy_error_handler()`** - DEPRECATED: Legacy error handler for backward compatibility testing
- **`legacy_logger()`** - DEPRECATED: Legacy logger for backward compatibility testing
- **`legacy_config_parser()`** - DEPRECATED: Legacy config parser for backward compatibility testing
- **`legacy_migration_utils()`** - DEPRECATED: Legacy migration utilities for backward compatibility testing

**Replacement Guidance:**
- Use `test_settings()` from `env_fixtures.py` instead of `legacy_settings()`
- Use `mock_connection_factory_methods()` from `connection_fixtures.py` instead of `legacy_connection_factory()`
- Use `test_pipeline_config()` from `config_fixtures.py` instead of `legacy_config()`
- Use `test_tables_config()` from `config_fixtures.py` instead of `legacy_table_config()`
- Use `postgres_loader()` from `loader_fixtures.py` instead of `legacy_loader()`
- Use `mock_table_processor_engines()` from `transformer_fixtures.py` instead of `legacy_transformer()`
- Use `mock_replicator()` from `replicator_fixtures.py` instead of `legacy_replicator()`
- Use `orchestrator()` from `orchestrator_fixtures.py` instead of `legacy_orchestrator()`
- Use `mock_metrics_collector()` from `metrics_fixtures.py` instead of `legacy_metrics()`
- Use `sample_validation_rules()` from `transformer_fixtures.py` instead of `legacy_validation()`
- Use `mock_connection_error()` from `connection_fixtures.py` instead of `legacy_error_handler()`
- Use `mock_logger()` from `logging_fixtures.py` instead of `legacy_logger()`
- Use `mock_schema_migration()` from `postgres_schema_fixtures.py` instead of `legacy_migration_utils()`

### Mock Utils (`mock_utils.py`)
- **`create_mock_engine()`** - Utility to create mock database engines
- **`create_mock_connection()`** - Utility to create mock database connections
- **`create_mock_settings()`** - Utility to create mock settings objects
- **`create_mock_data()`** - Utility to create mock test data

## Usage Examples

### Testing Database Configuration with Enums

```python
def test_database_config_with_enums(test_settings_with_enums, database_types, postgres_schemas):
    """Test database configuration using enums."""
    settings = test_settings_with_enums
    
    # Test source database configuration
    source_config = settings.get_database_config(database_types.SOURCE)
    assert 'host' in source_config
    assert 'port' in source_config
    
    # Test analytics database with specific schema
    analytics_config = settings.get_database_config(
        database_types.ANALYTICS, 
        postgres_schemas.RAW
    )
    assert analytics_config['schema'] == 'raw'
```

### Testing ConnectionFactory Methods

```python
def test_connection_factory_methods(mock_connection_factory_methods):
    """Test ConnectionFactory method mocking."""
    factory = mock_connection_factory_methods
    
    # Test production connection methods
    source_engine = factory.get_opendental_source_connection()
    assert source_engine is not None
    
    # Test schema-specific analytics connections
    raw_engine = factory.get_opendental_analytics_raw_connection()
    staging_engine = factory.get_opendental_analytics_staging_connection()
    assert raw_engine is not None
    assert staging_engine is not None
    
    # Test test connection methods
    test_source_engine = factory.get_opendental_source_test_connection()
    assert test_source_engine is not None
```

### Testing Environment Detection

```python
def test_environment_detection(environment_detection_test_cases):
    """Test environment detection logic."""
    for env_vars, expected_env, description in environment_detection_test_cases:
        with patch.dict(os.environ, env_vars):
            settings = create_test_settings(env_vars=env_vars)
            assert settings.environment == expected_env, f"Failed: {description}"
```

### Integration Testing with Real Databases

```python
def test_patient_data_extraction(test_data_manager):
    """Test patient data extraction using real test databases."""
    # Set up test data
    test_data_manager.setup_patient_data(
        include_all_fields=True,
        database_types=[DatabaseType.SOURCE, DatabaseType.REPLICATION]
    )
    
    # Verify data was inserted
    source_count = test_data_manager.get_patient_count(DatabaseType.SOURCE)
    replication_count = test_data_manager.get_patient_count(DatabaseType.REPLICATION)
    
    assert source_count > 0
    assert replication_count > 0
    
    # Test logic here...
    # Cleanup is automatic via fixture
```

### Testing with Populated Databases

```python
def test_with_populated_databases(populated_test_databases):
    """Test with databases that already have comprehensive test data."""
    # Databases already have patient and appointment data
    # SOURCE and REPLICATION databases are populated
    # ANALYTICS database starts empty (PostgresSchema will populate it)
    
    source_count = populated_test_databases.get_patient_count(DatabaseType.SOURCE)
    replication_count = populated_test_databases.get_patient_count(DatabaseType.REPLICATION)
    
    assert source_count > 0
    assert replication_count > 0
    
    # Test logic here...
    # Cleanup is automatic via fixture
```

## Architecture Alignment

### 1. Explicit Environment Separation

The fixtures now properly support the explicit environment separation:

- **Production Methods**: `get_opendental_source_connection()`, `get_mysql_replication_connection()`, etc.
- **Test Methods**: `get_opendental_source_test_connection()`, `get_mysql_replication_test_connection()`, etc.

### 2. Enum-Based Type Safety

All fixtures now use the proper enums:

```python
from etl_pipeline.config import DatabaseType, PostgresSchema

# ✅ CORRECT - Using enums
settings.get_database_config(DatabaseType.SOURCE)
settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)

# ❌ WRONG - Using raw strings (will cause errors)
settings.get_database_config("source")  # Type error
settings.get_database_config("analytics", "raw")  # Type error
```

### 3. ConnectionFactory Pattern

The fixtures properly mock all ConnectionFactory methods following the new architecture:

```python
# Production connections
factory.get_opendental_source_connection()
factory.get_mysql_replication_connection()
factory.get_postgres_analytics_connection()

# Schema-specific analytics connections
factory.get_opendental_analytics_raw_connection()
factory.get_opendental_analytics_staging_connection()
factory.get_opendental_analytics_intermediate_connection()
factory.get_opendental_analytics_marts_connection()

# Test connections
factory.get_opendental_source_test_connection()
factory.get_mysql_replication_test_connection()
factory.get_postgres_analytics_test_connection()
```

### 4. Settings Integration

The fixtures properly integrate with the new Settings class:

```python
# Create test settings with proper configuration
test_settings = create_test_settings(
    pipeline_config=test_pipeline_config,
    tables_config=test_tables_config,
    env_vars=test_env_vars
)

# Use enums for database configuration
config = test_settings.get_database_config(DatabaseType.SOURCE)
```

## Summary

The ETL pipeline test fixtures provide comprehensive coverage for:

1. **Configuration Management** - Pipeline and table configurations, environment variables, settings
2. **Database Connections** - Mock engines, connections, and connection factories for all database types
3. **Test Data Management** - Standardized test data that matches real schemas, data managers for integration tests
4. **Integration Testing** - Real database connections for end-to-end testing
5. **Component Testing** - Mocks for orchestrators, transformers, loaders, replicators, and other pipeline components
6. **Schema Management** - PostgreSQL schema handling, discovery, validation, and migration
7. **Monitoring & Metrics** - Logging, metrics collection, and monitoring capabilities
8. **Error Handling** - Mock errors, timeouts, and failure scenarios for robust testing

All fixtures follow the new architectural patterns with explicit environment separation, enum-based type safety, and proper integration with the Settings class and ConnectionFactory pattern. 