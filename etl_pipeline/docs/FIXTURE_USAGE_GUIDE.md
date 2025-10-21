# Fixture Usage Guide

This guide shows how to use the fixtures defined in the modular fixture system for ETL pipeline tests.

## Architecture Note

**Updated for ETL Pipeline v3.0**: This guide reflects the current modular fixture architecture where:
- **Modular Fixtures**: Fixtures are organized in separate files under `tests/fixtures/`
- **New Configuration System**: Uses `Settings` class and `ConnectionFactory` with explicit environment separation
- **Test Data Manager**: Centralized test data management with `IntegrationTestDataManager`
- **Explicit Environment Separation**: Clear distinction between production and test connections

> 💡 **See Also**: [pytest_debugging_notes.md](pytest_debugging_notes.md) - Comprehensive debugging guide for troubleshooting test failures, mock issues, and common patterns when working with these fixtures.

## Table of Contents
1. [Fixture Organization](#fixture-organization)
2. [Environment and Configuration Fixtures](#environment-and-configuration-fixtures)
3. [Database Connection Fixtures](#database-connection-fixtures)
4. [Test Data Fixtures](#test-data-fixtures)
5. [Integration Test Fixtures](#integration-test-fixtures)
6. [Mock Fixtures](#mock-fixtures)
7. [Component-Specific Fixtures](#component-specific-fixtures)
   - [Replicator Fixtures](#replicator-fixtures)
   - [Loader Fixtures](#loader-fixtures)
   - [Metrics Fixtures](#metrics-fixtures)
   - [Schema Analyzer Fixtures](#schema-analyzer-fixtures)
   - [CLI Fixtures](#cli-fixtures)
   - [ConfigReader Fixtures](#configreader-fixtures)
   - [Production Data Fixtures](#production-data-fixtures)
8. [Best Practices](#best-practices)
9. [Running Tests with Fixtures](#running-tests-with-fixtures)
10. [Fixture Reference](#fixture-reference)
11. [Debugging Tests with Fixtures](#debugging-tests-with-fixtures)

## Fixture Organization

The fixtures are organized in a modular structure under `tests/fixtures/`:

```
tests/fixtures/
├── __init__.py                      # Main fixture imports
├── env_fixtures.py                 # Environment and settings
├── config_fixtures.py              # Configuration management
├── config_reader_fixtures.py       # ConfigReader testing
├── connection_fixtures.py          # Database connection mocks
├── test_data_fixtures.py           # Standardized test data
├── test_data_manager.py            # Integration test data management
├── test_data_definitions.py        # Test data definitions
├── integration_fixtures.py         # Integration test setup
├── production_data_fixtures.py     # Production data testing (E2E)
├── replicator_fixtures.py          # MySQL replicator tests
├── loader_fixtures.py              # PostgreSQL loader tests
├── orchestrator_fixtures.py        # Pipeline orchestration tests
├── transformer_fixtures.py         # Data transformation tests
├── metrics_fixtures.py             # Metrics and monitoring tests
├── logging_fixtures.py             # Logging configuration tests
├── priority_processor_fixtures.py  # Priority processing tests
├── schema_analyzer_fixtures.py     # Schema analyzer tests
├── postgres_schema_fixtures.py     # PostgreSQL schema tests
├── cli_fixtures.py                 # CLI command tests
└── mock_utils.py                   # Mock utilities
```

All fixtures are automatically imported in `tests/conftest.py` for global availability.

## Environment and Configuration Fixtures

### Basic Environment Setup

```python
# tests/unit/test_basic_functionality.py
import pytest

def test_environment_setup(test_env_vars, test_settings):
    """Test that environment is properly configured."""
    assert test_env_vars['ETL_ENVIRONMENT'] == 'test'
    assert test_settings.environment == 'test'
    
    # Verify test database configuration
    source_config = test_settings.get_source_connection_config()
    assert 'TEST_OPENDENTAL_SOURCE_HOST' in test_env_vars
    assert source_config['host'] == test_env_vars['TEST_OPENDENTAL_SOURCE_HOST']
```

### Configuration Testing

```python
# tests/unit/config/test_configuration.py
import pytest

def test_pipeline_configuration(test_pipeline_config, test_settings):
    """Test pipeline configuration loading."""
    # Verify pipeline config structure
    assert 'general' in test_pipeline_config
    assert 'connections' in test_pipeline_config
    
    # Test configuration access
    batch_size = test_settings.get_pipeline_setting('general.batch_size')
    assert batch_size == 1000

def test_tables_configuration(test_tables_config, test_settings):
    """Test tables configuration loading."""
    # Verify tables config structure
    assert 'tables' in test_tables_config
    assert 'patient' in test_tables_config['tables']
    
    # Test table configuration access
    patient_config = test_settings.get_table_config('patient')
    assert patient_config['table_importance'] == 'critical'
    assert patient_config['batch_size'] == 100
```

### Settings Management

```python
# tests/unit/config/test_settings.py
import pytest

def test_settings_creation(test_settings):
    """Test settings creation and validation."""
    # Verify settings are properly initialized
    assert test_settings.environment == 'test'
    assert test_settings.env_prefix == 'TEST_'
    
    # Test configuration validation
    assert test_settings.validate_configs() is True

def test_database_configs(test_settings):
    """Test database configuration retrieval."""
    # Test source configuration
    source_config = test_settings.get_source_connection_config()
    assert 'host' in source_config
    assert 'database' in source_config
    
    # Test replication configuration
    repl_config = test_settings.get_replication_connection_config()
    assert 'host' in repl_config
    assert 'database' in repl_config
    
    # Test analytics configuration
    analytics_config = test_settings.get_analytics_connection_config()
    assert 'host' in analytics_config
    assert 'database' in analytics_config
```

## Database Connection Fixtures

### Real Database Connections (Integration Tests)

```python
# tests/integration/test_database_connections.py
import pytest
from sqlalchemy import text

def test_source_connection(test_source_engine):
    """Test source database connection."""
    with test_source_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1

def test_replication_connection(test_replication_engine):
    """Test replication database connection."""
    with test_replication_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1

def test_analytics_connection(test_analytics_engine):
    """Test analytics database connection."""
    with test_analytics_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1

def test_schema_specific_connections(test_raw_engine, test_staging_engine, 
                                   test_intermediate_engine, test_marts_engine):
    """Test schema-specific connections."""
    # Test raw schema
    with test_raw_engine.connect() as conn:
        result = conn.execute(text("SELECT current_schema()"))
        schema = result.scalar()
        assert 'raw' in schema.lower()
    
    # Test staging schema
    with test_staging_engine.connect() as conn:
        result = conn.execute(text("SELECT current_schema()"))
        schema = result.scalar()
        assert 'staging' in schema.lower()
```

### Mock Database Connections (Unit Tests)

```python
# tests/unit/core/test_connections.py
import pytest
from unittest.mock import patch

def test_mock_connection_factory(mock_connection_factory):
    """Test mock connection factory."""
    # Test source connection
    source_engine = mock_connection_factory.get_connection('source')
    assert source_engine.name == 'mysql'
    assert source_engine.url.database == 'test_opendental'
    
    # Test replication connection
    repl_engine = mock_connection_factory.get_connection('replication')
    assert repl_engine.name == 'mysql'
    assert repl_engine.url.database == 'test_replication'
    
    # Test analytics connection
    analytics_engine = mock_connection_factory.get_connection('analytics')
    assert analytics_engine.name == 'postgresql'
    assert analytics_engine.url.database == 'test_analytics'

def test_mock_engines(mock_source_engine, mock_replication_engine, mock_analytics_engine):
    """Test individual mock engines."""
    assert mock_source_engine.name == 'mysql'
    assert mock_replication_engine.name == 'mysql'
    assert mock_analytics_engine.name == 'postgresql'
```

## Test Data Fixtures

### Standardized Test Data

```python
# tests/unit/data/test_data_validation.py
import pytest
import pandas as pd

def test_patient_data_structure(standard_patient_test_data):
    """Test patient data structure."""
    assert len(standard_patient_test_data) == 3
    
    # Check required fields
    required_fields = ['PatNum', 'LName', 'FName', 'DateTStamp', 'PatStatus']
    for field in required_fields:
        assert field in standard_patient_test_data[0]
    
    # Check data types
    assert isinstance(standard_patient_test_data[0]['PatNum'], int)
    assert isinstance(standard_patient_test_data[0]['LName'], str)

def test_incremental_data(incremental_patient_test_data):
    """Test incremental data structure."""
    assert len(incremental_patient_test_data) == 1
    assert incremental_patient_test_data[0]['PatNum'] == 4

def test_comprehensive_patient_data(patient_with_all_fields_test_data):
    """Test comprehensive patient data."""
    patient = patient_with_all_fields_test_data[0]
    
    # Check all major field categories
    assert 'PatNum' in patient  # Primary key
    assert 'LName' in patient   # Basic info
    assert 'Email' in patient   # Contact info
    assert 'Birthdate' in patient  # Demographics
    assert 'EstBalance' in patient  # Financial
```

### Test Data Manager (Integration Tests)

```python
# tests/integration/test_data_management.py
import pytest

def test_test_data_manager_setup(test_data_manager):
    """Test test data manager initialization."""
    # Verify manager has all required connections
    assert test_data_manager.source_engine is not None
    assert test_data_manager.replication_engine is not None
    assert test_data_manager.analytics_engine is not None

def test_patient_data_setup(test_data_manager):
    """Test patient data setup."""
    # Set up patient data in source and replication databases
    test_data_manager.setup_patient_data(
        include_all_fields=True,
        database_types=['source', 'replication']
    )
    
    # Verify data was inserted
    source_count = test_data_manager.get_patient_count('source')
    repl_count = test_data_manager.get_patient_count('replication')
    
    assert source_count > 0
    assert repl_count > 0
    assert source_count == repl_count

def test_appointment_data_setup(test_data_manager):
    """Test appointment data setup."""
    # Set up appointment data
    test_data_manager.setup_appointment_data(
        database_types=['source', 'replication']
    )
    
    # Verify data was inserted
    source_count = test_data_manager.get_appointment_count('source')
    repl_count = test_data_manager.get_appointment_count('replication')
    
    assert source_count > 0
    assert repl_count > 0

def test_data_cleanup(test_data_manager):
    """Test data cleanup."""
    # Set up some data
    test_data_manager.setup_patient_data()
    
    # Verify data exists
    assert test_data_manager.get_patient_count('source') > 0
    
    # Clean up
    test_data_manager.cleanup_patient_data()
    
    # Verify data was cleaned up
    assert test_data_manager.get_patient_count('source') == 0
```

## Integration Test Fixtures

### Populated Test Databases

```python
# tests/integration/test_full_pipeline.py
import pytest

def test_pipeline_with_populated_databases(populated_test_databases):
    """Test pipeline with pre-populated test databases."""
    # Databases already have test data
    source_count = populated_test_databases.get_patient_count('source')
    repl_count = populated_test_databases.get_patient_count('replication')
    
    assert source_count > 0
    assert repl_count > 0
    
    # Run pipeline logic here
    # ...

def test_database_engines_compatibility(test_database_engines):
    """Test database engines compatibility."""
    replication_engine, analytics_engine = test_database_engines
    
    # Test replication engine
    with replication_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
    
    # Test analytics engine
    with analytics_engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
```

### ETL Tracking Setup

```python
# tests/integration/test_etl_tracking.py
import pytest

def test_etl_tracking_setup(setup_etl_tracking):
    """Test ETL tracking table setup."""
    # ETL tracking table is already set up
    # Test tracking functionality here
    pass

def test_patient_table_setup(setup_patient_table):
    """Test patient table setup."""
    test_data_manager, patient_data = setup_patient_table
    
    # Verify patient data was set up
    assert len(patient_data) > 0
    assert test_data_manager.get_patient_count('source') > 0
```

## Mock Fixtures

### Mock Settings

```python
# tests/unit/config/test_settings_mocks.py
import pytest

def test_mock_settings_environment(mock_settings_environment):
    """Test mock settings environment."""
    with mock_settings_environment() as mock_settings:
        # Test configuration access
        db_config = mock_settings.get_database_config('source')
        assert db_config['host'] == 'localhost'
        assert db_config['database'] == 'test_db'
        
        # Test table configuration
        table_config = mock_settings.get_table_config('patient')
        assert table_config['incremental'] is True
        assert table_config['batch_size'] == 1000

def test_mock_connection_factory_with_settings(mock_connection_factory, test_settings):
    """Test mock connection factory with settings."""
    # Mock the connection factory
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
        mock_factory.get_source_connection.return_value = mock_connection_factory.get_connection('source')
        
        # Test connection creation
        from etl_pipeline.config import create_test_settings
        test_settings = create_test_settings()
        engine = mock_factory.get_source_connection(test_settings)
        assert engine.name == 'mysql'
```

### Mock Database Engines

```python
# tests/unit/core/test_mock_engines.py
import pytest

def test_mock_engine_with_connection(mock_engine_with_connection):
    """Test mock engine with connection."""
    # Create mock connection
    mock_conn = MagicMock()
    mock_conn.execute.return_value.scalar.return_value = 42
    
    # Create engine with mock connection
    engine = mock_engine_with_connection(mock_conn)
    
    # Test connection usage
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 42"))
        assert result.scalar() == 42

def test_mock_connection_pool(mock_connection_pool):
    """Test mock connection pool."""
    # Test pool connection
    conn = mock_connection_pool.connect()
    assert conn is not None
    
    # Test pool disposal
    mock_connection_pool.dispose()
```

## Component-Specific Fixtures

### Replicator Fixtures

```python
# tests/unit/core/test_simple_mysql_replicator.py
import pytest

def test_replicator_with_mocks(mock_settings, standard_patient_test_data):
    """Test SimpleMySQLReplicator with mocked components."""
    from etl_pipeline.core.simple_mysql_replicator import SimpleMySQLReplicator
    
    with patch('etl_pipeline.core.connections.ConnectionFactory') as mock_factory:
        # Configure mock connections
        mock_source_engine = MagicMock()
        mock_target_engine = MagicMock()
        
        mock_factory.get_source_connection.return_value = mock_source_engine
        mock_factory.get_replication_connection.return_value = mock_target_engine
        
        # Create replicator
        replicator = SimpleMySQLReplicator(settings=mock_settings)
        
        # Test replicator initialization
        assert replicator.source_engine == mock_source_engine
        assert replicator.target_engine == mock_target_engine
```

### Loader Fixtures

```python
# tests/unit/loaders/test_postgres_loader.py
import pytest

def test_postgres_loader_with_mocks(mock_analytics_engine, standard_patient_test_data):
    """Test PostgresLoader with mocked components."""
    from etl_pipeline.loaders.postgres_loader import PostgresLoader
    
    # Create loader with mock engine
    loader = PostgresLoader(engine=mock_analytics_engine)
    
    # Test loader functionality
    # ...
```

### Metrics Fixtures

```python
# tests/unit/monitoring/test_unified_metrics.py
import pytest

def test_metrics_collector(mock_unified_metrics_connection, mock_metrics_data):
    """Test UnifiedMetricsCollector with mocked components."""
    from etl_pipeline.monitoring.unified_metrics import UnifiedMetricsCollector
    
    # Create metrics collector
    collector = UnifiedMetricsCollector(connection=mock_unified_metrics_connection)
    
    # Test metrics collection
    collector.record_table_processed('patient', 100, 'success')
    
    # Verify metrics were recorded
    # ...
```

### Schema Analyzer Fixtures

```python
# tests/unit/scripts/test_schema_analyzer.py
import pytest

def test_schema_analyzer_with_mock_data(mock_schema_data, mock_size_data):
    """Test OpenDentalSchemaAnalyzer with mocked schema and size data."""
    from etl_pipeline.scripts.analyze_opendental_schema import OpenDentalSchemaAnalyzer
    
    # Use mock schema data
    assert 'patient' in mock_schema_data
    assert mock_schema_data['patient']['primary_keys'] == ['PatNum']
    
    # Use mock size data
    assert mock_size_data['patient']['estimated_row_count'] == 50000
    assert mock_size_data['patient']['size_mb'] == 25.5

def test_dbt_model_discovery(mock_dbt_models):
    """Test DBT model discovery with mock models."""
    assert 'stg_opendental__patient' in mock_dbt_models['staging']
    assert 'dim_patient' in mock_dbt_models['mart']
```

### CLI Fixtures

```python
# tests/unit/cli/test_commands.py
import pytest

def test_cli_run_command(cli_runner, cli_with_injected_config):
    """Test CLI run command with injected configuration."""
    from etl_pipeline.cli.commands import cli
    
    result = cli_runner.invoke(cli, ['run', '--dry-run'])
    assert result.exit_code == 0
    assert 'DRY RUN MODE' in result.output

def test_cli_status_command(cli_runner, temp_cli_config_file):
    """Test CLI status command with temporary config file."""
    from etl_pipeline.cli.commands import cli
    
    result = cli_runner.invoke(cli, ['status', '--config', temp_cli_config_file])
    assert result.exit_code == 0

def test_cli_test_connections(cli_runner, mock_cli_database_connections):
    """Test CLI connection testing with mocked connections."""
    from etl_pipeline.cli.commands import cli
    
    result = cli_runner.invoke(cli, ['test-connections'])
    assert result.exit_code == 0
    assert 'Testing database connections' in result.output
```

### ConfigReader Fixtures

```python
# tests/unit/config/test_config_reader.py
import pytest

def test_config_reader_with_valid_config(temp_config_file):
    """Test ConfigReader with valid configuration file."""
    from etl_pipeline.config.config_reader import ConfigReader
    
    reader = ConfigReader(config_path=temp_config_file)
    patient_config = reader.get_table_config('patient')
    assert patient_config['table_importance'] == 'critical'

def test_config_reader_with_mock(mock_config_reader):
    """Test ConfigReader with mocked configuration."""
    patient_config = mock_config_reader.get_table_config('patient')
    assert patient_config is not None
    assert 'primary_key' in patient_config

def test_config_validation(valid_tables_config, invalid_tables_config):
    """Test configuration validation."""
    # Test valid config
    assert 'tables' in valid_tables_config
    assert 'patient' in valid_tables_config['tables']
    
    # Test invalid config
    assert 'patient' in invalid_tables_config['tables']
    # Should detect invalid batch_size
    assert invalid_tables_config['tables']['appointment']['batch_size'] == -1
```

### Production Data Fixtures

```python
# tests/e2e/test_production_data_pipeline.py
import pytest

@pytest.mark.e2e
def test_production_data_pipeline(production_settings, test_settings, test_data_cleanup):
    """Test pipeline with production data in isolated test environment."""
    # Production settings provide readonly access to source
    assert production_settings.environment == 'production'
    
    # Test settings provide read/write access to test databases
    assert test_settings.environment == 'test'
    
    # Test data cleanup ensures test data is cleaned up
    # ... run pipeline tests ...

def test_pipeline_performance(pipeline_performance_tracker):
    """Test pipeline performance with tracking."""
    pipeline_performance_tracker.start_tracking('patient_load')
    
    # ... run pipeline operations ...
    
    metrics = pipeline_performance_tracker.end_tracking('patient_load', record_count=1000)
    assert metrics['duration'] < 60  # Should complete in under 60 seconds
    assert metrics['throughput'] > 10  # Should process at least 10 records/sec

def test_data_quality(data_quality_validator):
    """Test data quality across pipeline stages."""
    patient_quality = data_quality_validator.validate_patient_data_quality()
    assert patient_quality['quality_consistent']
    assert patient_quality['replication_quality']['null_patnum'] == 0
```

## Best Practices

### 1. Fixture Scope and Performance

```python
# Use function scope for most fixtures (default)
def test_something(test_settings):
    """Function-scoped test - fresh settings for each test."""
    pass

# Use session scope for expensive setup
@pytest.fixture(scope="session")
def expensive_setup():
    """Session-scoped fixture - set up once for all tests."""
    # Expensive setup here
    yield setup_result
    # Cleanup here
```

### 2. Fixture Dependencies

```python
# Keep fixtures independent when possible
def test_independent(test_settings, standard_patient_test_data):
    """Test with independent fixtures."""
    pass

# Use dependency injection for complex scenarios
def test_complex_setup(test_data_manager, populated_test_databases):
    """Test with dependent fixtures."""
    # test_data_manager is used by populated_test_databases
    pass
```

### 3. Environment Separation

```python
# Always use test environment for tests
def test_production_vs_test(test_env_vars, production_env_vars):
    """Test environment separation."""
    # Test environment uses TEST_ prefix
    assert 'TEST_OPENDENTAL_SOURCE_HOST' in test_env_vars
    assert test_env_vars['ETL_ENVIRONMENT'] == 'test'
    
    # Production environment uses base variables
    assert 'OPENDENTAL_SOURCE_HOST' in production_env_vars
    assert production_env_vars['ETL_ENVIRONMENT'] == 'production'
```

### 4. Test Data Management

```python
# Use standardized test data
def test_with_standard_data(standard_patient_test_data):
    """Use standardized test data for consistency."""
    assert len(standard_patient_test_data) == 3

# Use test data manager for integration tests
def test_with_data_manager(test_data_manager):
    """Use test data manager for complex scenarios."""
    test_data_manager.setup_patient_data()
    # Test logic here
    test_data_manager.cleanup_patient_data()  # Automatic cleanup
```

### 5. Mock vs Real Connections

```python
# Use mocks for unit tests
def test_unit_with_mocks(mock_settings, mock_connection_factory):
    """Unit tests should use mocks."""
    pass

# Use real connections for integration tests
def test_integration_with_real_connections(test_source_engine, test_analytics_engine):
    """Integration tests should use real connections."""
    pass
```

## Running Tests with Fixtures

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/unit/config/test_settings.py

# Run tests with specific marker
pytest tests/ -m "unit"
pytest tests/ -m "integration"

# Run tests with verbose output
pytest tests/ -v

# Run tests with coverage
pytest tests/ --cov=etl_pipeline --cov-report=html

# Run specific fixture tests
pytest tests/ -k "test_data_manager"

# Run tests with specific environment
ETL_ENVIRONMENT=test pytest tests/
```

## Fixture Reference

### Environment Fixtures
- `test_env_vars`: Test environment variables
- `production_env_vars`: Production environment variables (read-only)
- `test_settings`: Test settings instance
- `production_settings`: Production settings instance (read-only)
- `reset_global_settings`: Reset settings between tests

### Configuration Fixtures
- `test_pipeline_config`: Test pipeline configuration
- `test_tables_config`: Test tables configuration
- `test_config_environment`: Test environment configuration
- `mock_settings_environment`: Mock settings environment

### ConfigReader Fixtures
- `valid_tables_config`: Valid tables configuration dictionary
- `minimal_tables_config`: Minimal configuration for testing
- `invalid_tables_config`: Invalid configuration for error testing
- `empty_tables_config`: Empty configuration for edge cases
- `malformed_yaml_config`: Malformed YAML for error handling
- `temp_config_file`: Temporary valid config file
- `temp_invalid_config_file`: Temporary invalid config file
- `temp_empty_config_file`: Temporary empty config file
- `mock_config_reader`: Mock ConfigReader with valid config
- `mock_config_reader_with_invalid_config`: Mock ConfigReader with invalid config
- `mock_config_reader_with_empty_config`: Mock ConfigReader with empty config
- `mock_file_system`: Mock file system operations
- `mock_yaml_loading`: Mock YAML loading operations
- `config_reader_test_cases`: Test cases for ConfigReader methods
- `config_reader_error_cases`: Error cases for testing
- `config_reader_validation_cases`: Validation test cases
- `config_reader_performance_data`: Performance test data
- `config_reader_dependency_test_data`: Dependency testing data

### Database Connection Fixtures
- `test_source_engine`: Real test source engine
- `test_replication_engine`: Real test replication engine
- `test_analytics_engine`: Real test analytics engine
- `test_raw_engine`: Real test raw schema engine
- `test_staging_engine`: Real test staging schema engine
- `test_intermediate_engine`: Real test intermediate schema engine
- `test_marts_engine`: Real test marts schema engine
- `production_database_engines`: Production database engines (read-only)
- `test_database_engines`: Test database engines (read/write)
- `mock_connection_factory`: Mock connection factory
- `mock_source_engine`: Mock source engine
- `mock_replication_engine`: Mock replication engine
- `mock_analytics_engine`: Mock analytics engine

### Test Data Fixtures
- `standard_patient_test_data`: Standard patient test data
- `incremental_patient_test_data`: Incremental patient test data
- `partial_patient_test_data`: Partial patient data for testing
- `patient_with_all_fields_test_data`: Comprehensive patient data
- `etl_tracking_test_data`: ETL tracking test data
- `invalid_schema_test_data`: Invalid schema for error testing
- `composite_pk_test_data`: Composite primary key test data
- `large_table_test_data`: Large table test data
- `simple_test_table_data`: Simple test table data
- `test_data_manager`: Integration test data manager
- `populated_test_databases`: Pre-populated test databases

### Schema Analyzer Fixtures
- `mock_schema_data`: Mock schema data for dental clinic tables
- `mock_size_data`: Mock size data for tables
- `mock_dbt_models`: Mock DBT models structure
- `mock_environment_variables`: Placeholder for environment variables

### CLI Fixtures
- `cli_runner`: Click CLI test runner
- `cli_test_config`: Test configuration for CLI
- `cli_test_env_vars`: Test environment variables for CLI
- `cli_config_provider`: CLI test configuration provider
- `cli_test_settings`: CLI test settings with injected config
- `cli_test_config_reader`: Test config reader for CLI
- `cli_with_injected_config`: Fixture that injects test configuration
- `temp_cli_config_file`: Temporary CLI configuration file
- `temp_tables_config_file`: Temporary tables configuration file
- `mock_cli_database_connections`: Mock database connections for CLI
- `cli_expected_outputs`: Expected CLI output patterns
- `cli_error_cases`: Common CLI error cases
- `cli_performance_thresholds`: Performance thresholds for CLI commands
- `cli_output_validators`: Validators for CLI output
- `cli_integration_test_data`: Test data for CLI integration testing
- `cli_mock_orchestrator`: Mock PipelineOrchestrator for CLI
- `cli_mock_metrics_collector`: Mock UnifiedMetricsCollector for CLI

### Production Data Fixtures (E2E Testing)
- `production_settings`: Production settings for readonly access
- `test_settings`: Test settings for read/write access (duplicates Environment fixture)
- `pipeline_validator`: Pipeline transformation validator
- `test_data_cleanup`: Test data cleanup fixture
- `production_database_engines`: Production database engines
- `test_database_engines`: Test database engines
- `pipeline_performance_tracker`: Pipeline performance tracking
- `data_quality_validator`: Data quality validation
- `error_injection_simulator`: Error condition simulator

### Component-Specific Fixtures
- `mock_unified_metrics_connection`: Mock metrics connection
- `mock_metrics_data`: Mock metrics data
- `mock_analytics_engine_for_metrics`: Mock analytics engine for metrics
- `postgres_schema_test_settings`: PostgreSQL schema test settings

This guide provides comprehensive examples of how to use the current fixture system in your ETL pipeline tests. The fixtures are designed to be flexible, reusable, and maintainable across different types of testing scenarios.

## Debugging Tests with Fixtures

When tests using fixtures fail, refer to [pytest_debugging_notes.md](pytest_debugging_notes.md) for comprehensive troubleshooting guidance:

### Common Fixture-Related Issues

**Mock Fixture Problems**
- **Context Manager Errors**: See [Section 2 - Context Manager Protocol](pytest_debugging_notes.md#2-context-manager-protocol)
- **SQLAlchemy Mock Issues**: See [Section 3 - SQLAlchemy Specific Fixes](pytest_debugging_notes.md#3-sqlalchemy-specific-fixes)
- **Variable Scope Issues**: See [Section 7.1 - Variable Scope Issues](pytest_debugging_notes.md#71-variable-scope-issues)

**Configuration Fixture Problems**
- **Settings vs Mock Settings**: See [Section 14 - Unit vs Integration Test Settings Pattern](pytest_debugging_notes.md#14-unit-vs-integration-test-settings-pattern)
- **Configuration File Testing**: See [Section 12.2 - Configuration File Testing Patterns](pytest_debugging_notes.md#122-configuration-file-testing-patterns)
- **Real vs Mock Configuration**: See [Section 12.3 - Real Configuration vs Mock Configuration](pytest_debugging_notes.md#123-real-configuration-vs-mock-configuration)

**Integration Test Fixture Problems**
- **Database Connection Issues**: See [Section 5 - Database Integration Patterns](pytest_debugging_notes.md#5-database-integration-patterns)
- **Test Data Manager Issues**: Verify `test_data_manager` fixture setup and cleanup
- **Environment Separation**: Ensure correct use of `test_settings` vs `production_settings`

**CLI Fixture Problems**
- **CLI Output Validation**: See [Section 12.1 - CLI Output Validation Patterns](pytest_debugging_notes.md#121-cli-output-validation-patterns)
- **Configuration Injection**: See [Section 9.3 - CLI Integration Testing with Real Configuration](pytest_debugging_notes.md#93-cli-integration-testing-with-real-configuration)

### Debugging Workflow

1. **Identify the fixture being used**: Check the test signature to see which fixtures are injected
2. **Review fixture definition**: Look in the appropriate fixture file to understand what it provides
3. **Check mock setup**: For mock fixtures, verify the mock configuration matches expected behavior
4. **Add debug logging**: Use `logger.debug()` to trace fixture values and mock calls
5. **Consult debugging guide**: Search [pytest_debugging_notes.md](pytest_debugging_notes.md) for similar patterns
6. **Test isolation**: Ensure fixtures are properly scoped and cleaned up between tests

### Quick Debugging Commands

```bash
# Run test with verbose output and print statements
pytest tests/path/test_file.py::test_method -v -s

# Run with fixture setup/teardown details
pytest tests/path/test_file.py::test_method -v --setup-show

# Show which fixtures a test uses
pytest tests/path/test_file.py::test_method --fixtures

# Run only tests that use a specific fixture
pytest tests/ -k "test_data_manager" -v
```

### Creating New Fixtures

When creating new fixtures, follow these guidelines:

1. **Use appropriate fixture files**: Add to existing fixture files based on responsibility
2. **Document fixture purpose**: Include docstrings explaining what the fixture provides
3. **Add to `__init__.py`**: Export from `tests/fixtures/__init__.py` for global availability
4. **Update this guide**: Add usage examples and reference documentation
5. **Test the fixture**: Write tests that use your new fixture to verify it works
6. **Consider scope**: Choose appropriate scope (function/session/module) for performance

**See Also**: [pytest_debugging_notes.md - Maintenance and Evolution](pytest_debugging_notes.md#maintenance-and-evolution) for patterns on documenting and validating new test patterns. 