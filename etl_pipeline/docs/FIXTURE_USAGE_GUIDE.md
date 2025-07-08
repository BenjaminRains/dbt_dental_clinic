# Fixture Usage Guide

This guide shows how to use the fixtures defined in the modular fixture system for ETL pipeline tests.

## Architecture Note

**Updated for ETL Pipeline v3.0**: This guide reflects the current modular fixture architecture where:
- **Modular Fixtures**: Fixtures are organized in separate files under `tests/fixtures/`
- **New Configuration System**: Uses `Settings` class and `ConnectionFactory` with explicit environment separation
- **Test Data Manager**: Centralized test data management with `IntegrationTestDataManager`
- **Explicit Environment Separation**: Clear distinction between production and test connections

## Table of Contents
1. [Fixture Organization](#fixture-organization)
2. [Environment and Configuration Fixtures](#environment-and-configuration-fixtures)
3. [Database Connection Fixtures](#database-connection-fixtures)
4. [Test Data Fixtures](#test-data-fixtures)
5. [Integration Test Fixtures](#integration-test-fixtures)
6. [Mock Fixtures](#mock-fixtures)
7. [Component-Specific Fixtures](#component-specific-fixtures)
8. [Best Practices](#best-practices)

## Fixture Organization

The fixtures are organized in a modular structure under `tests/fixtures/`:

```
tests/fixtures/
├── __init__.py                    # Main fixture imports
├── env_fixtures.py               # Environment and settings
├── config_fixtures.py            # Configuration management
├── connection_fixtures.py        # Database connection mocks
├── test_data_fixtures.py         # Standardized test data
├── test_data_manager.py          # Integration test data management
├── test_data_definitions.py      # Test data definitions
├── integration_fixtures.py       # Integration test setup
├── replicator_fixtures.py        # MySQL replicator tests
├── loader_fixtures.py            # PostgreSQL loader tests
├── orchestrator_fixtures.py      # Pipeline orchestration tests
├── transformer_fixtures.py       # Data transformation tests
├── metrics_fixtures.py           # Metrics and monitoring tests
├── logging_fixtures.py           # Logging configuration tests
├── priority_processor_fixtures.py # Priority processing tests
├── schema_discovery_fixtures.py  # Schema discovery tests
├── postgres_schema_fixtures.py   # PostgreSQL schema tests
└── mock_utils.py                 # Mock utilities
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
        mock_factory.get_opendental_source_test_connection.return_value = mock_connection_factory.get_connection('source')
        
        # Test connection creation
        engine = mock_factory.get_opendental_source_test_connection()
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
        
        mock_factory.get_opendental_source_test_connection.return_value = mock_source_engine
        mock_factory.get_mysql_replication_test_connection.return_value = mock_target_engine
        
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
- `production_env_vars`: Production environment variables
- `test_settings`: Test settings instance
- `reset_global_settings`: Reset settings between tests

### Configuration Fixtures
- `test_pipeline_config`: Test pipeline configuration
- `test_tables_config`: Test tables configuration
- `complete_config_environment`: Complete configuration environment
- `mock_settings_environment`: Mock settings environment

### Database Connection Fixtures
- `test_source_engine`: Real test source engine
- `test_replication_engine`: Real test replication engine
- `test_analytics_engine`: Real test analytics engine
- `test_raw_engine`: Real test raw schema engine
- `test_staging_engine`: Real test staging schema engine
- `test_intermediate_engine`: Real test intermediate schema engine
- `test_marts_engine`: Real test marts schema engine
- `mock_connection_factory`: Mock connection factory
- `mock_source_engine`: Mock source engine
- `mock_replication_engine`: Mock replication engine
- `mock_analytics_engine`: Mock analytics engine

### Test Data Fixtures
- `standard_patient_test_data`: Standard patient test data
- `incremental_patient_test_data`: Incremental patient test data
- `patient_with_all_fields_test_data`: Comprehensive patient data
- `test_data_manager`: Integration test data manager
- `populated_test_databases`: Pre-populated test databases

### Component-Specific Fixtures
- `mock_unified_metrics_connection`: Mock metrics connection
- `mock_metrics_data`: Mock metrics data
- `mock_analytics_engine_for_metrics`: Mock analytics engine for metrics
- `postgres_schema_test_settings`: PostgreSQL schema test settings
- `schema_discovery_test_settings`: Schema discovery test settings

This guide provides comprehensive examples of how to use the current fixture system in your ETL pipeline tests. The fixtures are designed to be flexible, reusable, and maintainable across different types of testing scenarios. 