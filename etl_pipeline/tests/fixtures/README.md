# ETL Pipeline Test Fixtures

This directory contains test fixtures for the ETL pipeline that have been updated to align with the new connection architecture documented in `etl_pipeline/docs/connection_architecture.md`.

## Overview

The fixtures have been updated to follow the new connection architecture principles:

1. **Explicit Environment Separation**: Clear distinction between production and test environments
2. **Enum-Based Type Safety**: Using `DatabaseType` and `PostgresSchema` enums
3. **ConnectionFactory Pattern**: Proper mocking of all ConnectionFactory methods
4. **Settings Integration**: Proper integration with the new Settings class

## Updated Fixtures

### 1. `config_fixtures.py`

**Key Changes:**
- Removed fallback imports and backward compatibility code
- Added proper imports for `DatabaseType` and `PostgresSchema` enums
- Added fixtures for enum-based testing
- Added test cases for database configuration validation
- Added schema-specific configuration test cases

**New Fixtures:**
- `database_types()`: Provides `DatabaseType` enum for testing
- `postgres_schemas()`: Provides `PostgresSchema` enum for testing
- `test_settings_with_enums()`: Creates test settings using enums
- `database_config_test_cases()`: Test cases for database configuration
- `schema_specific_configs()`: Schema-specific configuration test cases
- `connection_config_validation_cases()`: Connection validation test cases

### 2. `env_fixtures.py`

**Key Changes:**
- Updated imports to use new configuration system
- Added fixtures for environment detection testing
- Added database environment variable mappings
- Added schema-specific environment variable testing

**New Fixtures:**
- `environment_detection_test_cases()`: Test cases for environment detection
- `database_environment_mappings()`: Database environment variable mappings
- `test_environment_prefixes()`: Environment prefixes for different database types
- `schema_environment_variables()`: Schema-specific environment variables

### 3. `connection_fixtures.py`

**Key Changes:**
- Removed fallback mock classes for enums
- Added proper imports for `DatabaseType` and `PostgresSchema`
- Added comprehensive ConnectionFactory method mocking
- Added test cases for connection string building
- Added pool configuration test cases

**New Fixtures:**
- `mock_connection_factory_methods()`: Mocks all ConnectionFactory methods
- `connection_factory_test_cases()`: Test cases for ConnectionFactory methods
- `mock_connection_manager()`: Mock ConnectionManager for testing
- `connection_string_test_cases()`: Test cases for connection string building
- `pool_config_test_cases()`: Test cases for connection pool configuration

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

### Testing Connection String Building

```python
def test_connection_strings(connection_string_test_cases):
    """Test connection string building."""
    for test_case in connection_string_test_cases:
        config = test_case['config']
        expected_prefix = test_case['expected_prefix']
        expected_params = test_case['expected_params']
        
        # Test connection string building logic
        # (Implementation depends on your connection string building method)
        pass
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

## Migration Notes

### From Legacy Fixtures

1. **Remove Fallback Imports**: All fallback imports and backward compatibility code has been removed
2. **Use Enums**: Replace string-based database types with `DatabaseType` and `PostgresSchema` enums
3. **Update ConnectionFactory Mocks**: Use the new `mock_connection_factory_methods` fixture
4. **Environment Variables**: Ensure proper environment variable naming with `TEST_` prefix for test environments

### Testing Best Practices

1. **Use Enums**: Always use enums for database types and schemas in tests
2. **Environment Isolation**: Use `reset_global_settings` fixture for proper test isolation
3. **Mock ConnectionFactory**: Use `mock_connection_factory_methods` for comprehensive ConnectionFactory testing
4. **Validate Configuration**: Use the provided test cases for configuration validation

## Dependencies

The updated fixtures require the new configuration system:

```python
from etl_pipeline.config import (
    Settings,
    DatabaseType,
    PostgresSchema,
    reset_settings,
    create_test_settings
)
```

Make sure your ETL pipeline is using the new connection architecture before using these fixtures. 