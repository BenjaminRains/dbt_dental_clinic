# Setup Test Databases Refactor

## Overview

The `setup_test_databases.py` script has been refactored to align with the new architectural patterns and provide standardized test data management. This document explains the changes and how they improve the test setup process.

## Key Changes

### 1. **New ConnectionFactory Integration**

#### Before:
```python
# Manual connection string construction
admin_connection_string = (
    f"postgresql+psycopg2://{config['user']}:{config['password']}"
    f"@{config['host']}:{config['port']}/postgres"
)
```

#### After:
```python
# Use new ConnectionFactory with test methods
from etl_pipeline.config import create_test_settings, DatabaseType
from etl_pipeline.core.connections import ConnectionFactory

settings = create_test_settings()
analytics_engine = ConnectionFactory.get_analytics_test_connection(settings)
```

### 2. **Settings Integration**

#### Before:
```python
# Manual environment variable handling
config = {
    'host': os.environ.get('TEST_POSTGRES_ANALYTICS_HOST', 'localhost'),
    'port': int(os.environ.get('TEST_POSTGRES_ANALYTICS_PORT', 5432)),
    # ... more manual config
}
```

#### After:
```python
# Use Settings class for configuration
settings = create_test_settings()
# Settings automatically handles environment variables and validation
```

### 3. **Standardized Test Data**

#### Before:
```python
# Hardcoded test data inline
test_patients = [
    (1, 'Doe', 'John', 'M', 'Johnny', 0, 0, 0, '1980-01-01', '123-45-6789'),
    (2, 'Smith', 'Jane', 'A', 'Janey', 0, 1, 0, '1985-05-15', '234-56-7890'),
    # ... more hardcoded data
]
```

#### After:
```python
# Use standardized test data from fixtures
from etl_pipeline.tests.fixtures.test_data_definitions import get_test_patient_data

test_patients = get_test_patient_data(include_all_fields=False)
# Dynamic INSERT based on available fields
```

### 4. **Environment Separation Compliance**

The script now properly follows the connection environment separation guidelines:

- **Test Environment**: Uses `TEST_*` environment variables
- **Test Methods**: Uses `ConnectionFactory.get_*_test_connection()` methods
- **Safety Checks**: Validates test environment and database names
- **No Production Risk**: Cannot accidentally modify production databases

## New Architecture Benefits

### 1. **Consistency**
- All test setup uses the same connection methods as tests
- Standardized test data across all environments
- Consistent error handling and logging

### 2. **Maintainability**
- Test data defined once in `test_data_definitions.py`
- Connection logic centralized in `ConnectionFactory`
- Configuration handled by `Settings` class

### 3. **Safety**
- Environment validation prevents production accidents
- Database name validation ensures test-only operations
- Clear separation between test and production environments

### 4. **Flexibility**
- Easy to add new test data types
- Simple to extend for new database schemas
- Configurable test data (minimal vs. full records)

## Usage

### Running the Setup Script

```bash
# Set test environment
export ETL_ENVIRONMENT=test

# Run setup script
python setup_test_databases.py
```

### Integration with Tests

The setup script now works seamlessly with the new test fixtures:

```python
# In tests, use the new fixtures
def test_something(populated_test_databases):
    # Databases already have standardized test data
    manager = populated_test_databases
    
    # Access engines
    source_engine = manager.source_engine
    analytics_engine = manager.analytics_engine
    
    # Test logic...
```

## Migration Guide

### For Existing Tests

1. **Update imports**:
   ```python
   # Old
   from etl_pipeline.tests.fixtures import setup_patient_table
   
   # New
   from etl_pipeline.tests.fixtures import populated_test_databases
   ```

2. **Update fixture usage**:
   ```python
   # Old
   def test_something(setup_patient_table):
       replication_engine, analytics_engine = setup_patient_table
   
   # New
   def test_something(populated_test_databases):
       manager = populated_test_databases
       # Use manager.source_engine, manager.analytics_engine, etc.
   ```

3. **Use standardized test data**:
   ```python
   # Old: Hardcoded test data
   test_data = [{'id': 1, 'name': 'John'}]
   
   # New: Standardized test data
   from etl_pipeline.tests.fixtures import get_test_patient_data
   test_data = get_test_patient_data(include_all_fields=False)
   ```

## File Structure

```
etl_pipeline/
├── setup_test_databases.py          # Refactored setup script
├── tests/
│   └── fixtures/
│       ├── __init__.py              # Package exports
│       ├── test_data_definitions.py # Standardized test data
│       ├── test_data_manager.py     # Test data management
│       └── integration_fixtures.py  # Pytest fixtures
```

## Benefits for Development

### 1. **Faster Test Development**
- Pre-built test data fixtures
- Standardized database setup
- Consistent test environments

### 2. **Reduced Errors**
- Type-safe database connections
- Validated test data
- Environment separation prevents accidents

### 3. **Better Collaboration**
- Shared test data definitions
- Consistent test patterns
- Clear documentation

### 4. **Easier Maintenance**
- Centralized test data management
- Single source of truth for schemas
- Automated cleanup and setup

## Future Enhancements

The new architecture enables future enhancements:

1. **Additional Test Data Types**: Easy to add new tables and data
2. **Schema Validation**: Automated schema verification
3. **Performance Testing**: Large dataset generation
4. **Multi-Environment Testing**: Support for staging, QA environments
5. **Data Quality Testing**: Built-in data quality checks

## Conclusion

The refactored `setup_test_databases.py` script now provides a robust, maintainable, and safe foundation for test database setup. It aligns with the new architectural patterns and provides a standardized approach to test data management that benefits all integration tests. 