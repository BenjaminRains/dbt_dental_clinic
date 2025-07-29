# E2E Test Suite

This directory contains the refactored end-to-end (E2E) tests for the ETL pipeline. The tests have been reorganized for better maintainability, isolation, and clarity.

## Test Structure

The E2E tests are now organized into separate files based on functionality:

### 📁 Test Files

- **`test_basic_pipeline_e2e.py`** - Basic pipeline functionality tests
  - Patient data pipeline
  - Appointment data pipeline
  - Procedure data pipeline
  - Multi-table pipeline
  - Data integrity validation

- **`test_incremental_methods_e2e.py`** - Incremental method tests
  - Incremental logic functionality
  - PostgresLoader incremental methods
  - SimpleMySQLReplicator incremental methods
  - Bulk incremental methods
  - Comprehensive incremental methods

- **`test_copy_strategies_e2e.py`** - Copy strategy tests
  - Full Copy strategy
  - Incremental Copy strategy
  - Bulk Copy strategy
  - Upsert Copy strategy

- **`test_validation_e2e.py`** - Data validation tests
  - UPSERT functionality validation
  - Integrity validation
  - Data quality validation
  - Comprehensive validation

- **`pipeline_data_validator.py`** - Shared validation utilities
  - PipelineDataValidator class
  - Data transformation validation methods
  - Incremental logic validation
  - Data quality checks

### 🏗️ Base Classes

Each test file uses base classes for common functionality:

- **`BasePipelineE2ETest`** - Common test setup and utilities
- **`BaseCopyStrategyE2ETest`** - Copy strategy test utilities
- **`BaseValidationE2ETest`** - Validation test utilities

## Key Improvements

### ✅ Better Organization
- **Single Responsibility**: Each test file focuses on one aspect of the pipeline
- **Clear Separation**: Tests are grouped by functionality (basic, incremental, copy strategies, validation)
- **Easier Maintenance**: Related tests are in the same file

### ✅ Improved Isolation
- **Function-level Cleanup**: Each test cleans up the replication database before running
- **No Test Interference**: Tests don't affect each other's data
- **Proper Fixtures**: Class-level fixtures for settings and validators, function-level for cleanup

### ✅ Enhanced Maintainability
- **Shared Infrastructure**: Common functionality in base classes
- **Reusable Components**: PipelineDataValidator can be used across all tests
- **Clear Documentation**: Each test has detailed docstrings with AAA pattern

### ✅ Better Test Execution
- **Selective Testing**: Run only the tests you need
- **Faster Execution**: Smaller test files run faster
- **Easier Debugging**: Failures are isolated to specific functionality

## Running Tests

### Using the Test Runner

```bash
# Run basic pipeline tests
python run_e2e_tests.py --basic

# Run incremental method tests
python run_e2e_tests.py --incremental

# Run copy strategy tests
python run_e2e_tests.py --copy-strategies

# Run validation tests
python run_e2e_tests.py --validation

# Run all tests
python run_e2e_tests.py --all

# Run with verbose output
python run_e2e_tests.py --basic --verbose

# Run with specific markers
python run_e2e_tests.py --basic --markers e2e slow
```

### Using pytest directly

```bash
# Run all E2E tests
pipenv run pytest tests/e2e/ -m e2e

# Run specific test file
pipenv run pytest tests/e2e/test_basic_pipeline_e2e.py

# Run specific test class
pipenv run pytest tests/e2e/test_basic_pipeline_e2e.py::TestBasicPipelineE2E

# Run specific test method
pipenv run pytest tests/e2e/test_basic_pipeline_e2e.py::TestBasicPipelineE2E::test_patient_data_test_pipeline_e2e

# Run tests with specific markers
pipenv run pytest tests/e2e/ -m "e2e and not slow"
```

## Test Categories

### 🔄 Basic Pipeline Tests
Tests the core ETL pipeline functionality:
- Patient data processing
- Appointment data processing
- Procedure data processing
- Multi-table processing
- Data integrity validation

**Markers**: `e2e`, `test_data`

### ⏱️ Incremental Method Tests
Tests different incremental loading methods:
- PostgresLoader incremental methods
- SimpleMySQLReplicator incremental methods
- Bulk incremental methods
- Comprehensive incremental scenarios

**Markers**: `e2e`, `incremental`, `postgres_loader`, `simple_mysql_replicator`, `bulk`, `comprehensive`

### 📋 Copy Strategy Tests
Tests different copy strategies:
- Full Copy strategy
- Incremental Copy strategy
- Bulk Copy strategy
- Upsert Copy strategy

**Markers**: `e2e`, `copy_strategy`, `full_copy`, `incremental_copy`, `bulk_copy`, `upsert_copy`

### ✅ Validation Tests
Tests data validation and integrity:
- UPSERT functionality
- Data integrity validation
- Data quality validation
- Comprehensive validation scenarios

**Markers**: `e2e`, `upsert`, `integrity`, `data_quality`, `comprehensive_validation`

## Test Isolation

Each test now properly isolates itself:

1. **Function-level Cleanup**: `clean_replication_db` fixture runs before each test
2. **Database Cleanup**: Removes test data from replication database
3. **No Cross-contamination**: Tests don't interfere with each other
4. **Proper Resource Management**: Connections are properly disposed

## Test Data

Tests use standardized test data:
- **Patient IDs**: 1, 2, 3
- **Appointment IDs**: 1, 2, 3
- **Procedure IDs**: 1, 2, 3
- **Consistent Environment**: Test databases with known data

## Migration from Old Structure

The original `test_production_data_pipeline_e2e.py` file has been refactored:

- **PipelineDataValidator**: Moved to `pipeline_data_validator.py`
- **Basic Tests**: Moved to `test_basic_pipeline_e2e.py`
- **Incremental Tests**: Moved to `test_incremental_methods_e2e.py`
- **Copy Strategy Tests**: Moved to `test_copy_strategies_e2e.py`
- **Validation Tests**: Moved to `test_validation_e2e.py`

## Benefits

1. **Faster Development**: Easier to find and modify specific tests
2. **Better Debugging**: Failures are isolated to specific functionality
3. **Improved CI/CD**: Can run specific test categories in parallel
4. **Easier Maintenance**: Related tests are grouped together
5. **Better Documentation**: Each test file has a clear purpose
6. **Reduced Complexity**: Smaller, focused test files

## Future Enhancements

- Add more specific test categories as needed
- Implement test data factories for more complex scenarios
- Add performance benchmarking tests
- Create test result reporting and analytics
- Add integration with monitoring and alerting systems 