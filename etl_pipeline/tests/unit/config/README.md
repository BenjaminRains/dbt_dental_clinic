# Configuration Tests

This directory contains consolidated and refactored tests for the ETL pipeline configuration system.

## Test Structure

### Consolidated Files

The original three overlapping test files have been consolidated into focused test files:

1. **`test_config_component_unit.py`** - Unit tests for component integration (mocked)
2. **`test_pipeline_config_file.py`** - Pipeline.yml file-specific tests

**Note**: Validation tests have been moved to `etl_pipeline/tests/comprehensive/config/` for comprehensive testing.

### Shared Components

- **`validation_utils.py`** - Shared validation functions to eliminate duplication
- **`conftest.py`** - Shared fixtures (in parent directory)

## Test Categories

### Component Integration Unit Tests (`test_config_component_unit.py`)
- Environment variable integration (mocked)
- Database connection string generation (mocked)
- Table configuration integration (mocked)
- Multi-environment support (mocked)
- Logging integration (mocked)
- Error handling across components (mocked)

### File Operation Tests (`test_pipeline_config_file.py`)
- YAML file loading
- File extension handling (.yml vs .yaml)
- Error handling for missing/invalid files
- Settings class integration
- Edge cases and error conditions

## Benefits of Consolidation

1. **Eliminated Duplication**: Removed ~80% of duplicate test code
2. **Shared Fixtures**: All tests now use consistent test data
3. **Shared Validation**: Common validation logic is centralized
4. **Clear Boundaries**: Each test file has a distinct, focused purpose
5. **Maintainability**: Changes to validation logic only need to be made in one place

## Running Tests

```bash
# Run all configuration tests
pytest etl_pipeline/tests/unit/config/

# Run specific test categories
pytest etl_pipeline/tests/unit/config/test_config_component_unit.py
pytest etl_pipeline/tests/unit/config/test_pipeline_config_file.py

# Run comprehensive validation tests
pytest etl_pipeline/tests/comprehensive/config/test_config_validation.py

# Run with markers
pytest -m validation
pytest -m unit
```

## Test Coverage

The consolidated tests maintain the same comprehensive coverage as the original files while eliminating redundancy:

- **Integration Coverage**: All component interactions
- **Validation Coverage**: All schema and business rule validation
- **File Operation Coverage**: All YAML loading and parsing scenarios
- **Error Handling Coverage**: All edge cases and error conditions

## Migration Notes

The original files have been removed and reorganized:
- `test_config_unit.py` → `test_config_component_unit.py` (unit tests with mocks)
- `test_config_validation_unit.py` → `test_config_validation.py` (moved to comprehensive/)
- `test_pipeline_config.py` → `test_pipeline_config_file.py` (unit tests)

**Test Organization:**
- **Unit Tests** (`etl_pipeline/tests/unit/config/`): Fast, mocked tests
- **Integration Tests** (`etl_pipeline/tests/integration/config/`): Real database/file tests
- **Comprehensive Tests** (`etl_pipeline/tests/comprehensive/config/`): Thorough validation tests

All existing test functionality has been preserved and enhanced with shared utilities. 