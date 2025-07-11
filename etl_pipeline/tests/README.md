
# ETL Pipeline Tests

This directory contains comprehensive tests for the ETL pipeline components using a modern **three-tier testing approach** with **provider pattern dependency injection**.

## Modern Testing Architecture

### Three-Tier Testing Strategy

The ETL pipeline uses a **modern static configuration approach** with **provider pattern dependency injection**:

1. **Unit Tests** (`test_[component]_unit.py`)
   - Pure unit tests with comprehensive mocking using `DictConfigProvider`
   - Fast execution (< 1 second), isolated behavior, no real connections
   - Uses `Settings` injection for environment-agnostic testing

2. **Comprehensive Tests** (`test_[component].py`)
   - Full functionality testing with mocked dependencies using `DictConfigProvider`
   - Complete component behavior, error handling, all methods
   - Target 90%+ coverage (main test suite)

3. **Integration Tests** (`test_[component]_integration.py`)
   - Real database integration with test environment using `FileConfigProvider`
   - MariaDB v11.6 → PostgreSQL data flow validation
   - Uses order markers for proper ETL data flow execution
   - Uses `.env_test` file for test environment isolation

### Provider Pattern Benefits

- **Dependency Injection**: Easy to swap configuration sources without code changes
- **Test Isolation**: Tests use completely isolated configuration (no environment pollution)
- **Settings Injection**: Environment-agnostic connections using `Settings` objects
- **FAIL FAST**: System fails immediately if `ETL_ENVIRONMENT` not explicitly set
- **Environment Separation**: Clear production/test environment handling

### Test Directory Structure

```
tests/
├── unit/                    # Pure unit tests (mocked with DictConfigProvider)
├── comprehensive/           # Comprehensive tests (mocked dependencies)
├── integration/             # Real test database integration (FileConfigProvider)
├── e2e/                    # Production connection E2E tests
├── fixtures/               # Test data and configuration fixtures
└── docs/                   # Test documentation
```

## Environment Setup

### Prerequisites
1. PostgreSQL server running locally or accessible
2. Python dependencies installed (see `requirements-test.txt`)

### Environment Variables

The ETL pipeline uses **explicit environment separation**:

```bash
# Set ETL environment (REQUIRED - FAIL FAST if not set)
export ETL_ENVIRONMENT=test

# Test environment variables (.env_test file)
export TEST_OPENDENTAL_SOURCE_HOST=localhost
export TEST_OPENDENTAL_SOURCE_PORT=3306
export TEST_OPENDENTAL_SOURCE_DB=test_opendental
export TEST_OPENDENTAL_SOURCE_USER=readonly_user
export TEST_OPENDENTAL_SOURCE_PASSWORD=your_password

export TEST_POSTGRES_ANALYTICS_HOST=localhost
export TEST_POSTGRES_ANALYTICS_PORT=5432
export TEST_POSTGRES_ANALYTICS_USER=analytics_test_user
export TEST_POSTGRES_ANALYTICS_PASSWORD=your_password
export TEST_POSTGRES_ANALYTICS_DB=test_opendental_analytics
export TEST_POSTGRES_ANALYTICS_SCHEMA=raw
```

### Database Setup
Integration tests automatically:
1. Create test databases if they don't exist
2. Create required schemas (raw, staging, intermediate, marts)
3. Clean up after tests complete

## Running Tests

### Run All Tests
```bash
cd etl_pipeline
python -m pytest tests/ -v
```

### Run by Test Type
```bash
# Unit tests only (fast, mocked)
python -m pytest tests/ -v -m unit

# Integration tests only (real databases)
python -m pytest tests/ -v -m integration

# Comprehensive tests (mocked dependencies)
python -m pytest tests/ -v -m comprehensive
```

### Run with Coverage
```bash
python -m pytest tests/ --cov=etl_pipeline --cov-report=html
```

## Test Markers

- `@pytest.mark.unit` - Unit tests with DictConfigProvider mocking
- `@pytest.mark.integration` - Integration tests with real databases
- `@pytest.mark.comprehensive` - Comprehensive tests with mocked dependencies
- `@pytest.mark.postgres` - Tests requiring PostgreSQL
- `@pytest.mark.mysql` - Tests requiring MySQL/MariaDB v11.6
- `@pytest.mark.order(n)` - Integration test execution order for ETL data flow
- `@pytest.mark.provider_pattern` - Tests using provider pattern dependency injection
- `@pytest.mark.settings_injection` - Tests using Settings injection

## ETL Pipeline Architecture

The ETL pipeline follows a **modern static configuration approach**:
- **Source**: MySQL OpenDental database (readonly)
- **Replication**: MySQL local copy for staging
- **Analytics**: PostgreSQL with raw schema (ETL output)
- **Transformations**: Handled by dbt (raw → staging → intermediate → marts)

### Provider Pattern Usage
- **Production**: `FileConfigProvider` with `.env_production` and `tables.yml`
- **Testing**: `DictConfigProvider` with injected test configuration
- **Integration**: `FileConfigProvider` with `.env_test` file

## Performance Considerations

- Unit tests: < 1 second execution time
- Comprehensive tests: < 5 seconds execution time
- Integration tests: < 10 seconds execution time
- Database setup/teardown: < 5 seconds per test session

## Contributing

When adding new tests:
1. Follow the three-tier testing approach
2. Use appropriate markers and order for integration tests
3. Use provider pattern for dependency injection
4. Use Settings injection for environment-agnostic connections
5. Include both positive and negative test cases
6. Test error conditions and edge cases
7. Document ETL context in docstrings 