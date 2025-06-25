# MySQL Replicator Test Suite - Hybrid Approach

This directory contains three test files for the MySQL replicator, implementing a hybrid testing strategy that balances speed, accuracy, and maintainability.

## Test Files Overview

### 1. `test_mysql_replicator_unit.py` - **Pure Unit Tests** ⚡
**Purpose**: Fast, focused unit tests for business logic
**Strategy**: Pure mocking, no database dependencies
**Markers**: `@pytest.mark.unit`
**When to run**: Every commit, during development
**Speed**: Very fast (< 1 second)

**What it tests**:
- Business logic validation
- Error handling scenarios
- Data transformation logic
- Configuration validation
- Edge case handling
- Schema normalization
- Query building logic

**Example usage**:
```bash
# Run only unit tests
pytest -m unit tests/unit/core/test_mysql_replicator_unit.py -v

# Run specific test class
pytest -m unit tests/unit/core/test_mysql_replicator_unit.py::TestSchemaNormalization -v
```

### 2. `test_mysql_replicator.py` - **Comprehensive Mocked Tests** 🎯
**Purpose**: Extensive unit tests with detailed mocking
**Strategy**: Mocked database connections, comprehensive coverage
**Markers**: `@pytest.mark.unit` (main tests), `@pytest.mark.integration`, `@pytest.mark.performance`, `@pytest.mark.idempotency`
**When to run**: Pull requests, before releases
**Speed**: Fast (1-5 seconds)

**What it tests**:
- All unit test scenarios from file 1
- Complex database interaction scenarios
- Primary key operations
- Chunked data copying
- Replica verification
- Performance benchmarks (skipped)
- Idempotency tests (skipped)

**Example usage**:
```bash
# Run all unit tests (excluding integration/performance/idempotency)
pytest -m "unit and not integration and not performance and not idempotency" tests/unit/core/test_mysql_replicator.py -v

# Run specific test class
pytest -m unit tests/unit/core/test_mysql_replicator.py::TestDataCopying -v
```

### 3. `test_mysql_replicator_simple.py` - **SQLite Integration Tests** 🔧
**Purpose**: Real database operations using SQLite
**Strategy**: SQLite databases for actual SQL testing
**Markers**: `@pytest.mark.integration`
**When to run**: When SQLite compatibility is implemented
**Speed**: Medium (5-30 seconds)
**Status**: Currently skipped due to MySQL/SQLite dialect differences

**What it tests**:
- Actual SQL operations
- Real data flow
- Database transaction handling
- Data integrity verification
- Error handling with real databases

**Example usage**:
```bash
# Run integration tests (when fixed)
pytest -m integration tests/unit/core/test_mysql_replicator_simple.py -v

# Currently all tests are skipped
pytest -m integration tests/unit/core/test_mysql_replicator_simple.py -v -s
```

## Running Tests by Category

### Fast Development Feedback
```bash
# Run only the fastest unit tests
pytest -m unit tests/unit/core/test_mysql_replicator_unit.py -v

# Run all unit tests (both files)
pytest -m unit tests/unit/core/ -v
```

### Pre-Pull Request Testing
```bash
# Run all unit tests except integration/performance/idempotency
pytest -m "unit and not integration and not performance and not idempotency" tests/unit/core/ -v

# Run with coverage
pytest -m "unit and not integration and not performance and not idempotency" tests/unit/core/ --cov=etl_pipeline.core.mysql_replicator --cov-report=term-missing
```

### Full Test Suite
```bash
# Run all tests (including skipped ones)
pytest tests/unit/core/ -v

# Run with coverage and HTML report
pytest tests/unit/core/ --cov=etl_pipeline.core.mysql_replicator --cov-report=html --cov-report=term-missing
```

### Specific Test Categories
```bash
# Performance tests (currently skipped)
pytest -m performance tests/unit/core/ -v

# Idempotency tests (currently skipped)
pytest -m idempotency tests/unit/core/ -v

# Critical tests only
pytest -m critical tests/unit/core/ -v
```

## Test Strategy Benefits

### 1. **Fast Feedback Loop**
- Unit tests run in < 1 second
- Developers get immediate feedback
- No database setup required

### 2. **Comprehensive Coverage**
- Multiple testing approaches
- Different levels of abstraction
- Edge cases and error scenarios

### 3. **Flexible Execution**
- Run different test levels based on context
- CI/CD can run fast tests on every commit
- Full suite runs before releases

### 4. **Maintainable**
- Clear separation of concerns
- Each test file has specific purpose
- Easy to understand and modify

## Future Improvements

### 1. **Fix SQLite Integration Tests**
- Create SQLite adapter for MySQL replicator
- Implement SQLite-compatible SQL generation
- Enable real database testing

### 2. **Add E2E Tests**
- Create `test_mysql_replicator_e2e.py`
- Use real MySQL test databases
- Test complete production scenarios

### 3. **Performance Testing**
- Set up test data generation
- Implement performance benchmarks
- Monitor memory usage

### 4. **Idempotency Testing**
- Create test database setup/teardown
- Test multiple execution scenarios
- Verify data consistency

## Test File Naming Convention

- `*_unit.py` - Pure unit tests with mocking
- `*_simple.py` - Integration tests with SQLite
- `*_e2e.py` - End-to-end tests with real databases
- `*.py` - Comprehensive tests with multiple categories

## Best Practices

1. **Always run unit tests first** during development
2. **Use appropriate markers** for test categorization
3. **Keep unit tests fast** (< 1 second total)
4. **Mock external dependencies** in unit tests
5. **Use real databases** only when necessary
6. **Document test purposes** clearly
7. **Maintain test isolation** between files 