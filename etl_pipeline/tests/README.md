# ETL Pipeline Tests

This directory contains comprehensive tests for the ETL pipeline components.

## Test Structure

### Test Files
- `unit/core/test_postgres_schema_unit.py` - Pure unit tests with comprehensive mocking
- `unit/core/test_postgres_schema_simple.py` - Integration tests with real PostgreSQL database
- `unit/core/test_postgres_schema.py` - Main test file (hybrid approach)

### Test Strategy
The tests follow a hybrid testing approach:

1. **Unit Tests** (`test_postgres_schema_unit.py`)
   - Fast execution with comprehensive mocking
   - Isolated component behavior testing
   - No external dependencies

2. **Integration Tests** (`test_postgres_schema_simple.py`)
   - Real PostgreSQL database integration
   - Actual data flow testing
   - Safety and error handling validation

3. **Main Tests** (`test_postgres_schema.py`)
   - Hybrid approach combining unit and integration testing
   - Balanced coverage and performance

## PostgreSQL Integration Tests Setup

### Prerequisites
1. PostgreSQL server running locally or accessible
2. Python dependencies installed (see `requirements-test.txt`)

### Environment Variables
Set the following environment variables for PostgreSQL integration tests:

```bash
# PostgreSQL connection settings
export TEST_POSTGRES_HOST=localhost
export TEST_POSTGRES_PORT=5432
export TEST_POSTGRES_USER=postgres
export TEST_POSTGRES_PASSWORD=your_password
export TEST_POSTGRES_DB=test_analytics
```

### Default Configuration
If environment variables are not set, the tests will use these defaults:
- Host: `localhost`
- Port: `5432`
- User: `postgres`
- Password: `postgres`
- Database: `test_analytics`

### Database Setup
The integration tests will automatically:
1. Create the test database if it doesn't exist
2. Create the required schemas
3. Clean up after tests complete

## Running Tests

### Install Test Dependencies
```bash
pip install -r tests/requirements-test.txt
```

### Run All Tests
```bash
cd etl_pipeline
python -m pytest tests/ -v
```

### Run Unit Tests Only
```bash
python -m pytest tests/ -v -m unit
```

### Run Integration Tests Only
```bash
python -m pytest tests/ -v -m integration
```

### Run PostgreSQL Integration Tests
```bash
python -m pytest tests/unit/core/test_postgres_schema_simple.py -v -m integration
```

### Run with Coverage
```bash
python -m pytest tests/ --cov=etl_pipeline --cov-report=html
```

## Test Markers

- `@pytest.mark.unit` - Unit tests with mocking
- `@pytest.mark.integration` - Integration tests with real databases
- `@pytest.mark.postgres` - Tests requiring PostgreSQL

## Troubleshooting

### PostgreSQL Connection Issues
1. Ensure PostgreSQL server is running
2. Check connection parameters in environment variables
3. Verify user has permissions to create databases
4. Check firewall settings if using remote PostgreSQL

### Test Database Issues
1. Ensure the test user has `CREATEDB` privilege
2. Check if test database already exists and is locked
3. Verify no active connections to test database

### Dependencies Issues
1. Install PostgreSQL development headers if needed
2. Use `psycopg2-binary` for easier installation
3. Ensure SQLAlchemy version compatibility

## Test Data

The integration tests use:
- SQLite for MySQL simulation (safe, no external dependencies)
- Real PostgreSQL for actual database operations
- Sample dental clinic data structures
- Various data types and constraints

## Performance Considerations

- Unit tests: < 1 second execution time
- Integration tests: < 30 seconds execution time
- Database setup/teardown: < 5 seconds per test session

## Contributing

When adding new tests:
1. Follow the existing test structure
2. Use appropriate markers (`@pytest.mark.unit` or `@pytest.mark.integration`)
3. Add comprehensive docstrings
4. Include both positive and negative test cases
5. Test error conditions and edge cases 