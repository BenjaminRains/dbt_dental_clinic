# Real Integration Testing Approach

## Overview

This directory contains **real integration tests** that test actual component integration with 
minimal mocking. These tests use real SQLite databases and the actual `ConnectionFactory` class to
 test the complete data flow through the ETL pipeline.

## Key Improvements Over Previous Approach

### 1. **No New Environment Variables Needed**
- Uses **existing** environment variable names: `OPENDENTAL_SOURCE_*`, `MYSQL_REPLICATION_*`, `POSTGRES_ANALYTICS_*`
- No need to create test-specific environment variables
- Tests the real Settings class with real environment variable reading

### 2. **Real ConnectionFactory Testing**
- Tests the **actual** `ConnectionFactory.get_*_connection()` methods
- Only mocks the low-level `create_mysql_connection()` and `create_postgres_connection()` to return SQLite engines
- Tests real connection parameter validation and error handling

### 3. **Real Component Integration**
- Tests actual data flow through real components
- Verifies real schema discovery, table processing, and data movement
- Tests real error handling and cleanup

### 4. **Minimal Mocking Strategy**
```python
# Only mock the connection creation, not the entire ConnectionFactory
with patch('etl_pipeline.core.connections.ConnectionFactory.create_mysql_connection') as mock_mysql:
    def create_sqlite_connection(**kwargs):
        db_path = kwargs['database']  # Full SQLite file path
        return create_engine(f'sqlite:///{db_path}')
    
    mock_mysql.side_effect = create_sqlite_connection
    
    # Now test REAL components with REAL ConnectionFactory methods
    orchestrator = PipelineOrchestrator()
    success = orchestrator.initialize_connections()  # Uses real ConnectionFactory
```

## Test Files

### 1. `test_pipeline_orchestrator_real_integration.py`
- Tests real `PipelineOrchestrator` initialization and execution
- Tests real connection factory integration
- Tests real data flow verification
- Tests real error handling

### 2. `test_table_processor_real_integration.py`
- Tests real `TableProcessor` with actual data movement
- Tests multiple table processing
- Tests real schema discovery integration
- Tests real error handling

### 3. `test_schema_discovery_real_integration.py`
- Tests real `SchemaDiscovery` with actual SQLite databases
- Tests schema caching functionality
- Tests table size discovery
- Tests database schema overview

## Environment Variable Strategy

### Why Use Existing Variables?
1. **No Configuration Changes**: No need to modify `.env.template` or add new variables
2. **Real Settings Testing**: Tests the actual Settings class with real environment variable reading
3. **Consistency**: Uses the same variable names as production code
4. **Simplicity**: Less configuration to manage

### How It Works
```python
# Set environment variables to point to SQLite databases
test_env = {
    'OPENDENTAL_SOURCE_HOST': 'localhost',
    'OPENDENTAL_SOURCE_PORT': '3306',
    'OPENDENTAL_SOURCE_DB': source_db.name,  # Full SQLite file path
    'OPENDENTAL_SOURCE_USER': 'test_user',
    'OPENDENTAL_SOURCE_PASSWORD': 'test_password',
    # ... other variables
}

# Apply test environment
for key, value in test_env.items():
    os.environ[key] = value

# Now the real ConnectionFactory will use these values
# but we mock the connection creation to return SQLite engines
```

## SQLite Database Strategy

### Why SQLite?
1. **No External Dependencies**: No need for MySQL/PostgreSQL servers
2. **Fast**: In-memory or file-based databases
3. **Isolated**: Each test gets its own database
4. **Real SQL**: Tests actual SQL operations and data types

### Database Setup
```python
# Create temporary SQLite database files
source_db = tempfile.NamedTemporaryFile(suffix='_source.db', delete=False)
replication_db = tempfile.NamedTemporaryFile(suffix='_replication.db', delete=False)
analytics_db = tempfile.NamedTemporaryFile(suffix='_analytics.db', delete=False)

# Set up realistic test data
self._setup_test_data(source_db.name, replication_db.name, analytics_db.name)

# Clean up after tests
for db_file in [source_db.name, replication_db.name, analytics_db.name]:
    if os.path.exists(db_file):
        os.unlink(db_file)
```

## Running the Tests

### Run All Integration Tests
```bash
pytest tests/integration/ -v -m integration
```

### Run Specific Test File
```bash
pytest tests/integration/test_pipeline_orchestrator_real_integration.py -v
```

### Run with Coverage
```bash
pytest tests/integration/ -v -m integration --cov=etl_pipeline --cov-report=html
```

## Benefits of This Approach

### 1. **Real Integration Testing**
- Tests actual component interactions
- Tests real data flow through the pipeline
- Tests real error handling and recovery

### 2. **Minimal Mocking**
- Only mocks what's absolutely necessary (database connection creation)
- Tests real ConnectionFactory logic
- Tests real Settings class behavior

### 3. **Fast and Reliable**
- No external database dependencies
- Isolated test environments
- Consistent test results

### 4. **Maintainable**
- Uses existing environment variable names
- Clear test structure
- Easy to understand and modify

## Comparison with Previous Approach

| Aspect | Previous Approach | New Approach |
|--------|------------------|--------------|
| Mocking Level | Heavy mocking of ConnectionFactory | Minimal mocking of connection creation |
| Environment Variables | Required new test variables | Uses existing variables |
| Component Testing | Mocked components | Real components |
| Data Flow | Simulated | Real data movement |
| Error Handling | Mocked errors | Real error scenarios |
| Maintenance | High (many mocks) | Low (few mocks) |

## Migration from Old Tests

The old integration tests in the root `tests/` directory are now deprecated. They were heavily mocked and acted more like unit tests than integration tests.

### Deprecation Notice
- `test_pipeline_orchestrator_integration.py` → **DEPRECATED**
- `test_table_processor_integration.py` → **DEPRECATED**  
- `test_schema_discovery_integration.py` → **DEPRECATED**

### New Tests
- `test_pipeline_orchestrator_real_integration.py` → **RECOMMENDED**
- `test_table_processor_real_integration.py` → **RECOMMENDED**
- `test_schema_discovery_real_integration.py` → **RECOMMENDED**

## Future Enhancements

1. **Database Schema Validation**: Add tests to verify schema changes are detected
2. **Performance Testing**: Add tests for large dataset processing
3. **Concurrent Processing**: Test multiple tables processing simultaneously
4. **Recovery Testing**: Test pipeline recovery after failures
5. **Configuration Testing**: Test different configuration scenarios 