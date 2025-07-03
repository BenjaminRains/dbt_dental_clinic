# Connection Environment Separation

## Overview

The `ConnectionFactory` class now provides a clear separation between production and test environment connections. This ensures that:

1. **Production code** uses production databases
2. **Test code** uses test databases
3. **No accidental cross-contamination** between environments
4. **Clear method naming** makes the environment explicit

## Connection Method Organization

### Production Environment Methods

All production methods use environment variables **without** the `TEST_` prefix:

```python
# Production MySQL connections
ConnectionFactory.get_opendental_source_connection()           # Uses OPENDENTAL_SOURCE_* env vars
ConnectionFactory.get_mysql_replication_connection()          # Uses MYSQL_REPLICATION_* env vars

# Production PostgreSQL connections
ConnectionFactory.get_postgres_analytics_connection()         # Uses POSTGRES_ANALYTICS_* env vars
ConnectionFactory.get_opendental_analytics_raw_connection()   # Uses POSTGRES_ANALYTICS_* env vars + raw schema
ConnectionFactory.get_opendental_analytics_staging_connection() # Uses POSTGRES_ANALYTICS_* env vars + staging schema
ConnectionFactory.get_opendental_analytics_intermediate_connection() # Uses POSTGRES_ANALYTICS_* env vars + intermediate schema
ConnectionFactory.get_opendental_analytics_marts_connection() # Uses POSTGRES_ANALYTICS_* env vars + marts schema
```

### Test Environment Methods

All test methods use environment variables **with** the `TEST_` prefix:

```python
# Test MySQL connections
ConnectionFactory.get_opendental_source_test_connection()     # Uses TEST_OPENDENTAL_SOURCE_* env vars
ConnectionFactory.get_mysql_replication_test_connection()    # Uses TEST_MYSQL_REPLICATION_* env vars

# Test PostgreSQL connections
ConnectionFactory.get_postgres_analytics_test_connection()   # Uses TEST_POSTGRES_ANALYTICS_* env vars
ConnectionFactory.get_opendental_analytics_raw_test_connection() # Uses TEST_POSTGRES_ANALYTICS_* env vars + raw schema
ConnectionFactory.get_opendental_analytics_staging_test_connection() # Uses TEST_POSTGRES_ANALYTICS_* env vars + staging schema
ConnectionFactory.get_opendental_analytics_intermediate_test_connection() # Uses TEST_POSTGRES_ANALYTICS_* env vars + intermediate schema
ConnectionFactory.get_opendental_analytics_marts_test_connection() # Uses TEST_POSTGRES_ANALYTICS_* env vars + marts schema
```

## Environment Variables

### Production Environment Variables

```bash
# OpenDental Source (Production)
OPENDENTAL_SOURCE_HOST=prod-dental-server.com
OPENDENTAL_SOURCE_PORT=3306
OPENDENTAL_SOURCE_DB=opendental
OPENDENTAL_SOURCE_USER=readonly_user
OPENDENTAL_SOURCE_PASSWORD=readonly_password

# MySQL Replication (Production)
MYSQL_REPLICATION_HOST=prod-repl-server.com
MYSQL_REPLICATION_PORT=3306
MYSQL_REPLICATION_DB=opendental_repl
MYSQL_REPLICATION_USER=repl_user
MYSQL_REPLICATION_PASSWORD=repl_password

# PostgreSQL Analytics (Production)
POSTGRES_ANALYTICS_HOST=prod-analytics-server.com
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_DB=opendental_analytics
POSTGRES_ANALYTICS_SCHEMA=raw
POSTGRES_ANALYTICS_USER=analytics_user
POSTGRES_ANALYTICS_PASSWORD=analytics_password
```

### Test Environment Variables

```bash
# OpenDental Source (Test)
TEST_OPENDENTAL_SOURCE_HOST=test-dental-server.com
TEST_OPENDENTAL_SOURCE_PORT=3306
TEST_OPENDENTAL_SOURCE_DB=test_opendental
TEST_OPENDENTAL_SOURCE_USER=test_user
TEST_OPENDENTAL_SOURCE_PASSWORD=test_password

# MySQL Replication (Test)
TEST_MYSQL_REPLICATION_HOST=test-repl-server.com
TEST_MYSQL_REPLICATION_PORT=3306
TEST_MYSQL_REPLICATION_DB=test_opendental_repl
TEST_MYSQL_REPLICATION_USER=test_replication_user
TEST_MYSQL_REPLICATION_PASSWORD=test_repl_password

# PostgreSQL Analytics (Test)
TEST_POSTGRES_ANALYTICS_HOST=test-analytics-server.com
TEST_POSTGRES_ANALYTICS_PORT=5432
TEST_POSTGRES_ANALYTICS_DB=test_opendental_analytics
TEST_POSTGRES_ANALYTICS_SCHEMA=raw
TEST_POSTGRES_ANALYTICS_USER=test_analytics_user
TEST_POSTGRES_ANALYTICS_PASSWORD=test_analytics_password
```

## Usage Guidelines

### In Production Code

Always use production methods:

```python
# ✅ CORRECT - Production code
from etl_pipeline.core.connections import ConnectionFactory

# For ETL pipeline
source_engine = ConnectionFactory.get_opendental_source_connection()
repl_engine = ConnectionFactory.get_mysql_replication_connection()
analytics_engine = ConnectionFactory.get_postgres_analytics_connection()

# For dbt models
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()
staging_engine = ConnectionFactory.get_opendental_analytics_staging_connection()
marts_engine = ConnectionFactory.get_opendental_analytics_marts_connection()
```

### In Test Code

Always use test methods:

```python
# ✅ CORRECT - Test code
from etl_pipeline.core.connections import ConnectionFactory

# For unit tests
source_engine = ConnectionFactory.get_opendental_source_test_connection()
repl_engine = ConnectionFactory.get_mysql_replication_test_connection()
analytics_engine = ConnectionFactory.get_postgres_analytics_test_connection()

# For integration tests
raw_engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
staging_engine = ConnectionFactory.get_opendental_analytics_staging_test_connection()
marts_engine = ConnectionFactory.get_opendental_analytics_marts_test_connection()
```

### ❌ WRONG - Never Mix Environments

```python
# ❌ WRONG - Mixing production and test
source_engine = ConnectionFactory.get_opendental_source_connection()  # Production
test_engine = ConnectionFactory.get_postgres_analytics_test_connection()  # Test

# ❌ WRONG - Using production in tests
def test_something():
    engine = ConnectionFactory.get_opendental_source_connection()  # Should use _test_connection

# ❌ WRONG - Using test in production
def production_function():
    engine = ConnectionFactory.get_opendental_source_test_connection()  # Should use production
```

## Schema-Specific Connections

### Production Schemas

```python
# Raw schema (default)
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()

# Staging schema
staging_engine = ConnectionFactory.get_opendental_analytics_staging_connection()

# Intermediate schema
intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_connection()

# Marts schema
marts_engine = ConnectionFactory.get_opendental_analytics_marts_connection()
```

### Test Schemas

```python
# Raw schema (default)
raw_engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()

# Staging schema
staging_engine = ConnectionFactory.get_opendental_analytics_staging_test_connection()

# Intermediate schema
intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_test_connection()

# Marts schema
marts_engine = ConnectionFactory.get_opendental_analytics_marts_test_connection()
```

## Testing Strategy

### Unit Tests

Unit tests use **pure mocks** and don't require real database connections:

```python
@patch('etl_pipeline.core.connections.create_engine')
def test_connection_creation(self, mock_create_engine):
    # Mock environment variables
    with patch.dict(os.environ, {
        'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
        'TEST_OPENDENTAL_SOURCE_PORT': '3306',
        # ... other test env vars
    }):
        engine = ConnectionFactory.get_opendental_source_test_connection()
        # Test with mocked engine
```

### Integration Tests

Integration tests use **real test database connections**:

```python
def test_real_connection(self):
    try:
        engine = ConnectionFactory.get_opendental_source_test_connection()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.fetchone()[0] == 1
    except Exception as e:
        pytest.skip(f"Test database not available: {str(e)}")
```

## Benefits

1. **Clear Separation**: No confusion about which environment is being used
2. **Safety**: Impossible to accidentally connect to production from test code
3. **Explicit Naming**: Method names clearly indicate the environment
4. **Consistent Patterns**: All test methods follow the same naming convention
5. **Easy Maintenance**: Clear organization makes it easy to add new connections
6. **Environment Isolation**: Test and production environments are completely separate

## Migration Guide

If you have existing code that doesn't follow this pattern:

1. **Identify the environment** your code runs in
2. **Choose the appropriate method** (production vs test)
3. **Update method calls** to use the correct environment
4. **Update environment variables** to match the method being used
5. **Test thoroughly** to ensure the correct environment is being used

## Example Migration

### Before (Unclear Environment)

```python
# ❌ Before - Unclear which environment
engine = ConnectionFactory.get_opendental_source_connection()
```

### After (Clear Environment)

```python
# ✅ After - Clear environment separation

# For production code
engine = ConnectionFactory.get_opendental_source_connection()

# For test code
engine = ConnectionFactory.get_opendental_source_test_connection()
```

This clear separation ensures that your code always connects to the intended environment and prevents accidental data corruption or security issues. 