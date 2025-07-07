# Connection Architecture: Complete Guide

## Overview

The ETL pipeline uses a clean, modern connection architecture with explicit environment separation and clear separation of concerns. This document explains the complete connection handling system, including configuration management, connection creation, and usage patterns.

## Architecture Principles

### 1. Single Responsibility Principle
- **Settings Class**: Pure configuration management - reads environment variables and manages settings
- **ConnectionFactory Class**: Pure connection logic - creates database engines from configuration
- **ConnectionManager Class**: Connection lifecycle management - handles pooling, retries, and cleanup

### 2. Explicit Environment Separation
- **Production Environment**: Uses base environment variables (e.g., `OPENDENTAL_SOURCE_HOST`)
- **Test Environment**: Uses `TEST_` prefixed variables (e.g., `TEST_OPENDENTAL_SOURCE_HOST`)
- **No Automatic Detection**: Method names explicitly indicate the environment

### 3. Configuration Provider Pattern
- **FileConfigProvider**: Loads configuration from YAML files and environment variables
- **DictConfigProvider**: In-memory configuration for testing
- **Dependency Injection**: Easy to swap configuration sources

## Core Components

### 1. Settings Class (`etl_pipeline/config/settings.py`)

**Purpose**: Configuration management and environment detection.

**Key Features**:
- Environment detection from multiple sources
- Environment variable mapping for each database type
- Configuration caching for performance
- Validation of required configuration
- Support for both file-based and dictionary-based configuration

**Environment Detection**:
```python
# Priority order for environment detection
ETL_ENVIRONMENT > ENVIRONMENT > APP_ENV > 'production' (default)
```

**Database Types**:
```python
class DatabaseType(Enum):
    SOURCE = "source"           # OpenDental MySQL (readonly)
    REPLICATION = "replication" # Local MySQL copy  
    ANALYTICS = "analytics"     # PostgreSQL warehouse

class PostgresSchema(Enum):
    RAW = "raw"
    STAGING = "staging" 
    INTERMEDIATE = "intermediate"
    MARTS = "marts"
```

**Purpose of Enums**:
The enums provide type safety and prevent errors by ensuring only valid database types and schema names are used throughout the codebase. They serve as:

1. **Type Safety**: Prevents passing invalid strings as database types
2. **Documentation**: Self-documenting code that clearly shows available options
3. **IDE Support**: Enables autocomplete and refactoring support
4. **Validation**: Ensures configuration only uses valid enum values
5. **Maintainability**: Centralized definition of valid database types and schemas

**Usage Examples**:
```python
# ✅ CORRECT - Using enum values
settings.get_database_config(DatabaseType.SOURCE)
settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)

# ❌ WRONG - Using raw strings (will cause errors)
settings.get_database_config("source")  # Type error
settings.get_database_config("analytics", "raw")  # Type error

# Enum comparison and validation
if db_type == DatabaseType.SOURCE:
    # Handle source database logic
    pass

# Iterating through available schemas
for schema in PostgresSchema:
    print(f"Available schema: {schema.value}")
```

**Key Methods**:
```python
# Connection configuration methods (using enums internally)
settings.get_source_connection_config()           # Uses DatabaseType.SOURCE internally
settings.get_replication_connection_config()      # Uses DatabaseType.REPLICATION internally
settings.get_analytics_connection_config(schema)  # Uses DatabaseType.ANALYTICS + PostgresSchema

# Schema-specific methods (using PostgresSchema enum)
settings.get_analytics_raw_connection_config()        # Uses PostgresSchema.RAW
settings.get_analytics_staging_connection_config()    # Uses PostgresSchema.STAGING
settings.get_analytics_intermediate_connection_config() # Uses PostgresSchema.INTERMEDIATE
settings.get_analytics_marts_connection_config()      # Uses PostgresSchema.MARTS

# Direct enum usage methods
settings.get_database_config(DatabaseType.SOURCE)                    # Direct enum usage
settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)  # Direct enum usage

# Configuration validation
settings.validate_configs()  # Returns bool
```

**Enum Integration in Methods**:
The Settings class uses enums internally to ensure type safety. For example:

```python
def get_database_config(self, db_type: DatabaseType, schema: Optional[PostgresSchema] = None):
    """Get database configuration for specified type and optional schema."""
    # db_type parameter is typed as DatabaseType enum - prevents invalid values
    # schema parameter is typed as PostgresSchema enum - prevents invalid schemas
    
    cache_key = f"{db_type.value}_{schema.value if schema else 'default'}"
    # Uses .value to get the string representation for caching
    
    config = self._get_base_config(db_type)  # Passes enum to internal method
    # ...
```

### 2. ConnectionFactory Class (`etl_pipeline/core/connections.py`)

**Purpose**: Creates database engines with proper configuration and connection pooling.

**Key Features**:
- Explicit production and test connection methods
- Connection pooling with configurable parameters
- Database-specific optimizations (MySQL vs PostgreSQL)
- Comprehensive error handling and logging
- Rate limiting and retry logic

**Connection Pool Settings**:
```python
DEFAULT_POOL_SIZE = 5
DEFAULT_MAX_OVERFLOW = 10
DEFAULT_POOL_TIMEOUT = 30
DEFAULT_POOL_RECYCLE = 1800  # 30 minutes
```

#### Production Connection Methods

All production methods use environment variables **without** the `TEST_` prefix:

```python
# MySQL Production Connections
ConnectionFactory.get_opendental_source_connection()           # Uses OPENDENTAL_SOURCE_* env vars
ConnectionFactory.get_mysql_replication_connection()          # Uses MYSQL_REPLICATION_* env vars

# PostgreSQL Production Connections
ConnectionFactory.get_postgres_analytics_connection()         # Uses POSTGRES_ANALYTICS_* env vars
ConnectionFactory.get_opendental_analytics_raw_connection()   # Uses POSTGRES_ANALYTICS_* env vars + raw schema
ConnectionFactory.get_opendental_analytics_staging_connection() # Uses POSTGRES_ANALYTICS_* env vars + staging schema
ConnectionFactory.get_opendental_analytics_intermediate_connection() # Uses POSTGRES_ANALYTICS_* env vars + intermediate schema
ConnectionFactory.get_opendental_analytics_marts_connection() # Uses POSTGRES_ANALYTICS_* env vars + marts schema
```

#### Test Connection Methods

All test methods use environment variables **with** the `TEST_` prefix:

```python
# MySQL Test Connections
ConnectionFactory.get_opendental_source_test_connection()     # Uses TEST_OPENDENTAL_SOURCE_* env vars
ConnectionFactory.get_mysql_replication_test_connection()    # Uses TEST_MYSQL_REPLICATION_* env vars

# PostgreSQL Test Connections
ConnectionFactory.get_postgres_analytics_test_connection()   # Uses TEST_POSTGRES_ANALYTICS_* env vars
ConnectionFactory.get_opendental_analytics_raw_test_connection() # Uses TEST_POSTGRES_ANALYTICS_* env vars + raw schema
ConnectionFactory.get_opendental_analytics_staging_test_connection() # Uses TEST_POSTGRES_ANALYTICS_* env vars + staging schema
ConnectionFactory.get_opendental_analytics_intermediate_test_connection() # Uses TEST_POSTGRES_ANALYTICS_* env vars + intermediate schema
ConnectionFactory.get_opendental_analytics_marts_test_connection() # Uses TEST_POSTGRES_ANALYTICS_* env vars + marts schema
```

### 3. ConnectionManager Class (`etl_pipeline/core/connections.py`)

**Purpose**: Efficient connection management for batch operations.

**Key Features**:
- Single connection reuse for batch operations
- Automatic retry logic with exponential backoff
- Rate limiting to be respectful to source servers
- Proper connection cleanup through context managers

**Usage Pattern**:
```python
from etl_pipeline.core import create_connection_manager

# Efficient batch processing
source_engine = ConnectionFactory.get_opendental_source_connection()
with create_connection_manager(source_engine) as manager:
    result1 = manager.execute_with_retry("SELECT COUNT(*) FROM patient")
    result2 = manager.execute_with_retry("SELECT COUNT(*) FROM appointment")
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

## Usage Patterns

### 1. Production ETL Scripts

```python
from etl_pipeline.core import ConnectionFactory, create_connection_manager

def replicate_table(table_name: str):
    """Replicate a table from source to replication database."""
    
    # Get production connections (explicit)
    source_engine = ConnectionFactory.get_opendental_source_connection()
    replication_engine = ConnectionFactory.get_mysql_replication_connection()
    
    # Use connection manager for efficient batch operations
    with create_connection_manager(source_engine) as source_manager:
        # Extract data from source
        result = source_manager.execute_with_retry(f"SELECT * FROM {table_name}")
        data = result.fetchall()
        
        # Load to replication database
        with replication_engine.connect() as conn:
            # Insert data logic here
            pass
```

### 2. Schema-Specific Operations

```python
# Raw schema operations
raw_engine = ConnectionFactory.get_opendental_analytics_raw_connection()

# Staging schema operations
staging_engine = ConnectionFactory.get_opendental_analytics_staging_connection()

# Intermediate schema operations
intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_connection()

# Marts schema operations
marts_engine = ConnectionFactory.get_opendental_analytics_marts_connection()
```

### 3. Test Code

```python
# Unit tests use test connections (explicit)
def test_replicate_table():
    source_engine = ConnectionFactory.get_opendental_source_test_connection()
    replication_engine = ConnectionFactory.get_mysql_replication_test_connection()
    # Test logic here

# Integration tests use test connections (explicit)
def test_real_connection():
    try:
        engine = ConnectionFactory.get_opendental_source_test_connection()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.fetchone()[0] == 1
    except Exception as e:
        pytest.skip(f"Test database not available: {str(e)}")
```

## Configuration Files

### Pipeline Configuration (`pipeline.yml`)

```yaml
# Example pipeline configuration
connections:
  source:
    connect_timeout: 15
    read_timeout: 45
  replication:
    pool_size: 10
    max_overflow: 20
  analytics:
    application_name: "etl_pipeline_prod"
```

### Tables Configuration (`tables.yml`)

```yaml
# Example tables configuration
tables:
  patient:
    incremental_column: "DateModified"
    batch_size: 10000
    extraction_strategy: "incremental"
    table_importance: "critical"
    estimated_size_mb: 500
    estimated_rows: 100000
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

### Test Settings Creation

```python
from etl_pipeline.config.settings import create_test_settings, DatabaseType, PostgresSchema

# Create test settings with injected configuration
test_settings = create_test_settings(
    pipeline_config={'connections': {'source': {'connect_timeout': 5}}},
    tables_config={'tables': {'patient': {'batch_size': 1000}}},
    env_vars={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
)

# Using enums in test scenarios
def test_database_configs_with_enums(test_settings):
    """Test database configuration using enums."""
    # Test all database types using enums
    source_config = test_settings.get_database_config(DatabaseType.SOURCE)
    repl_config = test_settings.get_database_config(DatabaseType.REPLICATION)
    analytics_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
    
    assert source_config['database'] == 'test_opendental'
    assert repl_config['database'] == 'test_opendental_replication'
    assert analytics_config['schema'] == 'raw'

# Testing schema-specific configurations
def test_schema_configs_with_enums(test_settings):
    """Test schema-specific configurations using enums."""
    raw_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)
    staging_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.STAGING)
    intermediate_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.INTERMEDIATE)
    marts_config = test_settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.MARTS)
    
    assert raw_config['schema'] == 'raw'
    assert staging_config['schema'] == 'staging'
    assert intermediate_config['schema'] == 'intermediate'
    assert marts_config['schema'] == 'marts'
```

## Error Handling

### Configuration Errors

```python
try:
    engine = ConnectionFactory.get_opendental_source_connection()
except ValueError as e:
    logger.error(f"Configuration error: {e}")
    raise
except Exception as e:
    logger.error(f"Connection error: {e}")
    raise
```

### Connection Validation

```python
# Validate all configurations before starting ETL
settings = get_settings()
if not settings.validate_configs():
    logger.error("Configuration validation failed")
    sys.exit(1)
```

## Performance Optimizations

### 1. Connection Pooling
- **Pool Size**: 5 connections by default
- **Max Overflow**: 10 additional connections
- **Pool Timeout**: 30 seconds
- **Pool Recycle**: 30 minutes (prevents stale connections)

### 2. Configuration Caching
- Settings class caches connection configurations
- Reduces repeated environment variable lookups
- Improves performance for batch operations

### 3. Rate Limiting
- ConnectionManager adds 100ms delay between queries
- Prevents overwhelming source servers
- Configurable rate limiting parameters

### 4. Retry Logic
- Automatic retry with exponential backoff
- Maximum 3 retry attempts by default
- Fresh connection creation on retry

## Migration Guide

### From Legacy Code

1. **Replace ambiguous methods**:
   ```python
   # OLD (ambiguous)
   engine = ConnectionFactory.get_source_connection()
   
   # NEW (explicit)
   engine = ConnectionFactory.get_opendental_source_connection()  # Production
   # OR
   engine = ConnectionFactory.get_opendental_source_test_connection()  # Test
   ```

2. **Use environment variables**:
   ```python
   # OLD
   host = os.getenv('DB_HOST')
   port = os.getenv('DB_PORT')
   
   # NEW
   # All handled automatically by ConnectionFactory with explicit environment separation
   ```

3. **Add proper error handling**:
   ```python
   try:
       engine = ConnectionFactory.get_opendental_source_connection()
   except ValueError as e:
       logger.error(f"Configuration error: {e}")
       raise
   except Exception as e:
       logger.error(f"Connection error: {e}")
       raise
   ```

## Best Practices

### 1. Always Use Explicit Environment Methods
- Production code: Use methods without "test" in the name
- Test code: Use methods with "test" in the name
- Never mix environments in the same code path

### 2. Use ConnectionManager for Batch Operations
- Efficient connection reuse
- Automatic retry logic
- Proper resource cleanup

### 3. Validate Configuration Early
- Call `settings.validate_configs()` at startup
- Fail fast if configuration is incomplete
- Clear error messages for missing variables

### 4. Use Schema-Specific Connections
- Raw schema: `get_opendental_analytics_raw_connection()`
- Staging schema: `get_opendental_analytics_staging_connection()`
- Intermediate schema: `get_opendental_analytics_intermediate_connection()`
- Marts schema: `get_opendental_analytics_marts_connection()`

### 5. Handle Errors Gracefully
- Catch specific exception types
- Log meaningful error messages
- Implement proper cleanup in error cases

## Architecture Benefits

1. **Clear Separation of Concerns**: Configuration vs. connection logic
2. **Explicit Environment Separation**: No accidental cross-contamination
3. **Safety**: Impossible to accidentally use wrong environment
4. **Performance**: Optimized connection pooling and caching
5. **Maintainability**: Clean, well-documented architecture
6. **Testability**: Easy to mock and test individual components
7. **Flexibility**: Easy to add new database types or environments
8. **Reliability**: Comprehensive error handling and retry logic
9. **Type Safety**: Enums prevent invalid database types and schema names
10. **Developer Experience**: IDE autocomplete and refactoring support for database types

This architecture provides a robust, maintainable, and safe foundation for all ETL operations with clear separation between production and test environments. 