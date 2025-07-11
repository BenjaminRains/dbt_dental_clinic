# Connection Architecture: Complete Guide

## Overview

The ETL pipeline uses a clean, modern connection architecture with explicit environment separation,
 clear separation of concerns, and a robust provider pattern for dependency injection. This document
  explains the complete connection handling system, including configuration management, connection
   creation, and usage patterns.

## Architecture Principles

### 1. Single Responsibility Principle
- **Settings Class**: Pure configuration management - uses providers to get configuration
- **ConfigProvider Interface**: Abstract configuration source - enables dependency injection
- **ConnectionFactory Class**: Pure connection logic - creates database engines from configuration
- **ConnectionManager Class**: Connection lifecycle management - handles pooling, retries, and cleanup

### 2. Unified Interface with Settings Injection
- **Environment Agnostic**: Same methods work for both production and test environments
- **Settings Injection**: All connection methods take Settings objects as parameters
- **Provider Pattern**: Configuration source determined by Settings provider
- **No Method Proliferation**: Single method per database type, not per environment

### 3. Configuration Provider Pattern (Dependency Injection)
- **ConfigProvider Interface**: Abstract base for all configuration sources
- **FileConfigProvider**: Loads configuration from YAML files and real environment variables
- **DictConfigProvider**: In-memory configuration for testing with injected values
- **Dependency Injection**: Easy to swap configuration sources without code changes

## Core Components

### 1. ConfigProvider Interface (`etl_pipeline/config/providers.py`)

**Purpose**: Abstract configuration source that enables dependency injection.

**Key Features**:
- Abstract interface for all configuration sources
- Consistent API for pipeline, tables, and environment configuration
- Enables easy swapping between production and test configurations
- Supports both file-based and in-memory configuration

**Provider Types**:

#### FileConfigProvider (Production)
```python
class FileConfigProvider(ConfigProvider):
    def get_config(self, config_type: str) -> Dict[str, Any]:
        if config_type == 'pipeline':
            return self._load_yaml_config('pipeline')  # Loads pipeline.yml
        elif config_type == 'tables':
            return self._load_yaml_config('tables')    # Loads tables.yml
        elif config_type == 'env':
            # Load environment variables from appropriate .env file
            env_file = f".env_{self.environment}" if hasattr(self, 'environment') else ".env"
            return self._load_env_file(env_file) or dict(os.environ)
```

#### DictConfigProvider (Testing)
```python
class DictConfigProvider(ConfigProvider):
    def __init__(self, **configs):
        self.configs = {
            'pipeline': configs.get('pipeline', {}),
            'tables': configs.get('tables', {'tables': {}}),
            'env': configs.get('env', {})              # Injected test env vars
        }
```

### 2. Settings Class (`etl_pipeline/config/settings.py`)

**Purpose**: Configuration management using provider pattern for dependency injection.

**Key Features**:
- Environment detection from multiple sources
- Environment variable mapping for each database type
- Configuration caching for performance
- Validation of required configuration
- **Provider-based configuration loading** (not direct env var reading)
- Support for both file-based and dictionary-based configuration through providers

**Environment Detection**:
```python
# Environment detection - only ETL_ENVIRONMENT variable
ETL_ENVIRONMENT = 'production' or 'test'

# The get_settings() function automatically detects the environment:
# - If ETL_ENVIRONMENT=production: Uses production configuration
# - If ETL_ENVIRONMENT=test: Uses test configuration  
# - If not set: FAILS FAST with clear error message (no defaults)
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
The enums provide type safety and prevent errors by ensuring only valid database types and schema
 names are used throughout the codebase. They serve as:

1. **Type Safety**: Prevents passing invalid strings as database types
2. **Documentation**: Self-documenting code that clearly shows available options
3. **IDE Support**: Enables autocomplete and refactoring support
4. **Validation**: Ensures configuration only uses valid enum values
5. **Maintainability**: Centralized definition of valid database types and schemas

**Provider Integration**:
The Settings class uses the provider pattern to load configuration:

```python
class Settings:
    def __init__(self, environment: Optional[str] = None, provider = None):
        # Provider setup with dependency injection
        if provider is None:
            from .providers import FileConfigProvider
            provider = FileConfigProvider(Path(__file__).parent)
        self.provider = provider
        
        # Load configurations from provider (not direct env var reading)
        self.pipeline_config = self.provider.get_config('pipeline')
        self.tables_config = self.provider.get_config('tables') 
        self._env_vars = self.provider.get_config('env')  # Provider's env vars
```

**Configuration Flow**:
```
Settings → Provider → Configuration Sources
                ↓
        ┌─────────────────┐
        │ FileConfigProvider │ (Production)
        │ - pipeline.yml   │
        │ - tables.yml     │
        │ - os.environ     │
        └─────────────────┘
                ↓
        ┌─────────────────┐
        │ DictConfigProvider │ (Testing)
        │ - Injected configs │
        │ - Mock env vars   │
        └─────────────────┘
```

**Usage Examples**:
```python
# ✅ CORRECT - Using enum values
settings.get_database_config(DatabaseType.SOURCE)
settings.get_database_config(DatabaseType.ANALYTICS, PostgresSchema.RAW)

# Using raw strings (will cause errors)
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

# Provider-based configuration (internal)
settings._get_base_config(db_type)  # Uses provider's env vars first, falls back to os.getenv
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

**Provider-Based Configuration Loading**:
The Settings class uses the provider pattern for all configuration loading:

```python
def _get_base_config(self, db_type: DatabaseType) -> Dict:
    """Get base configuration from provider's environment variables."""
    env_mapping = self.ENV_MAPPINGS[db_type]
    config = {}
    
    for key, env_var in env_mapping.items():
        prefixed_var = f"{self.env_prefix}{env_var}"
        
        # ✅ Use provider's environment variables first, fallback to os.getenv
        value = self._env_vars.get(prefixed_var) or self._env_vars.get(env_var)
        if value is None:
            value = os.getenv(prefixed_var) or os.getenv(env_var)
        
        config[key] = value
    
    return config
```

### 3. ConnectionFactory Class (`etl_pipeline/core/connections.py`)

**Purpose**: Creates database engines with proper configuration and connection pooling using Settings injection.

**Key Features**:
- **Unified interface** with Settings injection for all environments
- Connection pooling with configurable parameters
- Database-specific optimizations (MySQL vs PostgreSQL)
- Comprehensive error handling and logging
- Rate limiting and retry logic
- **Settings integration** for configuration (recommended approach)

**Connection Pool Settings**:
```python
DEFAULT_POOL_SIZE = 5
DEFAULT_MAX_OVERFLOW = 10
DEFAULT_POOL_TIMEOUT = 30
DEFAULT_POOL_RECYCLE = 1800  # 30 minutes
```

#### Unified Connection Methods with Settings Injection

All connection methods use **Settings injection** for environment-agnostic operation:

```python
# Unified interface - works for both production and test environments
ConnectionFactory.get_source_connection(settings)                    # MySQL source database
ConnectionFactory.get_replication_connection(settings)              # MySQL replication database
ConnectionFactory.get_analytics_connection(settings, schema)        # PostgreSQL analytics with schema

# Convenience methods for common schemas
ConnectionFactory.get_analytics_raw_connection(settings)            # PostgreSQL raw schema
ConnectionFactory.get_analytics_staging_connection(settings)        # PostgreSQL staging schema
ConnectionFactory.get_analytics_intermediate_connection(settings)   # PostgreSQL intermediate schema
ConnectionFactory.get_analytics_marts_connection(settings)          # PostgreSQL marts schema
```

**Benefits of Unified Interface**:
- **Single method per database type** - no method proliferation
- **Environment agnostic** - same method works for production and test
- **Settings injection** - follows dependency injection principles
- **Type safety** - uses DatabaseType and PostgresSchema enums
- **Consistent API** - uniform interface across all environments

### 4. ConnectionManager Class (`etl_pipeline/core/connections.py`)

**Purpose**: Efficient connection management for batch operations.

**Key Features**:
- Single connection reuse for batch operations
- Automatic retry logic with exponential backoff
- Rate limiting to be respectful to source servers
- Proper connection cleanup through context managers

**Usage Pattern**:
```python
from etl_pipeline.core import create_connection_manager
from etl_pipeline.config import get_settings

# Efficient batch processing with Settings injection
settings = get_settings()
source_engine = ConnectionFactory.get_source_connection(settings)
with create_connection_manager(source_engine) as manager:
    result1 = manager.execute_with_retry("SELECT COUNT(*) FROM patient")
    result2 = manager.execute_with_retry("SELECT COUNT(*) FROM appointment")
```

## Environment Files and Variables

The ETL pipeline uses **separate environment files** for clean environment separation, following the principles outlined in the [Environment Setup Guide](environment_setup.md).

### Environment File Architecture

The system supports two separate environment files for complete isolation:

#### Production Environment File (`.env_production`)
```bash
# Environment declaration
ETL_ENVIRONMENT=production

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

#### Test Environment File (`.env_test`)
```bash
# Environment declaration
ETL_ENVIRONMENT=test

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

### Environment File Loading

The system automatically loads the correct environment file based on the `ETL_ENVIRONMENT` variable:

**⚠️ CRITICAL SECURITY REQUIREMENT**: The system will FAIL FAST if `ETL_ENVIRONMENT` is not
 explicitly set. No defaulting to production is allowed under any circumstances.

1. **Environment Detection**: The system uses only the `ETL_ENVIRONMENT` variable:
   - `ETL_ENVIRONMENT=production` - Uses production environment
   - `ETL_ENVIRONMENT=test` - Uses test environment

2. **File Loading**: The `FileConfigProvider` loads the appropriate `.env_{environment}` file:
   - Production: Loads `.env_production` (contains non-prefixed variables)
   - Test: Loads `.env_test` (contains TEST_ prefixed variables)

3. **Variable Resolution**: The Settings class automatically handles variable naming:
   ```python
   # Settings automatically prefixes variables for test environment
   settings = get_settings()  # Automatically uses correct environment
   config = settings.get_source_connection_config()  # Uses correct variable names
   ```
   
   **Variable Resolution Logic**:
- **Production**: Loads `.env_production` and uses `OPENDENTAL_SOURCE_HOST` from that file
- **Test**: Loads `.env_test` and uses `TEST_OPENDENTAL_SOURCE_HOST` from that file
   
   **⚠️ CRITICAL**: If ETL_ENVIRONMENT is not set, the system will FAIL FAST with a clear error
    message. No defaulting to production is allowed for security reasons.

### Environment Variable Naming Convention

#### Production Variables
- **Base names**: `OPENDENTAL_SOURCE_HOST`, `MYSQL_REPLICATION_HOST`, etc.
- **No prefix**: Used when `ETL_ENVIRONMENT=production`
- **File**: Defined in `.env_production`

#### Test Variables  
- **TEST_ prefix**: `TEST_OPENDENTAL_SOURCE_HOST`, `TEST_MYSQL_REPLICATION_HOST`, etc.
- **TEST_ prefix**: Used when `ETL_ENVIRONMENT=test`
- **File**: Defined in `.env_test`

**⚠️ CRITICAL**: Each environment file contains ONLY the variables for that environment:
- `.env_production` contains ONLY non-prefixed variables (OPENDENTAL_SOURCE_HOST, etc.)
- `.env_test` contains ONLY TEST_ prefixed variables (TEST_OPENDENTAL_SOURCE_HOST, etc.)
- The system automatically prefixes variables with "TEST_" when loading test environment
- No fallback logic is needed - each file is the single source of truth for its environment

### Benefits of Separate Environment Files

1. **Complete Isolation**: Production and test configurations are completely separate
2. **No Environment Pollution**: Test variables don't affect production
3. **Clear Naming**: TEST_ prefix makes test variables easily identifiable
4. **Single Source of Truth**: Each environment has its own dedicated file
5. **Easy Deployment**: Different environments can use different files
6. **Security**: Test credentials are separate from production credentials
7. **Simple Variable Resolution**: Each file contains only the variables it needs - no complex fallback logic required

### Provider Integration with Environment Files

The provider pattern works seamlessly with the separate environment files:

#### FileConfigProvider (Production)
```python
# Automatically loads .env_production when ETL_ENVIRONMENT=production
settings = Settings(environment='production')
# Loads from:
# - pipeline.yml
# - tables.yml
# - .env_production (contains: OPENDENTAL_SOURCE_HOST, MYSQL_REPLICATION_HOST, etc.)
```

#### FileConfigProvider (Test)
```python
# Automatically loads .env_test when ETL_ENVIRONMENT=test
settings = Settings(environment='test')
# Loads from:
# - pipeline.yml
# - tables.yml
# - .env_test (contains: TEST_OPENDENTAL_SOURCE_HOST, TEST_MYSQL_REPLICATION_HOST, etc.)
```

#### DictConfigProvider (Testing)
```python
# Uses injected configuration for unit testing
test_provider = DictConfigProvider(
    env={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
)
settings = Settings(environment='test', provider=test_provider)
```

**Variable Resolution in Settings**:
The Settings class uses environment-specific variable mappings:
```python
# Settings knows what variable names to look for in each environment
env_mapping = self.ENV_MAPPINGS[self.environment][db_type]  # Gets correct mapping for environment
value = self._env_vars.get(env_var)  # Uses variables from loaded .env file
```

**Environment-Specific Mappings**:
- **Production**: Settings looks for `OPENDENTAL_SOURCE_HOST` in `.env_production`
- **Test**: Settings looks for `TEST_OPENDENTAL_SOURCE_HOST` in `.env_test`

## Usage Patterns

### 1. Production ETL Scripts

```python
from etl_pipeline.core import ConnectionFactory, create_connection_manager
from etl_pipeline.config import get_settings

def replicate_table(table_name: str):
    """Replicate a table from source to replication database."""
    
    # Get settings for environment-agnostic connections
    settings = get_settings()
    
    # Get connections using unified interface
    source_engine = ConnectionFactory.get_source_connection(settings)
    replication_engine = ConnectionFactory.get_replication_connection(settings)
    
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
from etl_pipeline.config import get_settings

# Get settings for environment-agnostic connections
settings = get_settings()

# Raw schema operations
raw_engine = ConnectionFactory.get_analytics_raw_connection(settings)

# Staging schema operations
staging_engine = ConnectionFactory.get_analytics_staging_connection(settings)

# Intermediate schema operations
intermediate_engine = ConnectionFactory.get_analytics_intermediate_connection(settings)

# Marts schema operations
marts_engine = ConnectionFactory.get_analytics_marts_connection(settings)
```

### 3. Test Code

```python
from etl_pipeline.config import create_test_settings

# Unit tests use test settings with provider injection
def test_replicate_table():
    test_settings = create_test_settings(
        env_vars={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
    )
    source_engine = ConnectionFactory.get_source_connection(test_settings)
    replication_engine = ConnectionFactory.get_replication_connection(test_settings)
    # Test logic here

# Integration tests use test environment settings
def test_real_connection():
    try:
        # Uses test environment settings explicitly
        settings = Settings(environment='test')  # Explicit test environment
        engine = ConnectionFactory.get_source_connection(settings)
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

## Provider Integration

### Configuration Flow Architecture

The provider pattern enables clean dependency injection throughout the configuration system:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │───▶│     Settings    │───▶│    Provider     │
│                 │    │                 │    │                 │
│ - ETL Scripts   │    │ - Environment   │    │ - FileConfig    │
│ - Tests         │    │   Detection     │    │ - DictConfig    │
│ - API Endpoints │    │ - Config Caching│    │ - Custom Config │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │ ConnectionFactory│    │ Configuration   │
                       │                 │    │ Sources         │
                       │ - Engine Creation│    │                 │
                       │ - Pool Settings │    │ - pipeline.yml  │
                       └─────────────────┘    │ - tables.yml    │
                                               │ - os.environ    │
                                               │ - Test Configs  │
                                               └─────────────────┘
```

### Provider Usage Patterns

#### Production Usage (FileConfigProvider)
```python
# Default behavior - uses FileConfigProvider
settings = Settings(environment='production')
# Loads from:
# - pipeline.yml
# - tables.yml  
# - os.environ (real environment variables)

# Explicit FileConfigProvider usage
from etl_pipeline.config.providers import FileConfigProvider
provider = FileConfigProvider(Path('/path/to/config'))
settings = Settings(environment='production', provider=provider)
```

#### Testing Usage (DictConfigProvider)
```python
# Unit testing with injected configuration
from etl_pipeline.config.providers import DictConfigProvider

test_provider = DictConfigProvider(
    pipeline={'connections': {'source': {'pool_size': 5}}},
    tables={'tables': {'patient': {'batch_size': 1000}}},
    env={
        'TEST_OPENDENTAL_SOURCE_HOST': 'test-host',
        'TEST_OPENDENTAL_SOURCE_DB': 'test_db',
        'TEST_OPENDENTAL_SOURCE_USER': 'test_user',
        'TEST_OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
    }
)

settings = Settings(environment='test', provider=test_provider)
# Uses injected configuration instead of real files/env vars
```

#### Factory Functions for Testing
```python
from etl_pipeline.config.settings import create_test_settings

# Convenient test settings creation
test_settings = create_test_settings(
    pipeline_config={'connections': {'source': {'connect_timeout': 5}}},
    tables_config={'tables': {'patient': {'batch_size': 1000}}},
    env_vars={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
)
```

## Testing Strategy

### Unit Tests with Provider Pattern

Unit tests use **DictConfigProvider** for pure dependency injection:

```python
def test_database_config_with_provider():
    """Test database configuration using provider pattern."""
    # Create test provider with injected configuration
    test_provider = DictConfigProvider(
        pipeline={'connections': {'source': {'pool_size': 5}}},
        tables={'tables': {'patient': {'batch_size': 1000}}},
        env={
            'OPENDENTAL_SOURCE_HOST': 'test-host',
            'OPENDENTAL_SOURCE_DB': 'test_db',
            'OPENDENTAL_SOURCE_USER': 'test_user',
            'OPENDENTAL_SOURCE_PASSWORD': 'test_pass'
        }
    )
    
    settings = Settings(environment='test', provider=test_provider)
    
    # Test configuration loading from provider
    config = settings.get_source_connection_config()
    assert config['host'] == 'test-host'
    assert config['database'] == 'test_db'
    assert config['user'] == 'test_user'
    assert config['password'] == 'test_pass'
    
    # Test pipeline config overrides
    assert config['pool_size'] == 5
```

### Integration Tests with FileConfigProvider

Integration tests use **FileConfigProvider** with real configuration files:

```python
def test_real_config_loading():
    """Test loading configuration from real files."""
    # Uses default FileConfigProvider
    settings = Settings(environment='production')
    
    # Validate configuration is loaded correctly
    assert settings.validate_configs() is True
    
    # Test real configuration values
    config = settings.get_source_connection_config()
    assert config['host'] is not None
    assert config['database'] is not None
```



### Test Settings Creation with Enums

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
    settings = get_settings()
    engine = ConnectionFactory.get_source_connection(settings)
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

## Usage Guide

### Connection Creation

1. **Get Settings for Environment-Agnostic Connections**:
   ```python
   from etl_pipeline.config import get_settings
   
   # Get settings for current environment
   settings = get_settings()  # Environment determined by settings
   engine = ConnectionFactory.get_source_connection(settings)  # Works for both prod/test
   ```

2. **Use Settings for Configuration Access**:
   ```python
   # Get configuration from settings
   settings = get_settings()
   config = settings.get_database_config(DatabaseType.SOURCE)
   host = config['host']
   port = config['port']
   ```

3. **Add Proper Error Handling with Settings Validation**:
   ```python
   try:
       settings = get_settings()
       if not settings.validate_configs():
           raise ValueError("Configuration validation failed")
       engine = ConnectionFactory.get_source_connection(settings)
   except ValueError as e:
       logger.error(f"Configuration error: {e}")
       raise
   except Exception as e:
       logger.error(f"Connection error: {e}")
       raise
   ```

## Best Practices

### 1. Use Provider Pattern for Configuration
- **Production**: Use `FileConfigProvider` (default) for real configuration files
- **Testing**: Use `DictConfigProvider` for injected test configuration
- **Never**: Mix provider types in the same code path

### 2. Always Use Settings Injection
- Production code: Use `get_settings()` for production environment
- Test code: Use `create_test_settings()` for test environment
- **CRITICAL**: ETL_ENVIRONMENT must be explicitly set - no defaults
- Never mix environments in the same code path

### 3. Use Provider-Based Testing
```python
# Use DictConfigProvider for unit tests
def test_database_config():
    test_provider = DictConfigProvider(
        env={'OPENDENTAL_SOURCE_HOST': 'test-host'}
    )
    settings = Settings(environment='test', provider=test_provider)
    # Test with injected configuration
```

### 4. Use ConnectionManager for Batch Operations
- Efficient connection reuse
- Automatic retry logic
- Proper resource cleanup

### 5. Validate Configuration Early
- Call `settings.validate_configs()` at startup
- **CRITICAL**: ETL_ENVIRONMENT must be explicitly set - no defaults
- Fail fast if configuration is incomplete
- Clear error messages for missing variables

### 6. Use Schema-Specific Connections with Settings Injection
- Raw schema: `get_analytics_raw_connection(settings)`
- Staging schema: `get_analytics_staging_connection(settings)`
- Intermediate schema: `get_analytics_intermediate_connection(settings)`
- Marts schema: `get_analytics_marts_connection(settings)`

### 7. Handle Errors Gracefully
- Catch specific exception types
- Log meaningful error messages
- Implement proper cleanup in error cases

### 8. Use Factory Functions for Testing
```python
# Use factory functions for common test scenarios
test_settings = create_test_settings(
    pipeline_config={'connections': {'source': {'pool_size': 5}}},
    env_vars={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
)

# Use enums for type safety
config = test_settings.get_database_config(DatabaseType.SOURCE, PostgresSchema.RAW)
```

## Architecture Benefits

1. **Clear Separation of Concerns**: Configuration vs. connection logic
2. **Unified Interface**: Single method per database type, environment-agnostic
3. **Safety**: Impossible to accidentally use wrong environment through Settings injection
4. **Security**: FAIL FAST if ETL_ENVIRONMENT not set - no dangerous defaults
5. **Performance**: Optimized connection pooling and caching
6. **Maintainability**: Clean, well-documented architecture with no method proliferation
7. **Testability**: Easy to mock and test individual components with provider pattern
8. **Flexibility**: Easy to add new database types without environment-specific methods
9. **Reliability**: Comprehensive error handling and retry logic
10. **Type Safety**: Enums prevent invalid database types and schema names
11. **Developer Experience**: IDE autocomplete and refactoring support for database types
12. **Dependency Injection**: Provider pattern enables clean testing and configuration swapping
13. **Configuration Isolation**: Test configuration is completely isolated from production
14. **No Environment Pollution**: Tests don't affect real environment variables
15. **Consistent API**: Same interface for production and test configuration
16. **Extensibility**: Easy to add new provider types (e.g., database, API, etc.)
17. **No Method Proliferation**: Single method per database type instead of separate production/test methods

## Provider Pattern Benefits

### 1. **Dependency Injection**
- Easy to swap configuration sources without code changes
- Clean separation between production and test configuration
- No need to mock environment variables or files

### 2. **Test Isolation**
- Tests use completely isolated configuration
- No risk of test configuration affecting production
- No need to restore environment variables after tests

### 3. **Configuration Flexibility**
- Support for multiple configuration sources (files, environment, databases, APIs)
- Easy to add new configuration types
- Consistent interface across all configuration sources

### 4. **Type Safety**
- Enums ensure only valid database types and schemas are used
- Compile-time checking prevents runtime errors
- IDE support for autocomplete and refactoring

This architecture provides a robust, maintainable, and safe foundation for all ETL operations with
 clear separation between production and test environments, enabled by the provider pattern for
  clean dependency injection. 