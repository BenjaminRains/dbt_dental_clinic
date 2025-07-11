# Environment Setup Guide

## Overview

This guide explains how to properly set up environment files for the ETL pipeline according to the connection architecture principles.

## Connection Architecture Compliance

The ETL pipeline follows a **clean environment separation** approach where:

1. **Production and test environments are completely isolated**
2. **Environment variables are clearly separated by prefix**
3. **Single ETL_ENVIRONMENT variable controls which database connections to use**
4. **Provider pattern enables clean dependency injection**

## Environment File Setup

### Separate Environment Files (Recommended)

The ETL pipeline uses **separate environment files** for clean environment separation:

**Production File**: `.env_production`
```bash
ETL_ENVIRONMENT=production
OPENDENTAL_SOURCE_HOST=client-server
OPENDENTAL_SOURCE_DB=opendental
# ... only production variables
```

**Test File**: `.env_test`
```bash
ETL_ENVIRONMENT=test
TEST_OPENDENTAL_SOURCE_HOST=client-server
TEST_OPENDENTAL_SOURCE_DB=test_opendental
# ... only test variables
```

### Setup Instructions

1. **Create environment files**:
   ```bash
   # Create both files
   python scripts/setup_environments.py --both
   
   # Or create individually
   python scripts/setup_environments.py --production
   python scripts/setup_environments.py --test
   ```

2. **Configure each file** for your specific environment:
   - `.env_production`: Set production database connections
   - `.env_test`: Set test database connections

3. **Use appropriate file** for your operations:
   - Production ETL: Use `.env_production`
   - Testing/Development: Use `.env_test`

### Legacy Single File Approach (Deprecated)

**File**: `.env`

**Usage**:
1. Copy `.env.template` to `.env`
2. Set `ETL_ENVIRONMENT=production` or `ETL_ENVIRONMENT=test`
3. Configure variables for your chosen environment only

**Note**: This approach is deprecated in favor of separate environment files.

**Production File**: `.env_production`
```bash
ETL_ENVIRONMENT=production
OPENDENTAL_SOURCE_HOST=client-server
OPENDENTAL_SOURCE_DB=opendental
# ... only production variables
```

**Test File**: `.env_test`
```bash
ETL_ENVIRONMENT=test
TEST_OPENDENTAL_SOURCE_HOST=client-server
TEST_OPENDENTAL_SOURCE_DB=test_opendental
# ... only test variables
```

## Environment Variable Naming Convention

### Production Variables
- **Base names**: `OPENDENTAL_SOURCE_HOST`, `MYSQL_REPLICATION_HOST`, etc.
- **No prefix**: Used when `ETL_ENVIRONMENT=production`

### Test Variables
- **TEST_ prefix**: `TEST_OPENDENTAL_SOURCE_HOST`, `TEST_MYSQL_REPLICATION_HOST`, etc.
- **TEST_ prefix**: Used when `ETL_ENVIRONMENT=test`

## Database Types and Schemas

The architecture supports three database types with proper enum usage:

### Database Types
- `DatabaseType.SOURCE` - OpenDental MySQL (readonly)
- `DatabaseType.REPLICATION` - Local MySQL copy
- `DatabaseType.ANALYTICS` - PostgreSQL warehouse

### PostgreSQL Schemas
- `PostgresSchema.RAW` - Raw data
- `PostgresSchema.STAGING` - Staging data
- `PostgresSchema.INTERMEDIATE` - Intermediate data
- `PostgresSchema.MARTS` - Final marts

## Provider Pattern Integration

The environment setup works with the provider pattern:

### FileConfigProvider (Production)
```python
# Uses real environment variables and configuration files
settings = Settings(environment='production')
# Loads from:
# - pipeline.yml
# - tables.yml
# - os.environ (real environment variables)
```

### DictConfigProvider (Testing)
```python
# Uses injected configuration for testing
test_provider = DictConfigProvider(
    env={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
)
settings = Settings(environment='test', provider=test_provider)
```

## Best Practices

### 1. Environment Separation
- **Never mix production and test variables in the same environment**
- **Use separate files for different environments**
- **Clear naming convention with TEST_ prefix for test variables**

### 2. Configuration Validation
```python
# Validate configuration before use
settings = get_settings()
if not settings.validate_configs():
    raise ValueError("Configuration validation failed")
```

### 3. Environment Detection
```python
# The system detects environment in this priority order:
# 1. ETL_ENVIRONMENT environment variable
# 2. ENVIRONMENT environment variable
# 3. APP_ENV environment variable
# 4. Default to 'production'
```

### 4. Connection Usage
```python
# Unified interface works for both environments
settings = get_settings()
source_engine = ConnectionFactory.get_source_connection(settings)
replication_engine = ConnectionFactory.get_replication_connection(settings)
analytics_engine = ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
```

## Common Issues and Solutions

### Issue 1: Duplicate ETL_ENVIRONMENT
**Problem**: Multiple `ETL_ENVIRONMENT` declarations in same file
**Solution**: Use only one declaration per environment file

### Issue 2: Mixed Environment Variables
**Problem**: Production and test variables in same file
**Solution**: Use separate files or clear sections with comments

### Issue 3: Wrong Environment Detection
**Problem**: System using wrong environment
**Solution**: Ensure `ETL_ENVIRONMENT` is set correctly and has highest priority

### Issue 4: Missing Variables
**Problem**: Required variables not configured
**Solution**: Use `settings.validate_configs()` to check configuration completeness

## Testing Strategy

### Unit Tests
```python
# Use DictConfigProvider for pure dependency injection
test_provider = DictConfigProvider(
    env={'TEST_OPENDENTAL_SOURCE_HOST': 'test-host'}
)
settings = Settings(environment='test', provider=test_provider)
```

### Integration Tests
```python
# Use real test environment
settings = get_settings()  # Will use test environment if ETL_ENVIRONMENT=test
engine = ConnectionFactory.get_source_connection(settings)
```

## Migration Guide

### From Mixed Environment to Clean Separation

1. **Create separate environment files**:
   ```bash
   cp .env.template .env_production
   cp .env.template .env_test
   ```

2. **Configure each file for its environment**:
   - `.env_production`: Set `ETL_ENVIRONMENT=production`, configure production variables
   - `.env_test`: Set `ETL_ENVIRONMENT=test`, configure test variables

3. **Update deployment scripts** to use appropriate environment file

4. **Update CI/CD pipelines** to use test environment for testing

## Security Considerations

1. **Never commit real passwords** to version control
2. **Use environment-specific secrets management**
3. **Validate configuration** before use
4. **Use read-only connections** for source databases
5. **Implement proper access controls** for each environment

This setup ensures clean environment separation while maintaining the flexibility and safety outlined in the connection architecture. 