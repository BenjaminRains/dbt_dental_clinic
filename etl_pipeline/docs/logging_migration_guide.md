# ETL Pipeline Logging System Migration Guide

## Overview

The ETL pipeline logging system has been consolidated from two separate systems into a single, 
unified logging system. This guide explains the changes and how to migrate existing code.

## What Changed

### Before (Deprecated)
- **`etl_pipeline.core.logger`**: Simple utility functions and ETLLogger class
- **`etl_pipeline.config.logging`**: Comprehensive logging configuration
- Two separate systems with different APIs and capabilities
- Confusion about which system to use

### After (Unified)
- **`etl_pipeline.config.logging`**: Single, comprehensive logging system
- Combines configuration, utilities, and ETL-specific methods
- Enhanced performance logging capabilities
- Environment variable support
- Multiple format types (simple, detailed, json)

## Migration Steps

### 1. Update Imports

**OLD (deprecated):**
```python
from etl_pipeline.core.logger import get_logger, ETLLogger
```

**NEW (recommended):**
```python
from etl_pipeline.config.logging import get_logger, ETLLogger
```

### 2. Update Usage

The API is **drop-in compatible** - no code changes needed for basic usage:

```python
# This works the same way in both systems
logger = get_logger(__name__)
etl_logger = ETLLogger(__name__)

# All existing methods work identically
etl_logger.log_etl_start("patients", "extraction")
etl_logger.log_etl_complete("patients", "extraction", 1000)
```

### 3. New Features Available

The unified system provides additional capabilities:

#### Performance Logging
```python
import time

start_time = time.time()
# ... perform operation ...
duration = time.time() - start_time

etl_logger.log_performance("data_extraction", duration, 5000)
# Output: [PERF] data_extraction completed in 2.50s | 5000 records | 2000 records/sec
```

#### Enhanced Configuration
```python
from etl_pipeline.config.logging import setup_logging

# Configure with different formats
setup_logging(
    log_level="DEBUG",
    log_file="etl_debug.log",
    log_dir="logs",
    format_type="json"  # or "simple", "detailed"
)
```

#### SQL Logging Control
```python
from etl_pipeline.config.logging import configure_sql_logging

# Enable SQL query logging for debugging
configure_sql_logging(enabled=True, level="DEBUG")
```

## Files That Need Migration

The following files currently use the deprecated logging system and should be updated:

1. `etl_pipeline/core/schema_discovery.py`
2. `etl_pipeline/core/postgres_schema.py`
3. `etl_pipeline/orchestration/pipeline_orchestrator.py`
4. `etl_pipeline/orchestration/table_processor.py`
5. `etl_pipeline/loaders/postgres_loader.py`
6. `etl_pipeline/cli/commands.py`

## Migration Checklist

- [ ] Update import statements in all files
- [ ] Test that logging functionality works as expected
- [ ] Consider using new performance logging features
- [ ] Update any custom logging configuration
- [ ] Run tests to ensure compatibility

## Deprecation Timeline

1. **Phase 1 (Current)**: Deprecation warnings shown when using old system
2. **Phase 2**: All imports migrated to new system
3. **Phase 3**: Old `core/logger.py` file removed
4. **Phase 4**: Old test file removed

## Testing

The new logging system has comprehensive tests in `etl_pipeline/tests/config/test_logging.py`:

- Setup and configuration tests
- ETL-specific method tests
- Performance logging tests
- Error handling tests
- Environment variable tests

## Environment Variables

The unified system supports these environment variables:

- `ETL_LOG_LEVEL`: Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `ETL_LOG_PATH`: Set log directory path

## Example Migration

### Before Migration
```python
from etl_pipeline.core.logger import get_logger, ETLLogger

logger = get_logger(__name__)
etl_logger = ETLLogger(__name__)

def process_table(table_name):
    etl_logger.log_etl_start(table_name, "extraction")
    # ... processing logic ...
    etl_logger.log_etl_complete(table_name, "extraction", 100)
```

### After Migration
```python
from etl_pipeline.config.logging import get_logger, ETLLogger
import time

logger = get_logger(__name__)
etl_logger = ETLLogger(__name__)

def process_table(table_name):
    start_time = time.time()
    etl_logger.log_etl_start(table_name, "extraction")
    
    # ... processing logic ...
    records_processed = 100
    
    duration = time.time() - start_time
    etl_logger.log_etl_complete(table_name, "extraction", records_processed)
    etl_logger.log_performance(f"{table_name}_extraction", duration, records_processed)
```

## Support

If you encounter issues during migration:

1. Check that the new import path is correct
2. Verify that the API is being used correctly
3. Review the comprehensive test suite for examples
4. Check the deprecation warnings for guidance

The unified logging system maintains full backward compatibility while providing enhanced capabilities for better ETL pipeline monitoring and debugging. 