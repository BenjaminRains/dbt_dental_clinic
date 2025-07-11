# ETL Pipeline Custom Exceptions

This module provides custom exception classes for the ETL pipeline to enable better error handling, categorization, and recovery strategies.

## Overview

The ETL pipeline uses custom exception classes to provide structured error handling with context preservation. This enables:

- **Better Error Categorization**: Distinguish between different types of errors (schema, database, data, configuration)
- **Enhanced Error Context**: Include table names, operations, and additional details
- **Improved Error Recovery**: Implement specific recovery strategies for different error types
- **Structured Logging**: Serialize exceptions for monitoring and alerting

## Exception Hierarchy

```
ETLException (Base)
├── SchemaTransformationError
├── SchemaValidationError
├── TypeConversionError
├── DatabaseConnectionError
├── DatabaseQueryError
├── DatabaseTransactionError
├── DataExtractionError
├── DataTransformationError
├── DataLoadingError
├── ConfigurationError
└── EnvironmentError
```

## Usage

### Basic Import

```python
from etl_pipeline.exceptions import (
    ETLException,
    SchemaTransformationError,
    DatabaseConnectionError,
    DataLoadingError
)
```

### Exception Handling

```python
try:
    # ETL operation
    schema.adapt_schema('patient', mysql_schema)
except SchemaTransformationError as e:
    logger.error(f"Schema error for {e.table_name}: {e}")
    # Implement schema-specific recovery
except DatabaseConnectionError as e:
    logger.error(f"Connection error: {e}")
    # Implement connection retry logic
except ETLException as e:
    logger.error(f"General ETL error: {e}")
    # Handle any other ETL errors
```

### Creating Custom Exceptions

```python
# Schema transformation error
raise SchemaTransformationError(
    message="Failed to convert TINYINT to boolean",
    table_name="patient",
    mysql_schema=mysql_schema,
    details={"unsupported_type": "TINYINT"}
)

# Database connection error
raise DatabaseConnectionError(
    message="Connection timeout",
    database_type="mysql",
    connection_params={"host": "localhost", "port": 3306},
    details={"timeout": 30}
)

# Data extraction error
raise DataExtractionError(
    message="Incremental extraction failed",
    table_name="patient",
    extraction_strategy="incremental",
    batch_size=1000
)
```

### Exception Serialization

```python
# Serialize exception for logging/monitoring
exc = DataExtractionError(
    message="Extraction timeout",
    table_name="large_table",
    extraction_strategy="full",
    batch_size=5000
)

exc_dict = exc.to_dict()
# Returns: {
#     'exception_type': 'DataExtractionError',
#     'message': 'Extraction timeout',
#     'table_name': 'large_table',
#     'operation': 'data_extraction',
#     'details': {...},
#     'original_exception': None
# }
```

## Exception Types

### Schema Exceptions

- **SchemaTransformationError**: MySQL to PostgreSQL schema conversion failures
- **SchemaValidationError**: Schema validation failures
- **TypeConversionError**: MySQL type to PostgreSQL type conversion failures

### Database Exceptions

- **DatabaseConnectionError**: Database connection failures
- **DatabaseQueryError**: SQL query execution failures
- **DatabaseTransactionError**: Database transaction failures

### Data Exceptions

- **DataExtractionError**: Data extraction from source failures
- **DataTransformationError**: Data transformation failures
- **DataLoadingError**: Data loading to target failures

### Configuration Exceptions

- **ConfigurationError**: Configuration validation failures
- **EnvironmentError**: Environment setup failures

## Error Recovery Strategies

### Schema Errors

```python
try:
    schema.adapt_schema('patient', mysql_schema)
except SchemaTransformationError as e:
    if "Unsupported MySQL type" in str(e):
        # Implement fallback type conversion
        logger.info("Attempting fallback type conversion...")
        return fallback_conversion(e.table_name, e.mysql_schema)
    else:
        raise
```

### Database Errors

```python
try:
    connection.execute(query)
except DatabaseConnectionError as e:
    if e.details.get("retry_count", 0) < 3:
        # Implement retry logic
        logger.info("Retrying connection...")
        return retry_connection(e.connection_params)
    else:
        raise
```

### Data Errors

```python
try:
    extractor.extract_data('patient')
except DataExtractionError as e:
    if e.extraction_strategy == "incremental":
        # Fall back to full extraction
        logger.info("Falling back to full extraction...")
        return extractor.extract_data_full('patient')
    else:
        raise
```

## Integration with Existing Code

### Current Approach (Generic)

```python
def create_postgres_table(self, table_name: str, mysql_schema: Dict) -> bool:
    try:
        # ... table creation logic ...
        return True
    except Exception as e:
        logger.error(f"Error creating PostgreSQL table {table_name}: {str(e)}")
        return False  # Generic exception handling
```

### Improved Approach (Specific)

```python
def create_postgres_table(self, table_name: str, mysql_schema: Dict) -> bool:
    try:
        # ... table creation logic ...
        return True
    except SchemaTransformationError as e:
        logger.error(f"Schema transformation failed for {table_name}: {e}")
        return False
    except DatabaseConnectionError as e:
        logger.error(f"Database connection failed for {table_name}: {e}")
        return False
    except DatabaseTransactionError as e:
        logger.error(f"Database transaction failed for {table_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error creating PostgreSQL table {table_name}: {str(e)}")
        return False
```

## Benefits

1. **Better Error Categorization**: Distinguish between different error types
2. **Enhanced Debugging**: Rich error context for troubleshooting
3. **Improved Recovery**: Implement specific recovery strategies
4. **Structured Logging**: Serialize exceptions for monitoring
5. **Better Testing**: Test specific error scenarios
6. **Maintainability**: Clear error handling patterns

## Testing

Run the exception tests:

```bash
pipenv run python -m pytest tests/unit/exceptions/ -v
```

## Examples

See `examples/exception_usage_example.py` for comprehensive usage examples.

## Future Enhancements

1. **Integration**: Update core components to use custom exceptions
2. **Monitoring**: Integrate with logging and metrics collection
3. **Recovery**: Implement automatic recovery strategies
4. **Documentation**: Add more usage examples and best practices 