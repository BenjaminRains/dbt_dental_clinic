"""
ETL Pipeline Custom Exceptions

This module provides custom exception classes for the ETL pipeline to enable
better error handling, categorization, and recovery strategies.

STATUS: ACTIVE - Custom Exception Implementation
===============================================

This module contains custom exception classes that provide structured error
handling for the ETL pipeline, enabling better error categorization, context
preservation, and recovery strategies.

EXCEPTION HIERARCHY:
- ETLException (Base)
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

USAGE:
    from etl_pipeline.exceptions import (
        ETLException,
        SchemaTransformationError,
        DatabaseConnectionError,
        DataLoadingError
    )
    
    try:
        # ETL operation
        pass
    except SchemaTransformationError as e:
        logger.error(f"Schema error for {e.table_name}: {e}")
    except DatabaseConnectionError as e:
        logger.error(f"Connection error: {e}")
    except ETLException as e:
        logger.error(f"ETL error: {e}")

DEVELOPMENT NEEDS:
1. INTEGRATION: Update core components to use custom exceptions
2. TESTING: Add comprehensive tests for exception handling
3. DOCUMENTATION: Add usage examples and best practices
4. MONITORING: Integrate with logging and metrics collection
"""

from .base import ETLException
from .schema import (
    SchemaTransformationError,
    SchemaValidationError,
    TypeConversionError
)
from .database import (
    DatabaseConnectionError,
    DatabaseQueryError,
    DatabaseTransactionError
)
from .data import (
    DataExtractionError,
    DataTransformationError,
    DataLoadingError
)
from .configuration import (
    ConfigurationError,
    EnvironmentError
)

__all__ = [
    # Base exception
    'ETLException',
    
    # Schema exceptions
    'SchemaTransformationError',
    'SchemaValidationError',
    'TypeConversionError',
    
    # Database exceptions
    'DatabaseConnectionError',
    'DatabaseQueryError',
    'DatabaseTransactionError',
    
    # Data exceptions
    'DataExtractionError',
    'DataTransformationError',
    'DataLoadingError',
    
    # Configuration exceptions
    'ConfigurationError',
    'EnvironmentError',
] 