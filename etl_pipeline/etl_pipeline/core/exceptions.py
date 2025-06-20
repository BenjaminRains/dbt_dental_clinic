"""
Custom exceptions for the ETL pipeline.
Provides specific exception types for different error scenarios.

UNDERUTILIZED AND UNTESTED CODE
==============================
This exceptions module is underutilized and has not been tested.

Current Status:
- Only ETLPipelineError is actively used (in main.py)
- 12 out of 13 exceptions are defined but never used
- No unit tests or integration tests exist
- Exception hierarchy is well-designed but not implemented
- All exceptions are exported from core/__init__.py but unused

Usage Analysis:
- ETLPipelineError: Used in main.py for configuration/health check failures
- ConfigurationError, DatabaseError, ValidationError, etc.: Never used
- No exception handling patterns established in the codebase

Testing Needed:
- Unit tests for all exception classes
- Integration tests for exception handling
- Validation of exception hierarchy and inheritance
- Test exception context and error messages
- Verify exception propagation through pipeline

TODO: Implement comprehensive exception handling throughout the pipeline
TODO: Add unit tests for all exception classes
TODO: Establish exception handling patterns and best practices
"""

class ETLPipelineError(Exception):
    """Base exception for ETL pipeline errors."""
    pass

class ConfigurationError(ETLPipelineError):
    """Raised when there are configuration issues."""
    pass

class DatabaseError(ETLPipelineError):
    """Raised when there are database-related errors."""
    pass

class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass

class QueryError(DatabaseError):
    """Raised when a database query fails."""
    pass

class SchemaError(DatabaseError):
    """Raised when there are schema-related issues."""
    pass

class ValidationError(ETLPipelineError):
    """Raised when data validation fails."""
    pass

class TransformationError(ETLPipelineError):
    """Raised when data transformation fails."""
    pass

class LoadingError(ETLPipelineError):
    """Raised when data loading fails."""
    pass

class MonitoringError(ETLPipelineError):
    """Raised when monitoring operations fail."""
    pass

class AlertError(MonitoringError):
    """Raised when alert sending fails."""
    pass

class MetricsError(MonitoringError):
    """Raised when metrics collection fails."""
    pass

class HealthCheckError(MonitoringError):
    """Raised when health checks fail."""
    pass 