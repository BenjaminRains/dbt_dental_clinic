"""
Custom exceptions for the ETL pipeline.
Provides specific exception types for different error scenarios.
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