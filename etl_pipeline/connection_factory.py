"""
Compatibility layer for database connections.
Redirects to the new ConnectionFactory class in core/connections.py.

DEPRECATED: This module is deprecated and will be removed in a future version.
Please update your imports to use etl_pipeline.core.connections.ConnectionFactory directly.
"""
import logging
import warnings
from functools import wraps
from etl_pipeline.core.connections import ConnectionFactory

logger = logging.getLogger(__name__)

def deprecation_warning(func):
    """Decorator to add deprecation warning to functions."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        warnings.warn(
            f"{func.__name__} is deprecated. Please use etl_pipeline.core.connections.ConnectionFactory.{func.__name__} instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return func(*args, **kwargs)
    return wrapper

# Re-export the functions to maintain backward compatibility
@deprecation_warning
def get_source_connection(*args, **kwargs):
    return ConnectionFactory.get_source_connection(*args, **kwargs)

@deprecation_warning
def get_staging_connection(*args, **kwargs):
    return ConnectionFactory.get_staging_connection(*args, **kwargs)

@deprecation_warning
def get_target_connection(*args, **kwargs):
    return ConnectionFactory.get_target_connection(*args, **kwargs)

@deprecation_warning
def test_connections(*args, **kwargs):
    return ConnectionFactory.test_connections(*args, **kwargs)

@deprecation_warning
def execute_source_query(*args, **kwargs):
    return ConnectionFactory.execute_source_query(*args, **kwargs)

@deprecation_warning
def check_connection_health(*args, **kwargs):
    return ConnectionFactory.check_connection_health(*args, **kwargs)

@deprecation_warning
def dispose_all(*args, **kwargs):
    return ConnectionFactory.dispose_all(*args, **kwargs)

# Add module-level deprecation warning
warnings.warn(
    "etl_pipeline.connection_factory is deprecated. Please use etl_pipeline.core.connections.ConnectionFactory instead.",
    DeprecationWarning,
    stacklevel=2
)