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
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# DEPRECATED: Use etl_pipeline.core.connections.ConnectionFactory instead.
warnings.warn('etl_pipeline/connection_factory.py is deprecated. Use etl_pipeline.core.connections.ConnectionFactory instead.', DeprecationWarning)

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

def get_source_connection():
    """Get connection to source MySQL database."""
    return create_engine(
        f"mysql+pymysql://{os.getenv('SOURCE_MYSQL_USER')}:{os.getenv('SOURCE_MYSQL_PASSWORD')}@"
        f"{os.getenv('SOURCE_MYSQL_HOST')}:{os.getenv('SOURCE_MYSQL_PORT')}/{os.getenv('SOURCE_MYSQL_DB')}",
        pool_pre_ping=True,
        pool_recycle=3600
    )

def get_staging_connection():
    """Get connection to staging MySQL database."""
    return create_engine(
        f"mysql+pymysql://{os.getenv('REPLICATION_MYSQL_USER')}:{os.getenv('REPLICATION_MYSQL_PASSWORD')}@"
        f"{os.getenv('REPLICATION_MYSQL_HOST')}:{os.getenv('REPLICATION_MYSQL_PORT')}/{os.getenv('REPLICATION_MYSQL_DB')}",
        pool_pre_ping=True,
        pool_recycle=3600
    )

def get_target_connection():
    """Get connection to target PostgreSQL database."""
    return create_engine(
        f"postgresql://{os.getenv('ANALYTICS_POSTGRES_USER')}:{os.getenv('ANALYTICS_POSTGRES_PASSWORD')}@"
        f"{os.getenv('ANALYTICS_POSTGRES_HOST')}:{os.getenv('ANALYTICS_POSTGRES_PORT')}/{os.getenv('ANALYTICS_POSTGRES_DB')}",
        pool_pre_ping=True,
        pool_recycle=3600
    )