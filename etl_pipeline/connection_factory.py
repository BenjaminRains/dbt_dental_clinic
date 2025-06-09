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

def get_opendental_source_connection():
    """Get connection to source MySQL database (DEPRECATED: use ConnectionFactory.get_opendental_source_connection)."""
    # Use improved environment variable names with fallback to old names
    host = os.getenv('OPENDENTAL_SOURCE_HOST') or os.getenv('SOURCE_MYSQL_HOST')
    port = os.getenv('OPENDENTAL_SOURCE_PORT') or os.getenv('SOURCE_MYSQL_PORT')
    database = os.getenv('OPENDENTAL_SOURCE_DB') or os.getenv('SOURCE_MYSQL_DB')
    user = os.getenv('OPENDENTAL_SOURCE_USER') or os.getenv('SOURCE_MYSQL_USER')
    password = os.getenv('OPENDENTAL_SOURCE_PASSWORD') or os.getenv('SOURCE_MYSQL_PASSWORD')
    
    return create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        pool_pre_ping=True,
        pool_recycle=3600
    )

def get_mysql_replication_connection():
    """Get connection to staging MySQL database (DEPRECATED: use ConnectionFactory.get_mysql_replication_connection)."""
    # Use improved environment variable names with fallback to old names
    host = os.getenv('MYSQL_REPLICATION_HOST') or os.getenv('REPLICATION_MYSQL_HOST')
    port = os.getenv('MYSQL_REPLICATION_PORT') or os.getenv('REPLICATION_MYSQL_PORT')
    database = os.getenv('MYSQL_REPLICATION_DB') or os.getenv('REPLICATION_MYSQL_DB')
    user = os.getenv('MYSQL_REPLICATION_USER') or os.getenv('REPLICATION_MYSQL_USER')
    password = os.getenv('MYSQL_REPLICATION_PASSWORD') or os.getenv('REPLICATION_MYSQL_PASSWORD')
    
    return create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        pool_pre_ping=True,
        pool_recycle=3600
    )

def get_postgres_analytics_connection():
    """Get connection to target PostgreSQL database (DEPRECATED: use ConnectionFactory.get_postgres_analytics_connection)."""
    # Use improved environment variable names with fallback to old names
    host = os.getenv('POSTGRES_ANALYTICS_HOST') or os.getenv('ANALYTICS_POSTGRES_HOST')
    port = os.getenv('POSTGRES_ANALYTICS_PORT') or os.getenv('ANALYTICS_POSTGRES_PORT')
    database = os.getenv('POSTGRES_ANALYTICS_DB') or os.getenv('ANALYTICS_POSTGRES_DB')
    user = os.getenv('POSTGRES_ANALYTICS_USER') or os.getenv('ANALYTICS_POSTGRES_USER')
    password = os.getenv('POSTGRES_ANALYTICS_PASSWORD') or os.getenv('ANALYTICS_POSTGRES_PASSWORD')
    
    return create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        pool_pre_ping=True,
        pool_recycle=3600
    )