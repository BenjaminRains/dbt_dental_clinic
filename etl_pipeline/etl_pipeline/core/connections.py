"""
Database connection factory for the ETL pipeline.
Handles connections to source, replication, and analytics databases.
"""
import os
from typing import Optional, Dict
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ConnectionFactory:
    """Factory class for creating database connections."""
    
    # Default pool settings
    DEFAULT_POOL_SIZE = 5
    DEFAULT_MAX_OVERFLOW = 10
    DEFAULT_POOL_TIMEOUT = 30
    DEFAULT_POOL_RECYCLE = 1800  # 30 minutes
    
    @staticmethod
    def validate_connection_params(params: Dict[str, str], connection_type: str) -> None:
        """Validate that all required connection parameters are present and non-empty."""
        missing_params = [k for k, v in params.items() if not v]
        if missing_params:
            raise ValueError(f"Missing required {connection_type} connection parameters: {', '.join(missing_params)}")
    
    @staticmethod
    def create_mysql_connection(
        host: str,
        port: str,
        database: str,
        user: str,
        password: str,
        readonly: bool = False,  # Kept for API compatibility but not used
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """
        Create a MySQL connection with proper configuration.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            readonly: Whether to use read-only mode (handled at server level)
            pool_size: Number of connections to keep in the pool
            max_overflow: Maximum number of connections that can be created beyond pool_size
            pool_timeout: Number of seconds to wait before giving up on getting a connection
            pool_recycle: Number of seconds after which a connection is automatically recycled
            
        Returns:
            SQLAlchemy Engine instance
        """
        # Validate required parameters
        params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        ConnectionFactory.validate_connection_params(params, 'MySQL')
        
        try:
            # Create connection string
            connection_string = (
                f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            )
            
            # Create engine with connection pool settings
            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle
            )
            
            # Set SQL mode for proper handling of zero values in auto-increment columns
            with engine.connect() as conn:
                conn.execute(text("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'"))
            
            logger.info(f"Successfully created MySQL connection to {database}" + (" (read-only)" if readonly else ""))
            return engine
            
        except Exception as e:
            error_msg = f"Failed to create MySQL connection to {database}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
    
    @staticmethod
    def create_postgres_connection(
        host: str,
        port: str,
        database: str,
        schema: str = 'raw',  # Default to raw schema
        user: str = None,
        password: str = None,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """
        Create a PostgreSQL connection with proper configuration.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            schema: Schema name (defaults to 'raw')
            user: Database user
            password: Database password
            pool_size: Number of connections to keep in the pool
            max_overflow: Maximum number of connections that can be created beyond pool_size
            pool_timeout: Number of seconds to wait before giving up on getting a connection
            pool_recycle: Number of seconds after which a connection is automatically recycled
            
        Returns:
            SQLAlchemy Engine instance
        """
        # Validate required parameters
        params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        ConnectionFactory.validate_connection_params(params, 'PostgreSQL')
        
        try:
            # Use default schema if empty string or None
            if not schema:
                schema = 'raw'
                logger.warning(f"No schema specified for PostgreSQL connection to {database}, using default: raw")
            
            # Create connection string with schema
            connection_string = (
                f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            )
            
            # Create engine with connection pool settings
            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
                connect_args={
                    'options': f'-csearch_path={schema}'
                }
            )
            
            logger.info(f"Successfully created PostgreSQL connection to {database}.{schema}")
            return engine
            
        except Exception as e:
            error_msg = f"Failed to create PostgreSQL connection to {database}.{schema}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
    
    @classmethod
    def get_opendental_source_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to source OpenDental MySQL database (read-only)."""
        # Get environment variables
        host = os.getenv('OPENDENTAL_SOURCE_HOST')
        port = os.getenv('OPENDENTAL_SOURCE_PORT')
        database = os.getenv('OPENDENTAL_SOURCE_DB')
        user = os.getenv('OPENDENTAL_SOURCE_USER')
        password = os.getenv('OPENDENTAL_SOURCE_PASSWORD')
        
        # Log which environment variables were used
        logger.debug(f"Using source connection parameters: host={host}, port={port}, database={database}, user={user}")
        
        return cls.create_mysql_connection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            readonly=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle
        )
    
    @classmethod
    def get_mysql_replication_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to local MySQL replication database (full access)."""
        # Get environment variables
        host = os.getenv('MYSQL_REPLICATION_HOST')
        port = os.getenv('MYSQL_REPLICATION_PORT')
        database = os.getenv('MYSQL_REPLICATION_DB')
        user = os.getenv('MYSQL_REPLICATION_USER')
        password = os.getenv('MYSQL_REPLICATION_PASSWORD')
        
        # Log which environment variables were used
        logger.debug(f"Using replication connection parameters: host={host}, port={port}, database={database}, user={user}")
        
        return cls.create_mysql_connection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle
        )
    
    @classmethod
    def get_postgres_analytics_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics database."""
        # Get environment variables
        host = os.getenv('POSTGRES_ANALYTICS_HOST')
        port = os.getenv('POSTGRES_ANALYTICS_PORT')
        database = os.getenv('POSTGRES_ANALYTICS_DB')
        schema = os.getenv('POSTGRES_ANALYTICS_SCHEMA', 'raw')
        user = os.getenv('POSTGRES_ANALYTICS_USER')
        password = os.getenv('POSTGRES_ANALYTICS_PASSWORD')
        
        # Log which environment variables were used
        logger.debug(f"Using analytics connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
        return cls.create_postgres_connection(
            host=host,
            port=port,
            database=database,
            schema=schema,
            user=user,
            password=password,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle
        )