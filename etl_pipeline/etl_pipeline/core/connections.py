"""
Complete ConnectionFactory with ConnectionManager for ETL pipeline.
Uses Settings class as single source of truth for all environment and connection configuration.
"""

import os
import time
import logging
from typing import Optional, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from etl_pipeline.config.settings import get_settings, PostgresSchema

logger = logging.getLogger(__name__)


class ConnectionFactory:
    """Factory for creating database connections using Settings configuration."""
    
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
    def _build_mysql_connection_string(config: Dict) -> str:
        """Build MySQL connection string from configuration."""
        return (
            f"mysql+pymysql://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
            f"?connect_timeout={config.get('connect_timeout', 10)}"
            f"&read_timeout={config.get('read_timeout', 30)}"
            f"&write_timeout={config.get('write_timeout', 30)}"
            f"&charset={config.get('charset', 'utf8mb4')}"
        )
    
    @staticmethod
    def _build_postgres_connection_string(config: Dict) -> str:
        """Build PostgreSQL connection string from configuration."""
        conn_str = (
            f"postgresql+psycopg2://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
            f"?connect_timeout={config.get('connect_timeout', 10)}"
            f"&application_name={config.get('application_name', 'etl_pipeline')}"
        )
        
        # Add schema to search path if specified
        if config.get('schema'):
            conn_str += f"&options=-csearch_path%3D{config['schema']}"
        
        return conn_str
    
    @staticmethod
    def create_mysql_engine(
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE,
        **kwargs
    ) -> Engine:
        """Create a MySQL engine with proper configuration."""
        # Validate required parameters
        params = {
            'host': host,
            'port': str(port),
            'database': database,
            'user': user,
            'password': password
        }
        ConnectionFactory.validate_connection_params(params, 'MySQL')
        
        try:
            # Create connection string
            connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            
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
            
            logger.info(f"Successfully created MySQL connection to {database}")
            return engine
            
        except Exception as e:
            error_msg = f"Failed to create MySQL connection to {database}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
    
    @staticmethod
    def create_postgres_engine(
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        schema: Optional[str] = 'raw',
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE,
        **kwargs
    ) -> Engine:
        """Create a PostgreSQL engine with proper configuration."""
        # Validate required parameters
        params = {
            'host': host,
            'port': str(port),
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
            
            # Create connection string
            connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            
            # Create engine with connection pool settings and schema
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
    
    # ============================================================================
    # EXPLICIT PRODUCTION CONNECTION METHODS (Following Architecture Guidelines)
    # ============================================================================
    
    @staticmethod
    def get_opendental_source_connection() -> Engine:
        """Get OpenDental source database connection (explicit production environment)."""
        settings = get_settings()
        # Ensure we're using production environment
        if settings.environment != 'production':
            logger.warning("Creating production connection but environment is not 'production'")
        config = settings.get_source_connection_config()
        return ConnectionFactory.create_mysql_engine(**config)
    
    @staticmethod
    def get_mysql_replication_connection() -> Engine:
        """Get MySQL replication database connection (explicit production environment)."""
        settings = get_settings()
        # Ensure we're using production environment
        if settings.environment != 'production':
            logger.warning("Creating production connection but environment is not 'production'")
        config = settings.get_replication_connection_config()
        return ConnectionFactory.create_mysql_engine(**config)
    
    @staticmethod
    def get_postgres_analytics_connection() -> Engine:
        """Get PostgreSQL analytics database connection (explicit production environment)."""
        settings = get_settings()
        # Ensure we're using production environment
        if settings.environment != 'production':
            logger.warning("Creating production connection but environment is not 'production'")
        config = settings.get_analytics_connection_config(PostgresSchema.RAW)
        return ConnectionFactory.create_postgres_engine(**config)
    
    # Schema-specific production analytics connections
    @staticmethod
    def get_opendental_analytics_raw_connection() -> Engine:
        """Get PostgreSQL analytics raw schema connection (explicit production environment)."""
        settings = get_settings()
        # Ensure we're using production environment
        if settings.environment != 'production':
            logger.warning("Creating production connection but environment is not 'production'")
        config = settings.get_analytics_connection_config(PostgresSchema.RAW)
        return ConnectionFactory.create_postgres_engine(**config)
    
    @staticmethod
    def get_opendental_analytics_staging_connection() -> Engine:
        """Get PostgreSQL analytics staging schema connection (explicit production environment)."""
        settings = get_settings()
        # Ensure we're using production environment
        if settings.environment != 'production':
            logger.warning("Creating production connection but environment is not 'production'")
        config = settings.get_analytics_connection_config(PostgresSchema.STAGING)
        return ConnectionFactory.create_postgres_engine(**config)
    
    @staticmethod
    def get_opendental_analytics_intermediate_connection() -> Engine:
        """Get PostgreSQL analytics intermediate schema connection (explicit production environment)."""
        settings = get_settings()
        # Ensure we're using production environment
        if settings.environment != 'production':
            logger.warning("Creating production connection but environment is not 'production'")
        config = settings.get_analytics_connection_config(PostgresSchema.INTERMEDIATE)
        return ConnectionFactory.create_postgres_engine(**config)
    
    @staticmethod
    def get_opendental_analytics_marts_connection() -> Engine:
        """Get PostgreSQL analytics marts schema connection (explicit production environment)."""
        settings = get_settings()
        # Ensure we're using production environment
        if settings.environment != 'production':
            logger.warning("Creating production connection but environment is not 'production'")
        config = settings.get_analytics_connection_config(PostgresSchema.MARTS)
        return ConnectionFactory.create_postgres_engine(**config)

    # ============================================================================
    # EXPLICIT TEST CONNECTION METHODS (Following Architecture Guidelines)
    # ============================================================================

    @staticmethod
    def get_opendental_source_test_connection() -> Engine:
        """Get OpenDental source test database connection (explicit test environment)."""
        settings = get_settings()
        # Force test environment for test connections
        if settings.environment != 'test':
            logger.warning("Creating test connection but environment is not 'test'")
        config = settings.get_source_connection_config()
        return ConnectionFactory.create_mysql_engine(**config)

    @staticmethod
    def get_mysql_replication_test_connection() -> Engine:
        """Get MySQL replication test database connection (explicit test environment)."""
        settings = get_settings()
        # Force test environment for test connections
        if settings.environment != 'test':
            logger.warning("Creating test connection but environment is not 'test'")
        config = settings.get_replication_connection_config()
        return ConnectionFactory.create_mysql_engine(**config)

    @staticmethod
    def get_postgres_analytics_test_connection() -> Engine:
        """Get PostgreSQL analytics test database connection (explicit test environment)."""
        settings = get_settings()
        # Force test environment for test connections
        if settings.environment != 'test':
            logger.warning("Creating test connection but environment is not 'test'")
        config = settings.get_analytics_connection_config(PostgresSchema.RAW)
        return ConnectionFactory.create_postgres_engine(**config)

    @staticmethod
    def get_opendental_analytics_raw_test_connection() -> Engine:
        """Get PostgreSQL analytics raw schema test connection (explicit test environment)."""
        settings = get_settings()
        # Force test environment for test connections
        if settings.environment != 'test':
            logger.warning("Creating test connection but environment is not 'test'")
        config = settings.get_analytics_connection_config(PostgresSchema.RAW)
        return ConnectionFactory.create_postgres_engine(**config)

    @staticmethod
    def get_opendental_analytics_staging_test_connection() -> Engine:
        """Get PostgreSQL analytics staging schema test connection (explicit test environment)."""
        settings = get_settings()
        # Force test environment for test connections
        if settings.environment != 'test':
            logger.warning("Creating test connection but environment is not 'test'")
        config = settings.get_analytics_connection_config(PostgresSchema.STAGING)
        return ConnectionFactory.create_postgres_engine(**config)

    @staticmethod
    def get_opendental_analytics_intermediate_test_connection() -> Engine:
        """Get PostgreSQL analytics intermediate schema test connection (explicit test environment)."""
        settings = get_settings()
        # Force test environment for test connections
        if settings.environment != 'test':
            logger.warning("Creating test connection but environment is not 'test'")
        config = settings.get_analytics_connection_config(PostgresSchema.INTERMEDIATE)
        return ConnectionFactory.create_postgres_engine(**config)

    @staticmethod
    def get_opendental_analytics_marts_test_connection() -> Engine:
        """Get PostgreSQL analytics marts schema test connection (explicit test environment)."""
        settings = get_settings()
        # Force test environment for test connections
        if settings.environment != 'test':
            logger.warning("Creating test connection but environment is not 'test'")
        config = settings.get_analytics_connection_config(PostgresSchema.MARTS)
        return ConnectionFactory.create_postgres_engine(**config)


class ConnectionManager:
    """
    Manages database connections efficiently to avoid overwhelming the source server.
    
    Features:
    - Single connection reuse for batch operations
    - Connection pooling with proper cleanup
    - Rate limiting to be respectful to source server
    - Automatic retry logic for transient failures
    """
    
    def __init__(self, engine: Engine, max_retries: int = 3, retry_delay: float = 1.0):
        self.engine = engine
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._current_connection = None
        self._connection_count = 0
        self._last_query_time = 0
        
    def get_connection(self):
        """Get a connection, reusing existing one if available."""
        if self._current_connection is None:
            self._current_connection = self.engine.connect()
            self._connection_count += 1
            logger.debug(f"Created new connection (total: {self._connection_count})")
        return self._current_connection
    
    def close_connection(self):
        """Close the current connection if it exists."""
        if self._current_connection is not None:
            self._current_connection.close()
            self._current_connection = None
            logger.debug("Closed connection")
    
    def execute_with_retry(self, query, params=None, rate_limit: bool = True):
        """
        Execute a query with retry logic and optional rate limiting.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            rate_limit: Whether to add delay between queries
            
        Returns:
            Query result
        """
        # Rate limiting to be respectful to source server
        if rate_limit:
            current_time = time.time()
            time_since_last = current_time - self._last_query_time
            if time_since_last < 0.1:  # 100ms minimum between queries
                time.sleep(0.1 - time_since_last)
            self._last_query_time = time.time()
        
        # Retry logic
        for attempt in range(self.max_retries):
            try:
                conn = self.get_connection()
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return result
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Query failed (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                    time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                    # Close connection and retry with fresh one
                    self.close_connection()
                else:
                    raise
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures connection cleanup."""
        self.close_connection()


# Convenience function to create ConnectionManager
def create_connection_manager(engine: Engine,
                            max_retries: int = 3,
                            retry_delay: float = 1.0) -> ConnectionManager:
    """
    Create a ConnectionManager for the specified engine.
    
    Args:
        engine: SQLAlchemy engine to manage
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        ConnectionManager instance
    """
    return ConnectionManager(engine, max_retries, retry_delay)