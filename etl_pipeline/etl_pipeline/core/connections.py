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
from ..config.settings import get_settings, PostgresSchema, DatabaseType

# Import custom exceptions for structured error handling
from ..exceptions.database import DatabaseConnectionError, DatabaseQueryError
from ..exceptions.configuration import ConfigurationError

logger = logging.getLogger(__name__)


class ConnectionFactory:
    """Factory for creating database connections using Settings configuration."""
    
    # Default pool settings - UPDATED FOR PERFORMANCE
    DEFAULT_POOL_SIZE = 20        # 4x increase from 5
    DEFAULT_MAX_OVERFLOW = 40     # 4x increase from 10
    DEFAULT_POOL_TIMEOUT = 300    # 10x increase from 30
    DEFAULT_POOL_RECYCLE = 1800  # 30 minutes
    
    @staticmethod
    def validate_connection_params(params: Dict[str, str], connection_type: str) -> None:
        """Validate that all required connection parameters are present and non-empty."""
        missing_params = [k for k, v in params.items() if not v]
        if missing_params:
            raise ConfigurationError(
                message=f"Missing required {connection_type} connection parameters",
                missing_keys=missing_params,
                details={"connection_type": connection_type, "params": params}
            )
    
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
            
            # Apply MySQL performance optimizations for bulk operations
            ConnectionFactory._apply_mysql_performance_settings(engine)
            
            logger.info(f"Successfully created MySQL connection to {database}")
            return engine
            
        except Exception as e:
            raise DatabaseConnectionError(
                message=f"Failed to create MySQL connection to {database}",
                database_type="mysql",
                connection_params=params,
                details={
                    "error": str(e),
                    "pool_size": pool_size,
                    "max_overflow": max_overflow,
                    "pool_timeout": pool_timeout,
                    "pool_recycle": pool_recycle
                },
                original_exception=e
            )

    @staticmethod
    def _apply_mysql_performance_settings(engine: Engine) -> None:
        """Apply MySQL bulk operation optimizations."""
        try:
            with engine.connect() as conn:
                # Critical for bulk operations
                try:
                    conn.execute(text("SET SESSION bulk_insert_buffer_size = 268435456"))  # 256MB
                except Exception as e:
                    if "Access denied" in str(e) or "SUPER" in str(e) or "SYSTEM_VARIABLES_ADMIN" in str(e):
                        logger.warning(f"Failed to set bulk_insert_buffer_size due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set bulk_insert_buffer_size: {e}")
                
                # Try to set innodb_flush_log_at_trx_commit, but handle GLOBAL variable gracefully
                try:
                    conn.execute(text("SET SESSION innodb_flush_log_at_trx_commit = 2"))
                except Exception as e:
                    if "GLOBAL variable" in str(e) or "Access denied" in str(e):
                        logger.warning("innodb_flush_log_at_trx_commit requires GLOBAL privileges, skipping")
                    else:
                        logger.warning(f"Failed to set innodb_flush_log_at_trx_commit: {e}")
                
                try:
                    conn.execute(text("SET SESSION autocommit = 0"))
                except Exception as e:
                    if "Access denied" in str(e):
                        logger.warning(f"Failed to set autocommit due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set autocommit: {e}")
                
                try:
                    conn.execute(text("SET SESSION unique_checks = 0"))
                except Exception as e:
                    if "Access denied" in str(e):
                        logger.warning(f"Failed to set unique_checks due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set unique_checks: {e}")
                
                try:
                    conn.execute(text("SET SESSION foreign_key_checks = 0"))
                except Exception as e:
                    if "Access denied" in str(e):
                        logger.warning(f"Failed to set foreign_key_checks due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set foreign_key_checks: {e}")
                
                # Set SQL mode for proper handling of zero values in auto-increment columns
                try:
                    conn.execute(text("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'"))
                except Exception as e:
                    if "Access denied" in str(e):
                        logger.warning(f"Failed to set sql_mode due to privilege restrictions: {e}")
                    else:
                        logger.warning(f"Failed to set sql_mode: {e}")
                
                logger.debug("Applied MySQL performance optimizations")
        except Exception as e:
            logger.warning(f"Failed to apply MySQL performance settings: {e}")

    @staticmethod
    def _apply_postgres_performance_settings(engine: Engine) -> None:
        """Apply PostgreSQL bulk loading optimizations.
        
        Note: wal_buffers cannot be set at runtime and must be configured
        in postgresql.conf (requires server restart). See documentation in
        etl_pipeline/docs/postgres_wal_buffers_configuration.md for details.
        """
        try:
            with engine.connect() as conn:
                conn.execute(text("SET work_mem = '256MB'"))
                conn.execute(text("SET maintenance_work_mem = '1GB'"))
                conn.execute(text("SET synchronous_commit = off"))
                # Note: wal_buffers requires server restart - see documentation
                logger.debug("Applied PostgreSQL performance optimizations")
        except Exception as e:
            logger.warning(f"Failed to apply PostgreSQL performance settings: {e}")
    
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
            
            # Apply PostgreSQL performance optimizations for bulk operations
            ConnectionFactory._apply_postgres_performance_settings(engine)
            
            logger.info(f"Successfully created PostgreSQL connection to {database}.{schema}")
            return engine
            
        except Exception as e:
            raise DatabaseConnectionError(
                message=f"Failed to create PostgreSQL connection to {database}.{schema}",
                database_type="postgresql",
                connection_params=params,
                details={
                    "error": str(e),
                    "schema": schema,
                    "pool_size": pool_size,
                    "max_overflow": max_overflow,
                    "pool_timeout": pool_timeout,
                    "pool_recycle": pool_recycle
                },
                original_exception=e
            )

# ============================================================================
# UNIFIED INTERFACE METHODS WITH SETTINGS INJECTION (Recommended Approach)
# ============================================================================

    @staticmethod
    def get_source_connection(settings) -> Engine:
        """Get source database connection using provided settings."""
        config = settings.get_database_config(DatabaseType.SOURCE)
        return ConnectionFactory.create_mysql_engine(**config)
    
    @staticmethod
    def get_replication_connection(settings) -> Engine:
        """Get replication database connection using provided settings."""
        config = settings.get_database_config(DatabaseType.REPLICATION)
        return ConnectionFactory.create_mysql_engine(**config)
    
    @staticmethod
    def get_analytics_connection(settings, schema: PostgresSchema = PostgresSchema.RAW) -> Engine:
        """Get analytics database connection using provided settings and schema."""
        config = settings.get_database_config(DatabaseType.ANALYTICS, schema)
        return ConnectionFactory.create_postgres_engine(**config)

    # Convenience methods for common schemas
    @staticmethod
    def get_analytics_raw_connection(settings) -> Engine:
        """Get analytics raw schema connection using provided settings."""
        return ConnectionFactory.get_analytics_connection(settings, PostgresSchema.RAW)
    
    @staticmethod
    def get_analytics_staging_connection(settings) -> Engine:
        """Get analytics staging schema connection using provided settings."""
        return ConnectionFactory.get_analytics_connection(settings, PostgresSchema.STAGING)
    
    @staticmethod
    def get_analytics_intermediate_connection(settings) -> Engine:
        """Get analytics intermediate schema connection using provided settings."""
        return ConnectionFactory.get_analytics_connection(settings, PostgresSchema.INTERMEDIATE)
    
    @staticmethod
    def get_analytics_marts_connection(settings) -> Engine:
        """Get analytics marts schema connection using provided settings."""
        return ConnectionFactory.get_analytics_connection(settings, PostgresSchema.MARTS)


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
                    # Determine database type from engine URL
                    database_type = "unknown"
                    if hasattr(self.engine, 'url'):
                        if 'mysql' in str(self.engine.url):
                            database_type = "mysql"
                        elif 'postgresql' in str(self.engine.url):
                            database_type = "postgresql"
                    
                    raise DatabaseQueryError(
                        message="Query execution failed after retries",
                        query=query,
                        database_type=database_type,
                        details={
                            "attempts": self.max_retries,
                            "params": params,
                            "rate_limit": rate_limit,
                            "error": str(e)
                        },
                        original_exception=e
                    )
    
    def commit(self):
        """Commit the current transaction."""
        if self._current_connection is not None:
            try:
                self._current_connection.commit()
                logger.debug("Successfully committed transaction")
            except Exception as e:
                logger.error(f"Failed to commit transaction: {str(e)}")
                raise
    
    def rollback(self):
        """Rollback the current transaction."""
        if self._current_connection is not None:
            try:
                self._current_connection.rollback()
                logger.debug("Successfully rolled back transaction")
            except Exception as e:
                logger.error(f"Failed to rollback transaction: {str(e)}")
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