"""
Database connection factory for the ETL pipeline.
Handles connections to source, replication, and analytics databases.
"""
import os
import time
from typing import Optional, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Load environment variables
# Try to load from etl_pipeline/.env first, then fall back to parent directory
env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
    logger.info(f"Loaded environment from: {env_path}")
else:
    # Try parent directory
    parent_env_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env')
    if os.path.exists(parent_env_path):
        load_dotenv(parent_env_path)
        logger.info(f"Loaded environment from: {parent_env_path}")
    else:
        # Fall back to default behavior
        load_dotenv()
        logger.info("Loaded environment using default load_dotenv() behavior")

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
    
    # ============================================================================
    # PRODUCTION ENVIRONMENT CONNECTIONS
    # ============================================================================
    
    @classmethod
    def get_opendental_source_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to source OpenDental MySQL database (PRODUCTION - read-only)."""
        # Get environment variables
        host = os.getenv('OPENDENTAL_SOURCE_HOST')
        port = os.getenv('OPENDENTAL_SOURCE_PORT')
        database = os.getenv('OPENDENTAL_SOURCE_DB')
        user = os.getenv('OPENDENTAL_SOURCE_USER')
        password = os.getenv('OPENDENTAL_SOURCE_PASSWORD')
        
        # Log which environment variables were used
        logger.debug(f"Using PRODUCTION source connection parameters: host={host}, port={port}, database={database}, user={user}")
        
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
        """Get connection to local MySQL replication database (PRODUCTION - full access)."""
        # Get environment variables
        host = os.getenv('MYSQL_REPLICATION_HOST')
        port = os.getenv('MYSQL_REPLICATION_PORT')
        database = os.getenv('MYSQL_REPLICATION_DB')
        user = os.getenv('MYSQL_REPLICATION_USER')
        password = os.getenv('MYSQL_REPLICATION_PASSWORD')
        
        # Log which environment variables were used
        logger.debug(f"Using PRODUCTION replication connection parameters: host={host}, port={port}, database={database}, user={user}")
        
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
        """Get connection to PostgreSQL analytics database (PRODUCTION)."""
        # Get environment variables
        host = os.getenv('POSTGRES_ANALYTICS_HOST')
        port = os.getenv('POSTGRES_ANALYTICS_PORT')
        database = os.getenv('POSTGRES_ANALYTICS_DB')
        schema = os.getenv('POSTGRES_ANALYTICS_SCHEMA', 'raw')
        user = os.getenv('POSTGRES_ANALYTICS_USER')
        password = os.getenv('POSTGRES_ANALYTICS_PASSWORD')
        
        # Log which environment variables were used
        logger.debug(f"Using PRODUCTION analytics connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
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

    # Production-specific schema connection methods
    @classmethod
    def get_opendental_analytics_raw_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics database raw schema (PRODUCTION)."""
        return cls.get_postgres_analytics_connection(
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle
        )

    @classmethod
    def get_opendental_analytics_staging_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics database staging schema (PRODUCTION)."""
        # Get environment variables
        host = os.getenv('POSTGRES_ANALYTICS_HOST')
        port = os.getenv('POSTGRES_ANALYTICS_PORT')
        database = os.getenv('POSTGRES_ANALYTICS_DB')
        schema = 'staging'  # Use simplified staging schema
        user = os.getenv('POSTGRES_ANALYTICS_USER')
        password = os.getenv('POSTGRES_ANALYTICS_PASSWORD')
        
        logger.debug(f"Using PRODUCTION analytics staging connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
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

    @classmethod
    def get_opendental_analytics_intermediate_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics database intermediate schema (PRODUCTION)."""
        # Get environment variables
        host = os.getenv('POSTGRES_ANALYTICS_HOST')
        port = os.getenv('POSTGRES_ANALYTICS_PORT')
        database = os.getenv('POSTGRES_ANALYTICS_DB')
        schema = 'intermediate'  # Use simplified intermediate schema
        user = os.getenv('POSTGRES_ANALYTICS_USER')
        password = os.getenv('POSTGRES_ANALYTICS_PASSWORD')
        
        logger.debug(f"Using PRODUCTION analytics intermediate connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
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

    @classmethod
    def get_opendental_analytics_marts_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics database marts schema (PRODUCTION)."""
        # Get environment variables
        host = os.getenv('POSTGRES_ANALYTICS_HOST')
        port = os.getenv('POSTGRES_ANALYTICS_PORT')
        database = os.getenv('POSTGRES_ANALYTICS_DB')
        schema = 'marts'  # Use simplified marts schema
        user = os.getenv('POSTGRES_ANALYTICS_USER')
        password = os.getenv('POSTGRES_ANALYTICS_PASSWORD')
        
        logger.debug(f"Using PRODUCTION analytics marts connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
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

    # ============================================================================
    # TEST ENVIRONMENT CONNECTIONS
    # ============================================================================
    
    @classmethod
    def get_opendental_source_test_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to OpenDental test database on client server (TEST)."""
        # Get environment variables
        host = os.getenv('TEST_OPENDENTAL_SOURCE_HOST')
        port = os.getenv('TEST_OPENDENTAL_SOURCE_PORT')
        database = os.getenv('TEST_OPENDENTAL_SOURCE_DB')
        user = os.getenv('TEST_OPENDENTAL_SOURCE_USER')
        password = os.getenv('TEST_OPENDENTAL_SOURCE_PASSWORD')
        
        # Enhanced logging to debug the issue
        logger.info(f"TEST_OPENDENTAL_SOURCE_HOST: {host}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_PORT: {port}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_DB: {database}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_USER: {user}")
        logger.info(f"TEST_OPENDENTAL_SOURCE_PASSWORD: {'*' * len(password) if password else 'NOT SET'}")
        
        # Validate that we have all required parameters
        if not all([host, port, database, user, password]):
            missing = []
            if not host: missing.append('TEST_OPENDENTAL_SOURCE_HOST')
            if not port: missing.append('TEST_OPENDENTAL_SOURCE_PORT')
            if not database: missing.append('TEST_OPENDENTAL_SOURCE_DB')
            if not user: missing.append('TEST_OPENDENTAL_SOURCE_USER')
            if not password: missing.append('TEST_OPENDENTAL_SOURCE_PASSWORD')
            raise ValueError(f"Missing required test connection environment variables: {', '.join(missing)}")
        
        # Log which environment variables were used
        logger.debug(f"Using TEST OpenDental source connection parameters: host={host}, port={port}, database={database}, user={user}")
        
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
    def get_mysql_replication_test_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to MySQL replication test database (TEST)."""
        # Get environment variables
        host = os.getenv('TEST_MYSQL_REPLICATION_HOST')
        port = os.getenv('TEST_MYSQL_REPLICATION_PORT')
        database = os.getenv('TEST_MYSQL_REPLICATION_DB')
        user = os.getenv('TEST_MYSQL_REPLICATION_USER')
        password = os.getenv('TEST_MYSQL_REPLICATION_PASSWORD')
        
        # Enhanced logging to debug the issue
        logger.info(f"TEST_MYSQL_REPLICATION_HOST: {host}")
        logger.info(f"TEST_MYSQL_REPLICATION_PORT: {port}")
        logger.info(f"TEST_MYSQL_REPLICATION_DB: {database}")
        logger.info(f"TEST_MYSQL_REPLICATION_USER: {user}")
        logger.info(f"TEST_MYSQL_REPLICATION_PASSWORD: {'*' * len(password) if password else 'NOT SET'}")
        
        # Validate that we have all required parameters
        if not all([host, port, database, user, password]):
            missing = []
            if not host: missing.append('TEST_MYSQL_REPLICATION_HOST')
            if not port: missing.append('TEST_MYSQL_REPLICATION_PORT')
            if not database: missing.append('TEST_MYSQL_REPLICATION_DB')
            if not user: missing.append('TEST_MYSQL_REPLICATION_USER')
            if not password: missing.append('TEST_MYSQL_REPLICATION_PASSWORD')
            raise ValueError(f"Missing required replication test connection environment variables: {', '.join(missing)}")
        
        # Log which environment variables were used
        logger.debug(f"Using TEST replication connection parameters: host={host}, port={port}, database={database}, user={user}")
        
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
    def get_postgres_analytics_test_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics test database (TEST)."""
        host = os.getenv('TEST_POSTGRES_ANALYTICS_HOST')
        port = os.getenv('TEST_POSTGRES_ANALYTICS_PORT')
        database = os.getenv('TEST_POSTGRES_ANALYTICS_DB')
        schema = os.getenv('TEST_POSTGRES_ANALYTICS_SCHEMA', 'raw')
        user = os.getenv('TEST_POSTGRES_ANALYTICS_USER')
        password = os.getenv('TEST_POSTGRES_ANALYTICS_PASSWORD')
        
        logger.debug(f"Using TEST analytics connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
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

    # Test-specific schema connection methods
    @classmethod
    def get_opendental_analytics_raw_test_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics test database raw schema (TEST)."""
        return cls.get_postgres_analytics_test_connection(
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle
        )

    @classmethod
    def get_opendental_analytics_staging_test_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics test database staging schema (TEST)."""
        # Get environment variables
        host = os.getenv('TEST_POSTGRES_ANALYTICS_HOST')
        port = os.getenv('TEST_POSTGRES_ANALYTICS_PORT')
        database = os.getenv('TEST_POSTGRES_ANALYTICS_DB')
        schema = 'staging'  # Use simplified staging schema
        user = os.getenv('TEST_POSTGRES_ANALYTICS_USER')
        password = os.getenv('TEST_POSTGRES_ANALYTICS_PASSWORD')
        
        logger.debug(f"Using TEST analytics staging connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
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

    @classmethod
    def get_opendental_analytics_intermediate_test_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics test database intermediate schema (TEST)."""
        # Get environment variables
        host = os.getenv('TEST_POSTGRES_ANALYTICS_HOST')
        port = os.getenv('TEST_POSTGRES_ANALYTICS_PORT')
        database = os.getenv('TEST_POSTGRES_ANALYTICS_DB')
        schema = 'intermediate'  # Use simplified intermediate schema
        user = os.getenv('TEST_POSTGRES_ANALYTICS_USER')
        password = os.getenv('TEST_POSTGRES_ANALYTICS_PASSWORD')
        
        logger.debug(f"Using TEST analytics intermediate connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
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

    @classmethod
    def get_opendental_analytics_marts_test_connection(
        cls,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        pool_timeout: int = DEFAULT_POOL_TIMEOUT,
        pool_recycle: int = DEFAULT_POOL_RECYCLE
    ) -> Engine:
        """Get connection to PostgreSQL analytics test database marts schema (TEST)."""
        # Get environment variables
        host = os.getenv('TEST_POSTGRES_ANALYTICS_HOST')
        port = os.getenv('TEST_POSTGRES_ANALYTICS_PORT')
        database = os.getenv('TEST_POSTGRES_ANALYTICS_DB')
        schema = 'marts'  # Use simplified marts schema
        user = os.getenv('TEST_POSTGRES_ANALYTICS_USER')
        password = os.getenv('TEST_POSTGRES_ANALYTICS_PASSWORD')
        
        logger.debug(f"Using TEST analytics marts connection parameters: host={host}, port={port}, database={database}, schema={schema}, user={user}")
        
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