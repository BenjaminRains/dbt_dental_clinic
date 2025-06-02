"""
Database connection management module.
Handles connection pooling, health checks, and lifecycle management.
"""
import logging
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from etl_pipeline.config.database import DatabaseConfig
import os

logger = logging.getLogger(__name__)

class ConnectionFactory:
    """Factory class for managing database connections."""
    
    _instances: Dict[str, Engine] = {}
    
    @classmethod
    def get_source_connection(cls, readonly: bool = False) -> Engine:
        """Get source database connection."""
        key = 'source_readonly' if readonly else 'source'
        if key not in cls._instances:
            config = DatabaseConfig.get_source_config()
            if readonly:
                config['user'] = os.getenv('OPENDENTAL_READONLY_USER')
                config['password'] = os.getenv('OPENDENTAL_READONLY_PW')
            
            connection_string = DatabaseConfig.get_connection_string('source')
            cls._instances[key] = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
            )
        return cls._instances[key]
    
    @classmethod
    def get_staging_connection(cls) -> Engine:
        """Get staging database connection."""
        if 'staging' not in cls._instances:
            config = DatabaseConfig.get_staging_config()
            connection_string = DatabaseConfig.get_connection_string('staging')
            cls._instances['staging'] = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
            )
        return cls._instances['staging']
    
    @classmethod
    def get_target_connection(cls) -> Engine:
        """Get target database connection."""
        if 'target' not in cls._instances:
            config = DatabaseConfig.get_target_config()
            connection_string = DatabaseConfig.get_connection_string('target')
            cls._instances['target'] = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
            )
        return cls._instances['target']
    
    @classmethod
    def test_connections(cls) -> Dict[str, bool]:
        """Test all database connections."""
        results = {}
        for name, get_conn in [
            ('source', cls.get_source_connection),
            ('staging', cls.get_staging_connection),
            ('target', cls.get_target_connection)
        ]:
            try:
                engine = get_conn()
                with engine.connect() as conn:
                    conn.execute(text('SELECT 1'))
                results[name] = True
                logger.info(f"Successfully connected to {name} database")
            except Exception as e:
                results[name] = False
                logger.error(f"Failed to connect to {name} database: {str(e)}")
        return results
    
    @classmethod
    def execute_source_query(cls, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a query on the source database."""
        engine = cls.get_source_connection(readonly=True)
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result.fetchall()
        except SQLAlchemyError as e:
            logger.error(f"Error executing source query: {str(e)}")
            raise
    
    @classmethod
    def check_connection_health(cls, engine: Engine) -> bool:
        """Check if a database connection is healthy."""
        try:
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            return True
        except SQLAlchemyError as e:
            logger.error(f"Connection health check failed: {str(e)}")
            return False
    
    @classmethod
    def dispose_all(cls):
        """Dispose of all database connections."""
        for name, engine in cls._instances.items():
            try:
                engine.dispose()
                logger.info(f"Disposed connection to {name} database")
            except Exception as e:
                logger.error(f"Error disposing {name} connection: {str(e)}")
        cls._instances.clear() 