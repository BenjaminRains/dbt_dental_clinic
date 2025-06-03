"""
Database connection management module.
Handles connection pooling, health checks, and lifecycle management.
"""
from etl_pipeline.config.settings import settings
import logging
import os
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class ConnectionFactory:
    """Factory class for managing database connections."""
    
    _instances: Dict[str, Engine] = {}
    
    @classmethod
    def _get_connection_string(cls, db_type: str) -> str:
        """Get connection string for a database type."""
        return settings.get_connection_string(db_type)
    
    @classmethod
    def get_source_connection(cls, readonly: bool = False) -> Engine:
        """Get source database connection."""
        key = 'source_readonly' if readonly else 'source'
        if key not in cls._instances:
            connection_string = cls._get_connection_string('source')
            
            # For readonly connections, modify the connection string if needed
            if readonly:
                readonly_user = os.getenv('OPENDENTAL_READONLY_USER')
                readonly_pw = os.getenv('OPENDENTAL_READONLY_PW')
                if readonly_user and readonly_pw:
                    # Replace the user/password in connection string
                    parts = connection_string.split('@')
                    if len(parts) == 2:
                        proto_and_auth = parts[0]
                        proto = proto_and_auth.split('://')[0]
                        connection_string = f"{proto}://{readonly_user}:{readonly_pw}@{parts[1]}"
            
            cls._instances[key] = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True,
                echo=False
            )
            logger.info(f"Created {key} database connection")
        return cls._instances[key]
    
    @classmethod
    def get_staging_connection(cls) -> Engine:
        """Get staging database connection."""
        if 'staging' not in cls._instances:
            connection_string = cls._get_connection_string('staging')
            cls._instances['staging'] = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True,
                echo=False
            )
            logger.info("Created staging database connection")
        return cls._instances['staging']
    
    @classmethod
    def get_target_connection(cls) -> Engine:
        """Get target database connection."""
        if 'target' not in cls._instances:
            connection_string = cls._get_connection_string('target')
            cls._instances['target'] = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True,
                echo=False
            )
            logger.info("Created target database connection")
        return cls._instances['target']
    
    @classmethod
    def get_connection(cls, db_type: str, readonly: bool = False) -> Engine:
        """Get a connection by database type."""
        if db_type == 'source':
            return cls.get_source_connection(readonly)
        elif db_type == 'staging':
            return cls.get_staging_connection()
        elif db_type == 'target':
            return cls.get_target_connection()
        else:
            raise ValueError(f"Unknown database type: {db_type}")
    
    @classmethod
    def test_connections(cls) -> Dict[str, bool]:
        """Test all database connections."""
        results = {}
        
        # Test each connection type
        for db_type in ['source', 'staging', 'target']:
            try:
                engine = cls.get_connection(db_type)
                with engine.connect() as conn:
                    # Use appropriate test query for database type
                    if db_type in ['source', 'staging']:
                        # MySQL
                        result = conn.execute(text('SELECT 1 as test'))
                    else:
                        # PostgreSQL
                        result = conn.execute(text('SELECT 1 as test'))
                    
                    test_value = result.scalar()
                    results[db_type] = (test_value == 1)
                    
                logger.info(f"Successfully tested {db_type} database connection")
                
            except Exception as e:
                results[db_type] = False
                logger.error(f"Failed to test {db_type} database connection: {str(e)}")
        
        return results
    
    @classmethod
    def test_connection(cls, db_type: str) -> bool:
        """Test a specific database connection."""
        try:
            engine = cls.get_connection(db_type)
            with engine.connect() as conn:
                result = conn.execute(text('SELECT 1 as test'))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Failed to test {db_type} connection: {str(e)}")
            return False
    
    @classmethod
    def execute_source_query(cls, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a query on the source database."""
        engine = cls.get_source_connection(readonly=True)
        try:
            with engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return result.fetchall()
        except SQLAlchemyError as e:
            logger.error(f"Error executing source query: {str(e)}")
            raise
    
    @classmethod
    def execute_query(cls, db_type: str, query: str, params: Optional[Dict] = None) -> Any:
        """Execute a query on the specified database."""
        engine = cls.get_connection(db_type)
        try:
            with engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return result
        except SQLAlchemyError as e:
            logger.error(f"Error executing {db_type} query: {str(e)}")
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
    def get_connection_info(cls, db_type: str) -> Dict[str, Any]:
        """Get connection information and statistics."""
        try:
            engine = cls.get_connection(db_type)
            
            info = {
                'database_type': db_type,
                'driver': str(engine.driver),
                'pool_size': engine.pool.size() if hasattr(engine, 'pool') else 'N/A',
                'checked_out': engine.pool.checkedout() if hasattr(engine, 'pool') else 'N/A',
                'checked_in': engine.pool.checkedin() if hasattr(engine, 'pool') else 'N/A',
                'invalidated': engine.pool.invalidated() if hasattr(engine, 'pool') else 'N/A',
                'is_healthy': cls.check_connection_health(engine)
            }
            
            return info
            
        except Exception as e:
            logger.error(f"Failed to get connection info for {db_type}: {str(e)}")
            return {'database_type': db_type, 'error': str(e)}
    
    @classmethod
    def dispose_connection(cls, db_type: str):
        """Dispose of a specific database connection."""
        if db_type in cls._instances:
            try:
                cls._instances[db_type].dispose()
                del cls._instances[db_type]
                logger.info(f"Disposed {db_type} database connection")
            except Exception as e:
                logger.error(f"Error disposing {db_type} connection: {str(e)}")
    
    @classmethod
    def dispose_all(cls):
        """Dispose of all database connections."""
        for db_type in list(cls._instances.keys()):
            cls.dispose_connection(db_type)
        
        cls._instances.clear()
        logger.info("Disposed all database connections")
    
    @classmethod
    def reconnect(cls, db_type: Optional[str] = None):
        """Reconnect to database(s)."""
        if db_type:
            cls.dispose_connection(db_type)
            # Connection will be recreated on next access
            logger.info(f"Reconnection prepared for {db_type}")
        else:
            cls.dispose_all()
            logger.info("Reconnection prepared for all databases")

# Convenience functions
def get_source_connection(readonly: bool = False) -> Engine:
    """Get source database connection."""
    return ConnectionFactory.get_source_connection(readonly)

def get_staging_connection() -> Engine:
    """Get staging database connection."""
    return ConnectionFactory.get_staging_connection()

def get_target_connection() -> Engine:
    """Get target database connection."""
    return ConnectionFactory.get_target_connection()

def test_all_connections() -> Dict[str, bool]:
    """Test all database connections."""
    return ConnectionFactory.test_connections()