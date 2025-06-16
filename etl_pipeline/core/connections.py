"""
Database connection factory for the ETL pipeline.
Handles connections to source, replication, and analytics databases.
"""
import os
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ConnectionFactory:
    """Factory class for creating database connections."""
    
    @staticmethod
    def create_mysql_connection(
        host: str,
        port: str,
        database: str,
        user: str,
        password: str,
        readonly: bool = False
    ) -> Engine:
        """
        Create a MySQL connection with proper configuration.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            readonly: Whether to use read-only mode
            
        Returns:
            SQLAlchemy Engine instance
        """
        # Validate required parameters
        missing_params = []
        if not host:
            missing_params.append('host')
        if not port:
            missing_params.append('port')
        if not database:
            missing_params.append('database')
        if not user:
            missing_params.append('user')
        if not password:
            missing_params.append('password')
        
        if missing_params:
            raise ValueError(f"Missing required MySQL connection parameters: {', '.join(missing_params)}")
        
        try:
            connection_string = (
                f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            )
            
            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            logger.info(f"Successfully created MySQL connection to {database}")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create MySQL connection: {str(e)}")
            raise
    
    @staticmethod
    def create_postgres_connection(
        host: str,
        port: str,
        database: str,
        schema: str,
        user: str,
        password: str
    ) -> Engine:
        """
        Create a PostgreSQL connection with proper configuration.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            schema: Database schema
            user: Database user
            password: Database password
            
        Returns:
            SQLAlchemy Engine instance
        """
        # Validate required parameters
        missing_params = []
        if not host:
            missing_params.append('host')
        if not port:
            missing_params.append('port')
        if not database:
            missing_params.append('database')
        if not user:
            missing_params.append('user')
        if not password:
            missing_params.append('password')
        
        if missing_params:
            raise ValueError(f"Missing required PostgreSQL connection parameters: {', '.join(missing_params)}")
        
        # Default schema if not provided
        if not schema:
            schema = 'raw'
        
        try:
            connection_string = (
                f"postgresql://{user}:{password}@{host}:{port}/{database}"
            )
            
            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    'options': f'-csearch_path={schema}'
                }
            )
            
            logger.info(f"Successfully created PostgreSQL connection to {database}.{schema}")
            return engine
            
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL connection: {str(e)}")
            raise
    
    @classmethod
    def get_opendental_source_connection(cls) -> Engine:
        """Get connection to source OpenDental MySQL database (read-only)."""
        # Get environment variables with fallback to old naming
        host = os.getenv('OPENDENTAL_SOURCE_HOST') or os.getenv('SOURCE_MYSQL_HOST')
        port = os.getenv('OPENDENTAL_SOURCE_PORT') or os.getenv('SOURCE_MYSQL_PORT')
        database = os.getenv('OPENDENTAL_SOURCE_DB') or os.getenv('SOURCE_MYSQL_DB')
        user = os.getenv('OPENDENTAL_SOURCE_USER') or os.getenv('SOURCE_MYSQL_USER')
        password = os.getenv('OPENDENTAL_SOURCE_PASSWORD') or os.getenv('SOURCE_MYSQL_PASSWORD')
        
        return cls.create_mysql_connection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            readonly=True
        )
    
    @classmethod
    def get_mysql_replication_connection(cls) -> Engine:
        """Get connection to local MySQL replication database (full access)."""
        # Get environment variables with fallback to old naming
        host = os.getenv('MYSQL_REPLICATION_HOST') or os.getenv('REPLICATION_MYSQL_HOST')
        port = os.getenv('MYSQL_REPLICATION_PORT') or os.getenv('REPLICATION_MYSQL_PORT')
        database = os.getenv('MYSQL_REPLICATION_DB') or os.getenv('REPLICATION_MYSQL_DB')
        user = os.getenv('MYSQL_REPLICATION_USER') or os.getenv('REPLICATION_MYSQL_USER')
        password = os.getenv('MYSQL_REPLICATION_PASSWORD') or os.getenv('REPLICATION_MYSQL_PASSWORD')
        
        return cls.create_mysql_connection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    
    @classmethod
    def get_postgres_analytics_connection(cls) -> Engine:
        """Get connection to PostgreSQL analytics database."""
        # Get environment variables with fallback to old naming
        host = os.getenv('POSTGRES_ANALYTICS_HOST') or os.getenv('ANALYTICS_POSTGRES_HOST')
        port = os.getenv('POSTGRES_ANALYTICS_PORT') or os.getenv('ANALYTICS_POSTGRES_PORT')
        database = os.getenv('POSTGRES_ANALYTICS_DB') or os.getenv('ANALYTICS_POSTGRES_DB')
        schema = os.getenv('POSTGRES_ANALYTICS_SCHEMA') or os.getenv('ANALYTICS_POSTGRES_SCHEMA', 'raw')
        user = os.getenv('POSTGRES_ANALYTICS_USER') or os.getenv('ANALYTICS_POSTGRES_USER')
        password = os.getenv('POSTGRES_ANALYTICS_PASSWORD') or os.getenv('ANALYTICS_POSTGRES_PASSWORD')
        
        return cls.create_postgres_connection(
            host=host,
            port=port,
            database=database,
            schema=schema,
            user=user,
            password=password
        )
    
    @classmethod
    def get_staging_connection(cls) -> Engine:
        """
        Get connection to staging/replication MySQL database.
        
        DEPRECATED: Use get_mysql_replication_connection() instead.
        This method is maintained for backward compatibility with legacy code in:
        - etl_job/monitoring_utils.py
        - etl_job/mysql_postgre_incremental.py
        - etl_job/mysql_postgre_increm_ETL.py
        - airflow/dags/nightly_incremental_DAG.py
        
        The entire /etl_job directory is considered legacy and should be migrated to /etl_pipeline.
        """
        import warnings
        warnings.warn(
            "get_staging_connection() is deprecated. Use get_mysql_replication_connection() instead. "
            "This method is maintained for backward compatibility with legacy code.",
            DeprecationWarning,
            stacklevel=2
        )
        return cls.get_mysql_replication_connection()
    
    @classmethod
    def get_target_connection(cls) -> Engine:
        """
        Get connection to target PostgreSQL analytics database.
        
        DEPRECATED: Use get_postgres_analytics_connection() instead.
        This method is maintained for backward compatibility with legacy code in:
        - etl_job/monitoring_utils.py
        - etl_job/mysql_postgre_incremental.py
        - etl_job/mysql_postgre_increm_ETL.py
        - airflow/dags/nightly_incremental_DAG.py
        
        The entire /etl_job directory is considered legacy and should be migrated to /etl_pipeline.
        """
        import warnings
        warnings.warn(
            "get_target_connection() is deprecated. Use get_postgres_analytics_connection() instead. "
            "This method is maintained for backward compatibility with legacy code.",
            DeprecationWarning,
            stacklevel=2
        )
        return cls.get_postgres_analytics_connection()
    
    @classmethod
    def get_source_connection(cls) -> Engine:
        """
        Get connection to source OpenDental MySQL database.
        
        DEPRECATED: Use get_opendental_source_connection() instead.
        This method is maintained for backward compatibility with legacy code in:
        - etl_job/monitoring_utils.py
        - etl_job/mysql_postgre_incremental.py
        - etl_job/mysql_postgre_increm_ETL.py
        - airflow/dags/nightly_incremental_DAG.py
        
        The entire /etl_job directory is considered legacy and should be migrated to /etl_pipeline.
        """
        import warnings
        warnings.warn(
            "get_source_connection() is deprecated. Use get_opendental_source_connection() instead. "
            "This method is maintained for backward compatibility with legacy code.",
            DeprecationWarning,
            stacklevel=2
        )
        return cls.get_opendental_source_connection()
    
    @classmethod
    def get_replication_connection(cls) -> Engine:
        """
        Get connection to local MySQL replication database.
        
        DEPRECATED: Use get_mysql_replication_connection() instead.
        This method is maintained for backward compatibility with legacy code in:
        - etl_job/monitoring_utils.py
        - etl_job/mysql_postgre_incremental.py
        - etl_job/mysql_postgre_increm_ETL.py
        - airflow/dags/nightly_incremental_DAG.py
        
        The entire /etl_job directory is considered legacy and should be migrated to /etl_pipeline.
        """
        import warnings
        warnings.warn(
            "get_replication_connection() is deprecated. Use get_mysql_replication_connection() instead. "
            "This method is maintained for backward compatibility with legacy code.",
            DeprecationWarning,
            stacklevel=2
        )
        return cls.get_mysql_replication_connection()
    
    @classmethod
    def get_analytics_connection(cls) -> Engine:
        """
        Get connection to PostgreSQL analytics database.
        
        DEPRECATED: Use get_postgres_analytics_connection() instead.
        This method is maintained for backward compatibility with legacy code in:
        - etl_job/monitoring_utils.py
        - etl_job/mysql_postgre_incremental.py
        - etl_job/mysql_postgre_increm_ETL.py
        - airflow/dags/nightly_incremental_DAG.py
        
        The entire /etl_job directory is considered legacy and should be migrated to /etl_pipeline.
        """
        import warnings
        warnings.warn(
            "get_analytics_connection() is deprecated. Use get_postgres_analytics_connection() instead. "
            "This method is maintained for backward compatibility with legacy code.",
            DeprecationWarning,
            stacklevel=2
        )
        return cls.get_postgres_analytics_connection()
    
    @classmethod
    def test_connections(cls) -> dict:
        """
        Test all database connections using improved naming methods.
        
        Returns:
            Dictionary with connection test results
        """
        from sqlalchemy import text
        
        results = {
            'opendental_source': False,
            'mysql_replication': False,
            'postgres_analytics': False,
            # Legacy keys for backward compatibility
            'source': False,
            'replication': False,
            'analytics': False
        }
        
        try:
            # Test source connection
            source_engine = cls.get_opendental_source_connection()
            with source_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            results['opendental_source'] = True
            results['source'] = True  # Legacy compatibility
            source_engine.dispose()
        except Exception as e:
            logger.error(f"OpenDental source connection test failed: {str(e)}")
        
        try:
            # Test replication connection
            replication_engine = cls.get_mysql_replication_connection()
            with replication_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            results['mysql_replication'] = True
            results['replication'] = True  # Legacy compatibility
            replication_engine.dispose()
        except Exception as e:
            logger.error(f"MySQL replication connection test failed: {str(e)}")
        
        try:
            # Test analytics connection
            analytics_engine = cls.get_postgres_analytics_connection()
            with analytics_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            results['postgres_analytics'] = True
            results['analytics'] = True  # Legacy compatibility
            analytics_engine.dispose()
        except Exception as e:
            logger.error(f"PostgreSQL analytics connection test failed: {str(e)}")
        
        return results