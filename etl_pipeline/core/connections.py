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
    def get_source_connection(cls) -> Engine:
        """Get connection to source OpenDental MySQL database."""
        return cls.create_mysql_connection(
            host=os.getenv('SOURCE_MYSQL_HOST'),
            port=os.getenv('SOURCE_MYSQL_PORT'),
            database=os.getenv('SOURCE_MYSQL_DB'),
            user=os.getenv('SOURCE_MYSQL_USER'),
            password=os.getenv('SOURCE_MYSQL_PASSWORD'),
            readonly=True
        )
    
    @classmethod
    def get_replication_connection(cls) -> Engine:
        """Get connection to local MySQL replication database."""
        return cls.create_mysql_connection(
            host=os.getenv('REPLICATION_MYSQL_HOST'),
            port=os.getenv('REPLICATION_MYSQL_PORT'),
            database=os.getenv('REPLICATION_MYSQL_DB'),
            user=os.getenv('REPLICATION_MYSQL_USER'),
            password=os.getenv('REPLICATION_MYSQL_PASSWORD')
        )
    
    @classmethod
    def get_analytics_connection(cls) -> Engine:
        """Get connection to PostgreSQL analytics database."""
        return cls.create_postgres_connection(
            host=os.getenv('ANALYTICS_POSTGRES_HOST'),
            port=os.getenv('ANALYTICS_POSTGRES_PORT'),
            database=os.getenv('ANALYTICS_POSTGRES_DB'),
            schema=os.getenv('ANALYTICS_POSTGRES_SCHEMA'),
            user=os.getenv('ANALYTICS_POSTGRES_USER'),
            password=os.getenv('ANALYTICS_POSTGRES_PASSWORD')
        )
    
    @classmethod
    def test_connections(cls) -> dict:
        """
        Test all database connections.
        
        Returns:
            Dictionary with connection test results
        """
        results = {
            'source': False,
            'replication': False,
            'analytics': False
        }
        
        try:
            # Test source connection
            source_engine = cls.get_source_connection()
            with source_engine.connect() as conn:
                conn.execute("SELECT 1")
            results['source'] = True
            source_engine.dispose()
        except Exception as e:
            logger.error(f"Source connection test failed: {str(e)}")
        
        try:
            # Test replication connection
            replication_engine = cls.get_replication_connection()
            with replication_engine.connect() as conn:
                conn.execute("SELECT 1")
            results['replication'] = True
            replication_engine.dispose()
        except Exception as e:
            logger.error(f"Replication connection test failed: {str(e)}")
        
        try:
            # Test analytics connection
            analytics_engine = cls.get_analytics_connection()
            with analytics_engine.connect() as conn:
                conn.execute("SELECT 1")
            results['analytics'] = True
            analytics_engine.dispose()
        except Exception as e:
            logger.error(f"Analytics connection test failed: {str(e)}")
        
        return results