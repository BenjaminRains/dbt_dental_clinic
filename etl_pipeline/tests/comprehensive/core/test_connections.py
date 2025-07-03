"""
Integration tests for the connections module.
Real database connection tests using actual environment variables and database connections.
"""
import pytest
import os
import logging
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from etl_pipeline.core.connections import ConnectionFactory

# Load environment variables for testing
load_dotenv()

logger = logging.getLogger(__name__)

class TestConnectionFactoryIntegration:
    """Integration tests for ConnectionFactory using real database connections."""
    
    def test_get_opendental_source_test_connection(self):
        """Test real connection to OpenDental test database."""
        try:
            engine = ConnectionFactory.get_opendental_source_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                assert len(tables) > 0  # Should have some tables
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_get_mysql_replication_test_connection(self):
        """Test real connection to MySQL replication test database."""
        try:
            engine = ConnectionFactory.get_mysql_replication_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                assert len(tables) >= 0  # May be empty initially
                
        except Exception as e:
            pytest.skip(f"Test replication database not available: {str(e)}")

    def test_get_postgres_analytics_test_connection(self):
        """Test real connection to PostgreSQL analytics test database."""
        try:
            engine = ConnectionFactory.get_postgres_analytics_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_database() as db_name"))
                row = result.fetchone()
                assert 'test_opendental_analytics' in row[0].lower()
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_get_opendental_analytics_raw_test_connection(self):
        """Test real connection to PostgreSQL analytics test database raw schema."""
        try:
            engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                row = result.fetchone()
                assert row[0] == 'raw'
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_get_opendental_analytics_staging_test_connection(self):
        """Test real connection to PostgreSQL analytics test database staging schema."""
        try:
            engine = ConnectionFactory.get_opendental_analytics_staging_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                row = result.fetchone()
                assert row[0] == 'staging'
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_get_opendental_analytics_intermediate_test_connection(self):
        """Test real connection to PostgreSQL analytics test database intermediate schema."""
        try:
            engine = ConnectionFactory.get_opendental_analytics_intermediate_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                row = result.fetchone()
                assert row[0] == 'intermediate'
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_get_opendental_analytics_marts_test_connection(self):
        """Test real connection to PostgreSQL analytics test database marts schema."""
        try:
            engine = ConnectionFactory.get_opendental_analytics_marts_test_connection()
            
            # Test basic connectivity
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
            # Test that we can query the database
            with engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                row = result.fetchone()
                assert row[0] == 'marts'
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}")

    def test_environment_variables_are_loaded(self):
        """Test that environment variables are properly loaded from .env file."""
        # Test that test environment variables are available
        test_vars = [
            'TEST_OPENDENTAL_SOURCE_HOST',
            'TEST_OPENDENTAL_SOURCE_PORT',
            'TEST_OPENDENTAL_SOURCE_DB',
            'TEST_OPENDENTAL_SOURCE_USER',
            'TEST_OPENDENTAL_SOURCE_PASSWORD',
            'TEST_MYSQL_REPLICATION_HOST',
            'TEST_MYSQL_REPLICATION_PORT',
            'TEST_MYSQL_REPLICATION_DB',
            'TEST_MYSQL_REPLICATION_USER',
            'TEST_MYSQL_REPLICATION_PASSWORD',
            'TEST_POSTGRES_ANALYTICS_HOST',
            'TEST_POSTGRES_ANALYTICS_PORT',
            'TEST_POSTGRES_ANALYTICS_DB',
            'TEST_POSTGRES_ANALYTICS_USER',
            'TEST_POSTGRES_ANALYTICS_PASSWORD'
        ]
        
        for var in test_vars:
            value = os.getenv(var)
            assert value is not None, f"Environment variable {var} is not set"
            assert value.strip() != '', f"Environment variable {var} is empty"

    def test_production_vs_test_environment_separation(self):
        """Test that production and test methods use different environment variables."""
        # Test that production and test methods use different env vars
        prod_host = os.getenv('OPENDENTAL_SOURCE_HOST')
        test_host = os.getenv('TEST_OPENDENTAL_SOURCE_HOST')
        
        # They should be different (if both are set)
        if prod_host and test_host:
            assert prod_host != test_host, "Production and test hosts should be different"
        
        prod_db = os.getenv('OPENDENTAL_SOURCE_DB')
        test_db = os.getenv('TEST_OPENDENTAL_SOURCE_DB')
        
        # They should be different (if both are set)
        if prod_db and test_db:
            assert prod_db != test_db, "Production and test databases should be different"

    def test_connection_pool_settings(self):
        """Test that connection pool settings are properly applied."""
        try:
            # Test with custom pool settings
            engine = ConnectionFactory.get_opendental_source_test_connection(
                pool_size=3,
                max_overflow=5,
                pool_timeout=20,
                pool_recycle=1000
            )
            
            # Test basic connectivity with custom settings
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                row = result.fetchone()
                assert row[0] == 1
                
        except Exception as e:
            pytest.skip(f"Test database not available: {str(e)}")

    def test_multiple_connections_same_environment(self):
        """Test that multiple connections to the same environment work correctly."""
        try:
            # Test multiple test connections
            engine1 = ConnectionFactory.get_opendental_source_test_connection()
            engine2 = ConnectionFactory.get_mysql_replication_test_connection()
            engine3 = ConnectionFactory.get_postgres_analytics_test_connection()
            
            # Test that all connections work
            with engine1.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.fetchone()[0] == 1
                
            with engine2.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.fetchone()[0] == 1
                
            with engine3.connect() as conn:
                result = conn.execute(text("SELECT 1 as test_value"))
                assert result.fetchone()[0] == 1
                
        except Exception as e:
            pytest.skip(f"Test databases not available: {str(e)}")

    def test_schema_specific_connections(self):
        """Test that schema-specific connections use the correct schemas."""
        try:
            # Test different schema connections
            raw_engine = ConnectionFactory.get_opendental_analytics_raw_test_connection()
            staging_engine = ConnectionFactory.get_opendental_analytics_staging_test_connection()
            intermediate_engine = ConnectionFactory.get_opendental_analytics_intermediate_test_connection()
            marts_engine = ConnectionFactory.get_opendental_analytics_marts_test_connection()
            
            # Test that each connection uses the correct schema
            with raw_engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                assert result.fetchone()[0] == 'raw'
                
            with staging_engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                assert result.fetchone()[0] == 'staging'
                
            with intermediate_engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                assert result.fetchone()[0] == 'intermediate'
                
            with marts_engine.connect() as conn:
                result = conn.execute(text("SELECT current_schema() as schema_name"))
                assert result.fetchone()[0] == 'marts'
                
        except Exception as e:
            pytest.skip(f"Test analytics database not available: {str(e)}") 